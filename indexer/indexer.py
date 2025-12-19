import os
import asyncio
import asyncpg
from meilisearch_python_sdk import AsyncClient
# [FIX] Import model settings 
from meilisearch_python_sdk.models.settings import MeilisearchSettings 
from dotenv import load_dotenv
import logging
import signal
from minio import Minio
from bs4 import BeautifulSoup
import io
import time
import threading
from prometheus_client import start_http_server, Gauge, Counter

# --- 1. Logging configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 2. Load environment variables ---
load_dotenv()

# --- METRICS ---
INDEXING_QUEUE_DEPTH = Gauge('indexing_queue_depth', 'Pages waiting to be indexed')
INDEXED_PAGES = Counter('indexed_pages_total', 'Total pages indexed')

# --- 3. Config ---
DATABASE_URL = os.getenv("DATABASE_URL")
MEILI_HOST = os.getenv("MEILI_HOST")
MEILI_API_KEY = os.getenv("MEILI_API_KEY")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "crawler-data") # Default bucket if not set

INDEX_NAME = "pages"
BATCH_SIZE = 1000
SLEEP_INTERVAL = 30
CLEANUP_INTERVAL_SECONDS = 3600 * 6  # Run cleanup every 6 hours

# --- 4. Main logic ---
async def cleanup_deleted_pages(db_pool, meili_client):
    """
    Sync deletions: Remove documents from MeiliSearch that do not exist in PostgreSQL.
    Strategy: Iterate MeiliSearch documents in batches and verify existence in DB.
    This avoids loading all DB IDs into memory (OOM prevention).
    """
    logging.info("Starting cleanup phase...")
    try:
        # Iterate MeiliSearch documents
        limit = 2000
        offset = 0
        deleted_count = 0
        
        while True:
            # Fetch batch of docs from Meili
            docs = await meili_client.index(INDEX_NAME).get_documents({
                'limit': limit,
                'offset': offset,
                'fields': ['id', 'url'] 
            })
            
            if not docs.results:
                break
            
            # Extract IDs to check
            meili_ids = []
            doc_map = {} # Map ID -> URL (PK)
            
            for doc in docs.results:
                doc_id = doc.get('id')
                doc_url = doc.get('url')
                if doc_id is not None and doc_url:
                    try:
                        doc_id_int = int(doc_id)
                        meili_ids.append(doc_id_int)
                        doc_map[doc_id_int] = doc_url
                    except ValueError:
                        pass
            
            if not meili_ids:
                offset += limit
                continue

            # Check existence in DB
            ids_to_delete = []
            async with db_pool.acquire() as conn:
                # We want to find which of meili_ids are NOT in the DB.
                # Query: SELECT id FROM crawled_pages WHERE id = ANY($1)
                found_rows = await conn.fetch(
                    "SELECT id FROM crawled_pages WHERE id = ANY($1::bigint[])", 
                    meili_ids
                )
                found_ids = {r['id'] for r in found_rows}
                
                for m_id in meili_ids:
                    if m_id not in found_ids:
                        ids_to_delete.append(doc_map[m_id])

            if ids_to_delete:
                await meili_client.index(INDEX_NAME).delete_documents(ids_to_delete)
                deleted_count += len(ids_to_delete)
                logging.info(f"Deleted batch of {len(ids_to_delete)} stale documents.")
            
            offset += limit
            # Simple yield to event loop
            await asyncio.sleep(0.01)

        logging.info(f"Cleanup completed. Deleted {deleted_count} stale documents from MeiliSearch.")

    except Exception as e:
        logging.error(f"Error during cleanup: {e}")

async def main():
    # Start Prometheus Metrics Server
    try:
        threading.Thread(target=start_http_server, args=(8001,), daemon=True).start()
        logging.info("Prometheus metrics server started on port 8001")
    except Exception as e:
        logging.error(f"Failed to start metrics server: {e}")

    db_pool = None
    meili_client = None
    minio_client = None

    try:
        logging.info("Connecting to PostgreSQL...")
        db_pool = await asyncpg.create_pool(DATABASE_URL)
        
        logging.info(f"Connecting to Meilisearch at {MEILI_HOST}...")
        meili_client = AsyncClient(url=MEILI_HOST, api_key=MEILI_API_KEY)

        # --- MinIO Connection ---
        if MINIO_ACCESS_KEY and MINIO_SECRET_KEY:
             try:
                 logging.info(f"Connecting to MinIO at {MINIO_ENDPOINT}...")
                 # Minio client is synchronous
                 minio_client = Minio(
                     MINIO_ENDPOINT,
                     access_key=MINIO_ACCESS_KEY,
                     secret_key=MINIO_SECRET_KEY,
                     secure=False 
                 )
             except Exception as e:
                 logging.error(f"MinIO connection error: {e}")
        else:
             logging.warning("MinIO config missing. Feature to fetch body_text will be disabled.")

        # --- Initialize Index ---
        logging.info(f"Initializing index '{INDEX_NAME}'...")
        await meili_client.create_index(INDEX_NAME, primary_key='url')

        logging.info("Updating Lith Rank settings for index...")
        
        settings_dict = {
            'searchableAttributes': ['title', 'body_text', 'meta_description'],
            'filterableAttributes': ['domain', 'language'],
            'sortableAttributes': ['crawled_at', 'lith_score'],
            'rankingRules': [
                'words',
                'typo',
                'proximity',
                'attribute',
                'sort',
                'exactness',
                'lith_score:desc' 
            ]
        }
        
        # [FIX] Wrap settings dict in MeilisearchSettings model
        await meili_client.index(INDEX_NAME).update_settings(MeilisearchSettings(**settings_dict))
        
        logging.info("Index settings updated.")

    except Exception as e:
        logging.critical(f"Initialization error: {e}")
        if db_pool: await db_pool.close()
        return

    logging.info("Starting synchronization loop...")
    loop = asyncio.get_running_loop()

    # Graceful Shutdown Setup
    stop_event = asyncio.Event()

    def signal_handler():
        logging.info("Received stop signal. Shutting down...")
        stop_event.set()

    loop.add_signal_handler(signal.SIGTERM, signal_handler)
    loop.add_signal_handler(signal.SIGINT, signal_handler)

    last_cleanup_time = 0

    while not stop_event.is_set():
        try:
            # 1. Indexing Task
            async with db_pool.acquire() as connection:
                # Update Queue Depth Metric
                try:
                    queue_count = await connection.fetchval("SELECT COUNT(*) FROM crawled_pages WHERE indexed_at IS NULL OR crawled_at > indexed_at")
                    INDEXING_QUEUE_DEPTH.set(queue_count)
                except Exception as e:
                    logging.warning(f"Failed to update queue depth metric: {e}")

                query = f"""
                    SELECT id, url, title, meta_description, raw_html_path, domain, language, crawled_at, lith_score
                    FROM crawled_pages
                    WHERE indexed_at IS NULL OR crawled_at > indexed_at
                    LIMIT {BATCH_SIZE};
                """
                records = await connection.fetch(query)

                if records:
                    logging.info(f"Found {len(records)} pages to index. Queue depth: {queue_count}")
                    documents_batch = []
                    
                    for record in records:
                        doc = dict(record)
                        
                        # Fetch Body Text
                        body_text = ""
                        raw_path = doc.get('raw_html_path')
                        
                        if raw_path and minio_client:
                            try:
                                response = await loop.run_in_executor(
                                    None, 
                                    lambda: minio_client.get_object(MINIO_BUCKET, raw_path)
                                )
                                try:
                                    content = response.read()
                                    soup = BeautifulSoup(content, 'lxml')
                                    for script in soup(["script", "style", "nav", "footer", "header"]):
                                        script.decompose()
                                    body_text = soup.get_text(separator=' ', strip=True)[:100000]
                                finally:
                                    response.close()
                                    response.release_conn()
                            except Exception as e:
                                logging.warning(f"Error fetching MinIO for {doc['url']}: {e}")
                        
                        doc['body_text'] = body_text
                        if 'raw_html_path' in doc:
                             del doc['raw_html_path']

                        if doc.get('lith_score') is None:
                            doc['lith_score'] = 1.0
                        if doc.get('crawled_at'):
                            doc['crawled_at'] = doc['crawled_at'].timestamp()

                        documents_batch.append(doc)

                    await meili_client.index(INDEX_NAME).add_documents(documents_batch)
                    
                    indexed_ids = [record['id'] for record in records]
                    await connection.execute("UPDATE crawled_pages SET indexed_at = NOW() WHERE id = ANY($1::bigint[])", indexed_ids)
                    logging.info(f"Indexed {len(indexed_ids)} pages.")
                    INDEXED_PAGES.inc(len(indexed_ids))
                else:
                    # No new pages, maybe good time to cleanup or sleep
                    pass

            # 2. Cleanup Task (Periodic)
            now = time.time()
            if now - last_cleanup_time > CLEANUP_INTERVAL_SECONDS:
                await cleanup_deleted_pages(db_pool, meili_client)
                last_cleanup_time = now
            
            if not records:
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=SLEEP_INTERVAL)
                except asyncio.TimeoutError:
                    pass

        except Exception as e:
            if stop_event.is_set():
                break
            logging.error(f"Loop error: {e}")
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=SLEEP_INTERVAL)
            except asyncio.TimeoutError:
                pass

    logging.info("Indexer stopped safely.")
    if db_pool:
        await db_pool.close()
    if meili_client:
        await meili_client.close()

if __name__ == "__main__":
    asyncio.run(main())