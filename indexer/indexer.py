import os
import asyncio
import asyncpg
import meilisearch.aio
from dotenv import load_dotenv
import logging
from minio import Minio
from bs4 import BeautifulSoup
import io

# --- 1. Cấu hình logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 2. Tải biến môi trường ---
load_dotenv()

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

# --- 4. Logic chính ---
async def main():
    db_pool = None
    meili_client = None
    minio_client = None

    try:
        logging.info("Đang kết nối tới PostgreSQL...")
        db_pool = await asyncpg.create_pool(DATABASE_URL)
        
        logging.info(f"Đang kết nối tới Meilisearch tại {MEILI_HOST}...")
        meili_client = meilisearch.aio.Client(url=MEILI_HOST, api_key=MEILI_API_KEY)

        # --- MinIO Connection ---
        if MINIO_ACCESS_KEY and MINIO_SECRET_KEY:
             try:
                 logging.info(f"Đang kết nối tới MinIO tại {MINIO_ENDPOINT}...")
                 # Minio client is synchronous, which is a bottleneck in async loop if not careful.
                 # However, for batch processing, we can offload to executor or accept the hit for now.
                 # Given this is a background indexer, synchronous MinIO fetch per batch is acceptable 
                 # or we can wrap it.
                 minio_client = Minio(
                     MINIO_ENDPOINT,
                     access_key=MINIO_ACCESS_KEY,
                     secret_key=MINIO_SECRET_KEY,
                     secure=False # Internal usually http
                 )
             except Exception as e:
                 logging.error(f"Lỗi kết nối MinIO: {e}")
        else:
             logging.warning("Thiếu cấu hình MinIO (ACCESS_KEY/SECRET_KEY). Tính năng fetch body_text sẽ bị vô hiệu hóa.")

        # --- Khởi tạo Index & Cấu hình Ranking ---
        logging.info(f"Đang khởi tạo index '{INDEX_NAME}'...")
        await meili_client.create_index(INDEX_NAME, {'primaryKey': 'url'})

        logging.info("Đang cập nhật cài đặt Lith Rank cho index...")
        settings = {
            'searchableAttributes': ['title', 'body_text', 'meta_description'],
            'filterableAttributes': ['domain', 'language'],
            # QUAN TRỌNG: Thêm lith_score vào sortableAttributes
            'sortableAttributes': ['crawled_at', 'lith_score'],
            # Cấu hình Ranking Rules mặc định (Lith score cao -> lên top)
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
        await meili_client.index(INDEX_NAME).update_settings(settings)
        logging.info("Cài đặt index đã được cập nhật với Lith Rank.")

    except Exception as e:
        logging.critical(f"Lỗi khởi tạo: {e}")
        if db_pool: await db_pool.close()
        return

    logging.info("Bắt đầu vòng lặp đồng bộ...")
    loop = asyncio.get_running_loop()

    while True:
        try:
            async with db_pool.acquire() as connection:
                # Fetch raw_html_path to retrieve content from MinIO
                query = f"""
                    SELECT id, url, title, meta_description, raw_html_path, domain, language, crawled_at, lith_score
                    FROM crawled_pages
                    WHERE indexed_at IS NULL OR crawled_at > indexed_at
                    LIMIT {BATCH_SIZE};
                """
                records = await connection.fetch(query)

                if not records:
                    logging.info(f"Không có trang mới. Ngủ {SLEEP_INTERVAL}s...")
                    await asyncio.sleep(SLEEP_INTERVAL)
                    continue

                logging.info(f"Tìm thấy {len(records)} trang cần index.")
                documents_batch = []
                
                for record in records:
                    doc = dict(record)
                    
                    # 1. Fetch Body Text from MinIO if available
                    body_text = ""
                    raw_path = doc.get('raw_html_path')
                    
                    if raw_path and minio_client:
                        try:
                            # Run blocking MinIO call in thread pool
                            response = await loop.run_in_executor(
                                None, 
                                lambda: minio_client.get_object(MINIO_BUCKET, raw_path)
                            )
                            try:
                                content = response.read()
                                # Extract text using BeautifulSoup
                                # (CPU intensive, so also potential candidate for executor if heavily loaded, 
                                # but usually fast enough for simple pages)
                                soup = BeautifulSoup(content, 'lxml')
                                # Remove scripts/styles
                                for script in soup(["script", "style", "nav", "footer", "header"]):
                                    script.decompose()
                                body_text = soup.get_text(separator=' ', strip=True)[:100000] # Limit size
                            finally:
                                response.close()
                                response.release_conn()
                        except Exception as e:
                            logging.warning(f"Lỗi fetch MinIO cho {doc['url']}: {e}")
                    
                    doc['body_text'] = body_text
                    # Clean up
                    if 'raw_html_path' in doc:
                         del doc['raw_html_path'] # Don't index the path

                    # 2. Defaults
                    if doc.get('lith_score') is None:
                        doc['lith_score'] = 1.0
                    if doc.get('crawled_at'):
                        doc['crawled_at'] = doc['crawled_at'].timestamp()

                    documents_batch.append(doc)

                await meili_client.index(INDEX_NAME).add_documents(documents_batch)
                
                indexed_ids = [record['id'] for record in records]
                await connection.execute("UPDATE crawled_pages SET indexed_at = NOW() WHERE id = ANY($1::bigint[])", indexed_ids)
                logging.info(f"Đã index {len(indexed_ids)} trang (kèm full-text từ MinIO).")

        except Exception as e:
            logging.error(f"Lỗi vòng lặp: {e}")
            await asyncio.sleep(SLEEP_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())
