import asyncio
import aiohttp
from urllib.parse import urljoin, urlparse, urldefrag
from urllib.robotparser import RobotFileParser
from bs4 import BeautifulSoup
from collections import defaultdict, deque
from datetime import datetime
import json
import time
import os
import signal
import ssl
import hashlib
import io
from typing import Set, Dict, List, Optional, Tuple

# --- NÂNG CẤP THƯ VIỆN ---
import fasttext 
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.table import Table
from rich.panel import Panel
import asyncpg
from aiolimiter import AsyncLimiter
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv

console = Console()


class RobotsCache:
    """Quản lý việc cache file robots.txt."""
    def __init__(self):
        self.cache: Dict[str, RobotFileParser] = {}
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def can_fetch(self, url: str, user_agent: str = "*") -> bool:
        parsed = urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        
        if base_url not in self.cache:
            await self._load_robots(base_url, user_agent)
        
        return self.cache[base_url].can_fetch(user_agent, url)
    
    async def _load_robots(self, base_url: str, user_agent: str):
        robots_url = urljoin(base_url, "/robots.txt")
        rp = RobotFileParser()
        rp.set_url(robots_url)
        
        try:
            if not self.session:
                self.session = aiohttp.ClientSession()
            
            async with self.session.get(robots_url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    content = await response.text()
                    rp.parse(content.splitlines())
                else:
                    rp.parse([])
        except Exception as e:
            console.print(f"[yellow]Warning: Could not fetch robots.txt for {base_url}: {e}[/yellow]")
            rp.parse([])
        
        self.cache[base_url] = rp
    
    async def close(self):
        if self.session:
            await self.session.close()


class WebCrawler:
    def __init__(
        self,
        start_urls: List[str],
        config: Dict
    ):
        self.start_urls = start_urls
        self.config = config
        
        # Crawler settings
        self.max_depth = config.get('max_depth', 3)
        self.max_concurrent_requests = config.get('max_concurrent_requests', 10)
        self.delay_per_domain = config.get('delay_per_domain', 1.0)
        self.user_agent = config.get('user_agent', 'TeaserBot/1.0')
        self.max_pages = config.get('max_pages', None)
        self.max_retries = config.get('max_retries', 3)
        self.save_to_db = config.get('save_to_db', True)
        self.save_to_json = config.get('save_to_json', True)
        self.ssl_verify = config.get('ssl_verify', True)
        self.db_ssl_ca_cert_path = config.get('db_ssl_ca_cert_path', None)
        self.db_batch: List[Dict] = []
        self.db_batch_size = config.get('db_batch_size', 1000) 
        
        self.visited_urls: Set[str] = set()
        self.url_queue: deque = deque()
        self.results: List[Dict] = []
        
        self.robots_cache = RobotsCache()
        self.domain_last_request: Dict[str, float] = defaultdict(float)
        self.domain_locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self.session: Optional[aiohttp.ClientSession] = None
        self.db_pool: Optional[asyncpg.Pool] = None
        self.minio_client: Optional[Minio] = None
        
        # --- NÂNG CẤP: Tải model FastText một lần duy nhất ---
        self.lang_detector = self._init_language_detector()
        
        self.rate_limiter = AsyncLimiter(config.get('max_requests_per_minute', 60), 60)
        self.shutdown_event = asyncio.Event()
        
        self.stats = {
            'total_crawled': 0,
            'total_skipped': 0,
            'total_errors': 0,
            'cdn_uploads': 0,
            'cdn_errors': 0,
            'by_domain': defaultdict(lambda: {'crawled': 0, 'skipped': 0, 'errors': 0})
        }
    
    def _init_language_detector(self):
        """Tải model FastText khi crawler khởi động."""
        model_path = "/app/lid.176.bin" # Sử dụng đường dẫn tuyệt đối trong container
        if not os.path.exists(model_path):
            console.print(f"[bold red]Lỗi: Model nhận diện ngôn ngữ '{model_path}' không tồn tại.[/bold red]")
            console.print("Hãy đảm bảo file [cyan]lid.176.bin[/cyan] được đặt ở thư mục gốc của dự án và được mount.")
            return None
        
        try:
            detector = fasttext.load_model(model_path)
            console.print("[green]✓ Model FastText cho Crawler đã được tải.[/green]")
            return detector
        except Exception as e:
            console.print(f"[red]Không thể tải model FastText: {e}[/red]")
            return None

    async def _init_database(self):
        """Khởi tạo kết nối DB Pool và đảm bảo schema được áp dụng."""
        if not self.save_to_db:
            return
        
        # Dùng DATABASE_URL từ biến môi trường
        database_url = os.environ.get('DATABASE_URL')
        if not database_url:
            console.print("[yellow]Warning: DATABASE_URL not set. Skipping database storage.[/yellow]")
            self.save_to_db = False
            return
        
        ssl_context = None
        # Logic SSL được giữ nguyên
        if self.db_ssl_ca_cert_path and os.path.exists(self.db_ssl_ca_cert_path):
            console.print(f"[dim]Loading database CA certificate from: {self.db_ssl_ca_cert_path}[/dim]")
            ssl_context = ssl.create_default_context(cafile=self.db_ssl_ca_cert_path)
        elif not self.ssl_verify:
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            console.print("[yellow]Warning: Database SSL certificate verification is disabled.[/yellow]")
        else:
            ssl_context = ssl.create_default_context()

        try:
            self.db_pool = await asyncpg.create_pool(database_url, ssl=ssl_context, min_size=1, max_size=10)
            
            # Đảm bảo schema.sql được áp dụng
            # File schema.sql nằm trong thư mục crawler/, nên đường dẫn trong container là /app/crawler/schema.sql
            schema_path = '/app/crawler/schema.sql'
            if os.path.exists(schema_path):
                 with open(schema_path, 'r') as f:
                    schema_sql = f.read()
                 async with self.db_pool.acquire() as conn:
                    await conn.execute(schema_sql)
                 console.print("[green]✓ Database initialized successfully[/green]")
            else:
                 console.print(f"[bold red]Lỗi: Không tìm thấy schema.sql tại {schema_path}.[/bold red]")
                 self.save_to_db = False


        except Exception as e:
            console.print(f"[red]Error initializing database: {e}[/red]")
            self.save_to_db = False

    def _init_minio_client(self):
        """Khởi tạo MinIO client để lưu trữ raw HTML."""
        minio_config = self.config.get('minio_storage', {})
        if not minio_config.get('enabled', False):
            console.print("[yellow]MinIO storage is disabled in config. Skipping raw HTML storage.[/yellow]")
            return

        endpoint = os.environ.get('MINIO_ENDPOINT', minio_config.get('endpoint'))
        access_key = os.environ.get('MINIO_ACCESS_KEY', minio_config.get('access_key'))
        secret_key = os.environ.get('MINIO_SECRET_KEY', minio_config.get('secret_key'))
        
        if not all([endpoint, access_key, secret_key]):
            console.print("[red]Error: MinIO config (endpoint, access_key, secret_key) not found in config or environment variables.[/red]")
            return
        
        try:
            # MinIO endpoint trong docker-compose là minio:9000
            self.minio_client = Minio(
                endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=minio_config.get('secure', True) # Secure = false nếu dùng http, true nếu dùng https
            )
            bucket_name = minio_config.get('bucket_name')
            if bucket_name:
                found = self.minio_client.bucket_exists(bucket_name)
                if not found:
                    self.minio_client.make_bucket(bucket_name)
                    console.print(f"[green]✓ MinIO bucket '{bucket_name}' created.[/green]")
                else:
                    console.print(f"[green]✓ MinIO bucket '{bucket_name}' already exists.[/green]")
            console.print("[green]✓ MinIO client initialized successfully[/green]")
        except (S3Error, Exception) as e:
            console.print(f"[red]Error initializing MinIO client: {e}[/red]")
            self.minio_client = None

    async def _close_database(self):
        """Đóng DB pool."""
        if self.db_pool:
            await self.db_pool.close()

    def _extract_body_text(self, soup: BeautifulSoup) -> str:
        """Trích xuất và làm sạch body text."""
        for script in soup(["script", "style", "nav", "header", "footer", "aside"]):
            script.decompose()
        
        text = soup.get_text(separator=' ', strip=True)
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = ' '.join(chunk for chunk in chunks if chunk)
        
        return text[:5000]

    async def _store_raw_html_in_cdn(self, url: str, html_content: str) -> Optional[str]:
        """Lưu trữ raw HTML vào S3/MinIO và trả về đường dẫn."""
        if not self.minio_client:
            return None
        
        minio_config = self.config.get('minio_storage', {})
        bucket_name = minio_config.get('bucket_name')
        if not bucket_name:
            console.print("[red]Error: MinIO bucket name not configured.[/red]")
            return None

        try:
            url_hash = hashlib.sha256(url.encode('utf-8')).hexdigest()
            # Raw HTML được lưu dưới dạng raw_html/<hash>.html
            object_name = f"raw_html/{url_hash}.html"
            
            html_bytes = html_content.encode('utf-8')
            html_stream = io.BytesIO(html_bytes)
            
            self.minio_client.put_object(
                bucket_name,
                object_name,
                html_stream,
                len(html_bytes),
                content_type='text/html'
            )
            self.stats['cdn_uploads'] += 1
            return object_name
        except (S3Error, Exception) as e:
            console.print(f"[red]Error uploading to MinIO for {url}: {e}[/red]")
            self.stats['cdn_errors'] += 1
            return None
            
    async def _flush_db_batch(self):
        """Ghi lô dữ liệu hiện tại vào database và xóa lô."""
        if not self.save_to_db or not self.db_pool or not self.db_batch:
            return

        try:
            async with self.db_pool.acquire() as conn:
                data_to_insert = [
                    (
                        p['url'],
                        p['title'][:512] if p['title'] else '',
                        p.get('meta_description', ''),
                        p['domain'],
                        p['depth'],
                        p.get('body_text', ''),
                        p.get('raw_html_path'),
                        p.get('language', 'unknown')
                    ) for p in self.db_batch
                ]

                # Sử dụng executemany để chèn nhiều dòng cùng lúc
                await conn.executemany('''
                    INSERT INTO crawled_pages (url, title, meta_description, domain, depth, body_text, raw_html_path, language, tsv_document)
                    VALUES ($1, $2::varchar(512), $3, $4, $5, $6, $7, $8, to_tsvector('english', COALESCE($2, '') || ' ' || COALESCE($6, '')))
                    ON CONFLICT (url) DO UPDATE SET
                        title = EXCLUDED.title,
                        meta_description = EXCLUDED.meta_description,
                        domain = EXCLUDED.domain,
                        depth = EXCLUDED.depth,
                        body_text = EXCLUDED.body_text,
                        raw_html_path = EXCLUDED.raw_html_path,
                        language = EXCLUDED.language,
                        tsv_document = EXCLUDED.tsv_document,
                        crawled_at = CURRENT_TIMESTAMP
                ''', data_to_insert)

            # Xóa batch sau khi ghi thành công
            self.db_batch.clear()

        except Exception as e:
            console.print(f"[red]Error during batch database insert: {e}[/red]")
            # Có thể thêm logic để thử lại hoặc ghi log lỗi chi tiết hơn ở đây            
        
    async def _add_to_db_batch(self, page_data: Dict):
        """Thêm dữ liệu trang vào lô và ghi vào DB nếu lô đầy."""
        if not self.save_to_db:
            return

        self.db_batch.append(page_data)
        if len(self.db_batch) >= self.db_batch_size:
            await self._flush_db_batch()
        
    def _normalize_url(self, url: str) -> str:
        """Chuẩn hóa URL."""
        url, _ = urldefrag(url)
        return url.rstrip('/')
    
    def _is_valid_url(self, url: str) -> bool:
        """Kiểm tra xem URL có hợp lệ và không phải là file tĩnh không."""
        parsed = urlparse(url)
        if not parsed.scheme in ['http', 'https']: return False
        if not parsed.netloc: return False
        
        excluded_extensions = ['.pdf', '.jpg', '.jpeg', '.png', '.gif', '.svg', '.ico', '.css', '.js', '.zip', '.tar', '.gz', '.exe', '.dmg', '.mp4', '.mp3', '.avi', '.mov', '.wmv', '.flv', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx']
        
        if any(url.lower().endswith(ext) for ext in excluded_extensions): return False
        
        return True
    
    def _get_domain(self, url: str) -> str:
        """Lấy tên miền từ URL."""
        return urlparse(url).netloc
    
    async def _wait_for_rate_limit(self, domain: str):
        """Áp dụng giới hạn tốc độ theo tên miền."""
        async with self.domain_locks[domain]:
            last_request = self.domain_last_request[domain]
            time_since_last = time.time() - last_request
            
            if time_since_last < self.delay_per_domain:
                await asyncio.sleep(self.delay_per_domain - time_since_last)
            
            self.domain_last_request[domain] = time.time()
    
    async def _fetch_page(self, url: str) -> Optional[Tuple[str, str]]:
        """Fetch nội dung trang web, bao gồm cả robots.txt check và rate limiting."""
        domain = self._get_domain(url)
        
        if not await self.robots_cache.can_fetch(url, self.user_agent):
            self.stats['total_skipped'] += 1
            self.stats['by_domain'][domain]['skipped'] += 1
            return None
        
        await self._wait_for_rate_limit(domain)
        
        async with self.rate_limiter:
            for attempt in range(self.max_retries):
                try:
                    headers = {'User-Agent': self.user_agent}
                    if not self.session: return None
                    async with self.session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as response:
                        if response.status == 200:
                            content_type = response.headers.get('Content-Type', '')
                            if 'text/html' in content_type:
                                html = await response.text()
                                return html, response.headers.get('Content-Type', '')
                            else:
                                self.stats['total_skipped'] += 1
                                self.stats['by_domain'][domain]['skipped'] += 1
                                return None
                        elif response.status == 503:
                            if attempt < self.max_retries - 1:
                                await asyncio.sleep(2 ** attempt)
                                continue
                            else:
                                self.stats['total_errors'] += 1
                                self.stats['by_domain'][domain]['errors'] += 1
                                return None
                        else:
                            self.stats['total_errors'] += 1
                            self.stats['by_domain'][domain]['errors'] += 1
                            return None
                except (asyncio.TimeoutError, aiohttp.ServerTimeoutError, aiohttp.ClientConnectorError) as e:
                    if attempt < self.max_retries - 1:
                        backoff_time = 2 ** attempt
                        console.print(f"[yellow]Retry {attempt + 1}/{self.max_retries} for {url} after {backoff_time}s (Error: {type(e).__name__})[/yellow]")
                        await asyncio.sleep(backoff_time)
                        continue
                    else:
                        self.stats['total_errors'] += 1
                        self.stats['by_domain'][domain]['errors'] += 1
                        console.print(f"[red]Error fetching {url} after {self.max_retries} attempts: {str(e)[:100]}[/red]")
                        return None
                except Exception as e:
                    self.stats['total_errors'] += 1
                    self.stats['by_domain'][domain]['errors'] += 1
                    console.print(f"[red]Error fetching {url}: {str(e)[:100]}[/red]")
                    return None
            return None
    
    async def _parse_page(self, url: str, html: str, depth: int, cdn_path: Optional[str]) -> Dict:
        """Parse HTML, trích xuất dữ liệu, nhận diện ngôn ngữ và tìm liên kết mới."""
        soup = BeautifulSoup(html, 'lxml')
        
        title = soup.title.string.strip() if soup.title and soup.title.string else ""
        
        meta_desc = ""
        meta_tag = soup.find('meta', attrs={'name': 'description'})
        if meta_tag and meta_tag.get('content'): meta_desc = meta_tag.get('content', '').strip()
        
        body_text = self._extract_body_text(soup)

        # Logic Nhận diện Ngôn ngữ bằng FastText
        language = 'unknown' 
        if self.lang_detector and body_text and len(body_text) > 50:
            try:
                cleaned_text = body_text.replace('\n', ' ').strip()
                if cleaned_text:
                    loop = asyncio.get_running_loop()
                    predictions = await loop.run_in_executor(
                        None,  # ThreadPoolExecutor
                        self.lang_detector.predict, 
                        cleaned_text, 
                        1
                    )
                    language = predictions[0][0].replace('__label__', '')
            except Exception as e:
                console.print(f"[yellow]Warning: Lỗi nhận diện ngôn ngữ cho {url}: {e}[/yellow]")

        links = []
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            absolute_url = urljoin(url, href)
            normalized_url = self._normalize_url(absolute_url)
            if self._is_valid_url(normalized_url):
                links.append(normalized_url)
                if depth < self.max_depth and normalized_url not in self.visited_urls:
                    self.url_queue.append((normalized_url, depth + 1))
        
        return {
            'url': url,
            'title': title,
            'meta_description': meta_desc,
            'body_text': body_text,
            'depth': depth,
            'links_found': len(links),
            'crawled_at': datetime.utcnow().isoformat(),
            'domain': self._get_domain(url),
            'raw_html_path': cdn_path,
            'language': language
        }
    
    async def _crawl_url(self, url: str, depth: int, semaphore: asyncio.Semaphore):
        """Xử lý việc crawl một URL đơn lẻ."""
        async with semaphore:
            normalized_url = self._normalize_url(url)
            if normalized_url in self.visited_urls: return
            self.visited_urls.add(normalized_url)
            
            domain = self._get_domain(normalized_url)
            
            result = await self._fetch_page(normalized_url)
            
            if result:
                html, content_type = result
                
                cdn_path = await self._store_raw_html_in_cdn(normalized_url, html)
                
                page_data = await self._parse_page(normalized_url, html, depth, cdn_path)
                
                await self._add_to_db_batch(page_data)
                
                self.results.append(page_data)
                self.stats['total_crawled'] += 1
                self.stats['by_domain'][domain]['crawled'] += 1

    async def _discover_urls_from_sitemap(self, domain: str) -> List[str]:
        """Sử dụng sitemap để khám phá thêm URL."""
        base_url = f"https://{domain}"
        robots_url = urljoin(base_url, "/robots.txt")
        initial_sitemap_urls = []
        try:
            if not self.session:
                self.session = aiohttp.ClientSession()

            async with self.session.get(robots_url, timeout=10) as response:
                if response.status == 200:
                    text = await response.text()
                    for line in text.splitlines():
                        if line.lower().startswith('sitemap:'):
                            initial_sitemap_urls.append(line.split(':', 1)[1].strip())
        except Exception as e:
            console.print(f"[yellow]Could not parse robots.txt at {robots_url}: {e}[/yellow]")

        if not initial_sitemap_urls:
            initial_sitemap_urls.append(urljoin(base_url, "/sitemap.xml"))

        discovered_urls = deque()
        processed_sitemaps = set()
        
        async def fetch_and_parse_sitemap(sitemap_url: str):
            if sitemap_url in processed_sitemaps:
                return
            processed_sitemaps.add(sitemap_url)

            try:
                console.print(f"[dim]Parsing sitemap: {sitemap_url}[/dim]")
                async with self.session.get(sitemap_url, timeout=20) as response:
                    if response.status == 200:
                        content = await response.text()
                        soup = BeautifulSoup(content, 'lxml-xml')
                        
                        # Xử lý các sitemap lồng nhau
                        nested_sitemaps = []
                        for sitemap in soup.find_all('sitemap'):
                            loc = sitemap.find('loc')
                            if loc and loc.text:
                                nested_sitemaps.append(loc.text.strip())
                        
                        if nested_sitemaps:
                            await asyncio.gather(*[fetch_and_parse_sitemap(nested_url) for nested_url in nested_sitemaps])

                        # Trích xuất URL từ sitemap hiện tại
                        for url in soup.find_all('url'):
                            loc = url.find('loc')
                            if loc and loc.text:
                                discovered_urls.append(loc.text.strip())
            except Exception as e:
                console.print(f"[yellow]Could not parse sitemap {sitemap_url}: {e}[/yellow]")

        await asyncio.gather(*[fetch_and_parse_sitemap(s_url) for s_url in initial_sitemap_urls])

        return list(discovered_urls)

    async def crawl(self):
        """Chức năng crawl chính."""
        for url in self.start_urls:
            self.url_queue.append((url, 0))
        
        await self._init_database()
        self._init_minio_client()
        
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=10)
        self.session = aiohttp.ClientSession(headers={'User-Agent': self.user_agent}, connector=connector)
        
        try:
            console.print("[cyan]Discovering URLs from sitemaps...[/cyan]")
            initial_domains = {self._get_domain(url) for url in self.start_urls}
            sitemap_tasks = [self._discover_urls_from_sitemap(domain) for domain in initial_domains]
            
            sitemap_results = await asyncio.gather(*sitemap_tasks, return_exceptions=True)
            sitemap_url_count = 0
            for result in sitemap_results:
                if isinstance(result, list):
                    for url in result:
                        normalized_url = self._normalize_url(url)
                        if normalized_url not in self.visited_urls:
                            self.url_queue.append((normalized_url, 0))
                            self.visited_urls.add(normalized_url)
                            sitemap_url_count += 1
            
            if sitemap_url_count > 0:
                console.print(f"[green]✓ Discovered and added {sitemap_url_count} new URLs from sitemaps.[/green]")

            semaphore = asyncio.Semaphore(self.max_concurrent_requests)
            
            with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), BarColumn(), TaskProgressColumn(), console=console) as progress:
                crawl_task = progress.add_task("[cyan]Crawling...", total=None)
                
                while self.url_queue and not self.shutdown_event.is_set():
                    if self.max_pages and self.stats['total_crawled'] >= self.max_pages:
                        console.print(f"\n[yellow]Reached max pages limit ({self.max_pages}). Stopping crawl.[/yellow]")
                        break
                    
                    # Lấy batch nhỏ hơn để xử lý
                    batch_size = min(len(self.url_queue), self.max_concurrent_requests * 2)
                    batch = [self.url_queue.popleft() for _ in range(batch_size)]
                    
                    tasks = [self._crawl_url(url, depth, semaphore) for url, depth in batch]
                    
                    if tasks:
                        await asyncio.gather(*tasks)
                        progress.update(crawl_task, description=f"[cyan]Crawled: {self.stats['total_crawled']} | Queue: {len(self.url_queue)} | Visited: {len(self.visited_urls)}")
            
            if self.shutdown_event.is_set():
                console.print("\n[bold yellow]Shutdown signal received. Finishing active tasks before exiting.[/bold yellow]")

        finally:
            console.print("[cyan]Flushing remaining data to database...[/cyan]")
            await self._flush_db_batch()            
            if self.session: await self.session.close()
            await self.robots_cache.close()
            await self._close_database()
    
    def save_results(self, filename: str = "crawl_results.json"):
        """Lưu kết quả crawl vào file JSON."""
        if not self.save_to_json: return
        output = {
            'metadata': {
                'crawl_started': datetime.utcnow().isoformat(),
                'start_urls': self.start_urls,
                'max_depth': self.max_depth,
                'user_agent': self.user_agent,
                'total_pages_crawled': self.stats['total_crawled'],
                'total_pages_skipped': self.stats['total_skipped'],
                'total_errors': self.stats['total_errors'],
                'cdn_uploads': self.stats['cdn_uploads'],
                'cdn_errors': self.stats['cdn_errors']
            },
            'statistics': {'by_domain': dict(self.stats['by_domain'])},
            'pages': self.results
        }
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        console.print(f"\n[green]✓ Results saved to {filename}[/green]")
    
    # === KHẮC PHỤC LỖI Attribute Error TẠI ĐÂY ===
    def print_summary(self):
        """In tóm tắt kết quả crawl."""
        console.print("\n" + "="*60)
        console.print("[bold cyan]Crawl Summary[/bold cyan]")
        console.print("="*60)
        
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", justify="right", style="green")
        
        table.add_row("Total Pages Crawled", str(self.stats['total_crawled']))
        table.add_row("Total Pages Skipped", str(self.stats['total_skipped']))
        table.add_row("Total Errors", str(self.stats['total_errors']))
        table.add_row("CDN Uploads", str(self.stats['cdn_uploads']))
        table.add_row("CDN Errors", str(self.stats['cdn_errors']))
        table.add_row("Unique URLs Visited", str(len(self.visited_urls)))
        
        console.print(table)
        
        if self.stats['by_domain']:
            console.print("\n[bold cyan]By Domain:[/bold cyan]")
            domain_table = Table(show_header=True, header_style="bold magenta")
            domain_table.add_column("Domain", style="cyan")
            domain_table.add_column("Crawled", justify="right", style="green")
            domain_table.add_column("Skipped", justify="right", style="yellow")
            domain_table.add_column("Errors", justify="right", style="red")
            
            for domain, stats in sorted(self.stats['by_domain'].items()):
                domain_table.add_row(domain, str(stats['crawled']), str(stats['skipped']), str(stats['errors']))
            console.print(domain_table)
    # ============================================

def load_crawler_config() -> Dict:
    """Tải cấu hình từ file, sử dụng đường dẫn trong volume mount."""
    config_path = "/app/crawler/config.json"
    try:
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                config = json.load(f)
            console.print(f"[green]✓ Config loaded from {config_path}[/green]")
            return config
        else:
             console.print(f"[bold yellow]Warning: Config file not found at {config_path}. Using default empty config.[/bold yellow]")
             return {}
    except Exception as e:
        console.print(f"[red]Error loading config file: {e}. Using default empty config.[/red]")
        return {}


async def main():
    load_dotenv() 

    console.print(Panel.fit("[bold cyan]TeaserBot[/bold cyan]\n[dim]Self-Host Search Edition[/dim]", border_style="cyan"))
    
    # === KHẮC PHỤC LỖI QUAN TRỌNG: TẢI CONFIG ĐÃ ĐƯỢC GO BACKEND CẬP NHẬT ===
    config = load_crawler_config()
    
    start_urls = config.get('start_urls', ['https://teaserverse.dev'])
    if not start_urls:
         console.print("[bold red]Lỗi: Không có start_urls trong config. Dừng.[/bold red]")
         return

    crawler = WebCrawler(
        start_urls=start_urls,
        config=config
    )
    
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, crawler.shutdown_event.set)
    
    console.print(f"\n[cyan]Starting crawl with {len(crawler.start_urls)} seed URLs... (Press Ctrl+C to stop gracefully)[/cyan]")
    console.print(f"[dim]Max depth: {crawler.max_depth} | Concurrent requests: {crawler.max_concurrent_requests}[/dim]")
    if crawler.max_pages: console.print(f"[dim]Max pages: {crawler.max_pages}[/dim]\n")
    else: console.print()
    
    try:
        await crawler.crawl()
    finally:
        console.print("\n[cyan]Crawl finished or was interrupted. Finalizing...[/cyan]")
        crawler.print_summary() 
        output_file = config.get('output_file', 'crawl_results.json')
        crawler.save_results(output_file)

if __name__ == "__main__":
    asyncio.run(main())