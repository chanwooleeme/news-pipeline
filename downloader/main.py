import os
import json
import hashlib
import feedparser
import requests
import random
import time
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set
from redis import Redis
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from prometheus_client import CollectorRegistry, Counter, Gauge, push_to_gateway

# ============================================================
# 🧩 로깅 설정
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ============================================================
# 🧩 Prometheus 메트릭 (독립 레지스트리)
# ============================================================
registry = CollectorRegistry()

rss_fetch_failures_total = Counter(
    "rss_fetch_failures_total",
    "RSS fetch failures",
    registry=registry
)
download_success_total = Counter(
    "download_success_total",
    "Download success count",
    registry=registry
)
download_failure_total = Counter(
    "download_failure_total",
    "Download failure count",
    registry=registry
)
redis_items_pushed_total = Counter(
    "redis_items_pushed_total",
    "Items pushed to Redis",
    registry=registry
)
pipeline_batch_duration_seconds = Gauge(
    "pipeline_batch_duration_seconds",
    "Total pipeline duration",
    registry=registry
)

# ============================================================
# 🧩 STEP 0. 공통 설정
# ============================================================

class DownloaderConfig:
    """다운로더 전역 설정"""
    def __init__(self):
        # Redis 연결 및 기본 TTL
        self.redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
        self.redis_ttl = 72 * 3600  # 72시간 동안 중복 체크 유지
        
        # HTTP 요청 설정
        self.user_agent = (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        self.timeout = 10

        # ThreadPool 병렬 개수
        self.max_workers = int(os.getenv('MAX_WORKERS', '12'))

        # Docker / Local 환경에 따라 경로 분기
        self.is_docker = os.getenv('IS_DOCKER', '0') == '1'
        self.base_path = Path('/app') if self.is_docker else Path(__file__).parent.parent
        self.rss_config_path = self.base_path / 'shared' / 'rss_feeds.json'
        self.temp_base_dir = self.base_path / 'temp'
        
        # Pushgateway 설정
        self.pushgateway_url = os.getenv('PUSHGATEWAY_URL', 'localhost:9091')


# ============================================================
# 🧩 STEP 1. RSS 수집기 (feedparser)
# ============================================================

class RssFetcher:
    """RSS 피드 수집기"""
    
    def __init__(self, config: DownloaderConfig):
        self.config = config

    def load_feeds(self) -> Dict[str, List[str]]:
        """rss_feeds.json 로드 (주석 제거 후 파싱)"""
        with open(self.config.rss_config_path, 'r', encoding='utf-8') as f:
            raw_text = f.read().lstrip('\ufeff')
            lines = [
                line for line in raw_text.splitlines()
                if line.strip() and not line.strip().startswith('//')
            ]
            return json.loads("\n".join(lines))
    
    def fetch_urls(self, feed_url: str) -> List[str]:
        """단일 RSS 피드에서 URL 목록 수집"""
        try:
            # (특정 도메인 예외 처리)
            if "pressian.com" in feed_url:
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0)",
                    "Accept": "application/xml,text/html,application/xhtml+xml"
                }
                resp = requests.get(feed_url, headers=headers, timeout=(5, 60))
                resp.raise_for_status()
                feed = feedparser.parse(resp.content)
            else:
                feed = feedparser.parse(feed_url)
            return [entry.link for entry in feed.entries if hasattr(entry, "link")]
        except Exception as e:
            logger.warning(f"RSS 파싱 실패: {feed_url[:60]} - {e}")
            rss_fetch_failures_total.inc()
            return []


# ============================================================
# 🧩 STEP 2. Redis 관리 (중복 체크 + Burst 업데이트)
# ============================================================

class RedisManager:
    """Redis 관리 (burst insert 최적화)"""
    
    def __init__(self, config: DownloaderConfig):
        self.redis = Redis.from_url(
            config.redis_url,
            decode_responses=False,
            socket_timeout=10,           # 10초 읽기 타임아웃
            socket_connect_timeout=5,    # 5초 연결 타임아웃
            socket_keepalive=True,       # 연결 유지
            health_check_interval=30     # 30초마다 연결 확인
        )
        self.ttl = config.redis_ttl

    def get_existing_hashes(self, url_hashes: Set[str]) -> Set[str]:
        """이미 Redis에 존재하는 URL 해시 조회"""
        try:
            pipe = self.redis.pipeline()
            for h in url_hashes:
                pipe.exists(f"url:{h}")
            results = pipe.execute()
            return {h for h, exists in zip(url_hashes, results) if exists}
        except Exception as e:
            logger.error(f"Redis 해시 조회 실패: {e}")
            return set()

    def flush_burst(self, tasks: List[dict]) -> None:
        """다운로드 완료 후 Redis에 배치 삽입"""
        if not tasks:
            return
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                pipe = self.redis.pipeline()
                for t in tasks:
                    pipe.lpush("parse_queue", json.dumps(t))
                    pipe.setex(f"url:{t['url_hash']}", self.ttl, "1")
                pipe.execute()
                logger.info(f"Redis batch 저장 완료 ({len(tasks)}건)")
                redis_items_pushed_total.inc(len(tasks))
                return
            except Exception as e:
                logger.warning(f"Redis flush 실패 (attempt {attempt+1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(1)
                else:
                    logger.error(f"Redis flush 최종 실패 after {max_retries} attempts")
                    raise

    def send_batch_complete_signal(self, timestamp: str, total_tasks: int) -> None:
        """전체 배치 완료 신호"""
        try:
            signal_data = {
                'timestamp': timestamp,
                'total_tasks': total_tasks,
                'completed_at': datetime.now().isoformat()
            }
            self.redis.setex(f'batch_complete:{timestamp}', 3600, json.dumps(signal_data))
            logger.info(f"배치 완료 신호 전송: {timestamp} ({total_tasks}건)")
        except Exception as e:
            logger.error(f"배치 완료 신호 전송 실패: {e}")


# ============================================================
# STEP 3. HTML 다운로더 (Thread 기반 polite crawler)
# ============================================================

class HtmlDownloader:
    """HTML 다운로드 + 파일 저장"""
    
    def __init__(self, config: DownloaderConfig, temp_dir: Path):
        self.config = config
        self.temp_dir = temp_dir

    @staticmethod
    def generate_hash(url: str) -> str:
        return hashlib.md5(url.encode()).hexdigest()

    def download(self, url: str) -> bytes:
        response = requests.get(
            url,
            headers={'User-Agent': self.config.user_agent},
            timeout=self.config.timeout
        )
        response.raise_for_status()
        return response.content

    def save(self, publisher: str, url_hash: str, html: bytes) -> Path:
        """HTML 파일 로컬 저장"""
        filepath = self.temp_dir / publisher / f"{url_hash}.html"
        filepath.parent.mkdir(parents=True, exist_ok=True)
        filepath.write_bytes(html)
        return filepath


# ============================================================
# STEP 4. 뉴스 다운로드 파이프라인
# ============================================================

class NewsDownloadPipeline:
    """뉴스 다운로드 파이프라인 (ThreadPool + Redis Burst)"""
    
    def __init__(self, timestamp: str):
        self.timestamp = timestamp
        self.config = DownloaderConfig()
        self.temp_dir = self.config.temp_base_dir / 'html' / timestamp
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.test_mode = os.getenv("TEST_MODE", "false").lower() == "true"
        self.rss_fetcher = RssFetcher(self.config)
        self.html_downloader = HtmlDownloader(self.config, self.temp_dir)
        self.redis_manager = RedisManager(self.config)

        logger.info(f"저장 경로: {self.temp_dir}")
        logger.info(f"RSS 설정: {self.config.rss_config_path}")
        logger.info(f"Worker 수: {self.config.max_workers}")
        logger.info(f"테스트 모드: {self.test_mode}")

    def collect_urls(self) -> List[dict]:
        """RSS에서 신규 URL 수집"""
        rss_feeds = self.rss_fetcher.load_feeds()
        logger.info(f"{len(rss_feeds)}개 언론사 RSS 로드 완료")

        # 1️⃣ 모든 RSS를 한 번만 호출
        all_urls_by_publisher = {}
        for publisher, feed_urls in rss_feeds.items():
            urls = []
            for feed_url in feed_urls:
                fetched = self.rss_fetcher.fetch_urls(feed_url)
                urls.extend(fetched)
            all_urls_by_publisher[publisher] = urls

        # 2️⃣ 전체 URL set 생성 (중복 제거)
        all_urls = {u for urls in all_urls_by_publisher.values() for u in urls}

        # 3️⃣ Redis에서 기존 해시 확인
        url_hashes = {HtmlDownloader.generate_hash(u) for u in all_urls}
        existing = self.redis_manager.get_existing_hashes(url_hashes)

        # 4️⃣ 신규 URL만 필터링
        new_count = len(all_urls) - len(existing)
        logger.info(f"신규 URL {new_count}개 (중복 {len(existing)}개 제외)")

        # 5️⃣ publisher 정보 매핑
        url_pool = []
        for publisher, urls in all_urls_by_publisher.items():
            for url in urls:
                url_hash = HtmlDownloader.generate_hash(url)
                if url_hash not in existing:
                    url_pool.append({
                        "publisher": publisher,
                        "url": url,
                        "url_hash": url_hash
                    })

        # 6️⃣ 트래픽 분산 + 테스트 모드 처리
        random.shuffle(url_pool)
        if self.test_mode:
            url_pool = url_pool[:10]
            logger.warning(f"[TEST MODE] URL 10개만 다운로드합니다")

        return url_pool

    def download_task(self, item: dict) -> dict | None:
        """단일 URL 다운로드 (polite crawling)"""
        publisher, url, url_hash = item["publisher"], item["url"], item["url_hash"]
        try:
            time.sleep(random.uniform(0.1, 0.5))  # 폴라이트 크롤링
            html = self.html_downloader.download(url)
            filepath = self.html_downloader.save(publisher, url_hash, html)
            download_success_total.inc()
            return {
                "timestamp": self.timestamp,
                "publisher": publisher,
                "url": url,
                "filepath": str(filepath),
                "url_hash": url_hash
            }
        except Exception as e:
            download_failure_total.inc()
            logger.debug(f"다운로드 실패: {url[:80]} - {e}")
            return None

    def run(self) -> None:
        """파이프라인 실행"""
        logger.info(f"다운로더 시작: {self.timestamp}")
        logger.info(f"환경: {'Docker' if self.config.is_docker else '로컬'}")

        start_time = time.perf_counter()
        url_pool = self.collect_urls()
        
        if not url_pool:
            logger.info("다운로드할 신규 URL이 없습니다")
            return

        pending_tasks = []
        downloaded = 0
        failed = 0

        # ThreadPool 실행 (전체 타임아웃 1시간)
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            futures = {executor.submit(self.download_task, item): item for item in url_pool}
            
            try:
                for future in as_completed(futures, timeout=3600):
                    try:
                        result = future.result(timeout=30)  # 개별 작업 30초 타임아웃
                        if result:
                            pending_tasks.append(result)
                            downloaded += 1
                            
                            # 500개 단위로 Redis flush
                            if len(pending_tasks) >= 500:
                                self.redis_manager.flush_burst(pending_tasks)
                                pending_tasks.clear()
                        else:
                            failed += 1
                            
                    except TimeoutError:
                        failed += 1
                        item = futures[future]
                        logger.warning(f"작업 타임아웃: {item['url'][:60]}")
                    except Exception as e:
                        failed += 1
                        logger.error(f"작업 실패: {e}")
                    
                    # 진행 상황 출력 (20개마다)
                    total_done = downloaded + failed
                    if total_done % 20 == 0:
                        logger.info(f"진행률: {total_done}/{len(url_pool)} (성공:{downloaded}, 실패:{failed})")
                        
            except TimeoutError:
                logger.error("전체 작업 타임아웃 (1시간 초과)")

        # 남은 데이터 최종 flush
        if pending_tasks:
            self.redis_manager.flush_burst(pending_tasks)

        elapsed = time.perf_counter() - start_time
        pipeline_batch_duration_seconds.set(elapsed)

        # 배치 완료 신호
        if downloaded > 0:
            self.redis_manager.send_batch_complete_signal(self.timestamp, downloaded)

        # 결과 요약
        logger.info("=" * 60)
        logger.info(f"📊 결과 요약")
        logger.info(f"  다운로드 성공: {downloaded}개")
        logger.info(f"  다운로드 실패: {failed}개")
        logger.info(f"  총 소요 시간: {elapsed:.2f}초")
        if downloaded > 0:
            logger.info(f"  평균 처리 속도: {downloaded / elapsed:.2f}건/초")
        logger.info("=" * 60)
        
        # Pushgateway로 메트릭 전송
        try:
            push_to_gateway(
                self.config.pushgateway_url,
                job='news_downloader',
                grouping_key={'timestamp': self.timestamp},
                registry=registry
            )
            logger.info(f"메트릭 전송 완료: {self.config.pushgateway_url}")
        except Exception as e:
            logger.error(f"메트릭 전송 실패: {e}")


# ============================================================
# ENTRY POINT
# ============================================================

def main():
    timestamp = datetime.now().strftime("%Y%m%d%H")
    pipeline = NewsDownloadPipeline(timestamp)
    pipeline.run()


if __name__ == "__main__":
    main()