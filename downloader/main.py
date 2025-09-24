import os
import json
import hashlib
import feedparser
import requests
import random
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set
from redis import Redis
from concurrent.futures import ThreadPoolExecutor, as_completed
from prometheus_client import Counter, Summary, Gauge, start_http_server

rss_fetch_failures_total = Counter("rss_fetch_failures_total", "RSS fetch failures")
download_success_total = Counter("download_success_total", "Download success count")
download_failure_total = Counter("download_failure_total", "Download failure count")
redis_items_pushed_total = Counter("redis_items_pushed_total", "Items pushed to Redis")
pipeline_batch_duration_seconds = Gauge("pipeline_batch_duration_seconds", "Total pipeline duration")


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
            print(f"  ✗ RSS 파싱 실패: {feed_url[:60]} - {e}")
            return []


# ============================================================
# 🧩 STEP 2. Redis 관리 (중복 체크 + Burst 업데이트)
# ============================================================

class RedisManager:
    """Redis 관리 (burst insert 최적화)"""
    
    def __init__(self, config: DownloaderConfig):
        self.redis = Redis.from_url(config.redis_url, decode_responses=False)
        self.ttl = config.redis_ttl

    def get_existing_hashes(self, url_hashes: Set[str]) -> Set[str]:
        """이미 Redis에 존재하는 URL 해시 조회"""
        pipe = self.redis.pipeline()
        for h in url_hashes:
            pipe.exists(f"url:{h}")
        results = pipe.execute()
        return {h for h, exists in zip(url_hashes, results) if exists}

    def flush_burst(self, tasks: List[dict]) -> None:
        """다운로드 완료 후 Redis에 배치 삽입"""
        # TODO: redis 경량화. 경로만 넣고 parser에서 파싱.
        if not tasks:
            return
        pipe = self.redis.pipeline()
        for t in tasks:
            pipe.lpush("parse_queue", json.dumps(t))
            pipe.setex(f"url:{t['url_hash']}", self.ttl, "1")
        pipe.execute()
        print(f"💾 Redis batch 저장 완료 ({len(tasks)}건)")

    def send_batch_complete_signal(self, timestamp: str, total_tasks: int) -> None:
        """전체 배치 완료 신호"""
        signal_data = {
            'timestamp': timestamp,
            'total_tasks': total_tasks,
            'completed_at': datetime.now().isoformat()
        }
        self.redis.setex(f'batch_complete:{timestamp}', 3600, json.dumps(signal_data))


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

        print(f"📂 저장 경로: {self.temp_dir}")
        print(f"📂 RSS 설정: {self.config.rss_config_path}")


    # ----------------------------------------------------------
    # STEP 1~4 요약
    # 1. feedparser로 RSS URL 모두 수집
    # 2. URL 전체 set 생성 → 중복 자동 제거
    # 3. Redis에 TTL(72시간)로 등록된 기존 URL 해시 조회
    # 4. Redis에 존재하는 URL 제외하여 신규 URL만 남김
    # ----------------------------------------------------------
    def collect_urls(self) -> List[dict]:
        rss_feeds = self.rss_fetcher.load_feeds()
        print(f"📰 {len(rss_feeds)}개 언론사 RSS 로드 완료")

        # ✅ 1️⃣ 모든 RSS를 한 번만 호출하고 결과 저장
        all_urls_by_publisher = {}

        for publisher, feed_urls in rss_feeds.items():
            urls = []
            for feed_url in feed_urls:
                fetched = self.rss_fetcher.fetch_urls(feed_url)
                urls.extend(fetched)
            all_urls_by_publisher[publisher] = urls

        # ✅ 2️⃣ 전체 URL set 생성 (중복 제거)
        all_urls = {u for urls in all_urls_by_publisher.values() for u in urls}

        # ✅ 3️⃣ Redis에서 기존 해시 확인
        url_hashes = {HtmlDownloader.generate_hash(u) for u in all_urls}
        existing = self.redis_manager.get_existing_hashes(url_hashes)

        # ✅ 4️⃣ 신규 URL만 필터링
        print(f"📥 신규 URL {len(all_urls) - len(existing)}개 (중복 {len(existing)}개 제외)")

        # ✅ 5️⃣ publisher 정보 매핑 (이미 로드한 데이터 재활용)
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

        # ✅ 6️⃣ 트래픽 분산 + 테스트 모드 처리
        random.shuffle(url_pool)
        if self.test_mode:
            url_pool = url_pool[:10]
            print(f"🧪 [TEST MODE] URL 10개만 다운로드합니다.")

        return url_pool



    # ----------------------------------------------------------
    # STEP 5. ThreadPool로 URL 다운로드
    # - shuffle된 URL을 병렬 다운로드
    # - 0.3~1.2초 sleep으로 polite crawling
    # ----------------------------------------------------------
    def download_task(self, item: dict) -> dict | None:
        publisher, url, url_hash = item["publisher"], item["url"], item["url_hash"]
        try:
            time.sleep(random.uniform(0.3, 1.2))
            html = self.html_downloader.download(url)
            filepath = self.html_downloader.save(publisher, url_hash, html)
            return {
                "timestamp": self.timestamp,
                "publisher": publisher,
                "url": url,
                "filepath": str(filepath),
                "url_hash": url_hash
            }
        except Exception as e:
            print(f"  ✗ 실패: {url[:80]} - {e}")
            return None


    # ----------------------------------------------------------
    # STEP 6. Redis burst 업데이트 (500개 단위)
    # ----------------------------------------------------------
    def run(self) -> None:
        print(f"🚀 다운로더 시작: {self.timestamp}")
        print(f"🔧 환경: {'Docker' if self.config.is_docker else '로컬'}")

        start_time = time.perf_counter()
        url_pool = self.collect_urls()

        pending_tasks = []
        downloaded = 0

        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            futures = [executor.submit(self.download_task, item) for item in url_pool]
            for i, future in enumerate(as_completed(futures), 1):
                result = future.result()
                if result:
                    pending_tasks.append(result)
                    downloaded += 1
                    if len(pending_tasks) >= 500:
                        self.redis_manager.flush_burst(pending_tasks)
                        pending_tasks.clear()
                    if i % 20 == 0:
                        print(f"  진행률: {i}/{len(url_pool)} ({downloaded} 성공)")

        # 남은 데이터 최종 flush
        self.redis_manager.flush_burst(pending_tasks)
        elapsed = time.perf_counter() - start_time

        if downloaded > 0:
            self.redis_manager.send_batch_complete_signal(self.timestamp, downloaded)
            print(f"\n✅ 완료 신호 전송: {self.timestamp}")

        print(f"\n📊 결과 요약")
        print(f"  다운로드: {downloaded}개")
        print(f"⏱️ 총 소요 시간: {elapsed:.2f}초")
        print(f"⚡ 평균 처리 속도: {downloaded / elapsed:.2f}건/초")


# ============================================================
# ENTRY POINT
# ============================================================

def main():
    start_http_server(8001)  # Prometheus에서 수집할 /metrics 엔드포인트 오픈
    timestamp = datetime.now().strftime("%Y%m%d%H")
    pipeline = NewsDownloadPipeline(timestamp)
    pipeline.run()


if __name__ == "__main__":
    main()
