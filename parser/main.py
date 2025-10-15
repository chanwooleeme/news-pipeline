# parser/main.py
import os
import json
import time
import subprocess
import shutil
from pathlib import Path
from typing import Dict, Any
from redis import Redis
from datetime import datetime, timedelta
from multiprocessing import Process
from parser_factory import ParserFactory

from prometheus_client import Counter, Summary, start_http_server

parser_tasks_processed_total = Counter("parser_tasks_processed_total", "Total processed tasks")
parser_task_failures_total = Counter("parser_task_failures_total", "Total failed tasks")
parser_s3_upload_success_total = Counter("parser_s3_upload_success_total", "S3 upload success count")
parser_s3_upload_failure_total = Counter("parser_s3_upload_failure_total", "S3 upload failure count")
parser_task_duration_seconds = Summary("parser_task_duration_seconds", "Task processing duration in seconds")
parser_idle_seconds_total = Counter("parser_idle_seconds_total", "Total idle seconds")


class ParserConfig:
    """파서 설정"""
    def __init__(self):
        self.redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
        self.s3_bucket = os.getenv('S3_BUCKET')
        self.aws_region = os.getenv('AWS_REGION', 'ap-northeast-2')
        self.num_workers = int(os.getenv('NUM_WORKERS', '2'))
        self.is_docker = os.getenv('IS_DOCKER', '0') == '1'
        
        # 임시 디렉토리
        if self.is_docker:
            self.temp_base = Path('/app/temp')
        else:
            self.temp_base = Path(__file__).parent.parent / 'temp'


class NewsParserService:
    """뉴스 파서 서비스"""
    
    def __init__(self, worker_id: int = 0):
        self.worker_id = worker_id
        self.config = ParserConfig()
        self.redis = Redis.from_url(self.config.redis_url, decode_responses=False)
        self.parser_factory = ParserFactory()
        
        self.current_batch = ""
        self.processed_count = 0
        
        print(f"[Worker {worker_id}] 🚀 시작")
    
    def process_task(self, task: dict) -> None:
        """단일 파싱 작업 처리 (로컬 저장만)"""
        start_total = time.time()
        
        # 작업 정보 추출
        timestamp = task.get('timestamp', 'unknown')
        publisher = task.get('publisher', 'unknown')
        url = task.get('url', 'unknown')
        filepath = task.get('filepath', 'unknown')
        url_hash = task.get('url_hash', 'unknown')
        
        try:
            # 배치 전환 체크
            if self.current_batch != timestamp:
                if self.current_batch:
                    print(f"[Worker {self.worker_id}] ✅ 배치 전환: {self.current_batch} → {timestamp}")
                self.current_batch = timestamp
                self.processed_count = 0
            
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    html = f.read()
            except UnicodeDecodeError:
                if publisher == "시사저널":
                    print(f"[Worker {self.worker_id}] EUC-KR fallback ({publisher})")
                    with open(filepath, "r", encoding="euc-kr", errors="ignore") as f:
                        html = f.read()
                else:
                    raise
            
            # 2. 파싱
            parsed = self.parser_factory.parse(html, publisher)
            
            # 3. JSON 로컬 저장 (S3 업로드 안 함!)
            result = {
                'url': url,
                'publisher': publisher,
                'timestamp': timestamp,
                'parsed_at': datetime.now().isoformat(),
                **parsed
            }
            
            # parsed 디렉토리에 저장
            parsed_dir = self.config.temp_base / 'parsed' / timestamp / publisher
            parsed_dir.mkdir(parents=True, exist_ok=True)
            
            json_path = parsed_dir / f"{url_hash}.json"
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
        
            self.processed_count += 1
            
            # 간단한 로그
            total = time.time() - start_total
            print(f"[Worker {self.worker_id}] ✓ {publisher} | {total:.2f}초")
            
        except Exception as e:
            print(f"[Worker {self.worker_id}] ✗ 실패:")
            print(f"  URL: {url[:80]}")
            print(f"  Publisher: {publisher}")
            print(f"  Error: {e}")
            import traceback
            print(f"  Traceback:\n{traceback.format_exc()}")
    
    def upload_batch_to_s3(self, timestamp: str) -> None:
        """배치 완료 후 S3에 일괄 업로드 (Worker 0만)"""
        if self.worker_id != 0:
            return
        
        print(f"\n[Worker {self.worker_id}] 📤 S3 배치 업로드 시작: {timestamp}")
        
        try:
            # HTML 업로드
            html_local = str(self.config.temp_base / 'html' / timestamp)
            html_s3 = f"s3://{self.config.s3_bucket}/html/{timestamp}/"
            
            if Path(html_local).exists():
                result = subprocess.run([
                    'aws', 's3', 'sync',
                    html_local,
                    html_s3,
                    '--region', self.config.aws_region,
                    '--only-show-errors'
                ], capture_output=True, text=True, timeout=300)
                
                if result.returncode == 0:
                    print(f"[Worker {self.worker_id}]   ✓ HTML 업로드 완료")
                    # 업로드 성공하면 로컬 삭제
                    shutil.rmtree(html_local, ignore_errors=True)
                else:
                    print(f"[Worker {self.worker_id}]   ✗ HTML 업로드 실패: {result.stderr}")
            
            # JSON 업로드
            parsed_local = str(self.config.temp_base / 'parsed' / timestamp)
            parsed_s3 = f"s3://{self.config.s3_bucket}/parsed/{timestamp}/"
            
            if Path(parsed_local).exists():
                result = subprocess.run([
                    'aws', 's3', 'sync',
                    parsed_local,
                    parsed_s3,
                    '--region', self.config.aws_region,
                    '--only-show-errors'
                ], capture_output=True, text=True, timeout=300)
                
                if result.returncode == 0:
                    print(f"[Worker {self.worker_id}]   ✓ JSON 업로드 완료")
                    parser_s3_upload_success_total.inc()
                    # 업로드 성공하면 로컬 삭제
                    shutil.rmtree(parsed_local, ignore_errors=True)
                else:
                    print(f"[Worker {self.worker_id}]   ✗ JSON 업로드 실패: {result.stderr}")
                    parser_s3_upload_failure_total.inc()
            
            print(f"[Worker {self.worker_id}]   ✓ 배치 업로드 및 정리 완료")
            
        except subprocess.TimeoutExpired:
            print(f"[Worker {self.worker_id}]   ✗ S3 업로드 타임아웃 (5분 초과)")
        except Exception as e:
            print(f"[Worker {self.worker_id}]   ✗ S3 업로드 오류: {e}")
    
    def check_batch_complete(self) -> None:
        """배치 완료 신호 확인 및 S3 업로드 (Worker 0만)"""
        if self.worker_id != 0 or not self.current_batch:
            return
        
        signal_key = f"batch_complete:{self.current_batch}"
        signal = self.redis.get(signal_key)
        
        if signal:
            signal_data = json.loads(signal.decode('utf-8'))
            print(f"\n[Worker {self.worker_id}] 🎉 배치 완료: {self.current_batch}")
            print(f"   다운로드: {signal_data['total_tasks']}개")
            print(f"   파싱: {self.processed_count}개")
            
            # S3 배치 업로드
            self.upload_batch_to_s3(self.current_batch)
            
            # 신호 삭제
            self.redis.delete(signal_key)
            self.current_batch = ""
            self.processed_count = 0
    
    def run(self) -> None:
        """서비스 실행 - Redis 큐 구독"""
        print(f"[Worker {self.worker_id}] ⏳ Redis 큐 대기 중...\n")
        
        consecutive_empty = 0
        
        while True:
            try:
                # Pipeline으로 배치 가져오기
                pipe = self.redis.pipeline()
                batch_size = 20
                
                for _ in range(batch_size):
                    pipe.rpop('parse_queue')
                
                results = pipe.execute()
                tasks = [json.loads(r.decode('utf-8')) for r in results if r]
                
                if tasks:
                    consecutive_empty = 0
                    print(f"[Worker {self.worker_id}] 📦 배치: {len(tasks)}개")
                    
                    for task in tasks:
                        self.process_task(task)
                else:
                    consecutive_empty += 1
                    parser_idle_seconds_total.inc()

                    if consecutive_empty == 1:
                        self.check_batch_complete()  # S3 업로드 여기서!
                    
                    if consecutive_empty % 10 == 0:
                        print(f"[Worker {self.worker_id}] 💤 대기 중... ({consecutive_empty}초)")
                    
                    time.sleep(1)
                    
                    if consecutive_empty % 600 == 0 and self.worker_id == 0:
                        cleanup_old_dirs(self.config.temp_base / 'html', hours=24)
                        cleanup_old_dirs(self.config.temp_base / 'parsed', hours=24)

            except KeyboardInterrupt:
                print(f"\n[Worker {self.worker_id}] 🛑 종료")
                break
            except Exception as e:
                print(f"[Worker {self.worker_id}] ❌ 오류: {e}")
                time.sleep(1)


def worker_process(worker_id: int):
    """워커 프로세스 함수"""
    service = NewsParserService(worker_id)
    service.run()

def cleanup_old_dirs(base_path: Path, hours: int = 24):
    """지정 시간(hours) 이상 지난 배치 디렉토리 자동 정리"""
    cutoff = datetime.now() - timedelta(hours=hours)
    for subdir in base_path.iterdir():
        if not subdir.is_dir():
            continue
        try:
            ts = datetime.strptime(subdir.name, "%Y%m%d_%H")
            if ts < cutoff:
                shutil.rmtree(subdir, ignore_errors=True)
                print(f"🧹 오래된 디렉토리 삭제: {subdir}")
        except ValueError:
            continue

def main():
    config = ParserConfig()
    num_workers = config.num_workers
    start_http_server(8002)  # Prometheus에서 수집할 /metrics 엔드포인트 오픈
    
    print(f"🚀 Parser 서비스 시작 ({num_workers} workers)")
    print(f"🔧 환경: {'Docker' if config.is_docker else '로컬'}")
    print(f"📦 S3 버킷: {config.s3_bucket}")
    print(f"📂 임시 디렉토리: {config.temp_base}\n")
    
    # 멀티프로세스 시작
    processes = []
    for i in range(num_workers):
        p = Process(target=worker_process, args=(i,))
        p.start()
        processes.append(p)
    
    # 모든 워커 대기
    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        print("\n🛑 모든 워커 종료 중...")
        for p in processes:
            p.terminate()
            p.join()


if __name__ == '__main__':
    main()