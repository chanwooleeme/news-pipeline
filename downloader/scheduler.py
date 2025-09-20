# scheduler.py
import os
import signal
import subprocess
import time
import logging
import threading
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from prometheus_client import Counter, Gauge, Histogram, start_http_server

# =========================================================
# 🧩 설정
# =========================================================
INTERVAL_MINUTES = int(os.getenv("DOWNLOAD_INTERVAL_MINUTES", "10"))
JOB_MAX_INSTANCES = int(os.getenv("JOB_MAX_INSTANCES", "1"))
IMMEDIATE_RUN = os.getenv("IMMEDIATE_RUN", "true").lower() in ("1", "true", "yes")
PYTHON_BIN = os.getenv("PYTHON_BIN", "/usr/local/bin/python")
MAIN_SCRIPT = os.getenv("MAIN_SCRIPT", "/app/main.py")
TEST_MODE = os.getenv("TEST_MODE", "false").lower() in ("1", "true", "yes")
METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))

# =========================================================
# 🧩 로깅
# =========================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("scheduler")

# =========================================================
# 🧩 Prometheus 메트릭
# =========================================================
jobs_total = Counter("downloader_jobs_total", "Total downloader jobs executed")
jobs_success_total = Counter("downloader_jobs_success_total", "Successful downloader jobs")
jobs_failure_total = Counter("downloader_jobs_failure_total", "Failed downloader jobs")
last_run_timestamp = Gauge("downloader_last_run_timestamp", "Timestamp of last downloader run")
job_duration = Histogram(
    "downloader_job_duration_seconds",
    "Duration of downloader jobs (seconds)",
    buckets=[1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600],
)

# =========================================================
# 🧩 다운로드 실행 함수
# =========================================================
def run_downloader():
    ts = datetime.utcnow()
    jobs_total.inc()
    start_time = time.time()
    env = os.environ.copy()
    if TEST_MODE:
        env["TEST_MODE"] = "true"

    logger.info(f"🚀 Job started: {ts.isoformat()} (TEST_MODE={TEST_MODE})")

    try:
        # ✅ main.py 실행
        result = subprocess.run(
            [PYTHON_BIN, MAIN_SCRIPT],
            env=env,
            text=True,
            check=True,
        )

        # ✅ Prometheus 업데이트
        duration = time.time() - start_time
        job_duration.observe(duration)
        last_run_timestamp.set_to_current_time()
        jobs_success_total.inc()

        logger.info(f"✅ Job finished successfully ({duration:.1f}s)")
    except subprocess.CalledProcessError as e:
        jobs_failure_total.inc()
        logger.error(f"❌ Job failed with code {e.returncode}")
    except Exception as e:
        jobs_failure_total.inc()
        logger.exception(f"🔥 Unexpected error: {e}")

# =========================================================
# 🧩 Prometheus 서버
# =========================================================
def start_metrics_server():
    start_http_server(METRICS_PORT)
    logger.info(f"📡 Prometheus metrics available on :{METRICS_PORT}")

# =========================================================
# 🧩 스케줄러 실행
# =========================================================
def main():
    executors = {"default": ThreadPoolExecutor(2)}
    scheduler = BackgroundScheduler(executors=executors, job_defaults={"max_instances": JOB_MAX_INSTANCES})

    scheduler.add_job(
        run_downloader,
        "interval",
        minutes=INTERVAL_MINUTES,
        id="downloader_job",
        replace_existing=True,
    )

    # Prometheus exporter 백그라운드 실행
    threading.Thread(target=start_metrics_server, daemon=True).start()

    def shutdown(signum, frame):
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        scheduler.shutdown(wait=True)
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    scheduler.start()
    logger.info(f"⏰ Scheduler started: every {INTERVAL_MINUTES}min, max_instances={JOB_MAX_INSTANCES}")

    if IMMEDIATE_RUN:
        logger.info("Running first job immediately...")
        run_downloader()

    try:
        while True:
            time.sleep(5)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown(wait=True)

if __name__ == "__main__":
    main()
