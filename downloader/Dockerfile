# downloader/Dockerfile
FROM python:3.11-slim

WORKDIR /app

# 기본 의존성만 설치 (AWS CLI 제거)
RUN apt-get update && apt-get install -y curl unzip && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# 로그 폴더
RUN mkdir -p /app/logs

ENV IS_DOCKER=1
ENV PYTHONUNBUFFERED=1
ENV TZ=Asia/Seoul

# APScheduler 실행 (cron 제거)
CMD ["python", "scheduler.py"]
