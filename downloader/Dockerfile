FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y curl unzip cron && \
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && ./aws/install && \
    rm -rf awscliv2.zip aws && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# 크론 등록
RUN echo "*/10 * * * * python /app/main.py >> /app/logs/downloader_cron.log 2>&1" > /etc/cron.d/downloader \
    && chmod 0644 /etc/cron.d/downloader \
    && crontab /etc/cron.d/downloader

ENV IS_DOCKER=1
ENV PYTHONUNBUFFERED=1
ENV AWS_REGION=ap-northeast-2

# 크론 + 포그라운드 유지
CMD service cron start && tail -f /app/logs/downloader_cron.log
