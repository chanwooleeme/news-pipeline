# downloader/Dockerfile
FROM python:3.11-slim

WORKDIR /app

# 의존성 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 코드 복사
COPY main.py .

# Docker 환경 표시
ENV IS_DOCKER=1

CMD ["python", "main.py"]s