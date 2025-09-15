FROM python:3.11-slim

WORKDIR /app

# 1️⃣ AWS CLI 설치
RUN apt-get update && \
    apt-get install -y curl unzip && \
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf awscliv2.zip aws && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# 2️⃣ Python 의존성 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 3️⃣ 코드 복사
COPY main.py .
COPY parser_factory.py .
COPY parsers/ ./parsers/

# 4️⃣ 환경 변수
ENV IS_DOCKER=1
ENV PYTHONUNBUFFERED=1
ENV AWS_REGION=ap-northeast-2

# 5️⃣ 실행
CMD ["python", "main.py"]
