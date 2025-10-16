#!/bin/bash
# install.sh

set -e

echo "🚀 News Pipeline 설치/업데이트"
echo ""

# .env 없으면 생성
if [ ! -f .env ]; then
    echo "📝 환경변수 설정"
    cp .env.example .env
    read -p "AWS Access Key: " key
    read -p "AWS Secret Key: " secret
    read -p "S3 Bucket: " bucket
    
    sed -i "s/your_access_key_here/$key/" .env
    sed -i "s/your_secret_key_here/$secret/" .env
    sed -i "s/your-bucket-name/$bucket/" .env
    echo ""
fi

# 디렉토리 생성
mkdir -p temp/html temp/parsed logs

# Git pull (있으면)
[ -d .git ] && git pull || true

# Docker 빌드 및 실행
docker compose up -d --build

echo ""
echo "✅ 설치 및 실행 완료!"
echo "📜 로그 확인: docker compose logs -f"
echo "🧩 모니터링: http://localhost:9090 (Prometheus), http://localhost:3000 (Grafana)"
echo ""
