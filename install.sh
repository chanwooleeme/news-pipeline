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

# 디렉토리
mkdir -p temp/html temp/parsed logs

# Git pull (있으면)
[ -d .git ] && git pull || true

# 빌드 & 시작
docker compose up -d --build

echo ""
echo "✅ 완료!"
echo ""
echo "로그: docker compose logs -f"
echo "수동실행: ./run.sh"
echo ""

# Cron 설정 (처음만)
if ! crontab -l 2>/dev/null | grep -q "news-pipeline"; then
    read -p "Cron 설정? (Y/n): " -n 1 reply
    echo ""
    if [[ ! $reply =~ ^[Nn]$ ]]; then
        dir=$(pwd)
        (crontab -l 2>/dev/null; echo "*/10 * * * * cd $dir && flock -n /tmp/news_downloader.lock docker compose run --rm downloader >> $dir/logs/cron.log 2>&1 && find $dir/logs -name 'cron.log' -size +5M -exec truncate -s 0 {} \;") | crontab -
        echo "✅ Cron 설정 완료"
    fi
fi