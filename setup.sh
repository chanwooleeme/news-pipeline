#!/bin/bash
# setup.sh - 초기 설정 스크립트

set -e

echo "🚀 News Pipeline 초기 설정 시작"
echo ""

# 1. .env 파일 생성
if [ ! -f .env ]; then
    echo "📝 .env 파일 생성 중..."
    cp .env.example .env
    echo "✅ .env 파일이 생성되었습니다."
    echo "⚠️  .env 파일을 열어서 AWS 키와 S3 버킷을 설정하세요!"
    echo ""
else
    echo "✅ .env 파일이 이미 존재합니다."
    echo ""
fi

# 2. 필요한 디렉토리 생성
echo "📁 디렉토리 생성 중..."
mkdir -p temp/html
mkdir -p temp/parsed
mkdir -p logs
echo "✅ 디렉토리 생성 완료"
echo ""

# 3. Docker 이미지 빌드
echo "🐳 Docker 이미지 빌드 중..."
docker-compose build
echo "✅ Docker 이미지 빌드 완료"
echo ""

# 4. .env 파일 확인
echo "🔍 환경변수 확인 중..."
if grep -q "your_access_key_here" .env; then
    echo "⚠️  경고: AWS 키가 아직 설정되지 않았습니다!"
    echo "   .env 파일을 수정한 후 ./deploy.sh를 실행하세요."
    echo ""
    exit 1
fi

if grep -q "your-bucket-name" .env; then
    echo "⚠️  경고: S3 버킷이 아직 설정되지 않았습니다!"
    echo "   .env 파일을 수정한 후 ./deploy.sh를 실행하세요."
    echo ""
    exit 1
fi

echo "✅ 환경변수 설정 완료"
echo ""

echo "✨ 초기 설정이 완료되었습니다!"
echo ""
echo "다음 단계:"
echo "  1. ./start.sh          - 서비스 시작"
echo "  2. ./cron-setup.sh     - Cron 설정 (매시간 자동 실행)"
echo "  3. ./logs.sh           - 로그 확인"
echo ""