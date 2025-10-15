# parser/parsers/base_parser.py
from abc import ABC, abstractmethod
from bs4 import BeautifulSoup
from datetime import datetime
from typing import Dict, Any, Optional
import re
import logging


class BaseParser(ABC):
    """HTML 문서 파싱을 위한 기본 파서 클래스"""
    
    def __init__(self, html: str):
        self.html: str = html
        self.soup: BeautifulSoup = BeautifulSoup(html, 'html.parser')

    def clean_text(self, text: Optional[str]) -> str:
        """불필요한 공백, 줄바꿈 등을 제거"""
        if text:
            text = re.sub(r'\s+', ' ', text)
            return text.strip()
        return ''
    
    def extract_meta(self, property_name: str) -> str:
        """meta property 또는 name으로부터 content 값 추출"""
        tag = self.soup.find("meta", property=property_name)
        if tag and tag.get("content"):
            return self.clean_text(tag.get("content"))
        
        tag = self.soup.find("meta", attrs={"name": property_name})
        if tag and tag.get("content"):
            return self.clean_text(tag.get("content"))
        
        return ''
    
    def get_title(self) -> str:
        """기사 제목 추출"""
        try:
            # og:title meta 태그 우선
            title = self.extract_meta("og:title")
            if title:
                return title
            
            # fallback: <title> 태그
            title_tag = self.soup.find("title")
            if title_tag:
                return self.clean_text(title_tag.get_text())
            
            return ''
        except Exception as e:
            logging.warning(f"제목 추출 오류: {e}")
            return ''
    
    def get_category(self) -> str:
        """카테고리 추출"""
        try:
            # article:section meta 태그 우선
            category = self.extract_meta("article:section")
            if category:
                return category
            
            # fallback: Classification
            category = self.extract_meta("Classification")
            return category if category else ''
        except Exception as e:
            logging.warning(f"카테고리 추출 오류: {e}")
            return ''
    
    def get_published_time(self) -> float:
        """발행일 추출 (Unix timestamp 반환)"""
        try:
            # article:published_time meta 태그
            iso_str = self.extract_meta("article:published_time")
            if not iso_str:
                # fallback: 다른 메타 태그
                iso_str = self.extract_meta("meta:pubdate") or self.extract_meta("pubdate")
            
            if not iso_str:
                return datetime.now().timestamp()
            
            # ISO 8601 형식 처리 (Z를 +00:00으로)
            iso_str = iso_str.replace('Z', '+00:00')
            dt = datetime.fromisoformat(iso_str)
            return dt.timestamp()
            
        except (ValueError, TypeError) as e:
            logging.warning(f"발행일 파싱 오류: {e}, 현재 시간 사용")
            return datetime.now().timestamp()
    
    @abstractmethod
    def get_content(self) -> str:
        """본문 내용 추출 (각 파서에서 구현)"""
        pass
    
    def parse(self) -> Dict[str, Any]:
        """전체 HTML 파싱"""
        try:
            return {
                'title': self.get_title(),
                'content': self.get_content(),
                'published_time': self.get_published_time(),
                'category': self.get_category() or 'uncategorized',
            }
        except Exception as e:
            logging.error(f"파싱 중 예외 발생: {e}")
            return {
                'title': self.get_title() or '제목 없음',
                'content': '',
                'published_time': datetime.now().timestamp(),
                'category': 'uncategorized',
                'error': str(e)
            }