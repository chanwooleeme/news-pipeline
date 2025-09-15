from .base_parser import BaseParser
import re
import json
import logging
from typing import Tuple, Optional
from datetime import datetime
import html
class ChosunParser(BaseParser):
    def __init__(self, html: str):
        super().__init__(html)
        self._processed_data = None  # Fusion 데이터 캐싱

    def get_author(self) -> str:
        """BaseParser의 get_author 오버라이드"""
        try:
            processed = self._process_fusion_data()
            if processed and processed[1]:
                return self._clean_author(processed[1])
            
            # Fusion 데이터에서 작성자를 찾지 못한 경우 대안적 방법 시도
            byline = self.soup.select_one(".byline")
            if byline:
                return self._clean_author(byline.get_text(strip=True))
                
            return ""
        except Exception as e:
            logging.error(f"작성자 추출 오류: {e}")
            return ""

    def get_content(self) -> str:
        """BaseParser의 get_content 오버라이드"""
        try:
            processed = self._process_fusion_data()
            if processed and processed[0]:
                return self._clean_body(processed[0])
            
            # Fusion 데이터에서 본문을 찾지 못한 경우 대안적 방법 시도
            content_container = self.soup.select_one("div.article-body")
            if content_container:
                paragraphs = content_container.select("p")
                if paragraphs:
                    content = " ".join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))
                    return self.clean_article_text(self._clean_body(content))
            
            return ""
        except Exception as e:
            logging.error(f"본문 추출 오류: {e}")
            return ""

    def clean_article_text(text: str) -> str:
        # 1. HTML 태그 제거 (예: <b>, <i> 등)
        text = re.sub(r"<[^>]+>", "", text)
        
        # 2. HTML 엔티티 변환 (예: &amp; -> &)
        text = html.unescape(text)
        
        # 3. 대괄호로 감싸진 메타 정보 제거 (예: [편집자 주], [땅집고])
        text = re.sub(r"\[[^\]]+\]", "", text)
        
        # 4. 텍스트 양쪽에 붙은 불필요한 따옴표 및 공백 제거
        text = text.strip().strip('\"').strip()
        
        # 5. 내부의 여분의 공백을 하나로 정리
        text = re.sub(r"\s+", " ", text)
        
        return text


    def get_publication_date(self) -> str:
        try:
            iso_str = self.extract_meta("article:published_time")
            if not iso_str:
                # 다른 메타 태그 시도
                iso_str = self.extract_meta("og:published_time") or self.extract_meta("pubdate")
                
            if not iso_str:
                # HTML에서 발행일 찾기 시도
                date_elem = self.soup.select_one(".publication-date")
                if date_elem:
                    date_text = date_elem.get_text(strip=True)
                    # 여기서 날짜 텍스트를 파싱하는 로직이 필요하지만, 간단히 현재 시간 사용
                    return datetime.now().timestamp()
                
                return datetime.now().timestamp()
                
            # 'Z' 타임존을 표준 ISO 형식으로 변환
            iso_str = iso_str.replace("Z", "+00:00")
            dt = datetime.fromisoformat(iso_str)
            unix_timestamp = dt.timestamp()  # 초 단위 timestamp
            return unix_timestamp
        except Exception as e:
            logging.error(f"발행일 파싱 오류: {e}")
            return datetime.now().timestamp()
    
    def _process_fusion_data(self) -> Optional[Tuple[str, str]]:
        """Fusion 데이터 처리 (캐싱 포함)"""
        try:
            if self._processed_data is None:
                script_text = self._find_fusion_script()
                if not script_text:
                    return None
                self._processed_data = self._parse_fusion_script(script_text)
            return self._processed_data
        except Exception as e:
            logging.error(f"Fusion 데이터 처리 오류: {e}")
            return None

    def _find_fusion_script(self) -> Optional[str]:
        """Fusion.globalContent 스크립트 찾기"""
        try:
            # CSS 선택자로 스크립트 찾기
            script = self.soup.select_one('script#fusion-metadata')
            if script:
                return script.get_text(strip=True)
                
            # 대안: 모든 스크립트 태그 순회
            for script in self.soup.select("script"):
                if script.get("id") == "fusion-metadata" and script.get("type") == "application/javascript":
                    return script.get_text(strip=True)
                
                # 내용에서 Fusion.globalContent 패턴 찾기
                if "Fusion.globalContent" in script.get_text():
                    return script.get_text(strip=True)
                    
            return None
        except Exception as e:
            logging.error(f"Fusion 스크립트 찾기 오류: {e}")
            return None

    def _parse_fusion_script(self, script_text: str) -> Tuple[str, str]:
        """스크립트 파싱 및 데이터 추출"""
        if not script_text:
            return ("", "")
            
        try:
            match = re.search(r'Fusion\.globalContent\s*=\s*({.*?});', script_text, re.DOTALL)
            if not match:
                return ("", "")
            
            data = json.loads(match.group(1))
            content = self._extract_article_content(data)
            author = self._extract_raw_author(data)
            return (content, author)
        except (json.JSONDecodeError, re.error) as e:
            logging.error(f"Fusion 스크립트 파싱 오류: {e}")
            return ("", "")
        except Exception as e:
            logging.error(f"Fusion 데이터 추출 예상치 못한 오류: {e}")
            return ("", "")

    def _extract_article_content(self, data: dict) -> str:
        """본문 내용 추출"""
        try:
            body_parts = [
                el["content"].strip()
                for el in data.get("content_elements", [])
                if el.get("type") == "text" and "content" in el
            ]
            return " ".join(body_parts)
        except Exception as e:
            logging.error(f"기사 내용 추출 오류: {e}")
            return ""

    def _extract_raw_author(self, data: dict) -> str:
        """원시 작성자 정보 추출"""
        try:
            by_list = data.get("credits", {}).get("by", [])
            return by_list[0].get("name", "") if by_list else ""
        except Exception as e:
            logging.error(f"작성자 정보 추출 오류: {e}")
            return ""

    def _clean_author(self, author: str) -> str:
        """작성자 이름 정제 (기자, 괄호 내용 제거)"""
        try:
            if not author:
                return ""
            author = re.sub(r'\s*기자\s*\([^)]*\)', '', author)
            return super().clean_author(author).strip()
        except Exception as e:
            logging.error(f"작성자 정보 정제 오류: {e}")
            return author or ""

    def _clean_body(self, text: str) -> str:
        """본문 정제 (🌎 마커 이후 내용 제거)"""
        try:
            if not text:
                return ""
            return re.split(r'\s*-\s*🌎', text, maxsplit=1)[0].strip()
        except Exception as e:
            logging.error(f"본문 정제 오류: {e}")
            return text or ""
