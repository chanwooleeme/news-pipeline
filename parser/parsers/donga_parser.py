from .base_parser import BaseParser
import re

class DongaParser(BaseParser):
    
    def get_author(self) -> str:
        # 기자 이름 추출
        author = self.extract_meta("dd:author")
        return self.clean_author(author)
    
    def get_content(self) -> str:
        """동아일보 기사 본문 파싱"""
        # 뉴스 섹션 찾기
        news_section = self.soup.select_one('section.news_view')
        if not news_section:
            return ""
        
        # 불필요한 요소 제거
        # 광고 관련 div 제거
        for ad in news_section.select('div[class*="ad"], div[class*="view_m_ad"], div[id^="div-gpt-ad"]'):
            ad.decompose()
        
        # 이미지 캡션 제거
        for fig in news_section.select('figure.img_cont, figcaption'):
            fig.decompose()
        
        # 스크립트 제거
        for script in news_section.select('script'):
            script.decompose()
        
        # 본문 텍스트 추출
        content_text = news_section.get_text(strip=True)
        
        # 정제: 연속된 개행 문자 처리
        content_text = re.sub(r'\n+', '\n', content_text)
        
        return self.clean_text(content_text)