from .base_parser import BaseParser
import re
import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class PressianParser(BaseParser):
    
    def get_content(self) -> str:
        """기사 본문 추출"""
        article_body = self.soup.find('div', class_='article_body')
        if not article_body:
            return ""

        # 본문 내용 수집
        paragraphs = [p.get_text(strip=True) for p in article_body.find_all('p') if p.get_text(strip=True)]
        
        if not paragraphs:
            return ""

        # 단락을 개행 문자로 연결
        content = '\n\n'.join(paragraphs)
        
        # 연속된 공백 및 개행 정리
        content = re.sub(r'\s+', ' ', content)
    
        return self.clean_text(content)