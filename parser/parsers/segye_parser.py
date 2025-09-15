from .base_parser import BaseParser
import re

class SegyeParser(BaseParser):
    
    def get_author(self) -> str:
        # article:author meta 태그를 우선 사용
        author = self.extract_meta("dable:author")
        if author:
            return self.clean_author(author)
        return ''
    
    def get_content(self) -> str:
        article = self.soup.select_one('article')
        if not article:
            return ""
        
        # 본문 내용 수집
        paragraphs = []
        for p in article.select('p'):
            text = p.get_text(strip=True)
            if text:  # 빈 문자열 제외
                paragraphs.append(text)
        
        # 단락을 개행 문자로 연결
        paragraphs = paragraphs[:-1]
        content = '\n\n'.join(paragraphs)
        
        # 연속된 공백 및 개행 정리
        content = re.sub(r'\s+', ' ', content)
        
        return self.clean_text(content)