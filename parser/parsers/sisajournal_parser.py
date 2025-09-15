from .base_parser import BaseParser
import re

class SisajournalParser(BaseParser):
    def get_title(self) -> str:
        title = super().get_title()
        title = title.replace(" - 시사저널", "").strip()
        return title
    
    def get_author(self) -> str:
        # article:author meta 태그를 우선 사용
        author = self.extract_meta("dable:author")
        if author:
            return self.clean_author(author)
        return ''
    
    def clean_author(self, author: str) -> str:
        return author.split(' ')[0]
    
    def get_content(self) -> str:
        """매일노동뉴스 기사 본문 파싱"""
        # 기사 본문이 포함된 article 요소 찾기
        article = self.soup.select_one('article#article-view-content-div')
        if not article:
            return ""
        
        # 본문 내용 수집
        paragraphs = []
        for p in article.select('p'):
            text = p.get_text(strip=True)
            if text:  # 빈 문자열 제외
                paragraphs.append(text)
        
        # 단락을 개행 문자로 연결
        content = '\n\n'.join(paragraphs)
        
        # 이상한 문자 제거 (예: HTML 엔티티)
        content = re.sub(r'&[a-zA-Z0-9#]+;', '', content)  # HTML 엔티티 제거
        content = re.sub(r'\s+', ' ', content)  # 연속된 공백 제거
        content = content.strip()  # 앞뒤 공백 제거

        return content
    