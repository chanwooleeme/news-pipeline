from .base_parser import BaseParser

class LabortodayParser(BaseParser):

    def get_author(self) -> str:
        # 기자 이름 추출
        author = self.extract_meta("og:article:author")
        return self.clean_author(author)
    

    def get_content(self) -> str:
        """매일노동뉴스 기사 본문 파싱"""
        # 기사 본문이 포함된 article 요소 찾기
        article = self.soup.select_one('article#article-view-content-div')
        if not article:
            return ""
        
        # 불필요한 요소 제거
        # 이미지와 캡션 제거
        for figure in article.select('figure'):
            figure.decompose()
        
        # div 요소 제거 (이미지 컨테이너 등)
        for div in article.select('div'):
            div.decompose()
        
        # 단락 수집
        paragraphs = []
        for p in article.select('p'):
            # 공백만 있는 단락 제외
            text = p.get_text(strip=True)
            if text:
                paragraphs.append(text)
        
        # 단락이 없으면 전체 텍스트 반환
        if not paragraphs:
            return self.clean_text(article.get_text())
        
        # 단락을 개행 문자로 연결
        content = '\n\n'.join(paragraphs)
        
        # 특수 문자 정제
        import re
        # &ldquo; &rdquo; 등의 특수 HTML 엔티티 처리
        content = re.sub(r'&ldquo;', '"', content)
        content = re.sub(r'&rdquo;', '"', content)
        content = re.sub(r'&nbsp;', ' ', content)
        
        # 연속된 공백 제거
        content = re.sub(r'\s+', ' ', content)
        
        return self.clean_text(content)