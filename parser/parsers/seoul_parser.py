from .base_parser import BaseParser
from lxml import html
import re

class SeoulParser(BaseParser):

    def get_author(self) -> str:
        author = self.soup.find("div", "byline")
        if author:
            return self.clean_author(self.clean_author(author.text.strip()))
        else:
            return ""

    def get_title(self) -> str:
        """서울신문 제목 파싱"""
        # 기사 제목 요소 찾기
        title_elem = self.soup.select_one('h1.headline')
        if title_elem:
            return self.clean_text(title_elem.text)
        
        # 다른 가능한 제목 요소
        title_elem = self.soup.select_one('meta[property="og:title"]')
        if title_elem and title_elem.get('content'):
            return self.clean_text(title_elem.get('content'))
        
        return ""

    def get_content(self) -> str:
        # "articleContent" 아래 첫 번째 div 컨테이너 선택
        doc = html.fromstring(self.html)
        containers = doc.xpath('//*[@id="articleContent"]/div')
        if not containers:
            return ""
        container = containers[0]
        # container의 직계 text() 노드 전체를 가져옵니다.
        text_nodes = container.xpath("text()")
        total_nodes = len(text_nodes)
        print("총 text() 노드 개수:", total_nodes)
        
        # 각 text 노드의 앞뒤 공백을 제거하고, 빈 문자열은 제외한 후, 모두 공백(" ")으로 결합합니다.
        sentences = [node.strip() for node in text_nodes if node.strip()]
        # U+FEFF 문자를 제거한 후 반환합니다.
        return " ".join(sentences).replace("\ufeff", "")
    
    def clean_author(self, author_text: str) -> str:
        # 공백 기준으로 분리 (예: ["서울", "홍길동", ...])
        tokens = author_text.split(" ")
        if len(tokens) > 1:
            # 두번째 토큰을 후보로 사용 (예: "홍길동" 또는 "홍길동.ㅇㅇ")
            candidate = tokens[1]
            # 특수문자가 나오기 전까지의 연속된 문자(영문, 한글, 숫자, 밑줄)를 추출합니다.
            m = re.match(r'([\w가-힣]+)', candidate)
            if m:
                return m.group(1)
            else:
                return candidate
        return author_text
