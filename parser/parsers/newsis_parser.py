from .base_parser import BaseParser
import re

class NewsisParser(BaseParser):
    
    def get_author(self) -> str:
        # article:author meta 태그를 우선 사용
        description = self.extract_meta("og:description")
        match = re.search(r'\[.*?\]\s*([\S]+ 기자)', description)
        if match:
            author = match.group(1)
            return self.clean_author(author)
        return ''
    
    def get_content(self) -> str:
        sentences = []
        # 전체 HTML 내의 모든 <br /> 태그를 순회합니다.
        for br in self.soup.find_all("br"):
            next_node = br.next_sibling
            # 다음 노드가 존재하고 문자열인 경우 (공백 제거 후 값이 있으면)
            if next_node:
                # 만약 next_node가 NavigableString 혹은 태그여도, 문자열로 변환 후 strip합니다.
                text = str(next_node).strip()
                if text == "◎공감언론 뉴시스":
                    break
                if text:
                    sentences.append(text)
        return self.cleanse_content(" ".join(sentences))
    

    def cleanse_content(self, content: str) -> str:
        pattern = r'^\s*\"?\s*\[[^\]]+\]\s*[가-힣\s]+기자\s*=\s*'
    
        # 해당 패턴을 찾아 제거
        cleaned_text = re.sub(pattern, "", content)
    
        # 혹시 남아있을 수 있는 선행 따옴표 제거 (양쪽 공백과 함께)
        cleaned_text = cleaned_text.lstrip('\"').strip()
    
        return cleaned_text