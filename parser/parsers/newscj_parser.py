from .base_parser import BaseParser
import re
import logging

class NewscjParser(BaseParser):
    
    def get_author(self) -> str:
        """작성자 추출"""
        try:
            # CSS 선택자를 사용하여 작성자 정보 찾기
            author = self.extract_meta("dable:author")
            if author:
                return self.clean_author(author)
                
            # 대안: HTML에서 작성자 정보 찾기
            author_elem = self.soup.select_one(".report")
            if author_elem:
                return self.clean_author(author_elem.get_text(strip=True))
                
            return ''
        except Exception as e:
            logging.error(f"작성자 추출 중 오류 발생: {e}")
            return ''
    
    def get_content(self) -> str:
        """기사 본문 추출"""
        try:
            # CSS 선택자로 본문 컨테이너 찾기
            content_container = self.soup.select_one("div.article-body")
            
            if content_container:
                # 컨테이너 내의 모든 단락 추출
                paragraphs = content_container.select("p")
                if paragraphs:
                    # 본문 내용 수집
                    content = '\n\n'.join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))
                    return self.clean_article_text(self.clean_text(content))
            
            # 대안: 모든 <p> 태그 선택
            paragraphs = self.soup.select('p')
            
            if not paragraphs:
                logging.warning("본문 단락을 찾을 수 없습니다")
                return ""

            # 본문 내용 수집
            content = '\n\n'.join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))
            
            # 연속된 공백 및 개행 정리
            content = re.sub(r'\s+', ' ', content)
            
            return self.clean_article_text(self.clean_text(content))
        except Exception as e:
            logging.error(f"본문 추출 중 오류 발생: {e}")
            return ""
        
    def clean_article_text(self, text: str) -> str:
        pattern = r"^\s*\[[^\]]+\]\s*"
        cleaned_text = re.sub(pattern, "", text)
        return cleaned_text.strip()