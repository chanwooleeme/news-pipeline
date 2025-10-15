from .base_parser import BaseParser
import re
import logging

class KhanParser(BaseParser):
    
    def get_author(self) -> str:
        """작성자 정보 추출"""
        try:
            # CSS 선택자로 작성자 정보 찾기
            author_elem = self.soup.select_one(".article_byline")
            if author_elem:
                return self.clean_author(author_elem.get_text(strip=True))
                
            # 메타 태그에서 작성자 찾기
            author = self.extract_meta("article:author") or self.extract_meta("dable:author")
            if author:
                return self.clean_author(author)
                
            return ""
        except Exception as e:
            logging.error(f"작성자 추출 중 오류 발생: {e}")
            return ""
    
    def get_content(self) -> str:
        """본문 내용 추출"""
        try:
            # CSS 선택자로 기사 본문 찾기
            article_body = self.soup.select_one("#articleBody, .art_body, .article_content")
            if article_body:
                paragraphs = article_body.select("p")
                if paragraphs:
                    return " ".join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))
            
            # 클래스 속성에 "content"가 포함된 모든 <p> 태그 찾기
            p_tags = self.soup.select("p[class*=content]")
            if p_tags:
                # 각 태그의 텍스트 추출 후, 줄바꿈 문자로 결합
                all_content = " ".join(p.get_text(strip=True) for p in p_tags if p.get_text(strip=True))
                return all_content
                
            # 대안: 정규식으로 클래스 찾기
            p_tags = self.soup.find_all("p", class_=re.compile("content"))
            if p_tags:
                all_content = " ".join(p.get_text(strip=True) for p in p_tags if p.get_text(strip=True))
                return all_content
                
            # 대안: 일반적인 본문 위치
            all_p_tags = self.soup.select("p")
            if all_p_tags:
                return " ".join(p.get_text(strip=True) for p in all_p_tags if p.get_text(strip=True))
                
            return ""
        except Exception as e:
            logging.error(f"본문 추출 중 오류 발생: {e}")
            return ""