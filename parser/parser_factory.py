# parser/parser_factory.py
from typing import Dict, Type, Any
from parsers import (
    BaseParser,
    ChosunParser,
    NewsisParser,
    KhanParser,
    PressianParser,
    WomennewsParser,
    AblenewsParser,
    SisajournalParser,
    SegyeParser,
    SeoulParser,
    MediatodayParser,
    DongaParser,
    LabortodayParser,
    NewscjParser,
)


class ParserFactory:
    """신문사별 HTML 파서 팩토리"""
    
    def __init__(self):
        """파서 팩토리 초기화 - 신문사 이름과 파서 클래스 매핑"""
        self.parsers: Dict[str, Type[BaseParser]] = {
            '조선일보': ChosunParser,
            '뉴시스': NewsisParser,
            '경향신문': KhanParser,
            '프레시안': PressianParser,
            '여성신문': WomennewsParser,
            '에이블뉴스': AblenewsParser,
            '시사저널': SisajournalParser,
            '세계일보': SegyeParser,
            '서울신문': SeoulParser,
            '미디어오늘': MediatodayParser,
            '동아일보': DongaParser,
            '매일노동뉴스': LabortodayParser,
            '천지일보': NewscjParser,
        }
        
        print(f"[ParserFactory] {len(self.parsers)}개 파서 로드됨")
    
    def parse(self, html: str, newspaper: str) -> Dict[str, Any]:
        """
        신문사 이름으로 파서를 선택하고 기사 정보를 파싱하여 반환
        
        Args:
            html: 파싱할 HTML 문자열
            newspaper: 신문사 이름
            
        Returns:
            파싱된 기사 정보 딕셔너리
            {
                'title': 기사 제목,
                'author': 작성자,
                'category': 카테고리,
                'publication_date': 발행일 (타임스탬프),
                'content': 본문 내용
            }
            
        Raises:
            ValueError: 지원하지 않는 신문사인 경우
        """
        if newspaper not in self.parsers:
            available = ', '.join(self.parsers.keys())
            raise ValueError(
                f"신문사 '{newspaper}'에 대한 파서가 없습니다. "
                f"지원되는 신문사: {available}"
            )
        
        parser_class = self.parsers[newspaper]
        
        try:
            parser = parser_class(html)
            result = parser.parse()
            return result
        except Exception as e:
            print(f"[ParserFactory] 파싱 오류 ({newspaper}): {e}")
            raise
    
    def get_supported_newspapers(self) -> list:
        """지원되는 신문사 목록 반환"""
        return list(self.parsers.keys())