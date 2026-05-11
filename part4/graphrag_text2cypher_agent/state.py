from typing import Optional
from typing_extensions import TypedDict


class InputState(TypedDict):
    query: str


class OutputState(TypedDict):
    answer: str


class State(TypedDict):
    query: Optional[str] = None  # 사용자 질문
    cypher: Optional[str] = None  # 생성/수정된 Cypher 쿼리
    db_outputs: Optional[list] = None  # DB 조회 결과
    retry_count: Optional[int] = 0  # 쿼리 재시도 횟수
    is_db_related: Optional[bool] = None  # DB 관련 질문인지 여부
    validation_error: Optional[str] = None  # 쿼리 검증 오류
    result_empty: Optional[bool] = None  # 조회 결과가 비어있는지
    regenerate_count: Optional[int] = 0  # 빈 결과로 인한 재생성 횟수
    answer: Optional[str] = None  # 최종 답변
