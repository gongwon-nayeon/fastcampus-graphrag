from typing import Literal

from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field

from state import State
from schema import NEO4J_DATABASE, get_neo4j_driver, get_cached_schema
from prompts import (
    FEWSHOT_EXAMPLES_STR,
    GENERATE_CYPHER_PROMPTS,
    CORRECT_CYPHER_PROMPTS,
    FINAL_ANSWER_PROMPTS,
)


# LLM 초기화
llm = ChatOpenAI(model="gpt-4o")

# Neo4j 드라이버 및 스키마 초기화
driver = get_neo4j_driver()
schema_str = get_cached_schema()

# 데이터베이스 이름
database = NEO4J_DATABASE


# ============================================
# Pydantic 모델 정의
# ============================================

class GuardrailGrade(BaseModel):
    """Binary score for database relevance check."""
    binary_score: str = Field(description="Relevance score 'yes' or 'no'")


# ============================================
# 노드 함수들
# ============================================

def guardrail_node(state: State):
    """질문이 의료 지식 그래프 DB와 관련이 있는지 확인"""
    print("\n###### GUARDRAIL CHECK ######")

    user_question = state["query"]
    print(f"사용자 질문: {user_question}")

    grader = llm.with_structured_output(GuardrailGrade)

    guardrail_prompt = ChatPromptTemplate.from_messages([
        ("system", """
당신은 질문이 의료 지식 그래프 데이터베이스와 관련이 있는지 판단하는 AI입니다.

의료 지식 그래프는 다음 정보를 포함합니다:
- 증상(Symptom): 발열, 기침, 두통 등
- 질병(Disease): 폐렴, 당뇨병, 고혈압 등
- 검사(Test): 혈액 검사, X-ray, MRI 등
- 치료법(Treatment): 물리치료, 수술, 방사선 치료 등
- 약물(Medication): 아스피린, 인슐린, 항생제 등
- 신체 부위(Anatomy): 폐, 심장, 뇌 등

질문이 위 카테고리 중 하나와 관련이 있으면 'yes'를, 그렇지 않으면 'no'를 반환하세요.
예시:
- '기침 증상과 관련된 질병은?' → yes
- '오늘 날씨는?' → no
- '파이썬 코드를 작성해줘' → no
- '두통에 좋은 약은?' → yes
        """),
        ("user", "{question}")
    ])

    # 체인 구성 및 실행
    chain = guardrail_prompt | grader
    scored_result = chain.invoke({"question": user_question})
    score = scored_result.binary_score

    is_related = score.lower() == "yes"
    print(f"DB 관련 질문 여부: {is_related} (score: {score})")

    return {
        "is_db_related": is_related
    }


def generate_cypher_node(state: State):
    """Cypher 쿼리 생성"""
    print("\n###### GENERATE CYPHER ######")

    # 빈 결과로 인한 재생성인지 확인
    result_empty = state.get("result_empty", False)
    regenerate_count = state.get("regenerate_count", 0)

    if result_empty:
        regenerate_count += 1
        print(f"빈 결과로 인한 쿼리 재생성 ({regenerate_count}회차)")

    text2cypher_prompt = ChatPromptTemplate.from_messages(GENERATE_CYPHER_PROMPTS)

    response = llm.invoke(
        text2cypher_prompt.format_messages(
            question=state["query"],
            schema=schema_str,
            fewshot_examples=FEWSHOT_EXAMPLES_STR,
        )
    )

    print(f"생성된 쿼리: {response.content}")

    return {
        "cypher": response.content,
        "regenerate_count": regenerate_count,
        "result_empty": False,  # 리셋
    }


def validate_query_node(state: State):
    """Cypher 쿼리 검증"""
    print("\n###### VALIDATE QUERY ######")

    cypher_query = state.get("cypher")
    if not cypher_query:
        return {"validation_error": "쿼리를 찾을 수 없습니다"}

    print(f"검증할 쿼리: {cypher_query}")

    # 1. 위험한 쿼리 키워드 체크
    dangerous_keywords = ["DELETE", "DROP", "CREATE", "MERGE", "REMOVE", "SET", "DETACH"]
    cypher_upper = cypher_query.upper()

    for keyword in dangerous_keywords:
        if keyword in cypher_upper:
            error_msg = f"위험한 쿼리 감지: {keyword} 키워드는 사용할 수 없습니다"
            print(f"✗ 검증 실패: {error_msg}")
            return {
                "validation_error": error_msg
            }

    # 2. 기본 문법 검증
    if not cypher_upper.strip().startswith("MATCH"):
        error_msg = "쿼리는 MATCH로 시작해야 합니다"
        print(f"✗ 검증 실패: {error_msg}")
        return {
            "validation_error": error_msg
        }

    if "RETURN" not in cypher_upper:
        error_msg = "쿼리에 RETURN 절이 필요합니다"
        print(f"✗ 검증 실패: {error_msg}")
        return {
            "validation_error": error_msg
        }

    print("✓ 검증 통과")
    return {
        "validation_error": ""
    }


def execute_cypher_node(state: State):
    """Cypher 쿼리 실행"""
    print("\n###### EXECUTE CYPHER ######")

    cypher_query = state.get("cypher")
    if not cypher_query:
        raise ValueError("No cypher query found in state")

    print(f"실행 쿼리: {cypher_query}")

    try:
        result = driver.execute_query(cypher_query, database_=database)

        # result.records를 리스트로 변환
        database_output = [record.data() for record in result.records]

        print(f"조회 결과: {database_output}")

        # 빈 결과 플래그 설정
        result_empty = len(database_output) == 0

    except Exception as e:
        database_output = str(e)
        result_empty = False
        print(f"조회 실패: {database_output}")

    return {
        "db_outputs": [database_output],
        "result_empty": result_empty,
    }


def correct_cypher_node(state: State):
    """Cypher 쿼리 수정"""
    print("\n###### CORRECT CYPHER ######")

    if messages := state.get("db_outputs", []):
        db_result = messages[-1]
    else:
        raise ValueError("No DB result found")

    cypher = state.get("cypher")
    if not cypher:
        raise ValueError("No Cypher found")

    print(f"수정 전 쿼리: {cypher}")
    print(f"에러 내용: {db_result}")

    correct_cypher_prompt = ChatPromptTemplate.from_messages(CORRECT_CYPHER_PROMPTS)

    response = llm.invoke(
        correct_cypher_prompt.format_messages(
            question=state["query"],
            schema=schema_str,
            cypher=cypher,
            errors=db_result,
        )
    )

    print(f"수정 후 쿼리: {response.content}")

    return {
        "cypher": response.content,
        "retry_count": state.get("retry_count", 0) + 1
    }


def answer_node(state: State):
    """최종 답변 생성"""
    print("\n###### ANSWER GENERATION ######")

    is_db_related = state.get("is_db_related", True)

    # DB 관련 질문이 아닌 경우 직접 답변
    if not is_db_related:
        print("DB 관련 질문이 아니므로 일반 답변 생성")

        general_answer_prompt = ChatPromptTemplate.from_messages([
            ("system", """당신은 친절한 AI 어시스턴트입니다.
사용자의 질문이 의료 지식 그래프 데이터베이스와 관련이 없으므로,
데이터베이스를 사용하지 않고 일반적인 답변을 제공해주세요.
반드시 한국어로 답변하세요."""),
            ("user", "{question}")
        ])

        response = llm.invoke(
            general_answer_prompt.format_messages(
                question=state["query"]
            )
        )

        print(f"일반 답변: {response.content}\n")

        return {
            "answer": response.content
        }

    # DB 관련 질문인 경우 - DB 결과 기반 답변 (정상 결과 또는 에러 모두 처리)
    print("DB 결과 기반 답변 생성")

    if messages := state.get("db_outputs", []):
        db_result = messages[-1]
    else:
        raise ValueError("No DB result found")

    cypher = state.get("cypher")
    if not cypher:
        raise ValueError("No Cypher found")

    is_error = isinstance(db_result, str)

    if is_error:
        print(f"에러 기반 답변 생성: {db_result[:100]}...")
    else:
        print(f"정상 결과 기반 답변 생성: {len(db_result)}개 레코드")

    final_answer_prompt = ChatPromptTemplate.from_messages(FINAL_ANSWER_PROMPTS)

    response = llm.invoke(
        final_answer_prompt.format_messages(
            cypher_query=cypher,
            context=db_result,
            question=state["query"],
        )
    )

    print(f"최종 답변: {response.content}\n")

    return {
        "answer": response.content
    }


# ============================================
# 라우팅 함수들(조건 함수)
# ============================================

def route_guardrail(state: State) -> Literal["generate_cypher", "answer"]:
    """
    질문이 DB 관련 질문인지 확인하고 라우팅
    """
    is_db_related = state.get("is_db_related", True)

    print("\n###### ROUTE GUARDRAIL ######")
    print(f"DB 관련 질문: {is_db_related}")

    if is_db_related:
        print("→ Cypher 쿼리 생성으로 이동")
        return "generate_cypher"
    else:
        print("→ DB 관련 질문 아님 - 직접 답변")
        return "answer"


def route_validation(state: State) -> Literal["execute_cypher", "correct_cypher", "answer"]:
    """
    쿼리 검증 결과에 따라 라우팅
    """
    validation_error = state.get("validation_error", "")
    retry_count = state.get("retry_count", 0)

    print("\n###### ROUTE VALIDATION ######")
    print(f"재시도 횟수: {retry_count}/5")

    # 재시도 횟수가 5를 초과하면 답변 생성으로 이동
    if retry_count > 5:
        print(f"✗ 최대 재시도 횟수(5회) 초과 - 답변 생성으로 이동")
        return "answer"

    if not validation_error:
        print("✓ 검증 통과 - 쿼리 실행으로 이동")
        return "execute_cypher"
    else:
        print(f"✗ 검증 실패: {validation_error} - 쿼리 수정으로 이동")
        return "correct_cypher"


def route_execution(state: State, max_regenerate: int = 2) -> Literal["generate_cypher", "correct_cypher", "answer"]:
    """
    쿼리 실행 결과에 따라 라우팅
    - 빈 결과: 재생성 (최대 2회)
    - 에러: 수정 (기존 로직)
    - 정상: 답변 생성
    """
    if db_outputs := state.get("db_outputs", []):
        db_result = db_outputs[-1]
    else:
        raise ValueError("No DB result found")

    regenerate_count = state.get("regenerate_count", 0)
    retry_count = state.get("retry_count", 0)

    print("\n###### ROUTE EXECUTION ######")
    print(f"재생성 횟수: {regenerate_count}/{max_regenerate}")
    print(f"재시도 횟수: {retry_count}/5")

    # 재시도 횟수가 5를 초과하면 답변 생성으로 이동
    if retry_count > 5:
        print(f"✗ 최대 재시도 횟수(5회) 초과 - 답변 생성으로 이동")
        return "answer"

    # 정상 조회 (결과가 있음)
    if type(db_result) == list and len(db_result) > 0:
        print("✓ 정상 조회 완료!")
        return "answer"

    # 빈 결과 처리
    if type(db_result) == list and len(db_result) == 0:
        if regenerate_count < max_regenerate:
            print(f"✗ 빈 결과 - 쿼리 재생성 ({regenerate_count + 1}/{max_regenerate})")
            return "generate_cypher"
        else:
            print(f"✗ 최대 재생성 횟수({max_regenerate}회) 초과 - 답변 생성으로 이동")
            return "answer"

    # 에러 발생
    print("✗ 쿼리 실행 오류 - 쿼리 수정으로 이동")
    return "correct_cypher"
