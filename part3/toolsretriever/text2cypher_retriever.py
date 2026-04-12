from neo4j_graphrag.retrievers import Text2CypherRetriever
from schema import fetch_schema_from_neo4j, schema_to_text


def get_text2cypher_examples():
    """Text2Cypher를 위한 예시 질의-쿼리 쌍"""
    examples = [
        # 문서 구조 관련 쿼리
        "USER INPUT: 'AI 기본법에 대한 내용을 찾아줘' QUERY: MATCH (toc:TOC) WHERE toc.title CONTAINS 'AI 기본법' RETURN toc.title AS 제목, toc.page_start AS 페이지, toc.level AS 레벨",
        "USER INPUT: '목차 구조를 보여줘' QUERY: MATCH (toc:TOC) RETURN toc.title AS 목차_제목, toc.page_start AS 시작_페이지, toc.level AS 레벨, toc.document AS 문서_제목 ORDER BY toc.page_start, toc.level",
        "USER INPUT: '6페이지에 있는 내용을 보여줘' QUERY: MATCH (t:TextElement) WHERE t.page = 6 RETURN t.content AS 내용, t.page AS 페이지",

        # 도메인 엔티티 검색 쿼리
        "USER INPUT: 'OpenAI에 대해 알려줘' QUERY: MATCH (c:Company) WHERE c.name CONTAINS 'OpenAI' RETURN c.name AS 회사명, c.description AS 설명",
        "USER INPUT: 'AI 관련 정책이나 법률을 찾아줘' QUERY: MATCH (n) WHERE n:Policy OR n:Law OR n:Regulation RETURN labels(n)[0] AS 유형, n.name AS 이름",

        # 엔티티 간 관계 쿼리 (핵심: 임원, 소속, 역할 등)
        "USER INPUT: '알리바바의 CEO는 누구인가요?' QUERY: MATCH (c:Company)-[:HAS_EXECUTIVE|EMPLOYS|AFFILIATED_WITH]-(p:Person) WHERE c.name CONTAINS '알리바바' AND (p.role CONTAINS 'CEO' OR p.title CONTAINS 'CEO' OR p.position CONTAINS 'CEO') RETURN c.name AS 회사, p.name AS 이름, coalesce(p.role, p.title, p.position) AS 직책",
        "USER INPUT: '업스테이지의 기술책임자는 누군가요?' QUERY: MATCH (c:Company)-[:HAS_EXECUTIVE|EMPLOYS|AFFILIATED_WITH]-(p:Person) WHERE c.name CONTAINS '업스테이지' AND (p.role CONTAINS '기술책임자' OR p.role CONTAINS 'CTO' OR p.title CONTAINS 'CTO') RETURN c.name AS 회사, p.name AS 이름, coalesce(p.role, p.title, p.position) AS 직책",
        "USER INPUT: 'LG AI연구원 소속 인물은 누구인가요?' QUERY: MATCH (org:Company|Organization)-[:EMPLOYS|HAS_MEMBER|AFFILIATED_WITH]-(p:Person) WHERE org.name CONTAINS 'LG AI연구원' RETURN org.name AS 조직, p.name AS 인물, coalesce(p.role, p.title, p.position, '소속') AS 역할",

        # 포괄적 관계 탐색
        "USER INPUT: 'OpenAI와 관련된 모든 정보를 찾아줘' QUERY: MATCH (n)-[r]-(related) WHERE n.name CONTAINS 'OpenAI' RETURN n.name AS 중심_엔티티, type(r) AS 관계, labels(related)[0] AS 연관_타입, coalesce(related.name, related.title) AS 연관_엔티티 LIMIT 50"
    ]
    return examples


def create_text2cypher_retriever(driver, llm):
    """Text2Cypher 검색 Retriever 생성 (구조화 질의)

    Neo4j에서 동적으로 스키마를 조회하여 Retriever 생성
    """
    schema_dict = fetch_schema_from_neo4j(driver)
    schema_text = schema_to_text(schema_dict)

    print("=== Neo4j에서 조회한 스키마 정보 ===")
    print(schema_text)
    print("===================================")
    retriever = Text2CypherRetriever(
        driver=driver,
        llm=llm,
        neo4j_schema=schema_text,
        examples=get_text2cypher_examples()
    )
    print("[4/4] Text2Cypher 검색 Retriever 생성 완료")
    return retriever


def create_text2cypher_tool(retriever):
    """Text2CypherRetriever를 Tool로 변환"""

    tool = retriever.convert_to_tool(
        name="text2cypher_structured_query",
        description="""
        그래프 데이터베이스의 구조화된 엔티티와 관계를 활용한 정확한 질의를 수행합니다.

        **주요 사용 시나리오**:

        1. 도메인 엔티티 검색:
           - 특정 회사, 조직, 인물, AI 모델, 정책, 법률 등을 찾을 때
           - 예: "OpenAI에 대해 알려줘", "EXAONE 모델 정보"

        2. 엔티티 간 관계 조회:
           - "X의 Y는?" 형식의 질문 (소속, 직책, 관계 등)
           - 회사의 임원, 조직의 소속 인물, 제품의 개발사 등

        3. 문서 구조 탐색:
           - 목차 구조, 특정 페이지 내용, 표 데이터
           - 예: "목차 구조는?", "10페이지 내용", "표로 정리된 내용"

        4. 목록과 통계:
           - 엔티티 개수, 목록, 정확한 데이터
           - 예: "AI 정책 목록", "회사 목록", "모델 개수"
        """
    )

    return tool
