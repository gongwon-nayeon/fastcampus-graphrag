import os
from dotenv import load_dotenv
from neo4j import GraphDatabase
from neo4j_graphrag.retrievers import Text2CypherRetriever
from neo4j_graphrag.llm import OpenAILLM

load_dotenv()


def create_driver():
    uri = os.getenv("NEO4J_URI")
    username = os.getenv("NEO4J_USERNAME")
    password = os.getenv("NEO4J_PASSWORD")
    driver = GraphDatabase.driver(uri, auth=(username, password))
    driver.verify_connectivity()
    return driver


def create_llm():
    return OpenAILLM(
        model_name="gpt-4o",
        model_params={"temperature": 0},
    )


def build_retriever(driver, llm, neo4j_schema: str, examples: list[str] | None = None):
    """Text2CypherRetriever 인스턴스를 생성합니다.

    Args:
        driver: Neo4j 드라이버
        llm: LLM 인스턴스
        neo4j_schema: 스키마 텍스트
        examples: few-shot 예시 목록 (선택)
    """
    return Text2CypherRetriever(
        driver=driver,
        llm=llm,
        neo4j_schema=neo4j_schema,
        examples=examples,
    )


def generate_and_execute_cypher(
    retriever: Text2CypherRetriever, question: str
) -> tuple[str, list[dict], str | None]:
    """질문으로부터 Cypher를 생성하고 실행합니다.

    retriever.get_search_results()를 사용하여 Cypher 생성 + 실행을 한번에 처리하고,
    metadata에서 생성된 Cypher를 추출합니다.

    Returns:
        (generated_cypher, results, error): 생성된 Cypher, 결과 리스트, 에러 문자열
    """
    try:
        search_result = retriever.get_search_results(query_text=question)
        generated_cypher = search_result.metadata.get("cypher", "")
        results = [dict(record) for record in search_result.records]
        return generated_cypher, results, None
    except Exception as e:
        return "", [], str(e)


def execute_cypher(driver, cypher: str, database: str = "neo4j") -> tuple[list[dict], str | None]:
    """Cypher 쿼리를 실행하고 결과와 에러를 반환합니다.

    Returns:
        (results, error): 결과 리스트와 에러 문자열 (없으면 None)
    """
    try:
        records, _, _ = driver.execute_query(
            cypher,
            database_=database,
        )
        results = [dict(record) for record in records]
        return results, None
    except Exception as e:
        return [], str(e)
