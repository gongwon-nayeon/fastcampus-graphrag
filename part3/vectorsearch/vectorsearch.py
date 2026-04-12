import os
import ast
from dotenv import load_dotenv
from neo4j import GraphDatabase
from neo4j_graphrag.retrievers import VectorRetriever
from neo4j_graphrag.embeddings import OpenAIEmbeddings

load_dotenv()


# ============================================
# Neo4j 연결 관리 & Embedder 설정
# ============================================

def create_neo4j_driver():
    """Neo4j 드라이버 생성 및 연결 확인"""
    uri = os.getenv("NEO4J_URI")
    username = os.getenv("NEO4J_USERNAME")
    password = os.getenv("NEO4J_PASSWORD")

    if not all([uri, username, password]):
        raise ValueError("NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD 환경 변수를 설정해주세요.")

    driver = GraphDatabase.driver(uri, auth=(username, password))

    try:
        driver.verify_connectivity()
        print(f"Neo4j 연결 성공: {uri}")
    except Exception as e:
        print(f"Neo4j 연결 실패: {e}")
        raise

    return driver


def close_neo4j_driver(driver):
    """Neo4j 드라이버 종료"""
    if driver:
        driver.close()

def create_embedder(model: str = "text-embedding-3-small"):
    """
    OpenAI Embedder 생성

    Args:
        model: OpenAI 임베딩 모델명

    Returns:
        OpenAIEmbeddings 인스턴스
    """
    api_key = os.getenv("OPENAI_API_KEY")

    if not api_key:
        raise ValueError("OPENAI_API_KEY 환경 변수를 설정해주세요.")

    return OpenAIEmbeddings(api_key=api_key, model=model)


# ============================================
# 벡터 검색 함수들
# ============================================

def vector_search(
    driver,
    query_text: str,
    top_k: int = 5,
    index_name: str = "textElementEmbedding",
    embedding_model: str = "text-embedding-3-small",
    return_properties: list = None
):
    """
    기본 벡터 검색 수행 (VectorRetriever 사용)

    전제조건: TextElement 노드에 임베딩이 저장되어 있고, 벡터 인덱스가 생성되어 있어야 함

    Args:
        driver: Neo4j 드라이버
        query_text: 검색 쿼리
        top_k: 반환할 결과 수
        index_name: 벡터 인덱스 이름 (기본값: "textElementEmbedding")
        embedding_model: 쿼리 임베딩 생성에 사용할 모델
        return_properties: 반환할 노드 속성 리스트

    Returns:
        검색 결과 리스트 (각 결과는 RetrieverResultItem 객체)
    """
    if return_properties is None:
        return_properties = ["element_id", "content", "page"]

    print(f"\n{'=' * 60}")
    print(f"🔍 벡터 검색: {query_text}")
    print(f"{'=' * 60}")

    # Embedder 생성
    embedder = create_embedder(model=embedding_model)

    # VectorRetriever 생성
    retriever = VectorRetriever(
        driver=driver,
        index_name=index_name,
        embedder=embedder,
        return_properties=return_properties
    )

    try:
        results = retriever.search(query_text=query_text, top_k=top_k)
        return results

    except Exception as e:
        print(f"검색 실패: {e}")
        raise

# ============================================
# 결과 출력 함수
# ============================================

def print_search_results(results):
    """
    검색 결과를 보기 좋게 출력

    Args:
        results: VectorRetriever의 RetrieverResult 객체 또는 dict 리스트
        max_content_length: 출력할 최대 내용 길이
        debug: 디버깅 정보 출력 여부
    """
    print("\n" + "=" * 80)
    print("검색 결과")
    print("=" * 80)

    items = results.items if hasattr(results, 'items') else results

    for i, item in enumerate(items, 1):
        metadata = item.metadata or {}
        score = metadata.get("score", 0.0)

        if isinstance(item.content, str) and item.content.startswith('{'):
            # 문자열을 dict로 파싱
            parsed_content = ast.literal_eval(item.content)
            if isinstance(parsed_content, dict):
                content = parsed_content.get("content", "")
                page = parsed_content.get("page")
            else:
                content = str(item.content)
                page = None

        print(f"\n[{i}] 유사도: {score:.4f}")

        if page is not None:
            print(f"    페이지: {page}")

        if content:
            display_content = str(content)
            print(f"    내용: {display_content}")

    print("\n" + "=" * 80)


# ============================================
# 메인 함수
# ============================================

def main():
    import sys

    print("\n" + "=" * 60)
    print("PDF 지식그래프 Vector Search 시스템")
    print("=" * 60)

    query = sys.argv[1]
    TOP_K = 5

    # Neo4j 드라이버 생성
    driver = create_neo4j_driver()

    try:
        results = vector_search(driver, query, top_k=TOP_K)
        print_search_results(results)

    finally:
        close_neo4j_driver(driver)


if __name__ == "__main__":
    main()
