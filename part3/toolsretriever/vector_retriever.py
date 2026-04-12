from neo4j_graphrag.retrievers import VectorRetriever


def create_text_vector_retriever(driver, embedder, index_name):
    """TextElement 벡터 검색 Retriever 생성"""
    retriever = VectorRetriever(
        driver=driver,
        index_name=index_name,
        embedder=embedder,
        return_properties=["content", "page", "toc_id"]
    )
    print("[1/4] TextElement 벡터 검색 Retriever 생성")
    return retriever


def create_table_vector_retriever(driver, embedder, index_name):
    """TableElement 벡터 검색 Retriever 생성"""
    retriever = VectorRetriever(
        driver=driver,
        index_name=index_name,
        embedder=embedder,
        return_properties=["content", "page", "toc_id"]
    )
    print("[2/4] TableElement 벡터 검색 Retriever 생성")
    return retriever


def create_text_vector_tool(text_retriever):
    """TextVectorRetriever를 Tool로 변환"""

    tool = text_retriever.convert_to_tool(
        name="text_vector_search",
        description="""
        PDF 문서 내 텍스트 단락을 의미 기반으로 검색합니다.

        사용 시나리오:
        - 특정 주제나 개념에 대한 요약 등을 찾을 때
        """
    )
    return tool


def create_table_vector_tool(table_retriever):
    """TableVectorRetriever를 Tool로 변환"""

    tool = table_retriever.convert_to_tool(
        name="table_vector_search",
        description="""
        PDF 문서 내 표(테이블) 데이터를 의미 기반으로 검색합니다.

        사용 시나리오:
        - 표로 정리된 정보를 찾을 때
        - 비교 데이터, 통계, 목록 등을 찾을 때
        - "~를 비교한 표", "~통계", "~목록" 같은 질문

        예: "모델 성능 비교 표", "주요 일정 목록"
        """
    )
    return tool
