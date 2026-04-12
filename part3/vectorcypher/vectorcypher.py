import os
from dotenv import load_dotenv
from neo4j import GraphDatabase
from neo4j_graphrag.retrievers import VectorCypherRetriever
from neo4j_graphrag.embeddings import OpenAIEmbeddings
from neo4j_graphrag.llm.openai_llm import OpenAILLM
from neo4j_graphrag.generation import RagTemplate, GraphRAG

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
# VectorCypher Retriever 생성
# ============================================

def create_legal_vectorcypher_retriever(
    driver,
    index_name: str = "question_embedding_index",
    embedding_model: str = "text-embedding-3-small"
):
    """
    법령 해석례 질의를 벡터 검색하고, 연결된 답변, 이유, 법조항 구조를 함께 가져오는
    VectorCypher Retriever 생성

    그래프 구조:
    (LegalInterpretation)
        ├── HAS_QUESTION → (Question) [벡터 검색 대상]
        │                      └── ANSWERED_BY → (Answer)
        │                                           └── SUPPORTED_BY → (Reason)
        ├── HAS_ANSWER → (Answer)
        ├── INTERPRETS → (Law) [주 법령]
        └── CITES → (Law/Article/Paragraph/Item) [인용 조항]

    Args:
        driver: Neo4j 드라이버
        index_name: 벡터 인덱스 이름 (기본값: "question_embedding_index")
        embedding_model: 쿼리 임베딩 생성에 사용할 모델

    Returns:
        VectorCypherRetriever 인스턴스
    """
    # Embedder 생성
    embedder = create_embedder(model=embedding_model)

    # VectorCypher 검색 쿼리 정의
    # node: 벡터 검색으로 찾은 Question 노드
    retrieval_query = """
    // 질의(Question) 노드를 기준으로 해석례 정보 수집
    MATCH (node)<-[:HAS_QUESTION]-(interp:LegalInterpretation)

    // 회답(Answer) 가져오기
    OPTIONAL MATCH (node)-[:ANSWERED_BY]->(answer:Answer)

    // 이유(Reason) 가져오기
    OPTIONAL MATCH (answer)-[:SUPPORTED_BY]->(reason:Reason)

    // 주 법령(primary law) 가져오기
    OPTIONAL MATCH (interp)-[:INTERPRETS]->(primary_law:Law)

    // 인용된 법조문 계층구조 가져오기 (Law -> Article -> Paragraph -> Item)
    OPTIONAL MATCH (interp)-[:CITES]->(cited_law:Law)
    OPTIONAL MATCH (interp)-[:CITES]->(cited_article:Article)
    OPTIONAL MATCH (cited_article)<-[:HAS_ARTICLE]-(article_law:Law)
    OPTIONAL MATCH (interp)-[:CITES]->(cited_para:Paragraph)
    OPTIONAL MATCH (cited_para)<-[:HAS_PARAGRAPH]-(para_article:Article)
    OPTIONAL MATCH (interp)-[:CITES]->(cited_item:Item)
    OPTIONAL MATCH (cited_item)<-[:HAS_ITEM]-(item_para:Paragraph)

    // 결과 반환
    RETURN
        // 해석례 메타데이터
        interp.title AS interpretation_title,
        interp.case_number AS case_number,

        // 질의/회답/이유
        node.text AS question,
        answer.text AS answer_text,
        reason.text AS reason_text,

        // 주 법령
        primary_law.name AS primary_law_name,

        // 인용된 법령 (중복 제거)
        collect(DISTINCT cited_law.name) AS cited_laws,

        // 인용된 조문 정보 (법령명 + 조문번호)
        collect(DISTINCT {
            law: article_law.name,
            article_number: cited_article.number,
            article_title: cited_article.title,
            article_content: cited_article.content
        }) AS cited_articles,

        // 인용된 항 정보
        collect(DISTINCT {
            law: article_law.name,
            article_number: para_article.number,
            paragraph_number: cited_para.number,
            paragraph_content: cited_para.content
        }) AS cited_paragraphs,

        // 인용된 호 정보
        collect(DISTINCT {
            law: article_law.name,
            article_number: para_article.number,
            paragraph_number: item_para.number,
            item_number: cited_item.number,
            item_content: cited_item.content
        }) AS cited_items
    """

    # VectorCypherRetriever 생성
    retriever = VectorCypherRetriever(
        driver=driver,
        index_name=index_name,
        retrieval_query=retrieval_query,
        embedder=embedder
    )

    return retriever


# ============================================
# GraphRAG 파이프라인 생성
# ============================================

def create_legal_graphrag(
    retriever,
    llm_model: str = "gpt-4o"
):
    """
    법령 해석례 기반 GraphRAG 파이프라인 생성

    Args:
        retriever: VectorCypherRetriever 인스턴스
        llm_model: 사용할 LLM 모델 (기본값: "gpt-4o")

    Returns:
        GraphRAG 인스턴스
    """
    # LLM 생성
    llm = OpenAILLM(model_name=llm_model)

    # 프롬프트 템플릿 정의
    prompt_template = RagTemplate(
        template="""
당신은 대한민국의 법령 해석 전문가입니다. 제공된 법령 해석례 정보를 바탕으로 사용자의 질문에 명확하고 정확하게 답변해주세요.

답변 시 다음 정보를 포함하여 체계적으로 설명해주세요:
<output_format>
사용자 질문에 대한 명확한 답변을 1-2문장으로 간단명료하게 제시

1. **관련 해석례**: 해석례 제목과 안건번호
2. **질의 내용**: 해석례의 질의요지
3. **회답 결론**: 해석 결과 (가능/불가능/조건부 등)
4. **법적 근거**: 관련 법령명과 조문
5. **상세 이유**: 해석의 근거가 되는 법리적 설명
</output_format>

<guidelines>
검색된 여러 해석례가 있다면, 가장 관련성이 높은 것을 중심으로 설명하고, 다른 참고할 만한 해석례도 간략히 언급해주세요.
정보는 상세하게 불렛 포인트로 나누어 설명하되, 너무 길지 않도록 핵심 위주로 작성해주세요.
</guidelines>

<user_question>
{query_text}
</user_question>

<검색된 법령 해석례 정보>
{context}
</검색된 법령 해석례 정보>


답변:
        """,
        expected_inputs=["context", "query_text"]
    )

    # GraphRAG 생성
    graph_rag = GraphRAG(
        retriever=retriever,
        llm=llm,
        prompt_template=prompt_template
    )

    return graph_rag


# ============================================
# 검색 및 답변 생성 함수
# ============================================

def search_legal_interpretation(
    driver,
    query: str,
    top_k: int = 3,
    index_name: str = "question_embedding_index",
    embedding_model: str = "text-embedding-3-small",
    llm_model: str = "gpt-4o",
    return_context: bool = True
):
    """
    법령 해석례 검색 및 답변 생성

    Args:
        driver: Neo4j 드라이버
        query: 사용자 질문
        top_k: 반환할 검색 결과 수 (기본값: 3)
        index_name: 벡터 인덱스 이름
        embedding_model: 임베딩 모델
        llm_model: LLM 모델
        return_context: 검색 컨텍스트 반환 여부

    Returns:
        GraphRAG 검색 결과 (answer, retriever_result 포함)
    """
    print(f"\n{'=' * 60}")
    print(f"질문: {query}")
    print(f"{'=' * 60}")

    # Retriever 생성
    retriever = create_legal_vectorcypher_retriever(
        driver=driver,
        index_name=index_name,
        embedding_model=embedding_model
    )

    # GraphRAG 생성
    graph_rag = create_legal_graphrag(
        retriever=retriever,
        llm_model=llm_model
    )

    # 검색 수행
    response = graph_rag.search(
        query_text=query,
        retriever_config={"top_k": top_k},
        return_context=return_context
    )

    return response


# ============================================
# 메인 함수
# ============================================

def main():
    driver = create_neo4j_driver()

    try:
        # 예제 질문들 - 다양한 검색 유형

        test_query = "사찰에 직접적인 '피해자'가 없어도 '피해종교단체'로 인정받을 수 있는 기준이 뭐야?"
        # test_query = "사찰에 직접적인 피해자가 있어야만 피해종교단체로 인정받을 수 있어?"
        # test_query = "피해종교단체의 정의가 뭐야?"
        # test_query = "10·27법난 피해자의 명예회복 법률에 대해 알려줘"
        # test_query = " 공무원이 아닌 위원의 임기는?"


        RETURN_CONTEXT = True
        response = search_legal_interpretation(
            driver=driver,
            query=test_query,
            top_k=1,
            return_context=RETURN_CONTEXT
        )

        if RETURN_CONTEXT and hasattr(response, 'retriever_result'):
            print("\n" + "=" * 60)
            print("검색 결과")
            print("=" * 60)
            for i in response.retriever_result.items:
                print(i.content)

        print("\n" + "=" * 60)
        print("최종 답변")
        print("=" * 60)
        print(response.answer)

    finally:
        close_neo4j_driver(driver)


if __name__ == "__main__":
    main()
