from neo4j_graphrag.retrievers import ToolsRetriever
from neo4j_graphrag.generation import GraphRAG

from config import initialize_connection, create_vector_indexes
from vector_retriever import (
    create_text_vector_retriever,
    create_table_vector_retriever,
    create_text_vector_tool,
    create_table_vector_tool
)
from vectorcypher_retriever import create_vectorcypher_retriever, create_vectorcypher_tool
from text2cypher_retriever import create_text2cypher_retriever, create_text2cypher_tool
from prompts import create_rag_prompt_template


# ============================================
# ToolsRetriever 생성
# ============================================

def create_tools_retriever(driver, llm, embedder, text_index_name, table_index_name):
    """4가지 Retriever를 생성하고 ToolsRetriever로 통합"""

    # 1. TextElement 벡터 검색
    text_vector_retriever = create_text_vector_retriever(driver, embedder, text_index_name)

    # 2. TableElement 벡터 검색
    table_vector_retriever = create_table_vector_retriever(driver, embedder, table_index_name)

    # 3. VectorCypher 검색
    vectorcypher_retriever = create_vectorcypher_retriever(driver, embedder, text_index_name)

    # 4. Text2Cypher 검색
    text2cypher_retriever = create_text2cypher_retriever(driver, llm)

    # Tool로 변환
    text_vector_tool = create_text_vector_tool(text_vector_retriever)
    table_vector_tool = create_table_vector_tool(table_vector_retriever)
    vectorcypher_tool = create_vectorcypher_tool(vectorcypher_retriever)
    text2cypher_tool = create_text2cypher_tool(text2cypher_retriever)

    # ToolsRetriever 생성
    tools_retriever = ToolsRetriever(
        driver=driver,
        llm=llm,
        tools=[text_vector_tool, table_vector_tool, vectorcypher_tool, text2cypher_tool]
    )

    print("✓ ToolsRetriever 생성 완료 (4개 도구 등록)")
    return tools_retriever


# ============================================
# GraphRAG 실행 함수
# ============================================

def run_graphrag(tools_retriever, llm, query_text):
    """GraphRAG 실행 및 결과 반환"""

    # 프롬프트 템플릿 생성
    prompt_template = create_rag_prompt_template()

    # GraphRAG 생성
    graphrag = GraphRAG(
        llm=llm,
        retriever=tools_retriever,
        prompt_template=prompt_template
    )

    print(f"\n{'='*70}")
    print(f"질문: {query_text}")
    print(f"{'='*70}\n")
    print("🔍 검색 중...")

    result = graphrag.search(query_text=query_text, return_context=True)

    # 선택된 Tool 정보 출력
    if hasattr(result, 'retriever_result') and result.retriever_result:
        items = result.retriever_result.items if hasattr(result.retriever_result, 'items') else []
        if items:
            tools_used = set()
            for item in items:
                if hasattr(item, 'metadata') and item.metadata and 'tool' in item.metadata:
                    tools_used.add(item.metadata['tool'])

            if tools_used:
                print(f"\n🔧 선택된 Tool(검색 방식): {', '.join(sorted(tools_used))}")

    print("\n" + "="*70)
    print("📝 답변 결과")
    print("="*70)
    print(result.answer)
    print("="*70)

    # 검색 결과 상세 출력
    if hasattr(result, 'retriever_result') and result.retriever_result:
        items = result.retriever_result.items if hasattr(result.retriever_result, 'items') else []
        if items:
            print(f"\n검색된 항목: {len(items)}개")
            print("-"*70)

            for idx, item in enumerate(items, 1):
                print(f"\n[{idx}] 검색 결과:")

                # content 출력
                content = item.content if hasattr(item, 'content') else str(item)
                print(f"  내용: {content}")

                # metadata 출력
                if hasattr(item, 'metadata') and item.metadata:
                    if item.metadata.get('tool') == "vectorcypher_context_search":
                        continue
                    print(f"  메타데이터: {item.metadata}")

            print("-"*70)

    return result


# ============================================
# 메인 함수
# ============================================

def main():
    print("\n" + "="*70)
    print("PDF 지식그래프 기반 ToolsRetriever GraphRAG")
    print("="*70 + "\n")

    print("[1단계] 초기화")
    print("-"*70)
    driver, llm, embedder = initialize_connection()
    print()

    print("[2단계] 벡터 인덱스 생성")
    print("-"*70)
    text_index_name, table_index_name = create_vector_indexes(driver)
    print()

    print("[3단계] ToolsRetriever 생성")
    print("-"*70)
    tools_retriever = create_tools_retriever(driver, llm, embedder, text_index_name, table_index_name)
    print()

    print("[4단계] GraphRAG 대화형 실행")
    print("-"*70)
    print("\n여러 질문을 계속 입력할 수 있습니다. 종료하려면 'q' 또는 'exit'를 입력하세요.\n")

    try:
        while True:
            print("-" * 70)
            query = input("질문을 입력하세요 (종료: 'q' 또는 'exit'): ").strip()

            if query.lower() in ['q', 'exit']:
                print("\n프로그램을 종료합니다.")
                break

            # GraphRAG 실행
            try:
                result = run_graphrag(tools_retriever, llm, query)
            except Exception as e:
                print(f"\n오류 발생: {e}")
                continue

    except KeyboardInterrupt:
        print("\n\n프로그램을 종료합니다.")
    finally:
        driver.close()
        print("Neo4j 연결을 종료했습니다.")

    return driver, tools_retriever, llm

if __name__ == "__main__":
    main()
