from langgraph.graph import StateGraph, START, END

from state import State, InputState, OutputState
from nodes import (
    guardrail_node,
    validate_query_node,
    generate_cypher_node,
    execute_cypher_node,
    correct_cypher_node,
    answer_node,
    route_guardrail,
    route_validation,
    route_execution,
)


# ============================================
# 그래프 생성 함수
# ============================================

def create_agent_graph():
    """Text2Cypher Agent 그래프 생성"""

    # StateGraph 초기화
    graph_builder = StateGraph(State, input_schema=InputState, output_schema=OutputState)

    # 노드 추가
    graph_builder.add_node("guardrail", guardrail_node)
    graph_builder.add_node("validate_query", validate_query_node)
    graph_builder.add_node("generate_cypher", generate_cypher_node)
    graph_builder.add_node("execute_cypher", execute_cypher_node)
    graph_builder.add_node("correct_cypher", correct_cypher_node)
    graph_builder.add_node("answer", answer_node)

    # 엣지 연결
    graph_builder.add_edge(START, "guardrail")
    graph_builder.add_edge("generate_cypher", "validate_query")
    graph_builder.add_edge("correct_cypher", "validate_query")
    graph_builder.add_edge("answer", END)

    # 조건부 엣지 연결
    # 1. Guardrail: DB 관련 질문인지 확인
    graph_builder.add_conditional_edges(
        "guardrail",
        route_guardrail,
        {"generate_cypher": "generate_cypher", "answer": "answer"},
    )

    # 2. Validation: 쿼리 검증
    graph_builder.add_conditional_edges(
        "validate_query",
        route_validation,
        {
            "execute_cypher": "execute_cypher",
            "correct_cypher": "correct_cypher",
            "answer": "answer",  # 재시도 제한 초과
        },
    )

    # 3. Execution: 실행 결과에 따라 라우팅
    graph_builder.add_conditional_edges(
        "execute_cypher",
        route_execution,
        {
            "generate_cypher": "generate_cypher",  # 빈 결과 → 재생성
            "correct_cypher": "correct_cypher",     # 에러 → 수정
            "answer": "answer",                     # 정상 or 제한 초과
        },
    )

    # 그래프 컴파일
    graph = graph_builder.compile()

    return graph


def create_graph():
    return create_agent_graph()

graph = create_graph()


if __name__ == "__main__":
    graph = create_graph()

    try:
        png_data = graph.get_graph().draw_mermaid_png()
        output_file = "graph_visualization.png"

        with open(output_file, "wb") as f:
            f.write(png_data)

        print(f"✓ 그래프 시각화 저장: {output_file}\n")
    except Exception as e:
        print(f"그래프 시각화 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()
