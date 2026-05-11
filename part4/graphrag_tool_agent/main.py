import os
from dotenv import load_dotenv
from langchain.agents import create_agent
from tools import get_all_tools, initialize_tools

load_dotenv()


def create_graphrag_agent():
    initialize_tools()

    tools = get_all_tools()
    print(f"✓ 총 {len(tools)}개의 도구 등록:")
    for tool in tools:
        print(f"  - {tool.name}: {tool.description.split('.')[0]}...")

    system_prompt = """당신은 Neo4j 기반 그래프 데이터베이스를 활용하는 GraphRAG 전문 AI 어시스턴트입니다.

<Text2Cypher 워크플로우>
**Text2Cypher 워크플로우** (구조화된 정확한 질의):

- 특정 엔티티의 속성값을 찾을 때
- "X의 Y는?" 형식의 관계 질의 (소속, 직책, 개발사 등)
- 목차 구조, 페이지 내용 같은 문서 메타데이터
- 정확한 개수, 목록, 통계 데이터
- 도메인 그래프를 활용한 구조화된 정보 검색

*실행 흐름:*
1. schema_introspection - 스키마 확인
2. generate_cypher_query - 질문 → Cypher 쿼리 변환
3. execute_cypher_query - 쿼리 실행 및 결과 반환
</Text2Cypher 워크플로우>

<Vector Search 워크플로우>
**Vector Search** (의미 기반 종합 검색):

- "~에 관해 설명해줘" 같은 관련 내용 기반 질문
- 의미 기반 검색으로 관련 텍스트 찾기
- 그래프 관계를 통해 관련 정보 자동 확장
- 문서 계층, 엔티티 관계, 확장 컨텍스트 포함
</Vector Search 워크플로우>

**질문과 상황에 따라 다중 도구를 선택하여 사용자에게 최선의 답변을 하세요.**
"""

    agent = create_agent(
        model="gpt-4o",
        tools=tools,
        system_prompt=system_prompt
    )

    print("\n✓ GraphRAG Tool Agent 생성 완료\n")
    return agent


def run_agent_interactive(agent):
    print("=" * 70)
    print("GraphRAG Tool Agent")
    print("=" * 70)

    conversation_history = []

    while True:
        try:
            user_input = input("\n질문: ").strip()

            if user_input.lower() in ["exit", "quit", "q"]:
                break

            if not user_input:
                continue

            conversation_history.append({"role": "user", "content": user_input})

            print("=" * 70)

            final_messages = []
            current_tool_name = None

            for chunk in agent.stream({"messages": conversation_history}):
                # chunk: {node_name: {messages: [...]}}
                for node_name, node_data in chunk.items():
                    print(f"> 노드 진입: {node_name}")
                    print("-" * 70)

                    if "messages" in node_data:
                        messages = node_data["messages"]
                        for msg in messages:
                            msg_type = type(msg).__name__

                            if msg_type == "AIMessage":
                                if hasattr(msg, 'tool_calls') and msg.tool_calls:
                                    for tool_call in msg.tool_calls:
                                        print(f"🔧 Tool 호출: {tool_call['name']}")
                                        print(f"   입력: {tool_call['args']}")
                                        current_tool_name = tool_call['name']
                                elif msg.content:
                                    # 최종 응답
                                    print(f"💬 Agent 응답:")
                                    print(f"   {msg.content}")

                            elif msg_type == "ToolMessage":
                                # Tool 실행 결과
                                print(f"Tool 실행 결과 ({current_tool_name}):")
                                # 결과가 너무 길면 요약
                                result = str(msg.content)
                                if len(result) > 1000:
                                    print(f"   {result[:1000]}...")
                                    print(f"   (총 {len(result)} 글자)")
                                else:
                                    print(f"   {result}")

                            final_messages.append(msg)

                    print()

            conversation_history.extend(final_messages)

            print("=" * 70)

        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"\n오류 발생: {str(e)}")
            import traceback
            traceback.print_exc()


def save_graph_as_png(agent):
    """Agent 그래프를 PNG 파일로 저장"""
    try:
        png_data = agent.get_graph().draw_mermaid_png()
        filename = f"tool_agent.png"

        with open(filename, 'wb') as f:
            f.write(png_data)

        print(f"✓ 그래프 이미지 저장 완료: {filename}\n")
    except Exception as e:
        print(f"그래프 저장 실패: {e}\n")


def main():
    """메인 실행 함수"""
    if not os.environ.get("OPENAI_API_KEY"):
        return
    if not os.environ.get("NEO4J_URI"):
        return

    agent = create_graphrag_agent()
    save_graph_as_png(agent)
    run_agent_interactive(agent)


if __name__ == "__main__":
    main()
