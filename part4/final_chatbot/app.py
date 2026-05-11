import os
import sys
import streamlit as st
from dotenv import load_dotenv
import re
import time
import base64
from neo4j import GraphDatabase

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "graphrag_tool_agent"))

from main import create_graphrag_agent

load_dotenv()


def visualize_graph_results(cypher_query: str, results: str):
    """Cypher 쿼리 결과를 그래프로 시각화 - 실제 Neo4j 그래프 구조 기반"""
    try:
        from pyvis.network import Network
        import tempfile

        # Neo4j 연결
        driver = GraphDatabase.driver(
            os.getenv("NEO4J_URI"),
            auth=(os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD"))
        )

        net = Network(height="550px", width="100%", bgcolor="#222222", font_color="white")
        net.barnes_hut(gravity=-8000, central_gravity=0.3, spring_length=250)

        # 원본 쿼리를 수정해서 그래프 패스를 반환하도록
        # MATCH (m:AImodel)-[r:DEVELOPED_BY]->(c:Company) ... RETURN m, r, c
        modified_query = cypher_query

        # RETURN 절을 찾아서 수정
        if "RETURN" in modified_query.upper():
            # MATCH 패턴에서 변수 추출
            match_pattern = re.search(r'MATCH\s+(.+?)(?:WHERE|RETURN|$)', modified_query, re.IGNORECASE | re.DOTALL)
            if match_pattern:
                match_clause = match_pattern.group(1).strip()

                # 노드 변수 추출: (변수:라벨) 패턴
                nodes = re.findall(r'\((\w+):\w+\)', match_clause)

                # 관계 변수 추가: [:타입] → [r:타입] 형식으로 변경
                # 기존 관계 변수가 있는지 확인
                existing_rels = re.findall(r'\[(\w+):\w+\]', match_clause)

                # 관계 변수가 없으면 자동으로 추가
                if not existing_rels:
                    # [:TYPE] → [r0:TYPE] 형식으로 변경
                    rel_count = 0
                    def add_rel_var(match):
                        nonlocal rel_count
                        var_name = f"r{rel_count}"
                        rel_count += 1
                        return f"[{var_name}:{match.group(1)}"

                    modified_query = re.sub(r'\[:(\w+)', add_rel_var, modified_query)

                    # 다시 추출
                    match_pattern = re.search(r'MATCH\s+(.+?)(?:WHERE|RETURN|$)', modified_query, re.IGNORECASE | re.DOTALL)
                    if match_pattern:
                        match_clause = match_pattern.group(1).strip()
                        existing_rels = re.findall(r'\[(\w+):\w+\]', match_clause)

                # 그래프 요소를 반환하도록 쿼리 수정
                graph_elements = nodes + existing_rels

                if graph_elements:
                    # RETURN 절 교체
                    modified_query = re.sub(
                        r'RETURN\s+.+$',
                        f"RETURN {', '.join(graph_elements)} LIMIT 20",
                        modified_query,
                        flags=re.IGNORECASE | re.DOTALL
                    )

        # 쿼리 실행
        with driver.session() as session:
            result = session.run(modified_query)

            added_nodes = set()
            added_edges = set()

            for record in result:
                # 레코드의 각 값 처리
                for key in record.keys():
                    value = record[key]

                    # 노드인 경우
                    if hasattr(value, 'labels') and hasattr(value, 'element_id'):
                        node_id = value.element_id
                        if node_id not in added_nodes:
                            labels = list(value.labels)
                            label = labels[0] if labels else "Node"

                            # 노드 속성에서 name 또는 첫 번째 속성 가져오기
                            props = dict(value.items())
                            display_name = props.get('name') or props.get('title') or f"{label}_{node_id}"

                            # 속성 정보를 툴팁으로
                            hover_text = f"{label}\n" + "\n".join([f"{k}: {v}" for k, v in list(props.items())[:5]])

                            # 라벨별 색상
                            color = "#97C2FC"  # 기본 파란색
                            if label in ["Company", "Organization"]:
                                color = "#FB7E81"  # 빨간색
                            elif label in ["AImodel", "Model"]:
                                color = "#FFA807"  # 주황색
                            elif label in ["Person"]:
                                color = "#90EE90"  # 연두색

                            net.add_node(
                                node_id,
                                label=str(display_name),
                                title=hover_text,
                                color=color,
                                size=25
                            )
                            added_nodes.add(node_id)

                    # 관계인 경우
                    elif hasattr(value, 'type') and hasattr(value, 'start_node') and hasattr(value, 'end_node'):
                        edge_id = (value.start_node.element_id, value.end_node.element_id, value.type)
                        if edge_id not in added_edges:
                            # 시작/끝 노드도 추가
                            for node in [value.start_node, value.end_node]:
                                node_id = node.element_id
                                if node_id not in added_nodes:
                                    labels = list(node.labels)
                                    label = labels[0] if labels else "Node"
                                    props = dict(node.items())
                                    display_name = props.get('name') or props.get('title') or f"{label}_{node_id}"
                                    hover_text = f"{label}\n" + "\n".join([f"{k}: {v}" for k, v in list(props.items())[:5]])

                                    color = "#97C2FC"
                                    if label in ["Company", "Organization"]:
                                        color = "#FB7E81"
                                    elif label in ["AImodel", "Model"]:
                                        color = "#FFA807"
                                    elif label in ["Person"]:
                                        color = "#90EE90"

                                    net.add_node(node_id, label=str(display_name), title=hover_text, color=color, size=25)
                                    added_nodes.add(node_id)

                            # 관계 추가
                            net.add_edge(
                                value.start_node.element_id,
                                value.end_node.element_id,
                                label=value.type,
                                title=value.type
                            )
                            added_edges.add(edge_id)

        driver.close()

        # 노드가 없으면 시각화하지 않음
        if len(added_nodes) == 0:
            st.info("그래프 결과가 없습니다. 쿼리가 노드/관계를 반환하지 않았습니다.")
            return None

        # HTML 생성
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.html', encoding='utf-8') as f:
            html = net.generate_html()
            f.write(html)
            temp_path = f.name

        with open(temp_path, 'r', encoding='utf-8') as f:
            html_content = f.read()

        os.unlink(temp_path)

        # HTML에 명시적인 높이 추가
        html_content = html_content.replace(
            '<body>',
            '<body style="margin:0; padding:0; height:600px; overflow:hidden;">'
        )
        html_content = html_content.replace(
            'height: 550px',
            'height: 600px'
        )

        return html_content

    except Exception as e:
        st.warning(f"그래프 시각화 실패: {str(e)}")
        return None


# 페이지 설정
st.set_page_config(
    page_title="GraphRAG 챗봇",
    page_icon="🔗",
    layout="wide"
)

# 세션 상태 초기화
if "messages" not in st.session_state:
    st.session_state.messages = []

if "agent" not in st.session_state:
    with st.spinner("GraphRAG Agent 초기화 중..."):
        st.session_state.agent = create_graphrag_agent()

# 사이드바
with st.sidebar:
    st.title("🔗 GraphRAG 챗봇")
    st.markdown("""
    ### 지원 기능

    **Text2Cypher** (구조화된 질의)
    - 특정 엔티티 속성값 검색
    - 관계 기반 질의 (소속, 직책 등)
    - 정확한 개수, 목록, 통계

    **Vector Search** (의미 기반 검색)
    - 관련 내용 기반 질문
    - 의미 기반 텍스트 검색
    - 그래프 관계를 통한 컨텍스트 확장
    """)

    if st.button("대화 기록 초기화"):
        st.session_state.messages = []
        st.rerun()

    st.markdown("---")
    st.markdown("**연결 정보**")
    st.text(f"Neo4j: {os.getenv('NEO4J_URI', 'Not configured')}")
    st.text(f"Model: gpt-4o")

# 제목
st.title("💬 GraphRAG 챗봇")
st.caption("Neo4j 기반 지식 그래프 검색 및 질의응답 시스템")

# 이전 대화 기록 표시
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

        # 도구 호출 정보가 있으면 표시
        if "tool_calls" in message:
            for tool_call in message["tool_calls"]:
                with st.expander(f"🔧 도구 호출: {tool_call['name']}", expanded=False):
                    st.json(tool_call["args"])

                    # Cypher 쿼리인 경우 코드 블록으로 표시
                    if "result" in tool_call and tool_call["name"] == "generate_cypher_query":
                        st.code(tool_call["result"], language="cypher")
                    elif "result" in tool_call:
                        st.text(tool_call["result"])

        # 그래프 시각화가 있으면 표시
        if "graph_html" in message:
            b64_html = base64.b64encode(message["graph_html"].encode()).decode()
            iframe_html = f'<iframe src="data:text/html;base64,{b64_html}" width="100%" height="600" frameborder="0"></iframe>'
            st.markdown(iframe_html, unsafe_allow_html=True)

# 사용자 입력
if prompt := st.chat_input("질문을 입력하세요..."):
    # 사용자 메시지 표시
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # Agent 응답 생성
    with st.chat_message("assistant"):
        tool_calls_container = st.container()
        response_placeholder = st.empty()

        conversation_history = [
            {"role": msg["role"], "content": msg["content"]}
            for msg in st.session_state.messages
        ]

        agent_response = ""
        tool_calls_info = []
        cypher_query = None
        cypher_results = None

        try:
            # Agent 스트리밍
            for chunk in st.session_state.agent.stream({"messages": conversation_history}):
                for node_name, node_data in chunk.items():
                    if "messages" in node_data:
                        messages = node_data["messages"]

                        for msg in messages:
                            msg_type = type(msg).__name__

                            if msg_type == "AIMessage":
                                # Tool 호출
                                if hasattr(msg, 'tool_calls') and msg.tool_calls:
                                    for tool_call in msg.tool_calls:
                                        tool_info = {
                                            "name": tool_call['name'],
                                            "args": tool_call['args']
                                        }
                                        tool_calls_info.append(tool_info)

                                        # 도구 호출 표시
                                        with tool_calls_container:
                                            with st.expander(f"🔧 도구 호출: {tool_call['name']}", expanded=True):
                                                st.json(tool_call['args'])

                                # 최종 응답 (스트리밍 효과)
                                elif msg.content:
                                    agent_response = msg.content

                                    # 한 글자씩 스트리밍 효과
                                    displayed_text = ""
                                    for char in agent_response:
                                        displayed_text += char
                                        response_placeholder.markdown(displayed_text + "▌")
                                        time.sleep(0.01)

                                    # 커서 제거하고 최종 표시
                                    response_placeholder.markdown(agent_response)

                            elif msg_type == "ToolMessage":
                                # Tool 실행 결과
                                result = str(msg.content)

                                # 해당 도구 정보에 결과 추가
                                if tool_calls_info:
                                    tool_calls_info[-1]["result"] = result

                                    # generate_cypher_query 결과인 경우
                                    if tool_calls_info[-1]["name"] == "generate_cypher_query":
                                        cypher_query = result
                                        with tool_calls_container:
                                            with st.expander(f"📝 생성된 Cypher 쿼리", expanded=True):
                                                st.code(result, language="cypher")

                                    # execute_cypher_query 결과인 경우
                                    elif tool_calls_info[-1]["name"] == "execute_cypher_query":
                                        cypher_results = result
                                        with tool_calls_container:
                                            with st.expander(f"📊 쿼리 실행 결과", expanded=True):
                                                st.text(result)

                                    # 기타 도구 결과
                                    else:
                                        with tool_calls_container:
                                            with st.expander(f"✅ {tool_calls_info[-1]['name']} 결과", expanded=False):
                                                st.text(result)

            # 그래프 시각화 (Cypher 결과가 있는 경우)
            graph_html = None
            if cypher_query and cypher_results:
                try:
                    graph_html = visualize_graph_results(cypher_query, cypher_results)
                    if graph_html:
                        with st.expander("🌐 그래프 시각화", expanded=True):
                            b64_html = base64.b64encode(graph_html.encode()).decode()
                            iframe_html = f'<iframe src="data:text/html;base64,{b64_html}" width="100%" height="600" frameborder="0"></iframe>'
                            st.markdown(iframe_html, unsafe_allow_html=True)
                except Exception as e:
                    st.warning(f"그래프 시각화 중 오류 발생: {str(e)}")

            # 응답을 세션에 저장
            message_data = {
                "role": "assistant",
                "content": agent_response or "처리 완료"
            }

            if tool_calls_info:
                message_data["tool_calls"] = tool_calls_info

            if graph_html:
                message_data["graph_html"] = graph_html

            st.session_state.messages.append(message_data)

        except Exception as e:
            error_msg = f"오류 발생: {str(e)}"
            st.error(error_msg)
            st.session_state.messages.append({
                "role": "assistant",
                "content": error_msg
            })


def main():
    if not os.getenv("OPENAI_API_KEY"):
        st.error("OPENAI_API_KEY가 설정되지 않았습니다. .env 파일을 확인하세요.")
        st.stop()

    if not os.getenv("NEO4J_URI"):
        st.error("NEO4J_URI가 설정되지 않았습니다. .env 파일을 확인하세요.")
        st.stop()


if __name__ == "__main__":
    main()
