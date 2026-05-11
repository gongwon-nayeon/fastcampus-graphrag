import streamlit as st
import time

st.set_page_config(page_title="챗봇 UI", page_icon="💬", layout="wide")

st.title("💬 Streamlit 챗봇 UI 가이드")

st.markdown("""
- st.chat_message (메시지 표시)
- st.chat_input (사용자 입력)
- 세션 상태로 대화 기록 관리
- 도구 호출 정보 표시
- 스트리밍 효과
""")

# ============================================
# 1. 기본 채팅 메시지
# ============================================

st.header("1. 기본 채팅 메시지")

with st.chat_message("user"):
    st.write("안녕하세요! 사용자 메시지입니다.")

with st.chat_message("assistant"):
    st.write("안녕하세요! AI 어시스턴트입니다.")

with st.chat_message("user", avatar="👤"):
    st.write("커스텀 아바타도 사용할 수 있습니다.")

with st.chat_message("assistant", avatar="🤖"):
    st.write("다양한 이모지를 아바타로 사용 가능!")

st.divider()

# ============================================
# 2. 통합 챗봇
# ============================================

st.header("2. 통합 챗봇")

st.markdown("""
- 일반 메시지: 에코 응답 (예: "안녕하세요")
- **스키마 보여줘**: 스키마 조회 도구 호출
- **OpenAI 찾아줘**: Cypher 쿼리 생성 및 실행 도구 호출 (2개 도구 연속)
- **스트리밍**: 타이핑 효과로 응답
""")

# 세션 상태 초기화
if "messages" not in st.session_state:
    st.session_state.messages = [
        {"role": "assistant", "content": "안녕하세요! 무엇을 도와드릴까요?"}
    ]

# 이전 메시지 표시
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.write(message["content"])

        # 도구 호출 정보가 있으면 Expander로 표시
        if "tool_calls" in message:
            for tool_call in message["tool_calls"]:
                with st.expander(f"🔧 도구 호출: {tool_call['name']}", expanded=False):
                    st.write("**입력:**")
                    st.json(tool_call["args"])

                    if "result" in tool_call:
                        st.write("**결과:**")
                        # Cypher 쿼리면 코드 블록으로
                        if tool_call["name"] == "generate_cypher_query":
                            st.code(tool_call["result"], language="cypher")
                        else:
                            st.text(tool_call["result"])

# 새 메시지 입력
if prompt := st.chat_input("메시지를 입력하세요 (예: '안녕', '스키마 보여줘', 'OpenAI 찾아줘', '스트리밍')"):
    # 사용자 메시지 저장
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.write(prompt)

    # 봇 응답 생성
    with st.chat_message("assistant"):
        response_message = {"role": "assistant", "content": "", "tool_calls": []}

        # 1. 스키마 조회 명령
        if "스키마" in prompt:
            with st.expander("🔧 도구 호출: schema_introspection", expanded=True):
                st.write("**입력:** (파라미터 없음)")
                with st.spinner("스키마 조회 중..."):
                    time.sleep(0.5)
                result = "Node: Company, Person, TOC\nRelationship: EMPLOYS, HAS_CHUNK"
                st.write("**결과:**")
                st.code(result)

            response_message["tool_calls"].append({
                "name": "schema_introspection",
                "args": {},
                "result": result
            })
            response_message["content"] = "그래프 스키마를 조회했습니다. Company, Person, TOC 노드와 관계들이 있습니다."

        # 2. OpenAI 검색 명령 (2개 도구 연속 실행)
        elif "OpenAI" in prompt or "찾아" in prompt:
            # generate_cypher_query 도구
            with st.expander("🔧 도구 호출: generate_cypher_query", expanded=True):
                st.write("**입력:**")
                st.json({"question": prompt})
                with st.spinner("쿼리 생성 중..."):
                    time.sleep(0.5)
                query = "MATCH (c:Company) WHERE c.name CONTAINS 'OpenAI' RETURN c.name, c.description"
                st.write("**생성된 Cypher 쿼리:**")
                st.code(query, language="cypher")

            response_message["tool_calls"].append({
                "name": "generate_cypher_query",
                "args": {"question": prompt},
                "result": query
            })

            # execute_cypher_query 도구
            with st.expander("🔧 도구 호출: execute_cypher_query", expanded=True):
                st.write("**입력:**")
                st.code(query, language="cypher")
                with st.spinner("쿼리 실행 중..."):
                    time.sleep(0.5)
                result = "[1]\n  name: OpenAI\n  description: AI 연구 기업"
                st.write("**실행 결과:**")
                st.text(result)

            response_message["tool_calls"].append({
                "name": "execute_cypher_query",
                "args": {"cypher_query": query},
                "result": result
            })

            response_message["content"] = "OpenAI는 AI 연구 기업입니다. 데이터베이스에서 정보를 찾았습니다."

        # 3. 스트리밍 효과 명령
        elif "스트리밍" in prompt:
            message_placeholder = st.empty()
            full_response = f"'{prompt}'에 대한 답변입니다. 스트리밍 효과로 한 글자씩 표시됩니다!"

            displayed_text = ""
            for char in full_response:
                displayed_text += char
                message_placeholder.write(displayed_text + "▌")
                time.sleep(0.02)

            message_placeholder.write(full_response)
            response_message["content"] = full_response

        # 4. 일반 메시지 (에코)
        else:
            response_message["content"] = f"에코: {prompt}\n\n💡 **명령어를 입력해보세요:**\n- '스키마 보여줘'\n- 'OpenAI 찾아줘'\n- '스트리밍'"

        # 최종 응답 표시 (스트리밍이 아닌 경우)
        if "스트리밍" not in prompt:
            st.write(response_message["content"])

        st.session_state.messages.append(response_message)

# 대화 초기화 버튼
if len(st.session_state.messages) > 1:
    if st.button("🗑️ 대화 초기화"):
        st.session_state.messages = [
            {"role": "assistant", "content": "안녕하세요! 무엇을 도와드릴까요?"}
        ]
        st.rerun()

st.divider()


# ============================================
# 사이드바
# ============================================

with st.sidebar:
    st.title("⚙️ 챗봇 설정")

    st.subheader("모델 설정")
    model = st.selectbox("모델 선택", ["gpt-4o", "gpt-4o-mini", "gpt-5.4"])
    temperature = st.slider("Temperature", 0.0, 1.0, 0.7)

    st.subheader("연결 정보")
    st.text("Neo4j: bolt://localhost:7687")
    st.text(f"Model: {model}")

    st.divider()

    if st.button("모든 대화 초기화"):
        if "messages" in st.session_state:
            st.session_state.messages = [
                {"role": "assistant", "content": "안녕하세요! 무엇을 도와드릴까요?"}
            ]
        st.rerun()