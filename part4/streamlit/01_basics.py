import streamlit as st
import pandas as pd

st.set_page_config(
    page_title="Streamlit 기본",
    page_icon="📝",
    layout="wide"
)

st.title("📝 Streamlit 기본 - ChatBot 구현 핵심")

st.markdown("""
이 예제는 **GraphRAG 챗봇 구현에 필요한 핵심 기능**만 다룹니다.
- 텍스트 표시
- 레이아웃 (컬럼, 사이드바, Expander)
- 세션 상태 관리
- 기본 위젯 (버튼, 입력)
""")

# ============================================
# 1. 텍스트 표시
# ============================================

st.header("1. 텍스트 표시")

st.write("st.write()는 가장 자주 쓰는 만능 함수입니다.")
st.markdown("**마크다운**도 사용 가능: *기울임*, ~~취소선~~")
st.code("print('코드 블록')", language="python")

st.info("ℹ️ 정보 메시지")
st.success("✅ 성공 메시지")
st.warning("⚠️ 경고 메시지")
st.error("❌ 에러 메시지")

# ============================================
# 2. 레이아웃 - 컬럼
# ============================================

st.header("2. 컬럼 레이아웃")

col1, col2, col3 = st.columns(3)

col1.write("컬럼 1")
col1.metric("사용자", "1,234", "+12")

col2.write("컬럼 2")
col2.metric("매출", "$5.6M", "+$1.2M")

col3.write("컬럼 3")
col3.metric("에러율", "2.3%", "-0.5%", delta_color="inverse")

# ============================================
# 3. 사이드바
# ============================================

st.header("3. 사이드바")

st.write("왼쪽 사이드바를 확인하세요")

with st.sidebar:
    st.title("⚙️ 설정")
    st.write("사이드바에는 설정과 옵션을 배치합니다.")

    user_name = st.text_input("이름", placeholder="홍길동")
    age = st.slider("나이", 0, 100, 25)

    if st.button("정보 저장"):
        # 세션 상태에 저장
        st.session_state.saved_name = user_name
        st.session_state.saved_age = age
        st.success("저장 완료!")

# 저장된 정보 표시
if "saved_name" in st.session_state:
    st.info(f"💾 저장된 정보: {st.session_state.saved_name} ({st.session_state.saved_age}세)")

# ============================================
# 4. Expander (접기/펼치기)
# ============================================

st.header("4. Expander (접기/펼치기)")

st.write("**ChatBot에서 도구 호출 정보를 표시할 때 사용합니다.**")

with st.expander("🔧 도구 호출 정보 보기"):
    st.write("도구 이름: generate_cypher_query")
    st.json({"question": "OpenAI에 대해 알려줘"})
    st.code("MATCH (c:Company) WHERE c.name CONTAINS 'OpenAI' RETURN c", language="cypher")

with st.expander("📊 실행 결과", expanded=True):
    st.write("기본적으로 펼쳐진 상태로 표시할 수도 있습니다.")
    st.dataframe(pd.DataFrame({"이름": ["OpenAI"], "설명": ["AI 연구 기업"]}))

# ============================================
# 5. 세션 상태 (Session State)
# ============================================

st.header("5. 세션 상태 관리")

st.markdown("""
**세션 상태는 챗봇 구현의 핵심입니다!**
- 페이지가 재실행되어도 데이터를 유지
- 대화 기록, 사용자 설정 등을 저장
""")

st.subheader("5-1. 리스트 관리 (대화 기록과 유사)")

if "messages" not in st.session_state:
    st.session_state.messages = []

# 새 메시지 추가
new_msg = st.text_input("메시지 추가:", key="new_message")
if st.button("➕ 추가"):
    if new_msg:
        st.session_state.messages.append(new_msg)
        st.rerun()

# 메시지 목록 표시
if st.session_state.messages:
    st.write("**메시지 목록:**")
    for idx, msg in enumerate(st.session_state.messages):
        st.write(f"{idx + 1}. {msg}")

    if st.button("🗑️ 전체 삭제"):
        st.session_state.messages = []
        st.rerun()
else:
    st.info("메시지가 없습니다.")


st.subheader("5-2. 위젯에 key 지정")

st.write("key 파라미터를 사용하면 자동으로 세션 상태에 저장됩니다.")

# key를 지정하면 st.session_state.user_input으로 접근 가능
user_input = st.text_input("입력:", key="user_input")

if user_input:
    st.write(f"입력한 값: {st.session_state.user_input}")


# ============================================
# 6. DataFrame 표시
# ============================================

st.header("6. 데이터 표시")

df = pd.DataFrame({
    "이름": ["홍길동", "김철수", "이영희"],
    "나이": [25, 30, 28],
    "직업": ["개발자", "디자이너", "기획자"]
})

st.dataframe(df, width='stretch')

# JSON 표시
st.json({"name": "홍길동", "age": 25, "skills": ["Python", "JavaScript"]})

# ============================================
# 7. 세션 상태 디버깅
# ============================================

st.header("7. 세션 상태 디버깅")

with st.expander("현재 세션 상태 보기"):
    st.write("**모든 세션 상태 변수:**")
    st.json(dict(st.session_state))

st.divider()