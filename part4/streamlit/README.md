# Streamlit Tutorial

**Part 4. GraphRAG 에이전트와 챗봇 실전**

- Chapter 02. Agent 챗봇 만들기
    - 📒 Clip 01. [실습] Streamlit으로 챗봇 만들기

> final_chatbot 구현에 필요한 Streamlit 핵심 기능만 학습합니다. 세션 상태 관리, 채팅 인터페이스, Expander를 사용한 정보 표시 등 챗봇 UI 구축의 필수 요소를 다룹니다.

---

## 실습 순서

### 1. 패키지 설치

Python 3.10+

```bash
# uv 설치
# Windows (PowerShell)
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"

# macOS / Linux
curl -LsSf https://astral.sh/uv/install.sh | sh
```

```bash
# 방법 1: uv sync 사용 (권장)
uv sync
.venv\Scripts\activate
```

또는

```bash
# 방법 2: requirements.txt 사용
uv venv
.venv\Scripts\activate
uv pip install -r requirements.txt
```

또는

```bash
# 방법 3: pip 사용
pip install -r requirements.txt
```

### 2. 예제 실행

#### 예제 1: 기본 사용법

```bash
streamlit run 01_basics.py
```

**학습 내용:**
- 텍스트 표시, 레이아웃 (컬럼, 사이드바, Expander)
- 세션 상태 관리 (대화 기록 저장의 핵심!)
- 기본 위젯 (버튼, 입력)

#### 예제 2: 챗봇 UI

```bash
streamlit run 02_chatbot_ui.py
```

**학습 내용:**
- st.chat_message, st.chat_input 사용법
- 대화 기록 관리
- 도구 호출 정보 Expander로 표시
- 스트리밍 효과
