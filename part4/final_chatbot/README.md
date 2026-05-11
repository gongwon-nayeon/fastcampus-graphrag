# GraphRAG 챗봇 (Streamlit)

**Part 4. GraphRAG 에이전트와 챗봇 실전**

- Chapter 01. Agent 챗봇 만들기
    - 📒 Clip 02. [프로젝트] GraphRAG Agent를 챗봇으로 완성하기

> graphrag_tool_agent를 Streamlit으로 감싼 완전한 챗봇 애플리케이션입니다. 도구 호출 추적, Cypher 쿼리 시각화, 그래프 시각화 등 실전에 필요한 모든 기능을 갖추고 있습니다.

---

## 주요 기능

### 1. Text2Cypher 워크플로우
- 자연어 질문을 Cypher 쿼리로 자동 변환
- 생성된 Cypher 쿼리를 챗봇에 표시
- 구조화된 정확한 질의에 적합

### 2. Vector Search
- 의미 기반 벡터 검색
- 그래프 관계를 통한 자동 컨텍스트 확장
- 개방형 질문에 적합

### 3. 실시간 시각화
- **도구 호출 추적**: 어떤 도구가 실행되는지 실시간 표시
- **Cypher 쿼리 표시**: 생성된 쿼리를 코드 블록으로 표시
- **그래프 시각화**: 쿼리 결과를 인터랙티브 그래프로 시각화
- **쿼리 결과**: 실행 결과를 깔끔하게 포맷팅

## 실습 순서

### 1. 패키지 설치

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

### 2. 환경 변수 설정

`.env.example` 파일을 `.env`로 복사하고 값을 설정하세요:

```bash
# .env
OPENAI_API_KEY=your-openai-api-key-here
NEO4J_URI=bolt://localhost:7687
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=your-neo4j-password-here
```

### 3. Neo4j 데이터베이스 준비

- Neo4j 서버가 실행 중이어야 합니다
- 지식 그래프 데이터가 이미 로드되어 있어야 합니다
- 벡터 인덱스(`text_content_vector_index`)가 생성되어 있어야 합니다

### 4. 챗봇 실행

```bash
streamlit run app.py
```

브라우저에서 `http://localhost:8501`로 자동 접속됩니다.