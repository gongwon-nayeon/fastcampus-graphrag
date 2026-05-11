# Text2Cypher Agent - 의료 지식 그래프 기반 질의응답

**Part 4. GraphRAG 에이전트와 챗봇 실전**

- Chapter 04. GraphRAG Agent 구축하기
    - 📒 Clip 06. [프로젝트] GraphRAG Agent 구현하기 (2) - 쿼리 생성 수정 자동화

> 의료 지식 그래프를 활용하여 자연어 질문을 Cypher 쿼리로 변환하고 실행하여 답변을 생성하는 LangGraph 기반 에이전트를 구축합니다.

## 워크플로우

```
사용자 질문
    ↓
[가드레일] DB 관련?
    ├─ No → 일반 답변
    └─ Yes
        ↓
    [생성] Cypher 쿼리 생성
        ↓
    [검증] 보안/문법 체크
        ├─ 실패 → [수정] (최대 5회)
        └─ 통과
            ↓
        [실행] Neo4j에서 실행
            ├─ 에러 → [수정] (최대 5회)
            ├─ 빈 결과 → [재생성] (최대 2회)
            └─ 성공
                ↓
            [답변] 마크다운 형식 답변 생성
```

## 파일 구조

```
graphrag_text2cypher_agent/
├── schema.py       # 환경변수 + Neo4j 드라이버 + 스키마 추출
├── state.py        # 그래프 상태 정의 (MessagesState 상속)
├── prompts.py      # 프롬프트 템플릿 (생성/수정/답변)
├── nodes.py        # 노드 함수들 (guardrail, generate, validate, execute, correct, answer)
├── main.py         # 그래프 구성 및 라우팅
└── .env            # 환경변수 (OPENAI_API_KEY, NEO4J_URI 등)
```


## 설치 및 실행

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

### 2. 환경변수 설정

```bash
copy .env.example .env
```

---

## 3. 랭그래프 스튜디오 실행

```bash
uv run langgraph dev
```