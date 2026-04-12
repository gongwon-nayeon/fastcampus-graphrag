# Text2Cypher(Reasoning)

**Part 3. GraphRAG 핵심 패턴과 평가**

- Chapter 01. GraphRAG 구축하기
    - 📒 Clip 04. [프로젝트] 의료 지식그래프 기반 의사결정을 위한 추론 검색 구현하기

> 의료 지식그래프를 기반으로 추론이 필요한 사용자의 질문에 대해 답변하는 GraphRAG를 구현합니다.

---

## 실습 순서

### 1. 패키지 설치

Python 3.13

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
cp .env.example .env
```

```bash
OPENAI_API_KEY=sk-your_openai_api_key_here
```


### 3. 의료 지식그래프 로드 (선행 작업)

graphrag_reasoning.py를 실행하기 전에 의료 지식그래프를 Neo4j에 로드해야 합니다:

```bash
# part2/csv2kg 폴더로 이동
cd ../../part2/medical2kg

# 데이터 로드 실행
python medical2kg.py
```

---

### 4. Text2Cypher 추론 기반 GraphRAG 실행

```bash
python graphrag_reasoning.py
```
