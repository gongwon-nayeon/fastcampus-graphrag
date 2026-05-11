# Neo4j Text2Cypher Golden Dataset

**Part 3. GraphRAG 핵심 패턴과 평가**

- Chapter 02. GraphRAG 평가하기
    - 📒 Clip 03. [프로젝트] GraphRAG 평가 데이터셋 활용하여 평가하기

> Text2Cypher 성능 평가를 위해 만들어진 골든 데이터셋(혹은 평가용 데이터셋)을 사용하여 Text2Cypher 를 평가합니다. (LLM-as-a-judge)

- Execution :
    1. 실행 가능 여부 (에러 없이 실행 되는지 / 실행은 되지만 빈 결과가 나오지는 않는지)
    2. 관계 방향이 올바른가?(스키마 정보를 잘 따랐는가)
- LLM-as-a-Judge :
    1. 생성된 쿼리가 사용자의 의도를 정확히 반영했는가?
    2. 질문에서 요구한 대상, 조건, 범위, 집계 방식이 모두 반영되었는가?
    3. 불필요한 조건이나 잘못 해석된 부분은 없는가?

| 구분 | 지표 | 설명 |
| --- | --- | --- |
| Execution | 실행 가능 여부 | 에러 없이 실행되는지 |
| Execution | 결과 반환 여부 | 빈 결과가 아닌지 |
| Execution | 관계 방향 검사 | 스키마 기반 방향 일치 여부 |
| LLM Judge | intent_score (1-5) | 사용자 의도 반영 정확도 |
| LLM Judge | completeness_score (1-5) | 대상/조건/범위/집계 반영 여부 |
| LLM Judge | correctness_score (1-5) | 불필요한 조건/잘못 해석 여부 |



https://github.com/neo4j-labs/text2cypher/tree/main/datasets/synthetic_gpt4o_demodbs (실습에 활용)

https://huggingface.co/datasets/neo4j/text2cypher-2024v1

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
copy .env.example .env
```

