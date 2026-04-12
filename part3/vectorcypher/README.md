# VectorCypher Search

**Part 3. GraphRAG 핵심 패턴과 평가**

- Chapter 01. GraphRAG 구축하기
    - 📒 Clip 06. [프로젝트] Vector + Graph - 의미 검색과 서브 그래프 함께 가져오는 하이브리드 GraphRAG 구현하기

> law2kg로 구축한 법령 해석례 지식그래프에 VectorCypher 검색을 적용하여, 벡터 검색으로 관련 질의를 찾고 그 주변 그래프(답변, 이유, 법조문, 법령)를 함께 가져오는 하이브리드 GraphRAG를 구현합니다.

---

## VectorCypher Retriever란?

**VectorCypher Retriever**는 벡터 검색과 그래프 탐색을 결합한 하이브리드 검색 방식입니다:

1. **벡터 검색 단계**: 임베딩 기반으로 의미적으로 유사한 노드를 먼저 검색
2. **그래프 탐색 단계**: 검색된 노드를 시작점으로 Cypher 쿼리로 연결된 정보를 추가 수집

이를 통해 **의미적 유사성**(벡터)과 **구조적 관계**(그래프)를 동시에 활용할 수 있습니다.

---

## 그래프 구조

law2kg를 통해 구축된 법령 해석례 그래프 구조:

```
(LegalInterpretation - 해석례)
      │
      ├── HAS_QUESTION → (Question - 질의) ← 🎯 벡터 검색 대상
      │                      │
      │                      └── ANSWERED_BY → (Answer - 회답)
      │                                           │
      │                                           └── SUPPORTED_BY → (Reason - 이유)
      │
      ├── INTERPRETS → (Law - 주법령)
      │
      └── CITES → (Law/Article/Paragraph/Item - 인용 법조문)
```

**검색 전략**:
- **벡터 검색**: `Question` 노드의 `text` 속성에 대한 임베딩 벡터 검색
- **그래프 탐색**: 검색된 Question에 연결된 Answer, Reason, 관련 법령 및 조문 계층 구조를 Cypher로 수집

---

## 실습 순서

### 1. 사전 준비: law2kg 데이터 적재

이 프로젝트는 **part2/law2kg**를 통해 법령 및 해석례 데이터가 Neo4j에 이미 적재되어 있어야 합니다.

```bash
cd ../../part2/law2kg
# law2kg README.md의 지침에 따라 데이터 적재
python step1_load_laws.py
python step2_link_interpretations.py
```

### 2. 패키지 설치

Python 3.13

```bash
# uv 설치
# Windows (PowerShell)
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"

# macOS / Linux
curl -LsSf https://astral.sh/uv/install.sh | sh
```

```bash
cd part3/vectorcypher
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

### 3. 환경변수 설정

```bash
cp .env.example .env
```

```bash
NEO4J_URI=neo4j+s://<your-instance>.databases.neo4j.io
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=your_password_here

OPENAI_API_KEY=your_openai_api_key_here
```

---

## 4. 벡터 임베딩 및 인덱스 생성

VectorCypher 검색을 위해서는 먼저 `Question` 노드의 `text` 속성에 대한 벡터 임베딩을 생성하고 인덱스를 만들어야 합니다.

### 4-1. Question 노드에 벡터 임베딩 추가

```cypher
:param token => 'sk-...';
:param batch_size => 100;

// 배치 임베딩 생성 (100개씩)
CYPHER 25
MATCH (q:Question WHERE q.text IS NOT NULL AND q.embedding IS NULL)
WITH collect(q) AS questions,
     [question IN collect(q) | question.text] AS texts
     LIMIT $batch_size
CALL ai.text.embedBatch(
    texts,
    'openai',
    {
        token: $token,
        model: 'text-embedding-3-small'
    }
) YIELD index, vector
WITH questions[index] AS question, vector
SET question.embedding = vector
RETURN count(*) AS embedded_count;
```

### 4-2. 벡터 인덱스 생성

```cypher
CREATE VECTOR INDEX question_embedding_index IF NOT EXISTS
FOR (q:Question)
ON q.embedding
OPTIONS {
  indexConfig: {
    `vector.dimensions`: 1536,
    `vector.similarity_function`: 'cosine'
  }
}
```

**인덱스 확인**:

```cypher
SHOW INDEXES
```

**벡터 검색 테스트**:

```cypher
// 벡터 검색 테스트 (샘플 벡터로)
CALL db.index.vector.queryNodes(
  'question_embedding_index',
  5,
  [0.1, 0.2, 0.3, ..., 0.5]  // 1536차원 벡터
)
YIELD node, score
RETURN node.text, score
```

---

## 5. VectorCypher 기반 GraphRAG 실행

```bash
python vectorcypher.py
```