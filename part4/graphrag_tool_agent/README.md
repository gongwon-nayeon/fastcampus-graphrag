# GraphRAG Tool Agent

**Part 4. GraphRAG 에이전트와 챗봇 실전**

- Chapter 01. GraphRAG Agent 구축하기
    - 📒 Clip 03. [프로젝트] GraphRAG를 위한 도구 정의하기
    - 📒 Clip 04. [프로젝트] GraphRAG Agent 구현하기 (1) - 검색 방식 자동화

> LangChain의 `create_agent`를 사용하여 4가지 GraphRAG 도구를 활용하는 AI Agent를 구축합니다. Agent가 질문을 분석하여 적절한 도구(Schema Introspection, Text2Cypher, Cypher Execution, Vector Search)를 자동으로 선택하고 실행합니다.

---

## 시스템 아키텍처

```
┌─────────────────────────────────────────────────────────────────┐
│                          사용자 질문                              │
└─────────────────────┬───────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│                   LangChain Agent                               │
│                    (create_agent)                               │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ LLM이 질문 분석 → Tool Description 매칭 → 최적 Tool 선택     │   │
│  └──────────────────────────────────────────────────────────┘   │
└───────┬──────────┬──────────┬──────────┬────────────────────────┘
        │          │          │          │
        ▼          ▼          ▼          ▼
    ┌──────┐  ┌──────┐  ┌──────┐  ┌──────┐
    │@tool │  │@tool │  │@tool │  │@tool │
    │  1   │  │  2   │  │  3   │  │  4   │
    └──┬───┘  └──┬───┘  └──┬───┘  └──┬───┘
       │         │         │         │
       ▼         ▼         ▼         ▼
┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐
│  Schema  │ │Generate  │ │ Execute  │ │  Vector  │
│Introspec │ │ Cypher   │ │  Cypher  │ │  Search  │
│   tion   │ │  Query   │ │  Query   │ │(VectorCy │
│          │ │(Text2Cyp │ │          │ │pher)     │
└────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘
     │            │            │            │
     │            │            ▼            │
     │            │      ┌──────────┐       │
     │            │      │  Neo4j   │       │
     │            └─────>│  Driver  │<──────┘
     └──────────────────>│          │
                         └────┬─────┘
                              │
                              ▼
                    ┌─────────────────────┐
                    │   실행 결과          │
                    └─────────┬───────────┘
                              │
                              ▼
                    ┌─────────────────────┐
                    │   Agent가 답변 생성  │
                    └─────────────────────┘
```

## 주요 기능

### 4가지 GraphRAG 도구

Agent는 질문 유형에 따라 적절한 도구를 자동으로 선택합니다:

1. **Schema Introspection** (`@tool`)
   - Neo4j 그래프 스키마 조회 (노드, 관계, 속성)
   - 예: "스키마를 보여줘"

2. **Generate Cypher Query** (`@tool`)
   - 자연어 → Cypher 쿼리 변환 (Text2Cypher)
   - 특정 엔티티 검색, 관계 질의
   - 예: "OpenAI에 대해", "업스테이지의 CEO는?"

3. **Execute Cypher Query** (`@tool`)
   - 생성된 Cypher 쿼리를 Neo4j에서 실행
   - 구조화된 결과 반환 (최대 20개 레코드)

4. **Vector Search** (`@tool`)
   - 의미 기반 벡터 검색 + 그래프 컨텍스트 확장
   - HAS_ENTITY 관계 기반 연관 정보 수집
   - 예: "AI 윤리에 대해 설명해줘"

---

## 실행 방법

### 1. 사전 준비: PDF 지식그래프 구축

이 프로젝트는 **part2/pdf2kg**를 통해 PDF 데이터가 Neo4j에 이미 적재되어 있어야 합니다.

```bash
cd ../../part2/pdf2kg
python pdf2kg.py
python pdf2kg_2.py
```

벡터 검색을 위한 인덱스 생성:


### 2. 벡터 임베딩 및 인덱스 생성

벡터 검색을 위해서는 먼저 `TextElement`와 `TableElement` 노드의 `content` 속성에 대한 벡터 임베딩을 생성하고 인덱스를 만들어야 합니다.

#### 2-1. TextElement 임베딩 생성

```cypher
// TextElement 배치 임베딩 생성 (100개씩)
CYPHER 25
MATCH (t:TextElement WHERE t.content IS NOT NULL AND t.embedding IS NULL)
WITH collect(t) AS elements,
     [elem IN collect(t) | elem.content] AS texts
     LIMIT 100
CALL ai.text.embedBatch(
    texts,
    'openai',
    {
        token: '<YOUR_OPENAI_API_KEY>',
        model: 'text-embedding-3-small'
    }
) YIELD index, vector
WITH elements[index] AS element, vector
SET element.embedding = vector
RETURN count(*) AS embedded_count;
```

위 쿼리를 임베딩이 모두 생성될 때까지 반복 실행하세요.

#### 2-2. TableElement 임베딩 생성

```cypher
// TableElement 배치 임베딩 생성 (100개씩)
CYPHER 25
MATCH (t:TableElement WHERE t.content IS NOT NULL AND t.embedding IS NULL)
WITH collect(t) AS elements,
     [elem IN collect(t) | elem.content] AS texts
     LIMIT 100
CALL ai.text.embedBatch(
    texts,
    'openai',
    {
        token: '<YOUR_OPENAI_API_KEY>',
        model: 'text-embedding-3-small'
    }
) YIELD index, vector
WITH elements[index] AS element, vector
SET element.embedding = vector
RETURN count(*) AS embedded_count;
```

#### 2-3. 벡터 인덱스 생성

- 본 실습에서는 main.py 함수 실행 시 자동 생성됩니다. 아래는 `CREATE VECTOR INDEX`활용한 벡터 인덱스 생성 예시입니다.

TextElement용 벡터 인덱스:

```cypher
CREATE VECTOR INDEX text_content_vector_index IF NOT EXISTS
FOR (t:TextElement)
ON t.embedding
OPTIONS {
  indexConfig: {
    `vector.dimensions`: 1536,
    `vector.similarity_function`: 'cosine'
  }
}
```

TableElement용 벡터 인덱스:

```cypher
CREATE VECTOR INDEX table_content_vector_index IF NOT EXISTS
FOR (t:TableElement)
ON t.embedding
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

**임베딩 생성 확인**:

```cypher
// TextElement 임베딩 확인
MATCH (t:TextElement)
WHERE t.content IS NOT NULL
WITH count(t) as total
MATCH (t:TextElement)
WHERE t.embedding IS NOT NULL
RETURN total, count(t) as with_embedding,
       count(t) * 100.0 / total as percentage;

// TableElement 임베딩 확인
MATCH (t:TableElement)
WHERE t.content IS NOT NULL
WITH count(t) as total
MATCH (t:TableElement)
WHERE t.embedding IS NOT NULL
RETURN total, count(t) as with_embedding,
       count(t) * 100.0 / total as percentage;
```

### 3. 패키지 설치

```bash
# uv 설치 (권장)
# Windows (PowerShell)
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"

# macOS / Linux
curl -LsSf https://astral.sh/uv/install.sh | sh
```

```bash
cd part4/graphrag_tool_agent
```

```bash
# 방법 1: uv sync 사용 (권장)
uv sync
.venv\Scripts\activate  # Windows
source .venv/bin/activate  # macOS/Linux
```

또는

```bash
# 방법 2: pip 사용
pip install -r requirements.txt
```

### 4. 환경 변수 설정

```bash
cp .env.example .env
```

`.env` 파일을 편집하여 설정:

```
NEO4J_URI=bolt://localhost:7687
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=your_password

OPENAI_API_KEY=your_openai_api_key
```

### 5. 도구 테스트

(Clip 03. [프로젝트] GraphRAG를 위한 도구 정의하기)

Agent 구현 전에 개별 도구들이 정상적으로 동작하는지 테스트합니다.

```bash
python tools.py
```

### 6. Agent 실행

(Clip 04. [프로젝트] GraphRAG Agent 구현하기 (1) - 검색 방식 자동화)

```bash
python main.py
```
