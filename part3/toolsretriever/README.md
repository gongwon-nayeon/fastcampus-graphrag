# ToolsRetriever - PDF 지식그래프 기반 GraphRAG

**Part 3. GraphRAG 핵심 패턴과 평가**

- Chapter 01. GraphRAG 구축하기
    - 📒 Clip 09. [프로젝트] 다양한 검색기반 GraphRAG 파이프라인 완성하기 - ToolsRetriever

> PDF 문서에서 구축한 지식그래프를 활용하여 다양한 검색 방식을 적절히 조합하는 ToolsRetriever 기반 GraphRAG 시스템

---

## 전체 시스템 아키텍처

Neo4j 의 ToolsRetriever : https://neo4j.com/blog/developer/introducing-toolsretriever-graphrag-python-package/

```
┌─────────────────────────────────────────────────────────────────┐
│                          사용자 질문                              │
└─────────────────────┬───────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│                      ToolsRetriever                              │
│                   (질문 분석 및 Tool 선택)                         │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ LLM이 질문 분석 → Tool Description 매칭 → 최적 Tool 선택 │  │
│  └──────────────────────────────────────────────────────────┘  │
└───────┬──────────┬──────────┬──────────┬───────────────────────┘
        │          │          │          │
        ▼          ▼          ▼          ▼
    ┌──────┐  ┌──────┐  ┌──────┐  ┌──────┐
    │ Tool │  │ Tool │  │ Tool │  │ Tool │
    │  1   │  │  2   │  │  3   │  │  4   │
    └──┬───┘  └──┬───┘  └──┬───┘  └──┬───┘
       │         │         │         │
       ▼         ▼         ▼         ▼
┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐
│  Text    │ │  Table   │ │ Vector   │ │ Text2    │
│  Vector  │ │  Vector  │ │ Cypher   │ │ Cypher   │
│Retriever │ │Retriever │ │Retriever │ │Retriever │
└────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘
     │            │            │            │
     └────────────┴────────────┴────────────┘
                  │
                  ▼
        ┌─────────────────────┐
        │   검색 결과 (Context)│
        └─────────┬───────────┘
                  │
                  ▼
        ┌─────────────────────┐
        │   RagTemplate       │
        │  (프롬프트 생성)     │
        └─────────┬───────────┘
                  │
                  ▼
        ┌─────────────────────┐
        │   LLM (GPT-4o)      │
        │   (답변 생성)        │
        └─────────┬───────────┘
                  │
                  ▼
        ┌─────────────────────┐
        │    최종 답변         │
        └─────────────────────┘
```

## 주요 기능

### 4가지 검색 방식 자동 선택

ToolsRetriever가 질문 유형에 따라 최적의 검색 방법을 자동으로 선택합니다:

1. **텍스트 벡터 검색**: TextElement의 content 기반 의미 검색
2. **테이블 벡터 검색**: TableElement의 content 기반 표 데이터 검색
3. **VectorCypher**: 벡터 검색 + 주변 관계 탐색 (목차, 엔티티, 관계 포함)
4. **Text2Cypher**: 그래프 구조 기반 정확한 질의 (목차, 페이지, 엔티티 관계)

---

## 실행 방법

### 1. 사전 준비: PDF 지식그래프 구축

이 프로젝트는 **part2/pdf2kg**를 통해 PDF 데이터가 Neo4j에 이미 적재되어 있어야 합니다.

```bash
cd ../../part2/pdf2kg
python pdf2kg.py
python pdf2kg_2.py
```

### 2. 패키지 설치

```bash
# uv 설치 (권장)
# Windows (PowerShell)
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"

# macOS / Linux
curl -LsSf https://astral.sh/uv/install.sh | sh
```

```bash
cd part3/toolsretriever
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

### 3. 환경 변수 설정

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

### 4. 벡터 임베딩 및 인덱스 생성

벡터 검색을 위해서는 먼저 `TextElement`와 `TableElement` 노드의 `content` 속성에 대한 벡터 임베딩을 생성하고 인덱스를 만들어야 합니다.

#### 4-1. TextElement 임베딩 생성

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

#### 4-2. TableElement 임베딩 생성

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

#### 4-3. 벡터 인덱스 생성

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

### 5. GraphRAG 실행


```bash
python main.py
```

```
toolsretriever/
├── config.py                    # Neo4j/OpenAI 초기화 및 벡터 인덱스 생성
├── schema.py                    # Neo4j 스키마 동적 조회 및 변환
├── vector_retriever.py          # VectorRetriever
├── vectorcypher_retriever.py    # VectorCypherRetriever
├── text2cypher_retriever.py     # Text2CypherRetriever
├── prompts.py                   # GraphRAG 프롬프트 템플릿
├── main.py                      # 메인 실행 파일
└── README.md
```
