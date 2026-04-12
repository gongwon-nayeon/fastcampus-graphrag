# Vector Search

**Part 3. GraphRAG 핵심 패턴과 평가**

- Chapter 01. GraphRAG 구축하기
    - 📒 Clip 05. [실습] Vector Search - 문서 지식그래프에 벡터 인덱스 추가하고 의미 검색 구현하기

> PDF 문서 지식그래프에 GenAI 플러그인을 사용해 벡터 임베딩과 인덱스 추가하고 활용합니다.

---

## 프로젝트 구조

- **vector_index.ipynb**: TextElement 노드에 임베딩을 추가하고 벡터 인덱스를 생성합니다.
- **vectorsearch.py**: 생성된 벡터 인덱스를 활용하여 유사도 기반 벡터 검색을 수행합니다.

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

**Jupyter Notebook 사용시 커널 등록:**

```bash
.venv\Scripts\python.exe -m ipykernel install --user --name=vectorsearch --display-name="vectorsearch"
```


### 2. 환경변수 설정

```bash
cp .env.example .env
```

```bash
NEO4J_URI=neo4j+s://<your-instance>.databases.neo4j.io
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=your_password_here

OPENAI_API_KEY=your_openai_api_key_here
```

### 3. 벡터 인덱스 생성

`vector_index.ipynb` 노트북을 실행하여:
1. TextElement 노드에 임베딩 추가
2. 벡터 인덱스 생성


### 4. 벡터 검색 실행

```bash
python vectorsearch.py "AI와 관련된 내용"
```

---

## 기술 스택

### 임베딩 생성 (vector_index.ipynb)

**Neo4j GenAI Plugin**을 사용하여 Cypher 쿼리 내에서 직접 임베딩을 생성합니다.

- **핵심 함수**: `ai.text.embedBatch()` - 배치로 여러 텍스트의 임베딩 생성
- **모델**: OpenAI `text-embedding-3-small` (1536 차원)

**공식 문서**: https://neo4j.com/docs/genai/plugin/current/embeddings/

### 벡터 검색 (vectorsearch.py)

**neo4j-graphrag** Python 패키지의 `VectorRetriever`를 사용하여 벡터 검색을 수행합니다.

- **라이브러리**: `neo4j_graphrag.retrievers.VectorRetriever`
- **Embedder**: `neo4j_graphrag.embeddings.OpenAIEmbeddings`

**공식 문서**: https://neo4j.com/docs/neo4j-graphrag-python/current/api.html#vectorretriever

**사용 예시:**
```python
from neo4j_graphrag.retrievers import VectorRetriever
from neo4j_graphrag.embeddings import OpenAIEmbeddings

embedder = OpenAIEmbeddings(api_key=api_key, model="text-embedding-3-small")

retriever = VectorRetriever(
    driver=driver,
    index_name="textElementEmbedding",
    embedder=embedder,
    return_properties=["element_id", "content", "page"]
)

results = retriever.search(query_text="AI와 관련된 내용", top_k=5)

