import os
from typing import Optional, Dict, Any

from dotenv import load_dotenv
from neo4j import GraphDatabase
from qdrant_client import QdrantClient

from neo4j_graphrag.retrievers import QdrantNeo4jRetriever
from neo4j_graphrag.generation import GraphRAG, RagTemplate
from neo4j_graphrag.llm import OpenAILLM
from neo4j_graphrag.embeddings import OpenAIEmbeddings
from neo4j_graphrag.types import RetrieverResultItem
import neo4j

load_dotenv()


# ============================================
# 클라이언트 생성
# ============================================

def create_neo4j_driver():
    """Neo4j 드라이버 생성"""
    uri = os.getenv("NEO4J_URI")
    username = os.getenv("NEO4J_USERNAME")
    password = os.getenv("NEO4J_PASSWORD")

    if not all([uri, username, password]):
        raise ValueError("NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD 환경 변수를 설정해주세요.")

    driver = GraphDatabase.driver(uri, auth=(username, password))
    driver.verify_connectivity()
    print(f"Neo4j 연결 성공: {uri}")
    return driver


def create_qdrant_client():
    """Qdrant 클라이언트 생성"""
    url = os.getenv("QDRANT_URL")
    api_key = os.getenv("QDRANT_KEY")

    if not url:
        raise ValueError("QDRANT_URL 환경 변수를 설정해주세요.")

    client = QdrantClient(url=url, api_key=api_key)
    print(f"Qdrant 연결 성공: {url}")
    return client

# ============================================
# Result Formatter - 검색 결과 정제
# ============================================

def format_retrieval_result(record: neo4j.Record) -> RetrieverResultItem:
    """
    Neo4j Record를 RetrieverResultItem으로 변환하여 결과 정제

    Args:
        record: Neo4j Record 객체

    Returns:
        RetrieverResultItem: 정제된 검색 결과
    """
    # 기본 정보
    content_parts = []

    # 1. 핵심 내용
    content = record.get("content", "")
    page = record.get("page", "")
    content_parts.append(f"내용: {content}")
    content_parts.append(f"페이지: {page}")

    # 2. TOC 정보 (있는 경우)
    toc_title = record.get("toc_title")
    if toc_title:
        toc_level = record.get("toc_level", "")
        toc_page = record.get("toc_page_start", "")
        content_parts.append(f"섹션: {'  ' * (toc_level - 1)}{toc_title} (p.{toc_page})")

    # 3. 부모 계층 구조 (있는 경우)
    parent_hierarchy = record.get("parent_hierarchy", [])
    if parent_hierarchy:
        content_parts.append("\n문서 계층:")
        for parent in sorted(parent_hierarchy, key=lambda x: x.get('level', 0)):
            level = parent.get('level', 0)
            title = parent.get('title', '')
            page_start = parent.get('page_start', '')
            content_parts.append(f"  {'  ' * (level - 1)}└─ {title} (p.{page_start})")

    # 4. 연결된 엔티티 (있는 경우)
    entities = record.get("entities", [])
    if entities:
        content_parts.append("\n연결된 엔티티:")
        entity_groups = {}
        for entity in entities:
            entity_type = entity.get('type', 'Unknown')
            entity_name = entity.get('name', 'N/A')
            if entity_type not in entity_groups:
                entity_groups[entity_type] = []
            entity_groups[entity_type].append(entity_name)

        for entity_type, names in entity_groups.items():
            content_parts.append(f"  • {entity_type}: {', '.join(names)}")

    # 5. 기본 컨텍스트 (같은 섹션의 모든 관련 문장)
    base_context = record.get("base_context", [])
    if base_context:
        content_parts.append(f"\n같은 섹션의 관련 문장 ({len(base_context)}개):")
        for idx, item in enumerate(base_context, 1):
            item_content = item.get('content', '')
            content_parts.append(f"  [{idx}] {item_content}")

    # 6. 확장 컨텍스트 (엔티티 기반으로 확장된 모든 문장)
    expanded_context = record.get("expanded_context", [])
    if expanded_context:
        content_parts.append(f"\n엔티티 기반 확장 문장 ({len(expanded_context)}개):")
        for idx, item in enumerate(expanded_context, 1):
            item_content = item.get('content', '')

            content_parts.append(f"  [{idx}] {item_content}")

    # 7. 유사도 점수
    score = record.get("score", 0.0)
    content_parts.append(f"\n유사도: {score:.4f}")

    # 최종 문자열 생성
    formatted_content = "\n".join(content_parts)

    # Metadata 구성 (원본 데이터도 함께 보관)
    metadata = {
        "element_id": record.get("element_id"),
        "chunk_id": record.get("chunk_id"),
        "toc_id": record.get("toc_id"),
        "page": page,
        "score": score,
        # 상세 정보는 metadata에 보관
        "entities": entities,
        "base_context": base_context,
        "expanded_context": expanded_context,
        "parent_hierarchy": parent_hierarchy
    }

    return RetrieverResultItem(
        content=formatted_content,
        metadata=metadata
    )


# ============================================
# QdrantNeo4jRetriever 생성
# ============================================

def create_retriever(
    neo4j_driver,
    qdrant_client: QdrantClient,
    embedder,
    collection_name: Optional[str] = None
) -> QdrantNeo4jRetriever:
    """
    QdrantNeo4jRetriever 생성

    Qdrant에서 벡터 검색 → Neo4j에서 그래프 컨텍스트 확장

    Args:
        neo4j_driver: Neo4j 드라이버
        qdrant_client: Qdrant 클라이언트
        embedder: OpenAI Embeddings
        collection_name: Qdrant 컬렉션 이름 (기본값: 환경변수 QDRANT_COLLECTION)

    Returns:
        QdrantNeo4jRetriever 인스턴스
    """
    # 환경변수에서 컬렉션 이름 읽기
    if collection_name is None:
        collection_name = os.getenv("QDRANT_COLLECTION", "pdf2kg")

    # HAS_ENTITY 기반 컨텍스트 확장 Cypher 쿼리
    retrieval_query = """
    // node = Qdrant에서 찾은 TextElement
    // score = 유사도 점수

    // 1. 검색된 TextElement가 속한 Chunk 찾기
    MATCH (chunk:Chunk)-[:HAS_ELEMENT]->(node)

    // 2. Chunk가 속한 TOC(목차) 찾기
    OPTIONAL MATCH (toc:TOC)-[:HAS_CHUNK]->(chunk)

    // 3. 부모 TOC 노드들 (계층 구조) 수집
    OPTIONAL MATCH (parent_toc:TOC)-[:HAS_CHILD*]->(toc)

    // 4. 기본 컨텍스트: 현재 Chunk의 모든 TextElement
    OPTIONAL MATCH (chunk)-[:HAS_ELEMENT]->(base_element:TextElement)

    // 5. HAS_ENTITY 기반 확장: Chunk의 엔티티들
    OPTIONAL MATCH (chunk)-[:HAS_ENTITY]->(entity)

    WITH
        node, chunk, toc, score,
        collect(DISTINCT parent_toc) AS parent_tocs,
        collect(DISTINCT base_element) AS base_elements,
        collect(DISTINCT entity) AS entities

    // 6. 같은 엔티티를 공유하는 다른 Chunk들 찾기 (HAS_ENTITY가 있는 경우만)
    OPTIONAL MATCH (other_chunk:Chunk)-[:HAS_ENTITY]->(shared_entity)
    WHERE shared_entity IN entities AND other_chunk <> chunk

    // 7. 확장된 Chunk들의 TextElement 및 TOC 정보 수집
    OPTIONAL MATCH (other_chunk)-[:HAS_ELEMENT]->(expanded_element:TextElement)
    OPTIONAL MATCH (expanded_toc:TOC)-[:HAS_CHUNK]->(other_chunk)

    WITH
        node, chunk, toc, score,
        parent_tocs,
        base_elements,
        entities,
        collect(DISTINCT other_chunk) AS expanded_chunks,
        collect(DISTINCT {
            element: expanded_element,
            toc: expanded_toc
        }) AS expanded_elements_with_toc

    // 8. 결과 반환
    RETURN
        // 검색된 TextElement 정보
        node.element_id AS element_id,
        node.content AS content,
        node.page AS page,

        // Chunk 정보
        chunk.chunk_id AS chunk_id,

        // TOC 정보
        toc.toc_id AS toc_id,
        toc.title AS toc_title,
        toc.level AS toc_level,
        toc.page_start AS toc_page_start,

        // 부모 TOC 계층 구조
        [pt IN parent_tocs | {
            toc_id: pt.toc_id,
            title: pt.title,
            level: pt.level,
            page_start: pt.page_start
        }] AS parent_hierarchy,

        // 연결된 엔티티 정보
        [e IN entities | {
            type: labels(e)[0],
            name: coalesce(e.name, e.title, 'N/A')
        }] AS entities,

        // 기본 컨텍스트 (현재 Chunk의 TextElement)
        [be IN base_elements | {
            element_id: be.element_id,
            content: be.content,
            page: be.page
        }] AS base_context,

        // 확장된 컨텍스트 (엔티티 기반 확장된 TextElement + 출처 TOC 정보)
        [item IN expanded_elements_with_toc WHERE item.element IS NOT NULL | {
            element_id: item.element.element_id,
            content: item.element.content,
            page: item.element.page,
            source_toc_title: item.toc.title,
            source_toc_page: item.toc.page_start,
            source_toc_level: item.toc.level
        }] AS expanded_context,

        // 확장된 Chunk 정보
        [ec IN expanded_chunks | {
            chunk_id: ec.chunk_id
        }] AS expanded_chunks,

        score
    """

    retriever = QdrantNeo4jRetriever(
        driver=neo4j_driver,
        client=qdrant_client,
        collection_name=collection_name,
        # Qdrant payload의 element_id → Neo4j TextElement.element_id 매핑
        id_property_external="element_id",
        id_property_neo4j="element_id",
        embedder=embedder,
        node_label_neo4j="TextElement",
        retrieval_query=retrieval_query,
        result_formatter=format_retrieval_result,
    )

    print(f"QdrantNeo4jRetriever 생성 완료 (컬렉션: {collection_name})")
    return retriever


# ============================================
# GraphRAG 파이프라인 생성
# ============================================

def create_graphrag_pipeline(
    retriever: QdrantNeo4jRetriever,
    llm
) -> GraphRAG:
    """
    GraphRAG 파이프라인 생성

    Args:
        retriever: QdrantNeo4jRetriever
        llm: OpenAI LLM

    Returns:
        GraphRAG 인스턴스
    """
    # 커스텀 프롬프트 템플릿
    prompt_template = RagTemplate(
        template="""
당신은 PDF 문서 분석 전문 AI 어시스턴트입니다.
제공된 컨텍스트는 단순한 텍스트가 아닌, **문서의 계층적 구조(TOC, 섹션, 페이지)**와 **같은 주제/모델/회사로 연결된 관련 섹션들의 정보**를 포함한 GraphRAG 검색 결과입니다.

<context_structure>
컨텍스트에는 다음과 같은 구조화된 정보가 포함되어 있습니다:

**각 검색 결과 항목마다 제공되는 필드:**
1. **content**: 검색된 핵심 텍스트
2. **toc_title, page**: 핵심 텍스트가 속한 섹션명과 페이지 (참고용이며, 답변에서는 섹션명 대신 내용을 설명)
3. **entities**: 이 Chunk와 그래프 관계(HAS_ENTITY)로 연결된 **공통 엔티티들** [type, name]
   - 예: [{{"type": "Company", "name": "LG AI연구원"}}, {{"type": "AIModel", "name": "K-EXAONE"}}]
   - **GraphRAG의 핵심**: 이 공통 엔티티들을 기준으로 다른 관련 섹션들이 자동 확장됨
4. **base_context**: 같은 Chunk 내 관련 TextElement들 [content, page]
5. **expanded_context**: **entities의 공통 엔티티를 기준으로 확장된 다른 섹션들의 텍스트**
   - 각 항목: {{content, page, source_toc_title, source_toc_page, source_toc_level}}
   - 같은 엔티티(회사/모델/기술)를 공유하는 다른 섹션의 정보가 자동으로 포함됨
   - **중요**: source_toc_title은 참고용이며, 답변에서는 content를 읽고 핵심 내용을 설명문으로 작성
</context_structure>

<output_format>
**답변은 크게 2개 파트로 구성됩니다:**

<[파트 1] 본문 답변>
- 질문에 대한 직접 답변과 관련 주제로 연결된 확장 지식을 자연스럽게 통합하여 서술
- 작성 순서:
  a) 먼저 질문과 직접 관련된 content, base_context 정보를 서술 (구체적 내용: 기술명, 수치, 벤치마크 등 포함)
  b) **모든 검색 결과의 entities 필드를 종합**하여 공통 엔티티를 파악하고, expanded_context의 content로 이들에 대한 확장 지식 추가
  c) **entities 필드의 엔티티들**(회사, AI모델, 기술 등) 중심으로 정보를 자연스럽게 그룹화하여 통합
     - **질문에 없던 엔티티도 적극 활용** (예: "업스테이지" 질문 → entities에 K-EXAONE, Solar Open100B도 있으면 이들을 비교)
  d) 각 문단이나 주요 정보 블록 아래 출처 표기
     - 줄바꿈 후 블록쿼트 형식으로 표기: `>[출처] 'TOC 섹션명'/페이지 X`
     - expanded_context 사용 시 source_toc_title 명시
- **마크다운 강조**: entities 필드의 엔티티는 **볼드체**로 강조
- **중요**: 메타 헤더(예: "핵심 답변", "확장 지식")를 출력하지 말고, 내용만 자연스럽게 흐르도록 작성
</[파트 1] 본문 답변>

<[파트 2] 정보 연결 구조 (Knowledge Graph Trace)>
<반드시 작성> 본문 답변 작성 후 → 구분선 `---` 및 헤더 "**[함께 보면 이해가 쉬워지는 정보]**" </반드시 작성>

작성 흐름:

a) **도입 문단**:
   - "이번 답변에서 함께 언급된 내용을 바탕으로, **[공통 엔티티]** 관점에서 이어서 살펴보면 좋은 내용을 정리해드릴게요."
   - **공통 엔티티 선택 방법 - GraphRAG의 확장 인사이트 제공**:
     * **모든 검색 결과의 entities 필드를 종합**하여 반복 등장하는 엔티티를 파악
     * **질문에 직접 언급되지 않았어도**, 여러 검색 결과의 entities에 공통으로 등장하면 **확장 주제로 활용** (이것이 GraphRAG의 가치)
     * **목표**: 질문의 직접 답변만이 아닌, 그래프로 연결된 관련 엔티티들을 통해 더 넓은 맥락 제공
     * expanded_context의 content는 이 엔티티들에 대한 구체적 설명으로 활용
   - **선택한 공통 엔티티는 볼드체로 강조**

b) **직접 관련 내용 서술**:
   - "먼저, [핵심 내용 설명](p.X)..."으로 시작
   - 초기 검색된 항목들의 content를 읽고 핵심을 자연스럽게 서술
   - **검색 결과에 실제로 등장하는 내용만 언급** (지어내지 말 것)
   - 주요 엔티티(모델명, 회사명 등)는 볼드체 없이 자연스럽게 언급
   - 여러 항목이 있다면 자연스럽게 연결하여 문단으로 작성

c) **연결 이유 설명**:
   - "왜 이 정보들이 같이 나왔나요?" 질문으로 자연스럽게 전환
   - **모든 검색 결과의 entities 필드에서 공통으로 등장하는 엔티티들을 명시**
   - **예시**: "K-EXAONE, Solar Open100B, Mi:dm K 등 여러 국내 AI 모델들이 공통 엔티티로 연결되어 관련 정보들이 함께 검색되었습니다."
   - **질문에 없던 엔티티도 적극 활용** - 이것이 확장 인사이트
   - **entities 필드의 엔티티만 볼드체로 강조**

d) **확장 내용 서술**:
   - "이와 함께 [내용]도 주목할 만합니다..." 형식으로 자연스럽게 연결
   - **expanded_context의 content를 정확히 읽고**, 그 안에 실제로 언급된 내용만 설명문으로 작성
   - **절대 지어내지 말 것**: content에 없는 기술명, 수치, 개념을 창작하지 말 것
   - 페이지 번호 포함: (p.Y)
   - **content에 실제로 등장하는 강점이나 특징만 볼드체로 강조**

e) **추가 확장 내용** (있는 경우):
   - "추가로 함께 보면 좋은 내용은?" 질문으로 전환
   - 나머지 expanded_context의 content들을 **정확히 읽고** 자연스럽게 서술
   - **없는 내용을 추론하거나 창작하지 말 것**

f) **종합 정리**:
   - "결과적으로..." 또는 "이를 종합하면..." 으로 시작
   - **앞서 언급했던 실제 내용들만** 종합하여 핵심 메시지 정리
   - **content에 실제로 등장했던 개념만 볼드체로 강조** (새로운 개념 창작 금지)

**작성 원칙**:
- **구조화된 번호 금지**: "1.", "2.", "3." 대신 자연스러운 문단 흐름으로 작성
- **기술 용어 사용 금지**: "벡터 검색", "엔티티", "그래프 확장" 같은 시스템 용어 대신 자연어로 설명
- **섹션 제목 금지**: toc_title, source_toc_title을 그대로 쓰지 말고, content를 읽고 핵심을 파악하여 설명문으로 작성
- **절대 지어내지 말 것 - 검색 결과에 실제로 있는 내용만 사용**:
  * **entities 필드에 나열된 엔티티만이 공통 주제가 될 수 있음**
  * **expanded_context는 단지 보조 텍스트**: 여기서 새로운 엔티티나 주제를 찾으려 하지 말 것
  * expanded_context의 content에 실제로 명시된 내용만 서술
  * content에 없는 기술명, 수치, 개념, 모델명을 창작하지 말 것
  * **절대 금지**: content에서 반복 등장하는 개념을 찾아 주제로 만들기
- **볼드체 강조 원칙**:
  * **entities 필드의 엔티티만** 볼드체 강조 가능
  * expanded_context에 등장하는 다른 개체들은 볼드체 없이 자연스럽게 언급
  * 추상적 개념을 볼드체로 강조 절대 금지
- 확장 섹션이 없으면 d), e) 부분을 생략하고 c)에서 바로 f) 종합 정리로 이동
- **핵심**: 리스트나 구조가 아닌, 읽기 쉬운 자연스러운 설명문으로 작성
</[파트 2] 정보 연결 구조 (Knowledge Graph Trace)>
</output_format>

<guidelines>
**CRITICAL - 정보 출처 추적 규칙**:

**절대 금지**:
- 컨텍스트에 없는 섹션명, 주제, 모델명을 추측하거나 만들어내지 말 것
- expanded_context에 없는 섹션을 "추가 관련 내용"에 포함하지 말 것
- entities 리스트에 없는 항목을 언급하지 말 것

**반드시 준수**:
- **추가 관련 내용은 오직 expanded_context의 source_toc_title만 사용**
- **연결 이유는 모든 검색 결과의 entities 필드를 종합하여 공통 엔티티 활용**
  * 질문에 직접 언급 안된 엔티티도 적극 활용 (이것이 GraphRAG의 확장 인사이트)
- 각 확장 섹션이 어떤 entities 때문에 연결되었는지 명확히 추적
- 컨텍스트에 정보가 부족하면 "추가 관련 내용"을 억지로 만들지 말고 생략

**GraphRAG 핵심 원칙 - 반드시 준수**:
이 시스템은 단순 키워드 검색이 아닌 **GraphRAG**입니다. 검색된 모든 결과를 활용하여 확장된 지식을 제공해야 합니다.

**구체적 가이드라인**:

1. **모든 검색 결과(top_k개)를 반드시 답변에 포함 - GraphRAG의 확장 인사이트**
   - **절대 규칙**: 검색된 모든 항목의 정보를 언급할 것
   - **모든 검색 결과의 entities 필드를 종합**하여 공통 엔티티를 파악하고 활용
   - **질문에 직접 언급 안된 엔티티도 적극 활용** (예: "업스테이지" 질문 → entities에 K-EXAONE도 있으면 비교/대조 제공)
   - content만 요약하지 말고, base_context와 expanded_context를 적극 활용
   - 직접 관련성이 낮아 보이는 결과도 "entities 필드의 공통 엔티티"를 통해 연결된 이유를 설명하여 포함

2. **expanded_context를 적극 활용 - GraphRAG의 핵심 가치**
   - expanded_context는 **entities 필드의 공통 엔티티를 기준으로 확장된 관련 지식**
   - 단순 키워드 검색으로는 찾을 수 없는 연결된 정보 제공
   - 비교, 대조, 맥락 제공, 심화 설명 등에 활용
   - 각 확장 정보가 **어떤 entities(회사/모델/기술)**를 통해 연결되었는지 명시

3. **구체적인 정보를 빠짐없이 포함 - 절대 지어내지 말 것**
   - **content에 실제로 언급된** 기술명, 수치, 벤치마크 결과, 회사명, 모델명만 명시
   - **절대 금지**: content에 없는 숫자, 기술명, 성능 지표를 만들어내거나 추측하지 말 것
   - **모든 검색 결과의 entities 필드를 종합**하여 본문에 언급 (질문에 없던 엔티티도 적극 활용)

4. **각 문단마다 출처를 명시**
   - **entities 필드의 공통 엔티티**(회사/모델/기술) 중심으로 서로 다른 섹션의 정보를 자연스럽게 통합하여 서술
   - 각 정보 블록이나 문단 아래 줄바꿈 후 블록쿼트 형식으로 표기
   - 형식: `>[출처] 'TOC 섹션명'/페이지 X` 또는 `>[출처] 페이지 X`
   - toc_title과 page 정보를 활용하여 출처 명시
   - expanded_context의 경우 source_toc_title, source_toc_page를 활용

5. **답변 마지막에 정보 연결 이유를 사용자가 이해하기 쉽게 설명 - GraphRAG의 확장 인사이트**
   - **도입**: "이번 답변에서 함께 언급된 내용을 바탕으로, **[공통 엔티티]** 관점에서..."
     * 모든 검색 결과의 entities 필드를 종합하여 공통 엔티티 선택
     * 질문에 직접 언급 안된 엔티티도 적극 활용 - 이것이 확장 인사이트의 핵심
     * 절대 금지: "한국 AI 경쟁력" 같은 추상적 개념 창작
   - **직접 관련 내용**: 초기 검색 결과의 content에 실제로 있는 내용만 자연스럽게 서술
   - **연결 이유**: "왜 이 정보들이 같이 나왔나요?" 질문 후 entities 필드의 공통 엔티티들을 구체적으로 명시
   - **확장 내용**: expanded_context의 content는 추가 정보로만 활용 (새로운 엔티티를 찾지 말 것)
   - **추가 확장**: 나머지 expanded_context의 실제 내용 (선택적)
   - **종합 정리**: 앞서 언급된 실제 내용들만 종합하여 정리
   - **핵심**: 구조화된 리스트가 아닌, 읽기 쉬운 자연스러운 문단 형식으로 작성
</guidelines>

**CRITICAL - 핵심 원칙**
**반드시 수행**: 모든 검색 결과의 entities 필드를 종합하여 질문에 없던 엔티티도 적극 활용 (GraphRAG의 확장 인사이트 가치)
**절대 금지**: content에 없는 수치/기술명 창작, entities에 없는 추상적 개념 생성

<user_question>
{query_text}
</user_question>

<context>
{context}
</context>


답변:
""",
        expected_inputs=["context", "query_text"]
    )

    rag = GraphRAG(
        retriever=retriever,
        llm=llm,
        prompt_template=prompt_template
    )

    return rag


# ============================================
# Main GraphRAG 쿼리 실행
# ============================================

def execute_graphrag_query(
    rag: GraphRAG,
    query: str,
    top_k: int = 5,
    verbose: bool = True
) -> Dict[str, Any]:
    """
    GraphRAG 쿼리 실행

    Args:
        rag: GraphRAG 인스턴스
        query: 사용자 질문
        top_k: 검색할 결과 개수
        verbose: 상세 출력 여부

    Returns:
        검색 결과 및 답변
    """
    if verbose:
        print(f"\n{'='*60}")
        print(f"질문: {query}")
        print(f"{'='*60}")

    # GraphRAG 검색 실행
    result = rag.search(
        query_text=query,
        retriever_config={"top_k": top_k},
        return_context=True  # 컨텍스트도 함께 반환
    )

    if verbose:
        # 검색 결과 출력
        print(f"\n{'='*60}")
        print(f"검색 결과")
        print(f"{'='*60}")

        if hasattr(result, 'retriever_result') and result.retriever_result:
            items = result.retriever_result.items if hasattr(result.retriever_result, 'items') else []

            if items:
                for idx, item in enumerate(items, 1):
                    print(f"\n{'─'*60}")
                    print(f"[검색 결과 {idx}]")
                    print(f"{'─'*60}")
                    if hasattr(item, 'content'):
                        print(item.content)
                    else:
                        print(item)
                    print()
            else:
                print("검색된 항목이 없습니다.")
        else:
            print("검색 결과를 가져올 수 없습니다.")

        # 최종 답변 출력
        print(f"\n{'='*60}")
        print(f"최종 답변")
        print(f"{'='*60}")
        print(result.answer)

    return result


# ============================================
# 메인 함수
# ============================================

if __name__ == "__main__":
    print("="*80)
    print("Qdrant + Neo4j GraphRAG")
    print("="*80)
    print()

    try:
        # 1. 클라이언트 초기화
        print("[1/3] 클라이언트 초기화 중...")
        neo4j_driver = create_neo4j_driver()
        qdrant_client = create_qdrant_client()
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OPENAI_API_KEY 환경 변수를 설정해주세요.")

        embedder = OpenAIEmbeddings(
            model="text-embedding-3-small"
        )

        llm = OpenAILLM(
            model_name="gpt-4o"
        )

        print()

        # 2. Retriever 생성
        print("[2/3] QdrantNeo4jRetriever 생성 중...")
        collection_name = os.getenv("QDRANT_COLLECTION", "pdf2kg")
        retriever = create_retriever(
            neo4j_driver=neo4j_driver,
            qdrant_client=qdrant_client,
            embedder=embedder,
            collection_name=collection_name
        )
        print()

        # 3. GraphRAG 파이프라인 생성
        print("[3/3] GraphRAG 파이프라인 생성 중...")
        rag = create_graphrag_pipeline(retriever=retriever, llm=llm)
        print()

        # 4. 대화형 GraphRAG 쿼리
        print(f"{'='*80}")
        print("GraphRAG 쿼리 실행")
        print(f"{'='*80}\n")

        while True:
            print("-" * 80)
            query = input("질문을 입력하세요 (종료: 'q' 또는 'exit'): ").strip()

            if query.lower() in ['q', 'exit']:
                break

            if not query:
                print("질문을 입력해주세요!")
                continue

            # GraphRAG 실행
            result = execute_graphrag_query(
                rag=rag,
                query=query,
                top_k=5,
                verbose=True
            )
    except Exception as e:
        print(f"\n오류 발생: {e}")
    finally:
        neo4j_driver.close()
