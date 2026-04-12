import neo4j
from neo4j_graphrag.retrievers import VectorCypherRetriever
from neo4j_graphrag.types import RetrieverResultItem


def format_vectorcypher_result(record: neo4j.Record) -> RetrieverResultItem:
    """
    VectorCypher 검색 결과를 RetrieverResultItem으로 변환하여 정제

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


def get_vectorcypher_retrieval_query():
    """VectorCypher을 위한 검색 쿼리 (벡터 검색 + 주변 관계)

    vectorcypher_qdrant와 동일한 로직 사용:
    HAS_ENTITY 기반으로 컨텍스트를 확장하여 관련 정보 수집
    """
    return """
    // node = 벡터 검색으로 찾은 TextElement
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


def create_vectorcypher_retriever(driver, embedder, index_name):
    """VectorCypher 검색 Retriever 생성 (텍스트 벡터 + 주변 관계)"""
    retriever = VectorCypherRetriever(
        driver=driver,
        index_name=index_name,
        retrieval_query=get_vectorcypher_retrieval_query(),
        embedder=embedder,
        result_formatter=format_vectorcypher_result
    )
    print("[3/4] VectorCypher 검색 Retriever 생성")
    return retriever


def create_vectorcypher_tool(retriever):
    """VectorCypherRetriever를 Tool로 변환"""

    tool = retriever.convert_to_tool(
        name="vectorcypher_context_search",
        description="""
        텍스트 검색과 함께 문맥 정보를 종합적으로 제공합니다.

        검색 결과에 포함되는 정보:
        - 검색된 텍스트 내용
        - 해당 텍스트가 속한 목차 정보 (장/절 제목)
        - 상위 목차 및 문서 구조
        - 관련 도메인 엔티티 (인물, 조직, 기술 등)
        - 엔티티 간 관계 및 확장 컨텍스트

        사용 시나리오:
        - 특정 엔티티에 대한 종합 정보가 필요할 때 (회사, 제품, 기술, 인물, 모델 등)
        - 문서 내 여러 곳에 흩어진 관련 정보를 한번에 모아야 할 때
        - 내용과 함께 문서 상의 위치를 알고 싶을 때
        - "~에 대해 설명해줘" 같은 개방형 질문

        예시:
        - "업스테이지의 Solar 모델 성능이 어떤가요?"
        - "ChatGPT Health에 대해 알려주세요"
        - "OpenAI에 대해 관련 정보를 모두 찾아줘"
        - "EXAONE 모델의 특징은 무엇인가요?"
        """
    )

    return tool
