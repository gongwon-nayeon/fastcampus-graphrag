import os
from typing import Dict, Any, List
from dotenv import load_dotenv
import neo4j
from langchain.tools import tool
from neo4j_graphrag.llm import OpenAILLM
from neo4j_graphrag.embeddings.openai import OpenAIEmbeddings
from neo4j_graphrag.retrievers import VectorCypherRetriever
from neo4j_graphrag.types import RetrieverResultItem

load_dotenv()

_driver = None
_llm = None
_embedder = None
_vectorcypher_retriever = None


def initialize_tools():
    global _driver, _llm, _embedder, _vectorcypher_retriever

    if _driver is None:
        uri = os.getenv("NEO4J_URI")
        username = os.getenv("NEO4J_USERNAME", "neo4j")
        password = os.getenv("NEO4J_PASSWORD")
        _driver = neo4j.GraphDatabase.driver(uri, auth=(username, password))
        print(f"✓ Neo4j 연결: {uri}")

    if _llm is None:
        _llm = OpenAILLM(
            model_name="gpt-4o",
            model_params={"temperature": 0}
        )
        print("✓ OpenAI LLM 초기화")

    if _embedder is None:
        _embedder = OpenAIEmbeddings(model="text-embedding-3-small")
        print("✓ OpenAI Embeddings 초기화")

    if _vectorcypher_retriever is None:
        _vectorcypher_retriever = _create_vectorcypher_retriever()
        print("✓ VectorCypher Retriever 생성")

    return _driver, _llm, _embedder


def _create_vectorcypher_retriever():
    """VectorCypher Retriever 생성 (벡터 검색 + 그래프 컨텍스트 확장)"""

    retrieval_query = """
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

    // 6. 같은 엔티티를 공유하는 다른 Chunk들 찾기
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
        elementId(node) AS element_id,
        elementId(chunk) AS chunk_id,
        elementId(toc) AS toc_id,
        node.content AS content,
        node.page AS page,
        score,

        // TOC 정보
        toc.title AS toc_title,
        toc.level AS toc_level,
        toc.page_start AS toc_page_start,

        // 부모 계층 구조
        [p IN parent_tocs | {
            title: p.title,
            level: p.level,
            page_start: p.page_start
        }] AS parent_hierarchy,

        // 엔티티 정보
        [e IN entities | {
            type: labels(e)[0],
            name: e.name
        }] AS entities,

        // 기본 컨텍스트 (같은 Chunk)
        [b IN base_elements | {
            content: b.content,
            page: b.page
        }] AS base_context,

        // 확장된 컨텍스트 (엔티티 기반)
        [item IN expanded_elements_with_toc WHERE item.element IS NOT NULL | {
            content: item.element.content,
            page: item.element.page,
            source_toc_title: item.toc.title,
            source_toc_page: item.toc.page_start,
            source_toc_level: item.toc.level
        }] AS expanded_context

    ORDER BY score DESC
    """

    def format_result(record: neo4j.Record) -> RetrieverResultItem:
        """검색 결과를 포맷팅"""
        content_parts = []

        # 핵심 내용
        content = record.get("content", "")
        page = record.get("page", "")
        content_parts.append(f"[내용] {content}")
        content_parts.append(f"[페이지] {page}")

        # TOC 정보
        toc_title = record.get("toc_title")
        if toc_title:
            content_parts.append(f"[섹션] {toc_title}")

        # 엔티티
        entities = record.get("entities", [])
        if entities:
            entity_strs = [f"{e['type']}: {e['name']}" for e in entities]
            content_parts.append(f"[연결된 엔티티] {', '.join(entity_strs)}")

        # 기본 컨텍스트
        base_context = record.get("base_context", [])
        if base_context and len(base_context) > 1:
            content_parts.append(f"\n[같은 섹션의 관련 문장 {len(base_context)}개]")
            for idx, item in enumerate(base_context, 1):
                content_parts.append(f"  {idx}. {item['content']}")

        # 확장 컨텍스트
        expanded_context = record.get("expanded_context", [])
        if expanded_context:
            content_parts.append(f"\n[엔티티 기반 확장 문장 {len(expanded_context)}개]")
            for idx, item in enumerate(expanded_context, 1):
                content_parts.append(f"  {idx}. {item['content']}")

        formatted_content = "\n".join(content_parts)

        metadata = {
            "page": page,
            "toc_title": toc_title,
            "score": record.get("score", 0.0),
            "entities": entities,
        }

        return RetrieverResultItem(content=formatted_content, metadata=metadata)

    retriever = VectorCypherRetriever(
        driver=_driver,
        index_name="text_content_vector_index",
        embedder=_embedder,
        retrieval_query=retrieval_query,
        result_formatter=format_result
    )

    return retriever


# ============================================
# 도구 1: Schema Introspection
# ============================================

@tool(parse_docstring=True)
def schema_introspection() -> str:
    """그래프 스키마(노드, 관계, 속성)를 조회합니다.
    사용자가 "어떤 데이터가 있어?", "스키마를 보여줘" 같은 질문을 할 때 사용하거나
    Text2Cypher 워크플로우에서 generate_cypher_query가 스키마 정보를 필요로 할 때 사용합니다.

    Returns:
        그래프 스키마를 설명하는 텍스트 (노드, 관계, 속성 정보 포함)
    """
    driver, _, _ = initialize_tools()

    try:
        # 1. 노드 레이블과 속성 조회
        node_query = """
        CALL db.schema.nodeTypeProperties()
        YIELD nodeLabels, propertyName, propertyTypes
        RETURN nodeLabels[0] AS label,
               collect({
                 name: propertyName,
                 types: propertyTypes
               }) AS properties
        ORDER BY label
        """
        node_result = driver.execute_query(node_query)

        nodes_info = []
        for record in node_result.records:
            label = record["label"]
            if not label:
                continue
            properties = record["properties"]
            if properties and properties[0]["name"]:
                # types는 리스트로 반환되므로 첫 번째 요소를 Python에서 추출
                props_str = ", ".join([f"{p['name']}: {p['types'][0] if p['types'] else 'Unknown'}" for p in properties if p.get('name')])
                nodes_info.append(f"  - {label} {{{props_str}}}")
            else:
                nodes_info.append(f"  - {label}")

        # 2. 관계 타입과 속성 조회
        rel_props_query = """
        CALL db.schema.relTypeProperties()
        YIELD relType, propertyName, propertyTypes
        RETURN relType,
               collect({
                 name: propertyName,
                 types: propertyTypes
               }) AS properties
        """
        rel_props_result = driver.execute_query(rel_props_query)

        rel_properties = {}
        for record in rel_props_result.records:
            rel_type = record["relType"]
            if rel_type and record["properties"] and record["properties"][0]["name"]:
                rel_properties[rel_type] = record["properties"]

        # 3. 관계 패턴 조회
        rel_pattern_query = """
        MATCH (a)-[r]->(b)
        WITH labels(a) as sourceLabels, type(r) as relType, labels(b) as targetLabels
        WHERE size(sourceLabels) > 0 AND size(targetLabels) > 0
        RETURN DISTINCT sourceLabels[0] as source, relType, targetLabels[0] as target
        ORDER BY relType
        """
        rel_pattern_result = driver.execute_query(rel_pattern_query)

        relations_info = []
        for record in rel_pattern_result.records:
            source = record["source"]
            rel_type = record["relType"]
            target = record["target"]

            if rel_type in rel_properties:
                props = rel_properties[rel_type]
                # types는 리스트로 반환되므로 첫 번째 요소를 Python에서 추출
                props_str = ", ".join([f"{p['name']}: {p['types'][0] if p['types'] else 'Unknown'}" for p in props if p.get('name')])
                relations_info.append(f"  - ({source})-[:{rel_type} {{{props_str}}}]->({target})")
            else:
                relations_info.append(f"  - ({source})-[:{rel_type}]->({target})")

        result = "=== Neo4j 그래프 스키마 ===\n\n"
        result += "📦 노드 레이블 및 속성:\n"
        result += "\n".join(nodes_info) if nodes_info else "  (노드 없음)"
        result += "\n\n🔗 관계 패턴:\n"
        result += "\n".join(relations_info) if relations_info else "  (관계 없음)"

        return result

    except Exception as e:
        return f"스키마 조회 중 오류 발생: {str(e)}"


# ============================================
# 도구 2: Generate Cypher Query (Text2Cypher)
# ============================================

@tool(parse_docstring=True)
def generate_cypher_query(question: str, schema_text: str = None) -> str:
    """자연어 질문을 Neo4j Cypher 쿼리로 변환합니다.
    Text2Cypher 검색방식을 선택할 때 schema_introspection 결과를 전달받아 사용할 수 있습니다.
    execute_cypher_query와 함께 사용합니다.

    Args:
        question: 자연어 질문 (예: "OpenAI에 대해 알려줘", "목차 구조를 보여줘")
        schema_text: 스키마 텍스트

    Returns:
        생성된 Cypher 쿼리 문자열
    """
    driver, llm, _ = initialize_tools()

    try:
        # 스키마 조회 (전달받지 못한 경우에만)
        if schema_text is None:
            schema_text = _get_schema_text(driver)

        # 예시 질의-쿼리 쌍
        examples = _get_text2cypher_examples()

        # LLM을 사용하여 Cypher 쿼리 생성
        prompt = f"""당신은 Neo4j Cypher 쿼리 생성 전문가입니다.

주어진 스키마와 예시를 참고하여, 사용자의 질문을 Cypher 쿼리로 변환하세요.

<Neo4j 스키마>
{schema_text}
</Neo4j 스키마>

<예시 질의-쿼리 쌍>
{chr(10).join(examples)}
</예시 질의-쿼리 쌍>

<사용자 질문>
{question}
</사용자 질문>

<RULES>
1. 스키마에 정의된 노드 레이블과 관계 타입만 사용하세요
2. 검색 시 CONTAINS를 사용하여 부분 매칭을 허용하세요
3. 결과는 사용자가 이해하기 쉬운 별칭(AS)을 사용하세요
4. 쿼리만 출력하고, 설명이나 추가 텍스트는 포함하지 마세요
</RULES>

생성된 Cypher 쿼리:"""

        response = llm.invoke(prompt)
        cypher_query = response.content.strip()

        # 마크다운 코드 블록 제거
        if cypher_query.startswith("```"):
            lines = cypher_query.split("\n")
            cypher_query = "\n".join(lines[1:-1])

        return cypher_query

    except Exception as e:
        return f"Cypher 쿼리 생성 중 오류 발생: {str(e)}"


def _get_schema_text(driver) -> str:
    """스키마를 텍스트로 변환"""
    node_query = """
    CALL db.schema.nodeTypeProperties()
    YIELD nodeLabels, propertyName, propertyTypes
    RETURN nodeLabels[0] AS label,
           collect({
             name: propertyName,
             types: propertyTypes
           }) AS properties
    ORDER BY label
    """
    node_result = driver.execute_query(node_query)

    lines = ["Node properties:"]
    for record in node_result.records:
        label = record["label"]
        if not label:
            continue
        properties = record["properties"]
        if properties and properties[0].get("name"):
            props = ", ".join([f"{p['name']}: {p['types'][0] if p['types'] else 'Unknown'}" for p in properties if p.get('name')])
            lines.append(f"{label} {{{props}}}")

    rel_pattern_query = """
    MATCH (a)-[r]->(b)
    RETURN DISTINCT labels(a)[0] as source, type(r) as relType, labels(b)[0] as target
    ORDER BY relType
    """
    rel_result = driver.execute_query(rel_pattern_query)

    lines.append("\nThe relationships:")
    for record in rel_result.records:
        lines.append(f"(:{record['source']})-[:{record['relType']}]->(:{record['target']})")

    return "\n".join(lines)


def _get_text2cypher_examples() -> List[str]:
    """예시 질의-쿼리 쌍"""
    return [
        "USER INPUT: 'AI 기본법에 대한 내용을 찾아줘' QUERY: MATCH (toc:TOC) WHERE toc.title CONTAINS 'AI 기본법' RETURN toc.title AS 제목, toc.page_start AS 페이지",
        "USER INPUT: '목차 구조를 보여줘' QUERY: MATCH (toc:TOC) RETURN toc.title AS 제목, toc.page_start AS 페이지, toc.level AS 레벨 ORDER BY toc.page_start",
        "USER INPUT: 'OpenAI에 대해 알려줘' QUERY: MATCH (c:Company) WHERE c.name CONTAINS 'OpenAI' RETURN c.name AS 회사명, c.description AS 설명",
        "USER INPUT: '업스테이지의 CEO는 누구인가요?' QUERY: MATCH (c:Company)-[:HAS_EXECUTIVE|EMPLOYS|AFFILIATED_WITH]-(p:Person) WHERE c.name CONTAINS '업스테이지' AND (p.role CONTAINS 'CEO' OR p.title CONTAINS 'CEO') RETURN c.name AS 회사, p.name AS 이름, coalesce(p.role, p.title) AS 직책",
    ]


# ============================================
# 도구 3: Execute Cypher Query
# ============================================

@tool(parse_docstring=True)
def execute_cypher_query(cypher_query: str) -> str:
    """Neo4j에서 Cypher 쿼리를 실행하고 결과를 반환합니다.
    Text2Cypher 검색방식 사용시 generate_cypher_query가 생성한 쿼리를 실행합니다.

    Args:
        cypher_query: 실행할 Cypher 쿼리 문자열

    Returns:
        쿼리 실행 결과를 포맷팅한 텍스트 (최대 20개 레코드)
    """
    driver, _, _ = initialize_tools()

    try:
        result = driver.execute_query(cypher_query)

        if not result.records:
            return "쿼리 실행 성공. 결과가 없습니다."

        # 결과 포맷팅
        output = ["=== 쿼리 실행 결과 ===\n"]
        output.append(f"총 {len(result.records)}개의 레코드\n")

        for idx, record in enumerate(result.records, 1):
            output.append(f"[{idx}]")
            for key in record.keys():
                value = record[key]
                output.append(f"  {key}: {value}")
            output.append("")

        return "\n".join(output)

    except Exception as e:
        return f"쿼리 실행 중 오류 발생: {str(e)}\n\n실행하려던 쿼리:\n{cypher_query}"


# ============================================
# 도구 4: Vector Search
# ============================================

@tool(parse_docstring=True)
def vector_search(question: str, top_k: int = 5) -> str:
    """의미 기반 벡터 검색을 수행하고 그래프 컨텍스트를 자동으로 확장합니다.
    "~와 관련 내용 찾아줘"와 같은 개방형 질문에 적합합니다.

    Args:
        question: 검색할 자연어 질문
        top_k: 반환할 최대 결과 개수 (기본값: 5)

    Returns:
        검색된 텍스트와 관련 컨텍스트를 포함한 결과
    """
    initialize_tools()

    try:
        # VectorCypher 검색 수행
        results = _vectorcypher_retriever.search(
            query_text=question,
            top_k=top_k
        )

        if not results.items:
            return "검색 결과가 없습니다."

        # 결과 포맷팅
        output = [f"=== 벡터 검색 결과 (총 {len(results.items)}개) ===\n"]

        for idx, item in enumerate(results.items, 1):
            output.append(f"[검색 결과 {idx}]")
            output.append(item.content)
            output.append("-" * 50)

        return "\n".join(output)

    except Exception as e:
        return f"벡터 검색 중 오류 발생: {str(e)}"


def get_all_tools():
    return [
        schema_introspection,
        generate_cypher_query,
        execute_cypher_query,
        vector_search
    ]


if __name__ == "__main__":
    print("=" * 70)
    print("GraphRAG 도구 테스트")
    print("=" * 70)
    print()

    try:
        initialize_tools()
        print()
    except Exception as e:
        exit(1)

    # 테스트할 도구와 입력 파라미터
    tests = [
        {
            "tool": schema_introspection,
            "args": {},
        },
        {
            "tool": generate_cypher_query,
            "args": {"question": "OpenAI에 대해 알려줘"},
        },
        {
            "tool": execute_cypher_query,
            "args": {"cypher_query": "MATCH (n) RETURN n LIMIT 5"},
        },
        {
            "tool": vector_search,
            "args": {"question": "OpenAI 관련 내용 요약해줘", "top_k": 3},
        },
    ]

    for idx, test in enumerate(tests, 1):
        print("\n" + "=" * 70)
        print("=" * 70)
        print(f"도구: {test['tool'].name}")
        print(f"파라미터: {test['args']}")
        print("-" * 70)

        try:
            result = test['tool'].invoke(test['args'])
            print("\n실행 결과:")
            print(result)
        except Exception as e:
            print(f"\n오류 발생: {str(e)}")

        print()