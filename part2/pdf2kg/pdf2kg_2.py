import os
import json
import sys
import textwrap
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass

from neo4j import GraphDatabase
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()


# ============================================
# 데이터 클래스
# ============================================

@dataclass
class Entity:
    """추출된 엔티티"""
    entity_id: str
    entity_type: str
    name: str
    properties: Dict[str, Any]


@dataclass
class Relationship:
    """추출된 관계"""
    source_name: str
    source_type: str
    relationship_type: str
    target_name: str
    target_type: str
    properties: Dict[str, Any]


@dataclass
class ExtractionResult:
    """엔티티/관계 추출 결과"""
    element_id: str
    entities: List[Entity]
    relationships: List[Relationship]


# ============================================
# Chunk 조회 함수
# ============================================

def get_chunks(driver, max_chunks: int = 100) -> List[Dict]:
    """Chunk 조회 (content가 있는 것만)"""
    query = """
    MATCH (c:Chunk)
    WHERE c.content IS NOT NULL AND size(c.content) > 20
    RETURN c.chunk_id AS chunk_id,
           c.content AS content,
           c.toc_id AS toc_id
    LIMIT $limit
    """
    records, summary, keys = driver.execute_query(
        query,
        {"limit": max_chunks},
        database_="neo4j"
    )
    return [dict(record) for record in records]


# ============================================
# 스키마 자동 생성
# ============================================

def generate_schema(
    driver,
    openai_client: OpenAI,
    sample_size: int = 20
) -> str:
    """
    Neo4j에서 Chunk 샘플을 가져와 LLM이 적합한 그래프 스키마를 생성

    Args:
        driver: Neo4j driver
        openai_client: OpenAI 클라이언트
        sample_size: 샘플링할 Chunk 개수

    Returns:
        생성된 그래프 스키마 문자열
    """
    print("\n" + "=" * 60)
    print("그래프 스키마 생성 중...")
    print("=" * 60)

    # Chunk 샘플 조회
    print(f"\nChunk 샘플 조회 중 (최대 {sample_size}개)...")
    samples = get_chunks(driver, max_chunks=sample_size)

    if not samples:
        raise ValueError(
            "스키마 생성을 위한 Chunk 샘플을 찾을 수 없습니다. "
            "Neo4j에 Chunk 데이터가 존재하는지 확인하세요."
        )

    print(f"   {len(samples)}개 Chunk 샘플 수집 완료")

    # 샘플 텍스트 결합
    sample_texts = [s["content"] for s in samples]
    combined_samples = "\n\n---\n\n".join(sample_texts)

    # 스키마 생성 프롬프트
    schema_generation_prompt = textwrap.dedent(f"""
        당신은 지식그래프 설계 전문가입니다.
        주어진 문서 속 텍스트들을 분석하여, 이 문서에 적합한 지식그래프 스키마를 설계해주세요.

        <requirements>
        1. 위 텍스트들에서 추출할 수 있는 주요 엔티티 타입을 5-8개 정의하세요.
        2. 각 엔티티 타입의 주요 속성을 정의하세요.
        3. 엔티티 간의 관계 타입을 8-10개 정의하세요.
        4. 실제로 텍스트에 나타나는 정보를 기반으로 설계하세요.
        </requirements>

        <rules>
        1. 노드 타입과 관계 타입은 영문 PascalCase/UPPER_CASE로 작성하세요.
        2. 각 노드 타입과 관계 타입 옆에 한글 설명을 괄호로 함께 제공하세요.
        </rules>

        <output_format>
        ```
        ## 노드 타입 (Node Types)

        1. **EntityType1 (한글명)**
        - property1: string (설명)
        - property2: string (설명)

        2. **EntityType2 (한글명)**
        - property1: string (설명)

        ...

        ## 관계 타입 (Relationship Types)

        1. **RELATION_TYPE1** (EntityType1)-[:RELATION_TYPE1]->(EntityType2)
        - 관계 설명

        2. **RELATION_TYPE2** (EntityType2)-[:RELATION_TYPE2]->(EntityType3)
        - 관계 설명

        ...
        ```
        </output_format>
        """).strip()

    print("\nLLM에 스키마 생성 요청 중...")

    try:
        response = openai_client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "system",
                    "content": schema_generation_prompt
                },
                {
                    "role": "user",
                    "content": textwrap.dedent(f"""
                        <chunks>
                        {combined_samples}
                        </chunks>
                        위 텍스트 샘플들을 분석하여, 이 문서에 적합한 지식그래프 스키마를 설계해주세요.
                    """)
                }
            ],
            temperature=0.3,
            max_tokens=2000
        )

        schema = response.choices[0].message.content.strip()

        # ``` 제거
        if "```" in schema:
            schema = schema.replace("```", "").strip()

        print("\n생성된 스키마:")
        print("-" * 60)
        print(schema)
        print("-" * 60)
        print("스키마 생성 완료\n")

        return schema

    except Exception as e:
        print(f"\n스키마 생성 중 오류 발생: {e}")


# ============================================
# LLM 기반 엔티티/관계 추출 함수
# ============================================

def build_extraction_prompt(text: str, graph_schema: str) -> str:
    """추출 프롬프트 생성"""
    return textwrap.dedent(f"""
        당신은 텍스트에서 지식그래프를 추출하는 전문가입니다.
        아래 텍스트를 분석하여, 주어진 그래프 스키마에 맞는 엔티티(노드)와 관계를 추출해주세요.

        <graph_schema>
        {graph_schema}
        </graph_schema>

        <rules>
        1. 그래프 스키마에 정의된 노드 타입과 관계 타입만 사용하세요.
        2. 텍스트에 명시적으로 언급된 정보만 추출하세요.
        3. 엔티티 이름은 원문 그대로 사용하되, 불필요한 조사는 제거하세요.
        4. 관계는 두 엔티티가 모두 추출된 경우에만 생성하세요.
        5. 추출할 엔티티나 관계가 없으면 빈 배열을 반환하세요.
        </rules>

        반드시 아래 JSON 형식으로만 응답하세요. 다른 설명은 포함하지 마세요.
        <output_format>
        ```json
        {{
        "entities": [
            {{
            "entity_type": "스키마에 정의된 노드 타입",
            "name": "엔티티 이름 (고유 식별자로 사용)",
            "properties": {{
                "속성명": "속성값"
            }}
            }}
        ],
        "relationships": [
            {{
            "source_name": "출발 엔티티 이름",
            "source_type": "출발 엔티티 타입",
            "relationship_type": "스키마에 정의된 관계 타입",
            "target_name": "도착 엔티티 이름",
            "target_type": "도착 엔티티 타입",
            "properties": {{}}
            }}
        ]
        }}
        ```
        </output_format>
    """).strip()


def extract_entities_relationships(
    text: str,
    element_id: str,
    openai_client: OpenAI,
    graph_schema: str
) -> Optional[ExtractionResult]:
    """텍스트에서 엔티티와 관계 추출"""
    if not text or len(text.strip()) < 10:
        return ExtractionResult(element_id=element_id, entities=[], relationships=[])

    prompt = build_extraction_prompt(text, graph_schema)

    try:
        response = openai_client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "system",
                    "content": prompt
                },
                {
                    "role": "user",
                    "content": textwrap.dedent(f"""<text>
                        {text}
                        </text>
                        위 텍스트에서 그래프 스키마에 맞는 엔티티와 관계를 추출해주세요.
                    """)
                }
            ],
            response_format={"type": "json_object"},
            temperature=0.1,
            max_tokens=2000
        )

        data = json.loads(response.choices[0].message.content)

        # Entity 객체 생성
        entities = [
            Entity(
                entity_id=f"{element_id}_e{idx}",
                entity_type=e.get("entity_type", "Unknown"),
                name=e.get("name", ""),
                properties=e.get("properties", {})
            )
            for idx, e in enumerate(data.get("entities", []))
        ]

        # Relationship 객체 생성
        relationships = [
            Relationship(
                source_name=r.get("source_name", ""),
                source_type=r.get("source_type", ""),
                relationship_type=r.get("relationship_type", ""),
                target_name=r.get("target_name", ""),
                target_type=r.get("target_type", ""),
                properties=r.get("properties", {})
            )
            for r in data.get("relationships", [])
        ]

        return ExtractionResult(
            element_id=element_id,
            entities=entities,
            relationships=relationships
        )

    except Exception as e:
        print(f"   추출 실패 ({element_id}): {e}")
        return ExtractionResult(element_id=element_id, entities=[], relationships=[])


# ============================================
# 도메인 지식그래프 구축 함수들
# ============================================

def sanitize_relationship_type(rel_type: str) -> str:
    """관계 타입을 Neo4j에 안전한 형식으로 변환"""
    import re
    # 영문, 숫자, 언더스코어만 허용
    safe_type = re.sub(r'[^A-Za-z0-9_]', '_', rel_type)
    # 대문자로 변환
    safe_type = safe_type.upper()
    # 빈 문자열이거나 숫자로 시작하면 접두어 추가
    if not safe_type or safe_type[0].isdigit():
        safe_type = f"REL_{safe_type}"
    return safe_type


def create_entity_node(driver, entity: Entity, entity_name_to_id: Dict[str, str]) -> str:
    """엔티티 노드 생성 또는 기존 노드 반환"""
    # 이미 생성된 엔티티인지 확인 (이름 + 타입으로 식별)
    entity_key = f"{entity.entity_type}:{entity.name}"

    if entity_key in entity_name_to_id:
        return entity_name_to_id[entity_key]

    # 새 엔티티 노드 생성
    entity_node_id = f"entity_{entity.entity_type}_{len(entity_name_to_id)}"

    query = f"""
    MERGE (e:{entity.entity_type} {{name: $name}})
    ON CREATE SET e.entity_id = $entity_id, e += $properties
    RETURN e.entity_id AS id
    """

    try:
        driver.execute_query(
            query,
            {"name": entity.name, "entity_id": entity_node_id, "properties": entity.properties},
            database_="neo4j"
        )
        entity_name_to_id[entity_key] = entity_node_id
        return entity_node_id

    except Exception as e:
        print(f"   엔티티 노드 생성 실패 ({entity.entity_type}): {e}")


def create_relationship(driver, rel: Relationship, entity_name_to_id: Dict[str, str]):
    """관계 생성"""
    source_key = f"{rel.source_type}:{rel.source_name}"
    target_key = f"{rel.target_type}:{rel.target_name}"

    # 양쪽 엔티티가 모두 존재하는지 확인
    if source_key not in entity_name_to_id or target_key not in entity_name_to_id:
        return

    # 관계 타입을 안전한 형식으로 변환
    safe_rel_type = sanitize_relationship_type(rel.relationship_type)

    # 동적 관계 타입을 사용한 Cypher 쿼리
    query = f"""
    MATCH (source {{name: $source_name}})
    MATCH (target {{name: $target_name}})
    MERGE (source)-[r:{safe_rel_type}]->(target)
    SET r += $properties
    """

    try:
        driver.execute_query(
            query,
            {"source_name": rel.source_name, "target_name": rel.target_name, "properties": rel.properties},
            database_="neo4j"
        )
    except Exception as e:
        print(f"   관계 생성 실패 ({safe_rel_type}): {e}")


def link_entity_to_chunk(driver, entity_name: str, chunk_id: str):
    """엔티티를 Chunk에 HAS_ENTITY 관계로 연결"""
    query = """
    MATCH (e {name: $entity_name})
    MATCH (c:Chunk {chunk_id: $chunk_id})
    MERGE (c)-[:HAS_ENTITY]->(e)
    """
    driver.execute_query(
        query,
        {"entity_name": entity_name, "chunk_id": chunk_id},
        database_="neo4j"
    )


def process_chunk(
    driver,
    chunk: Dict,
    openai_client: OpenAI,
    graph_schema: str,
    entity_name_to_id: Dict[str, str]
) -> Tuple[int, int]:
    """단일 Chunk 처리"""
    chunk_id = chunk["chunk_id"]
    content = chunk["content"]

    # LLM으로 엔티티/관계 추출
    result = extract_entities_relationships(content, chunk_id, openai_client, graph_schema)

    if not result:
        return 0, 0

    # 엔티티 노드 생성
    for entity in result.entities:
        create_entity_node(driver, entity, entity_name_to_id)
        link_entity_to_chunk(driver, entity.name, chunk_id)

    # 관계 생성
    for rel in result.relationships:
        create_relationship(driver, rel, entity_name_to_id)

    return len(result.entities), len(result.relationships)


def build_domain_graph(
    driver,
    openai_client: OpenAI,
    graph_schema: str,
    max_chunks: int = 100
):
    """도메인 그래프 구축"""
    entity_name_to_id = {}

    # Chunk 조회
    print("\nChunk 조회 중...")
    chunks = get_chunks(driver, max_chunks)
    print(f"   조회된 Chunk: {len(chunks)}개")

    if not chunks:
        print("처리할 Chunk가 없습니다.")
        return

    # 배치 처리
    total_entities = 0
    total_relationships = 0

    for i, chunk in enumerate(chunks):
        chunk_id = chunk["chunk_id"]
        content_preview = chunk["content"][:50].replace("\n", " ") + "..."

        print(f"\n[{i+1}/{len(chunks)}] {chunk_id}")
        print(f"   내용: {content_preview}")

        try:
            entity_count, rel_count = process_chunk(
                driver, chunk, openai_client, graph_schema, entity_name_to_id
            )
            total_entities += entity_count
            total_relationships += rel_count

            print(f"   → 엔티티 {entity_count}개, 관계 {rel_count}개 추출")

        except Exception as e:
            print(f"   처리 실패: {e}")

    print("\n" + "=" * 60)
    print("도메인 지식그래프 구축 완료")
    print("=" * 60)
    print(f"   처리된 Chunk: {len(chunks)}개")
    print(f"   생성된 엔티티: {total_entities}개")
    print(f"   생성된 관계: {total_relationships}개")
    print(f"   고유 엔티티: {len(entity_name_to_id)}개")


# ============================================
# 그래프 통계 및 검증
# ============================================

def print_graph_stats(driver):
    """그래프 통계 출력"""
    # 노드 통계
    node_query = """
    MATCH (n)
    RETURN labels(n)[0] AS label, count(n) AS count
    ORDER BY count DESC
    """
    records, summary, keys = driver.execute_query(node_query, database_="neo4j")
    nodes = [dict(record) for record in records]
    print("\n노드 타입별 개수:")
    for row in nodes:
        print(f"   {row['label']}: {row['count']}개")

    # 관계 통계
    rel_query = """
    MATCH ()-[r]->()
    RETURN type(r) AS rel_type, count(r) AS count
    ORDER BY count DESC
    """
    records, summary, keys = driver.execute_query(rel_query, database_="neo4j")
    rels = [dict(record) for record in records]
    print("\n관계 타입별 개수:")
    for row in rels:
        print(f"   {row['rel_type']}: {row['count']}개")

    # 엔티티 타입별 통계 (Chunk, Document 등 제외)
    entity_type_query = """
    MATCH (n)
    WHERE NOT n:Chunk AND NOT n:Document AND NOT n:TOC AND NOT n:TextElement AND NOT n:TableElement
    RETURN labels(n)[0] AS entity_type, count(n) AS count
    ORDER BY count DESC
    """
    records, summary, keys = driver.execute_query(entity_type_query, database_="neo4j")
    entity_types = [dict(record) for record in records]
    if entity_types:
        print("\n도메인 엔티티 타입별 개수:")
        for row in entity_types:
            print(f"   {row['entity_type']}: {row['count']}개")


# ============================================
# 메인
# ============================================

if __name__ == "__main__":
    print("=" * 50)
    print("PDF 지식 그래프 추출 - (2) 도메인 엔티티/관계 추출")
    print("=" * 50)

    MAX_CHUNKS = 100
    SCHEMA_SAMPLE_SIZE =100

    NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    NEO4J_USER = os.getenv("NEO4J_USERNAME", "neo4j")
    NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

    if not OPENAI_API_KEY:
        print("OPENAI_API_KEY 환경 변수가 설정되지 않았습니다.")
        sys.exit(1)

    print(f"\nNeo4j 연결: {NEO4J_URI}")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    try:
        driver.verify_connectivity()

        openai_client = OpenAI(api_key=OPENAI_API_KEY)
        graph_schema = generate_schema(
            driver,
            openai_client,
            sample_size=SCHEMA_SAMPLE_SIZE
        )

        build_domain_graph(
            driver,
            openai_client,
            graph_schema,
            max_chunks=MAX_CHUNKS
        )

        print_graph_stats(driver)

    finally:
        driver.close()
