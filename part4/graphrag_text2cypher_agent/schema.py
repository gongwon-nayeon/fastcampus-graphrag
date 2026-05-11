import os
from typing import Dict, Any
from dotenv import load_dotenv
from neo4j import GraphDatabase, basic_auth
from neo4j.time import Date

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", NEO4J_USERNAME)


def validate():
    """필수 환경 변수가 설정되어 있는지 확인합니다."""
    required = {
        "OPENAI_API_KEY": OPENAI_API_KEY,
        "NEO4J_URI": NEO4J_URI,
        "NEO4J_PASSWORD": NEO4J_PASSWORD,
    }

    missing = [name for name, value in required.items() if not value]

    if missing:
        raise ValueError(
            f"필수 환경 변수가 누락되었습니다: {', '.join(missing)}\n"
            f".env 파일에 설정해주세요"
        )

    return True


# ============================================
# Neo4j 스키마 관련 함수들
# ============================================


def get_node_datatype(value: Any) -> str:
    """
    입력된 노드 Value의 데이터 타입을 반환하는 함수

    Args:
        value: 노드의 속성 값

    Returns:
        str: 데이터 타입 문자열 (STRING, INTEGER, FLOAT, BOOLEAN, LIST, DATE 등)
    """
    if isinstance(value, str):
        return "STRING"
    elif isinstance(value, int):
        return "INTEGER"
    elif isinstance(value, float):
        return "FLOAT"
    elif isinstance(value, bool):
        return "BOOLEAN"
    elif isinstance(value, list):
        return f"LIST[{get_node_datatype(value[0])}]" if value else "LIST"
    elif isinstance(value, Date):
        return "DATE"
    else:
        return "UNKNOWN"


def get_schema_dict(driver: GraphDatabase.driver) -> Dict[str, Any]:
    """
    Graph DB의 정보를 받아 노드 및 관계의 프로퍼티를 추출하고 스키마 딕셔너리를 반환하는 함수

    Args:
        driver: Neo4j 드라이버 인스턴스

    Returns:
        Dict: 노드, 관계, 관계 방향 정보를 담은 딕셔너리
              {"nodes": {...}, "relationships": {...}, "relations": [...]}
    """
    with driver.session() as session:
        # 노드 프로퍼티 및 타입 추출
        node_query = """
        MATCH (n)
        WITH DISTINCT labels(n) AS node_labels, keys(n) AS property_keys, n
        UNWIND node_labels AS label
        UNWIND property_keys AS key
        RETURN label, key, n[key] AS sample_value
        """
        nodes = session.run(node_query)

        # 관계 프로퍼티 및 타입 추출
        rel_query = """
        MATCH ()-[r]->()
        WITH DISTINCT type(r) AS rel_type, keys(r) AS property_keys, r
        UNWIND property_keys AS key
        RETURN rel_type, key, r[key] AS sample_value
        """
        relationships = session.run(rel_query)

        # 관계 유형 및 방향 추출
        rel_direction_query = """
        MATCH (a)-[r]->(b)
        RETURN DISTINCT labels(a) AS start_label, type(r) AS rel_type, labels(b) AS end_label
        ORDER BY start_label, rel_type, end_label
        """
        rel_directions = session.run(rel_direction_query)

        # 스키마 딕셔너리 생성
        schema = {"nodes": {}, "relationships": {}, "relations": []}

        for record in nodes:
            label = record["label"]
            key = record["key"]
            sample_value = record["sample_value"]  # 데이터 타입을 추론하기 위한 샘플 데이터
            inferred_type = get_node_datatype(sample_value)
            if label not in schema["nodes"]:
                schema["nodes"][label] = {}
            schema["nodes"][label][key] = inferred_type

        for record in relationships:
            rel_type = record["rel_type"]
            key = record["key"]
            sample_value = record["sample_value"]  # 데이터 타입을 추론하기 위한 샘플 데이터
            inferred_type = get_node_datatype(sample_value)
            if rel_type not in schema["relationships"]:
                schema["relationships"][rel_type] = {}
            schema["relationships"][rel_type][key] = inferred_type

        for record in rel_directions:
            start_label = record["start_label"][0]
            rel_type = record["rel_type"]
            end_label = record["end_label"][0]
            schema["relations"].append(f"(:{start_label})-[:{rel_type}]->(:{end_label})")

        return schema


def get_schema_str(schema: Dict[str, Any]) -> str:
    """
    스키마 딕셔너리를 LLM에 제공하기 위해 원하는 형태로 formatting 하는 함수

    Args:
        schema: get_schema_dict()에서 반환된 스키마 딕셔너리

    Returns:
        str: 포맷팅된 스키마 문자열
    """
    result = []

    # 노드 프로퍼티 출력
    result.append("Node properties:")
    for label, properties in schema["nodes"].items():
        props = ", ".join(f"{k}: {v}" for k, v in properties.items())
        result.append(f"{label} {{{props}}}")

    # 관계 프로퍼티 출력
    result.append("\nRelationship properties:")
    for rel_type, properties in schema["relationships"].items():
        props = ", ".join(f"{k}: {v}" for k, v in properties.items())
        result.append(f"{rel_type} {{{props}}}")

    # 관계 방향 출력
    result.append("\nThe relationships:")
    for relation in schema["relations"]:
        result.append(relation)

    return "\n".join(result)


def get_neo4j_driver() -> GraphDatabase.driver:
    """
    Neo4j 드라이버 인스턴스 생성

    Returns:
        GraphDatabase.driver: Neo4j 드라이버 인스턴스
    """
    validate()

    driver = GraphDatabase.driver(
        NEO4J_URI,
        auth=basic_auth(NEO4J_USERNAME, NEO4J_PASSWORD)
    )
    return driver


# 전역 스키마 캐시
_schema_cache = {
    "schema_dict": None,
    "schema_str": None,
}


def get_cached_schema(refresh: bool = False) -> str:
    """
    스키마 문자열 반환 (캐싱 지원)

    Args:
        refresh: True면 스키마를 다시 추출

    Returns:
        str: 포맷팅된 스키마 문자열
    """
    if _schema_cache["schema_str"] is None or refresh:
        driver = get_neo4j_driver()
        try:
            _schema_cache["schema_dict"] = get_schema_dict(driver)
            _schema_cache["schema_str"] = get_schema_str(_schema_cache["schema_dict"])
        finally:
            driver.close()

    return _schema_cache["schema_str"]
