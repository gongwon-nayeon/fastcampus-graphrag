def fetch_schema_from_neo4j(driver):
    """Neo4j에서 동적으로 스키마 정보를 조회하여 딕셔너리 형태로 반환"""

    # 1. 노드 라벨과 속성 정보 조회
    node_query = """
    CALL db.schema.nodeTypeProperties()
    YIELD nodeType, nodeLabels, propertyName, propertyTypes, mandatory
    RETURN nodeLabels[0] as label,
           collect({name: propertyName, type: propertyTypes[0]}) as properties
    ORDER BY label
    """

    node_result = driver.execute_query(node_query)
    nodes_data = {}
    for record in node_result.records:
        label = record["label"]
        if label and label not in nodes_data:
            nodes_data[label] = []
        if record["properties"]:
            for prop in record["properties"]:
                if prop["name"] and prop not in nodes_data[label]:
                    nodes_data[label].append(prop)

    # 2. 관계 타입과 속성 정보 조회
    rel_props_query = """
    CALL db.schema.relTypeProperties()
    YIELD relType, propertyName, propertyTypes, mandatory
    RETURN relType,
           collect({name: propertyName, type: propertyTypes[0]}) as properties
    """

    rel_props_result = driver.execute_query(rel_props_query)
    rel_properties = {}
    for record in rel_props_result.records:
        rel_type = record["relType"]
        if rel_type and record["properties"]:
            rel_properties[rel_type] = record["properties"]

    # 3. 관계 패턴 조회 (source -> relation -> target)
    rel_pattern_query = """
    MATCH (a)-[r]->(b)
    WITH labels(a) as sourceLabels, type(r) as relType, labels(b) as targetLabels
    WHERE size(sourceLabels) > 0 AND size(targetLabels) > 0
    RETURN DISTINCT sourceLabels[0] as source, relType, targetLabels[0] as target
    ORDER BY relType
    """

    rel_pattern_result = driver.execute_query(rel_pattern_query)
    relations_data = []
    for record in rel_pattern_result.records:
        rel_info = {
            "label": record["relType"],
            "source": record["source"],
            "target": record["target"]
        }
        # 관계 속성 추가
        if record["relType"] in rel_properties:
            rel_info["properties"] = rel_properties[record["relType"]]

        relations_data.append(rel_info)

    # 4. 딕셔너리 형태로 스키마 구성
    schema = {
        "entities": [
            {
                "label": label,
                "properties": properties
            }
            for label, properties in sorted(nodes_data.items())
        ],
        "relations": relations_data
    }

    return schema


def schema_to_text(schema):
    """딕셔너리 스키마를 Text2CypherRetriever가 이해할 수 있는 문자열 형식으로 변환"""
    lines = ["Node properties:"]

    # 엔티티(노드) 정보 추가
    for entity in schema["entities"]:
        if entity["properties"]:
            props = ", ".join([f"{p['name']}: {p['type']}" for p in entity["properties"] if p.get('name')])
            if props:
                lines.append(f"{entity['label']} {{{props}}}")
        else:
            lines.append(f"{entity['label']}")

    # 관계 속성 추가
    lines.append("\nRelationship properties:")
    for relation in schema["relations"]:
        if "properties" in relation and relation["properties"]:
            props = ", ".join([f"{p['name']}: {p['type']}" for p in relation["properties"] if p.get('name')])
            if props:
                lines.append(f"{relation['label']} {{{props}}}")

    # 관계 정의 추가
    lines.append("\nThe relationships:")
    for relation in schema["relations"]:
        source = relation["source"]
        target = relation["target"]
        label = relation["label"]
        if "properties" in relation and relation["properties"]:
            prop_names = ", ".join([p['name'] for p in relation["properties"] if p.get('name')])
            if prop_names:
                lines.append(f"(:{source})-[:{label} {{{prop_names}}}]->(:{target})")
            else:
                lines.append(f"(:{source})-[:{label}]->(:{target})")
        else:
            lines.append(f"(:{source})-[:{label}]->(:{target})")

    return "\n".join(lines)
