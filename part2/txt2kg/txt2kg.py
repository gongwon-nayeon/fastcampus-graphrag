import os
import json
import textwrap
from openai import OpenAI
from neo4j import GraphDatabase
from dotenv import load_dotenv
from langchain_text_splitters import RecursiveCharacterTextSplitter

load_dotenv()


# ============================================
# 1단계: 텍스트 청킹
# ============================================

def split_text_into_chunks(text, chunk_size=500, chunk_overlap=100):
    """텍스트를 청크로 분할"""
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        length_function=len,
        separators=["\n\n", "\n", ". ", " ", ""]
    )

    chunks = text_splitter.split_text(text)

    print(f"텍스트 분할: {len(text)}자 -> {len(chunks)}개 청크")

    return chunks


# ============================================
# 2단계: 지식 추출
# ============================================

def extract_knowledge_from_chunk(chunk, chunk_idx, api_key, model="gpt-4o"):
    """
    청크에서 엔티티, 관계, 속성을 추출
    """

    client = OpenAI(api_key=api_key)

    system_prompt = textwrap.dedent("""
        당신은 텍스트에서 지식 그래프를 추출하는 전문가입니다. 반드시 JSON 형식으로 응답하세요.

        <extraction_strategy>
        **관계 우선 추출 (Relation-First Extraction)**:
        1. 문장에서 동사/서술어를 먼저 찾는다
        2. 그 동사의 실제 주어(agent)와 목적어(patient)를 정확히 식별한다
        3. 상위 개념이 아닌, 문장에서 직접 언급된 구체적인 개념을 엔티티로 사용한다
        4. 정보 손실 없이 원문의 의미를 최대한 보존한다
        </extraction_strategy>

        <rules>
        **엔티티 추출 규칙**:
        - 문장에서 직접 언급된 구체적인 개념을 추출 (추상화하지 말 것)
        - 각 엔티티에는 반드시 "name"과 "type"을 포함해야 함
        - type은 엔티티의 범주를 나타냄 (예: 인물, 장소, 조직, 개념, 물질, 기술 등)
        - "A는 B에 의해 C된다" → A와 B 둘 다 엔티티로 추출
        - "A의 B" → A와 B 각각 별도 엔티티
        - 상위 개념보다 하위/구체적 개념을 우선 추출
        - 열거형 "A, B, C" → 각각 별도 엔티티
        - 중간 매개체(~에 의해, ~를 통해, ~로 인해)는 반드시 엔티티로 추출

        **속성(properties) 추출 규칙** - 엔티티가 아닌 부가정보 예시:
        - 숫자/수량 → {"count": "값", "unit": "단위"}
        - 연도/날짜 → {"year": "연도", "date": "날짜"}
        - 위치/장소 → {"location": "장소"}
        - 역할/기능 → {"role": "역할"}
        - 영어명/별칭 → {"english_name": "영어명", "alias": "별칭"}
        - 국적/출신 → {"nationality": "국적"}
        - 직업/직함 → {"profession": "직업"}
        - 화학식/공식 → {"formula": "공식"}
        - 수식어/형용사적 정보는 엔티티가 아닌 properties로

        **관계 추출 규칙**:
        - 관계 라벨(type)은 반드시 영어 대문자와 언더스코어로 작성
        - 피동형 "A는 B에 의해 ~된다" → (B)-[동사]->(A)
        - 능동형 "A는 B를 ~한다" → (A)-[동사]->(B)
        - 구성 "A는 B로 구성된다" → (A)-[COMPOSED_OF]->(B)
        - 하나의 문장에서 여러 관계가 있으면 모두 추출
        - 관계의 부가정보(연도, 장소 등)는 관계의 properties에 저장

        **관계 라벨 작성 가이드**:
        - 문장의 동사/서술어를 기반으로 의미가 명확한 영어 관계 라벨을 선택하세요
        - 아래는 일반적인 카테고리별 예시일 뿐이며, 문맥에 맞는 다른 라벨을 자유롭게 생성하세요
        - 예시 카테고리:
          * 구성/포함: COMPOSED_OF, CONTAINS, INCLUDES 등
          * 생성/형성: FORMS, CREATES, PRODUCES 등
          * 연결/상호작용: BINDS_TO, INTERACTS_WITH 등
          * 위치/소속: LOCATED_IN, PART_OF 등
          * 유래/기원: DERIVED_FROM, ORIGINATES_FROM 등
          * 작용/영향: CAUSES, AFFECTS, REGULATES 등
          * 변환/전환: CONVERTS_TO, TRANSFORMS_INTO 등
        </rules>

        <examples>
        예시 1 - 피동형 문장:
        원문: "X는 Y에 의해 형성된다"
        → 엔티티: [{"name": "X", "type": "구조"}, {"name": "Y", "type": "구성요소"}]
        → 관계: (Y)-[FORMS]->(X)

        예시 2 - 구성 관계:
        원문: "A는 B와 C로 구성된다"
        → 엔티티: [{"name": "A", "type": "구조"}, {"name": "B", "type": "구성요소"}, {"name": "C", "type": "구성요소"}]
        → 관계: (A)-[COMPOSED_OF]->(B), (A)-[COMPOSED_OF]->(C)

        예시 3 - 수량 정보:
        원문: "100개의 X가 Y를 이룬다"
        → 엔티티: [{"name": "X", "type": "구성요소", "properties": {"count": "100"}}]
        → 관계: (X)-[FORMS]->(Y)

        예시 4 - 인물과 행위:
        원문: "1900년 독일의 과학자 A가 B를 발견했다"
        → 엔티티: [{"name": "A", "type": "인물", "properties": {"nationality": "독일", "profession": "과학자"}}, {"name": "B", "type": "개념"}]
        → 관계: (A)-[DISCOVERED]->(B) with properties: {"year": "1900"}

        예시 5 - 별칭/영어명:
        원문: "A(영어명: X)는 B에서 유래했다"
        → 엔티티: [{"name": "A", "type": "개념", "properties": {"english_name": "X"}}, {"name": "B", "type": "개념"}]
        → 관계: (A)-[DERIVED_FROM]->(B)

        예시 6 - 복합 문장:
        원문: "A는 B를 통해 C에 작용한다"
        → 엔티티: [{"name": "A", "type": "개념"}, {"name": "B", "type": "개념"}, {"name": "C", "type": "개념"}]
        → 관계: (A)-[USES]->(B), (A)-[AFFECTS]->(C)
        </examples>

        <output_format>
        {
        "entities": [
            {"name": "엔티티명", "type": "타입", "properties": {"key": "value"}}
        ],
        "relations": [
            {
            "source": "엔티티명(실제 행위자/원인)",
            "target": "엔티티명(행위 대상/결과)",
            "type": "ENGLISH_RELATION_TYPE",
            "properties": {"key": "value"},
            "evidence": "엔티티와 관계를 추출한 원본 문장(원문에서 발췌)"
            }
        ]
        }
        </output_format>
    """).strip()

    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": system_prompt
                },
                {
                    "role": "user",
                    "content": textwrap.dedent(f"""
                        <text>
                        {chunk}
                        </text>
                        위 텍스트에서 지식 그래프를 추출하세요.
                    """)
                }
            ],
            response_format={"type": "json_object"}, # json 응답 강제
            temperature=0,
        )

        result = json.loads(response.choices[0].message.content)
        return result

    except Exception as e:
        print(f"  청크 {chunk_idx+1} 오류: {str(e)[:50]}")
        return {"entities": [], "relations": []}


def extract_knowledge_batch(chunks, api_key, model="gpt-4o-mini"):
    """모든 청크에서 지식 추출 및 병합"""

    print(f"\n지식 추출 중 ({len(chunks)}개 청크)...")

    all_entities = []
    all_relations = []

    for i, chunk in enumerate(chunks):
        print(f"  청크 {i+1}/{len(chunks)}...", end=" ")

        result = extract_knowledge_from_chunk(chunk, i, api_key, model)

        entities = result.get("entities", [])
        relations = result.get("relations", [])

        all_entities.extend(entities)
        all_relations.extend(relations)

        print(f"엔티티 {len(entities)}개, 관계 {len(relations)}개")

        if entities:
            entity_names = [e.get("name") for e in entities]
            print(f"    → 엔티티: {', '.join(entity_names)}", end="")
            print()

        if relations:
            for j, rel in enumerate(relations):
                source = rel.get("source", "?")
                target = rel.get("target", "?")
                rel_type = rel.get("type", "?")
                evidence = rel.get("evidence", "")
                print(f"    → 관계 {j+1}: ({source})-[{rel_type}]->({target}) with evidence: \"{evidence}\"")
        print()

    unique_entities = {}
    for entity in all_entities:
        name = entity.get("name")
        if not name:
            continue

        if name not in unique_entities:
            unique_entities[name] = entity
        else:
            # 속성 병합
            existing = unique_entities[name]
            existing_props = existing.get("properties", {})
            new_props = entity.get("properties", {})
            existing["properties"] = {**existing_props, **new_props}

    # 관계 중복 제거 (source, target, type 기준)
    unique_relations = []
    seen = set()
    for rel in all_relations:
        key = (rel.get("source"), rel.get("target"), rel.get("type"))
        if key not in seen and all(key):
            seen.add(key)
            unique_relations.append(rel)

    entities_list = list(unique_entities.values())

    print(f"\n추출 완료:")
    print(f"  엔티티: {len(entities_list)}개")
    print(f"  관계: {len(unique_relations)}개")

    return {"entities": entities_list, "relations": unique_relations}


# ============================================
# 3단계: 검증
# ============================================

def validate_knowledge_graph(kg):
    """참조 무결성 검증 및 수정"""

    print("\n검증 중...")

    entity_names = {e["name"] for e in kg["entities"]}

    valid_relations = []
    invalid_count = 0

    for rel in kg["relations"]:
        if rel["source"] in entity_names and rel["target"] in entity_names: # source/target 존재 확인
            valid_relations.append(rel)
        else:
            invalid_count += 1

    kg["relations"] = valid_relations

    print(f"  유효 관계: {len(valid_relations)}개")
    if invalid_count:
        print(f"  제거된 관계: {invalid_count}개")

    return kg


# ============================================
# 4단계: Neo4j 저장
# ============================================

def save_to_neo4j(kg, driver):
    print("\nNeo4j 저장 중...")

    entities = kg.get("entities", [])
    relations = kg.get("relations", [])

    try:
        # 1. 기존 데이터 삭제
        driver.execute_query(
            "MATCH (n) DETACH DELETE n",
            database_="neo4j"
        )
        print("  기존 데이터 삭제 완료")

        # 2. 노드 생성
        def create_nodes(tx, entities_list):
            """트랜잭션 함수: 모든 노드 생성"""
            created_count = 0
            for entity in entities_list:
                name = entity["name"]
                node_type = entity.get("type", "Entity").replace("`", "").strip()
                props = entity.get("properties", {})

                # 속성을 Cypher 파라미터로 변환
                props_str = ", ".join([f"{k}: ${k}" for k in props.keys()])
                if props_str:
                    props_str = ", " + props_str

                query = f"MERGE (n:`{node_type}` {{name: $name{props_str}}})"
                tx.run(query, name=name, **props)
                created_count += 1

            return created_count

        with driver.session(database="neo4j") as session:
            node_count = session.execute_write(create_nodes, entities)
            print(f"  {node_count}개 노드 생성 완료")

        # 3. 관계 생성
        def create_relations(tx, relations_list):
            """트랜잭션 함수: 모든 관계 생성"""
            created_count = 0
            for rel in relations_list:
                rel_type = rel["type"].replace("`", "").strip()
                evidence = rel.get("evidence", "")
                props = rel.get("properties", {})

                # evidence를 props에 추가
                props["evidence"] = evidence

                props_str = ", ".join([f"{k}: ${k}" for k in props.keys()])

                query = f"""
                MATCH (source {{name: $source}})
                MATCH (target {{name: $target}})
                CREATE (source)-[r:`{rel_type}` {{{props_str}}}]->(target)
                """
                tx.run(query, source=rel["source"], target=rel["target"], **props)
                created_count += 1

            return created_count

        with driver.session(database="neo4j") as session:
            rel_count = session.execute_write(create_relations, relations)
            print(f"  {rel_count}개 관계 생성 완료")

        print("  완료!")

    except Exception as e:
        print(f"  오류: {e}")


# ============================================
# 메인
# ============================================

def main():
    print("=" * 50)
    print("지식 그래프 추출")
    print("=" * 50)

    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    NEO4J_URI = os.getenv("NEO4J_URI")
    NEO4J_USER = os.getenv("NEO4J_USERNAME")
    NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

    MODEL = "gpt-4o"
    INPUT_FILE = "wiki_protein.txt"
    # INPUT_FILE = "wiki_bus.txt"
    OUTPUT_FILE = "knowledge_graph.json"

    print(f"\n텍스트 로드: {INPUT_FILE}")
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        text = f.read()
    print(f"  {len(text)}자")

    # 1. 청킹
    chunks = split_text_into_chunks(text, chunk_size=500, chunk_overlap=100)

    # 2. 지식그래프(엔티티, 관계) 추출
    kg = extract_knowledge_batch(chunks, OPENAI_API_KEY, model=MODEL)

    # 3. 검증
    kg = validate_knowledge_graph(kg)

    # 4. JSON 저장
    print(f"\n추출 결과 JSON 저장: {OUTPUT_FILE}")
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(kg, f, ensure_ascii=False, indent=2)

    # 5. Neo4j 저장
    if NEO4J_URI and NEO4J_USER and NEO4J_PASSWORD:
        print(f"\nNeo4j 연결: {NEO4J_URI}")
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        try:
            driver.verify_connectivity()
            save_to_neo4j(kg, driver)
        except Exception as e:
            print(f"  연결 실패: {e}")
        finally:
            driver.close()

    print("\n" + "=" * 50)
    print("샘플 결과:")
    print("=" * 50)

    print("\n[엔티티 샘플]")
    for e in kg["entities"][:5]:
        props = e.get("properties", {})
        props_str = f" {props}" if props else ""
        print(f"  ({e['name']}) [{e.get('type', '?')}]{props_str}")

    print("\n[관계 샘플]")
    for r in kg["relations"][:5]:
        props = r.get("properties", {})
        props_str = f" {props}" if props else ""
        print(f"  ({r['source']})-[{r['type']}]->({r['target']}){props_str}")

    print(f"\n최종: 엔티티 {len(kg['entities'])}개, 관계 {len(kg['relations'])}개")


if __name__ == "__main__":
    main()
