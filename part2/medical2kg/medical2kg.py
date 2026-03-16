import os
import sys
import json
import textwrap
from pathlib import Path
from typing import List, Dict, Optional
import time

from neo4j import GraphDatabase
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

DEPARTMENTS = ["내과", "산부인과", "소아청소년과", "응급의학과"]
DATA_DIR = Path("data")
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=OPENAI_API_KEY)


def load_all_qa_data(max_qa_per_dept: Optional[int] = None) -> Dict[str, List[Dict]]:
    """모든 진료과의 QA 데이터 로드

    Args:
        max_qa_per_dept: 각 진료과별 최대 QA 개수 (None이면 전체 로드)
    """
    print(f"\n[Step 1/3] QA 데이터 로딩 중...")
    if max_qa_per_dept:
        print(f"   (각 진료과별 최대 {max_qa_per_dept}개 샘플링)")

    all_qa_data = {}
    total_count = 0

    for dept in DEPARTMENTS:
        dept_path = DATA_DIR / dept
        json_files = list(dept_path.rglob("*.json"))

        qa_list = []
        for json_file in json_files:
            try:
                with open(json_file, 'r', encoding='utf-8-sig') as f:
                    data = json.load(f)
                    data['department'] = dept  # 진료과 정보 추가
                    qa_list.append(data)
            except Exception as e:
                continue

            # 샘플링: 최대 개수 도달시 중단
            if max_qa_per_dept and len(qa_list) >= max_qa_per_dept:
                break

        all_qa_data[dept] = qa_list
        total_count += len(qa_list)
        print(f"   {dept}: {len(qa_list)}개 QA")

    print(f"\n   총 {total_count}개 QA 로드 완료")

    return all_qa_data

# ============================================
# Step 0: 스키마 추출 (Schema Extraction)
# ============================================

def extract_domain_schema(samples: List[Dict]) -> Dict:
    """LLM으로 도메인 통합 스키마 추출"""

    # 샘플 데이터를 텍스트로 변환
    samples_text = "\n\n".join([
        f"[QA {i+1}]\nQ: {s.get('question', '')}...\nA: {s.get('answer', '')}..."
        for i, s in enumerate(samples[:5])  # 처음 5개만 사용
    ])

    prompt = textwrap.dedent(f"""
        당신은 의료 지식 그래프 전문가입니다.

        <requirements>
        1. 엔티티 타입 추출 (예: Symptom, Disease, Test, Treatment, Anatomy, Medication)
        2. 각 타입별 주요 엔티티 (3-5개 예시)
        3. 엔티티 타입 간 관계 정의
        </requirements>

        <output_format>
        JSON만 반환 (설명 없이):
        {{
          "entity_types": {{
            "Symptom": ["예시1", "예시2", "예시3"],
            "Disease": ["예시1", "예시2", "예시3"],
            "Test": ["예시1", "예시2"],
            "Treatment": ["예시1", "예시2"],
            "Medication": ["예시1", "예시2"],
            "Anatomy": ["예시1", "예시2"]
          }},
          "relationships": [
            {{"from": "Symptom", "to": "Disease", "type": "INDICATES"}},
            {{"from": "Test", "to": "Disease", "type": "DIAGNOSES_FOR"}},
            {{"from": "Treatment", "to": "Disease", "type": "TREATS"}},
            {{"from": "Medication", "to": "Disease", "type": "TREATS"}}
          ]
        }}
        </output_format>
    """).strip()

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": textwrap.dedent(f"""
                    다음은 내과, 산부인과, 소아청소년과, 응급의학과의 QA 샘플 텍스트입니다.
                    이를 분석하여 그래프 스키마를 추출하세요.

                    <qa_samples>
                    {samples_text}
                    </qa_samples>
                """).strip()}
            ],
            response_format={"type": "json_object"},
            temperature=0.3,
            max_tokens=1500
        )

        result_text = response.choices[0].message.content.strip()

        if "```json" in result_text:
            result_text = result_text.split("```json")[1].split("```")[0].strip()
        elif "```" in result_text:
            result_text = result_text.split("```")[1].split("```")[0].strip()

        schema = json.loads(result_text)
        return schema

    except Exception as e:
        print(f"   오류: 스키마 추출 실패 - {e}")
        return {
            "entity_types": {},
            "relationships": [],
        }


def extract_unified_schema() -> Dict:
    """모든 진료과의 샘플을 통합하여 통일된 스키마 추출"""
    print(f"\n[Step 0/3] 통일된 도메인 스키마 추출 중...")

    # 모든 진료과의 샘플 수집 (각 3개씩)
    qa_data = load_all_qa_data(max_qa_per_dept=3)

    all_samples = []
    for dept, qa_list in qa_data.items():
        all_samples.extend(qa_list)
        print(f"   {dept}: {len(qa_list)}개 샘플 수집")

    if not all_samples:
        return {
            "entity_types": {},
            "relationships": [],
        }

    print(f"   총 {len(all_samples)}개 샘플로 통일된 스키마 추출 중...")

    unified_schema = extract_domain_schema(all_samples)

    print(f"   스키마: {len(unified_schema['entity_types'])}개 엔티티 타입, "
          f"{len(unified_schema['relationships'])}개 관계 타입")

    # 스키마 저장
    schema_path = OUTPUT_DIR / "domain_schemas.json"
    with open(schema_path, 'w', encoding='utf-8') as f:
        json.dump(unified_schema, f, ensure_ascii=False, indent=2)

    print(f"   스키마 저장: {schema_path}")

    return unified_schema


# ============================================
# Step 1: QA 그래프 구축 (QA Graph Construction)
# ============================================

def save_qa_graph_to_neo4j(all_qa_data: Dict[str, List[Dict]], driver):
    """QA 그래프를 Neo4j에 저장"""
    print(f"\n   Neo4j에 QA 그래프 저장 중...")

    # 1. 기존 데이터 삭제
    driver.execute_query(
        "MATCH (n) DETACH DELETE n",
        database_="neo4j"
    )
    print(f"      step 1 - 기존 데이터 삭제")

    # 2. Department 노드 생성
    for dept in DEPARTMENTS:
        driver.execute_query(
            "CREATE (d:Department {name: $name})",
            name=dept,
            database_="neo4j"
        )
    print(f"      step 2 - Department 노드 {len(DEPARTMENTS)}개 생성")

    # 3. Question-Answer 노드 및 관계 생성
    created = 0

    for dept, qa_list in all_qa_data.items():
        # 배치 처리 (50개씩)
        for i in range(0, len(qa_list), 50):
            batch = qa_list[i:i+50]

            driver.execute_query("""
                UNWIND $batch AS qa
                CREATE (q:Question {
                    qa_id: qa.qa_id,
                    content: qa.question,
                    q_type: qa.q_type,
                    domain: qa.domain,
                    department: qa.department
                })
                CREATE (a:Answer {
                    qa_id: qa.qa_id,
                    content: qa.answer
                })
                CREATE (q)-[:HAS_ANSWER]->(a)
                WITH q, qa
                MATCH (d:Department {name: qa.department})
                CREATE (q)-[:BELONGS_TO]->(d)
            """, batch=batch, database_="neo4j")

            created += len(batch)

    print(f"      step 3 - Question/Answer 노드 {created}개 생성 및 관계 연결")

    print(f"\n   QA 그래프 저장 완료!")


# ============================================
# Step 2: 엔티티 및 관계 추출 (Entity & Relationship Extraction)
# ============================================

def extract_entities_and_relationships_from_qa(qa: Dict, schema: Dict) -> Dict:
    """LLM으로 QA에서 의료 엔티티와 관계를 한번에 추출"""

    question = qa.get('question', '')
    answer = qa.get('answer', '')

    # 스키마 정보 요약
    entity_types_list = list(schema.get('entity_types', {}).keys())
    entity_types_lower = [t.lower() for t in entity_types_list]
    entity_types_str = ", ".join(entity_types_list)
    entity_types_enum = "|".join(entity_types_lower)

    relationship_types = schema.get('relationships', [])

    allowed_rels = []
    for r in relationship_types:
        from_type = r['from'].lower()
        to_type = r['to'].lower()
        rel_type = r['type']
        allowed_rels.append(f"{from_type} -[{rel_type}]-> {to_type}")
    allowed_rels_str = "\n".join(allowed_rels)

    prompt = textwrap.dedent(f"""
        당신은 의료 지식 그래프 전문가입니다.
        아래 스키마에 정의된 관계만 추출해야 합니다.

        <entity_types>
        {entity_types_str}
        </entity_types>

        <allowed_relationships>
        아래에 명시된 조합만 추출 가능합니다 (방향 준수 필수):
        {allowed_rels_str}
        </allowed_relationships>

        <rules>
        1. 엔티티는 원문에 명시된 것만 추출
        2. 관계는 위 "허용된_관계_조합"에 있는 것만 추출
        3. from_type과 to_type의 방향을 정확히 지킬 것
        4. 엔티티 이름은 2-6 단어로 간결하게
        </rules>

        <output_format>
        다음은 출력 예시입니다.
        JSON만 반환 (설명 없이):
        {{
          "entities": [
            {{"name": "기침", "type": "symptom"}},
            {{"name": "폐렴", "type": "disease"}},
            {{"name": "아목시실린", "type": "medication"}}
          ],
          "relationships": [
            {{
              "from": "기침",
              "from_type": "symptom",
              "to": "폐렴",
              "to_type": "disease",
              "type": "INDICATES",
              "evidence": "환자가 기침으로 내원"
            }},
            {{
              "from": "아목시실린",
              "from_type": "medication",
              "to": "폐렴",
              "to_type": "disease",
              "type": "TREATS",
              "evidence": "아목시실린으로 치료"
            }}
          ]
        }}
        </output_format>
    """).strip()

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": textwrap.dedent(f"""
                    <qa_pair>
                    질문: {question}
                    답변: {answer}
                    </qa_pair>

                    위 QA에서 엔티티와 관계를 추출하세요.
                """).strip()}
            ],
            response_format={"type": "json_object"},
            temperature=0.2,
            max_tokens=1500
        )

        result_text = response.choices[0].message.content.strip()

        # JSON 추출
        if "```json" in result_text:
            result_text = result_text.split("```json")[1].split("```")[0].strip()
        elif "```" in result_text:
            result_text = result_text.split("```")[1].split("```")[0].strip()

        result = json.loads(result_text)

        # 기본 구조 보장
        if "entities" not in result:
            result["entities"] = []
        if "relationships" not in result:
            result["relationships"] = []

        # ===== 스키마 기반 관계 검증 =====
        allowed_rel_map = {}
        for r in relationship_types:
            key = (r['from'].lower(), r['to'].lower(), r['type'])
            allowed_rel_map[key] = True

        # 관계 필터링
        valid_relationships = []
        filtered_count = 0

        for rel in result.get("relationships", []):
            from_type = rel.get('from_type', '').lower()
            to_type = rel.get('to_type', '').lower()
            rel_type = rel.get('type', '')

            # 스키마에서 허용된 조합인지 확인
            if (from_type, to_type, rel_type) in allowed_rel_map:
                valid_relationships.append(rel)
            else:
                filtered_count += 1

        # 필터링 결과 적용
        result["relationships"] = valid_relationships

        return result

    except Exception as e:
        print(f"      오류: QA {qa.get('qa_id')} 추출 실패 - {e}")
        return {
            "entities": [],
            "relationships": []
        }


def extract_all_entities_and_relationships(all_qa_data: Dict[str, List[Dict]],
                                           unified_schema: Dict) -> Dict[int, Dict]:
    """모든 QA에서 엔티티와 관계를 한번에 추출"""
    print(f"\n[Step 2/3] 엔티티 및 관계 추출 중...")

    all_results = {}
    total_count = sum(len(qas) for qas in all_qa_data.values())
    processed = 0

    for dept, qa_list in all_qa_data.items():
        print(f"\n   {dept} 처리 중... ({len(qa_list)}개)")

        for i, qa in enumerate(qa_list):
            if (i + 1) % 10 == 0:
                print(f"      진행: {i+1}/{len(qa_list)} ({(i+1)/len(qa_list)*100:.1f}%)")

            result = extract_entities_and_relationships_from_qa(qa, unified_schema)
            all_results[qa['qa_id']] = result

            processed += 1

            # API 요청 제한 (필요시)
            if processed % 10 == 0:
                time.sleep(0.5)

    # 결과 저장
    results_path = OUTPUT_DIR / "extracted_graph.json"
    with open(results_path, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)

    # 통계 출력
    total_entities = sum(len(r.get('entities', [])) for r in all_results.values())
    total_relationships = sum(len(r.get('relationships', [])) for r in all_results.values())

    print(f"\n   결과 저장: {results_path}")
    print(f"   처리 완료: {processed}개 QA | {total_entities}개 엔티티 | {total_relationships}개 관계")

    return all_results


def save_graph_to_neo4j(all_results: Dict[int, Dict], driver):
    """추출된 엔티티와 관계를 Neo4j에 저장"""
    print(f"\n   Neo4j에 그래프 저장 중...")

    # 1. 엔티티 노드 생성 (type을 레이블로 사용)
    entity_data = []

    for qa_id, result in all_results.items():
        for entity in result.get('entities', []):
            entity_name = entity.get('name', '')
            entity_type = entity.get('type', '')
            if entity_name and entity_type:
                entity_data.append({
                    'name': entity_name,
                    'type': entity_type
                })

    # 유니크한 엔티티만 추출
    unique_entities = {}
    for e in entity_data:
        key = (e['name'], e['type'])
        if key not in unique_entities:
            unique_entities[key] = e

    # 타입별로 그룹화
    from collections import defaultdict
    entities_by_type = defaultdict(list)
    for e in unique_entities.values():
        entities_by_type[e['type']].append(e)

    # 타입별로 배치 생성 (동적 레이블 사용)
    total_entities = 0
    for entity_type, entities in entities_by_type.items():
        # type을 레이블로 변환 (첫 글자 대문자)
        label = entity_type.capitalize()

        for i in range(0, len(entities), 100):
            batch = entities[i:i+100]
            # 동적 레이블 사용
            driver.execute_query(f"""
                UNWIND $batch AS entity
                MERGE (e:{label} {{name: entity.name}})
            """, batch=batch, database_="neo4j")

        total_entities += len(entities)
        print(f"         {label}: {len(entities)}개")

    print(f"      step 1 - 엔티티 노드 {total_entities}개 생성")

    # 2. 인덱스 생성 (각 엔티티 타입별)
    entity_types = list(entities_by_type.keys())
    for entity_type in entity_types:
        label = entity_type.capitalize()
        driver.execute_query(
            f"CREATE INDEX {entity_type}_name_idx IF NOT EXISTS FOR (e:{label}) ON (e.name)",
            database_="neo4j"
        )

    driver.execute_query(
        "CREATE INDEX question_qa_id_idx IF NOT EXISTS FOR (q:Question) ON (q.qa_id)",
        database_="neo4j"
    )
    driver.execute_query(
        "CREATE INDEX answer_qa_id_idx IF NOT EXISTS FOR (a:Answer) ON (a.qa_id)",
        database_="neo4j"
    )
    print(f"      step 2 - 인덱스 생성 ({len(entity_types)}개 엔티티 타입)")

    # 3. QA-Entity 관계 데이터 수집
    mentions_data = []

    for qa_id, result in all_results.items():
        for entity in result.get('entities', []):
            entity_name = entity.get('name', '')
            entity_type = entity.get('type', '')
            if not entity_name or not entity_type:
                continue

            # Question-Entity 관계 데이터
            mentions_data.append({
                'qa_id': qa_id,
                'name': entity_name,
                'type': entity_type
            })

    # 4. MENTIONS 관계 생성
    if mentions_data:
        print(f"      step 3 - MENTIONS 관계 생성 중... ({len(mentions_data)}개)")

        # 타입별로 그룹화
        mentions_by_type = defaultdict(list)
        for data in mentions_data:
            mentions_by_type[data['type']].append(data)

        # 타입별로 배치 생성 (동적 레이블 사용)
        for entity_type, mentions in mentions_by_type.items():
            label = entity_type.capitalize()

            for i in range(0, len(mentions), 100):
                batch = mentions[i:i+100]
                driver.execute_query(f"""
                    UNWIND $batch AS data
                    MATCH (q:Question {{qa_id: data.qa_id}})
                    MATCH (e:{label} {{name: data.name}})
                    MERGE (q)-[:MENTIONS]->(e)
                """, batch=batch, database_="neo4j")

        print(f"      step 3 - MENTIONS 관계 {len(mentions_data)}개 생성 완료")

    # 5. 엔티티 간 관계 생성
    relationship_data = []

    for qa_id, result in all_results.items():
        for rel in result.get('relationships', []):
            from_name = rel.get('from', '')
            from_type = rel.get('from_type', '')
            to_name = rel.get('to', '')
            to_type = rel.get('to_type', '')
            rel_type = rel.get('type', 'RELATED_TO')

            if not from_name or not to_name:
                continue

            relationship_data.append({
                'from_name': from_name,
                'from_type': from_type,
                'to_name': to_name,
                'to_type': to_type,
                'rel_type': rel_type,
                'evidence': rel.get('evidence', ''),
                'qa_id': qa_id
            })

    if relationship_data:
        print(f"      step 4 - 엔티티 간 관계 생성 중... ({len(relationship_data)}개)")

        # 관계 타입별로 그룹화하여 처리
        rels_by_type = defaultdict(list)
        for rel in relationship_data:
            rels_by_type[rel['rel_type']].append(rel)

        total_created = 0
        for rel_type, rels in rels_by_type.items():
            # 엔티티 타입 조합별로 한번 더 그룹화
            rels_by_entity_types = defaultdict(list)
            for rel in rels:
                key = (rel['from_type'], rel['to_type'])
                rels_by_entity_types[key].append(rel)

            # 엔티티 타입 조합별로 처리
            for (from_type, to_type), type_rels in rels_by_entity_types.items():
                from_label = from_type.capitalize()
                to_label = to_type.capitalize()

                for i in range(0, len(type_rels), 50):
                    batch = type_rels[i:i+50]
                    driver.execute_query(f"""
                        UNWIND $batch AS rel
                        MATCH (e1:{from_label} {{name: rel.from_name}})
                        MATCH (e2:{to_label} {{name: rel.to_name}})
                        MERGE (e1)-[r:{rel_type}]->(e2)
                        ON CREATE SET
                            r.count = 1,
                            r.evidence = rel.evidence,
                            r.qa_ids = [rel.qa_id]
                        ON MATCH SET
                            r.count = r.count + 1,
                            r.qa_ids = CASE
                                WHEN rel.qa_id IN r.qa_ids THEN r.qa_ids
                                ELSE r.qa_ids + rel.qa_id
                            END
                    """, batch=batch, database_="neo4j")
                    total_created += len(batch)

        print(f"      step 4 - 엔티티 간 관계 {total_created}개 생성 완료")

    print(f"\n   그래프 저장 완료!")


# ============================================
# 메인
# ============================================

if __name__ == "__main__":
    print("=" * 70)
    print("의료 지식 그래프 구축 (Medical Knowledge Graph)")
    print("=" * 70)

    NEO4J_URI = os.getenv("NEO4J_URI")
    NEO4J_USER = os.getenv("NEO4J_USERNAME")
    NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

    if not all([NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD]):
        print("\n오류: Neo4j 연결 정보가 .env 파일에 없습니다")
        print("필요한 환경변수: NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD")
        sys.exit(1)

    if not OPENAI_API_KEY:
        print("\n오류: OpenAI API 키가 .env 파일에 없습니다")
        print("필요한 환경변수: OPENAI_API_KEY")
        sys.exit(1)

    if not DATA_DIR.exists():
        print(f"\n오류: 데이터 디렉토리가 없습니다: {DATA_DIR}")
        print("data/ 폴더에 진료과별(내과, 산부인과, 소아청소년과, 응급의학과) JSON 파일을 넣어주세요")
        sys.exit(1)

    MAX_QA_PER_DEPT = 50  # 각 진료과별 처리 데이터 개수

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    try:
        # ============================================
        # Step 0: 통일된 도메인 스키마
        # ============================================
        schema_path = OUTPUT_DIR / "domain_schemas.json"

        if schema_path.exists():  # 기존 스키마가 있으면 로드, 없으면 새로 추출
            print(f"\n[Step 0/3] 기존 스키마 로드 중...")
            with open(schema_path, 'r', encoding='utf-8') as f:
                unified_schema = json.load(f)
            print(f"   ✓ 기존 스키마 로드: {schema_path}")
        else:
            unified_schema = extract_unified_schema()

        # ============================================
        # Step 1: QA 그래프 구축
        # ============================================
        all_qa_data = load_all_qa_data(max_qa_per_dept=MAX_QA_PER_DEPT)
        save_qa_graph_to_neo4j(all_qa_data, driver)

        # ============================================
        # Step 2: 엔티티 및 관계 추출
        # ============================================
        results_path = OUTPUT_DIR / "extracted_graph.json"

        if results_path.exists():
            print(f"\n[Step 2/3] 기존 추출 결과 로드 중...")
            with open(results_path, 'r', encoding='utf-8') as f:
                all_results_raw = json.load(f)
                # JSON에서 로드할 때 qa_id가 문자열이므로 정수로 변환
                all_results = {int(k): v for k, v in all_results_raw.items()}

            total_entities = sum(len(r.get('entities', [])) for r in all_results.values())
            total_relationships = sum(len(r.get('relationships', [])) for r in all_results.values())
            print(f"   ✓ 기존 추출 결과 로드: {results_path}")
            print(f"   데이터: {len(all_results)}개 QA | {total_entities}개 엔티티 | {total_relationships}개 관계")
        else:
            all_results = extract_all_entities_and_relationships(all_qa_data, unified_schema)

        save_graph_to_neo4j(all_results, driver)

    finally:
        driver.close()