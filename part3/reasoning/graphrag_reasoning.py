import os
from dotenv import load_dotenv
from neo4j import GraphDatabase
from neo4j_graphrag.retrievers import Text2CypherRetriever
from neo4j_graphrag.llm import OpenAILLM
from neo4j_graphrag.generation import RagTemplate, GraphRAG
from neo4j_graphrag.types import RetrieverResultItem, RetrieverResult

load_dotenv()


# ============================================
# 커스텀 Retriever: Cypher 쿼리를 컨텍스트에 포함
# ============================================

class Text2CypherWithQueryRetriever(Text2CypherRetriever):
    """생성된 Cypher 쿼리를 결과에 포함시키는 커스텀 Retriever

    기존 Text2CypherRetriever를 확장하여 LLM에게 전달되는 컨텍스트에
    생성된 Cypher 쿼리문을 함께 포함시킵니다.

    이를 통해 LLM이:
    1. 어떤 쿼리가 실행되었는지 알 수 있음
    2. 쿼리 결과를 더 정확하게 해석할 수 있음
    3. 추론 경로를 명확히 설명할 수 있음
    """

    def search(self, query_text: str, **kwargs) -> RetrieverResult:
        # 1) 부모 클래스의 search 실행
        result = super().search(query_text, **kwargs)

        # 2) 생성된 Cypher 쿼리 가져오기
        cypher_query = result.metadata.get("cypher", "")

        # 3) Cypher 쿼리는 한 번만 포함하고, 모든 결과를 순차적으로 붙이기
        # 모든 결과 아이템의 content를 하나로 합침
        all_results = "\n\n".join([item.content for item in result.items])

        # Cypher 쿼리 + 전체 결과를 하나의 컨텍스트로 생성
        combined_content = f"""### 실행된 Cypher 쿼리:
```cypher
{cypher_query}
```

### 쿼리 실행 결과:
{all_results}
"""

        # 하나의 아이템으로 반환
        new_items = [
            RetrieverResultItem(
                content=combined_content,
                metadata=result.items[0].metadata if result.items else {}
            )
        ]

        return RetrieverResult(
            items=new_items,
            metadata=result.metadata
        )


# ============================================
# 의료 지식그래프 스키마 정의
# ============================================

def get_medical_schema():
    schema = {
        "entities": [
            {
                "label": "Question",
                "description": "의료 QA 질문",
                "properties": [
                    {
                        "name": "qa_id",
                        "type": "INTEGER",
                        "description": "질문 고유 식별자"
                    },
                    {
                        "name": "content",
                        "type": "STRING",
                        "description": "질문 내용"
                    },
                    {
                        "name": "q_type",
                        "type": "STRING",
                        "description": "질문 유형"
                    },
                    {
                        "name": "domain",
                        "type": "STRING",
                        "description": "도메인"
                    },
                    {
                        "name": "department",
                        "type": "STRING",
                        "description": "진료과 (내과, 산부인과, 소아청소년과, 응급의학과)"
                    }
                ]
            },
            {
                "label": "Answer",
                "description": "의료 QA 답변",
                "properties": [
                    {
                        "name": "qa_id",
                        "type": "INTEGER",
                        "description": "답변 고유 식별자"
                    },
                    {
                        "name": "content",
                        "type": "STRING",
                        "description": "답변 내용"
                    }
                ]
            },
            {
                "label": "Department",
                "description": "진료과",
                "properties": [
                    {
                        "name": "name",
                        "type": "STRING",
                        "description": "진료과 (내과, 산부인과, 소아청소년과, 응급의학과)"
                    }
                ]
            },
            {
                "label": "Symptom",
                "description": "증상 (환자가 경험하는 의료적 증후)",
                "properties": [
                    {
                        "name": "name",
                        "type": "STRING",
                        "description": "증상 이름"
                    }
                ]
            },
            {
                "label": "Disease",
                "description": "질병 (진단된 의료 상태)",
                "properties": [
                    {
                        "name": "name",
                        "type": "STRING",
                        "description": "질병 이름"
                    }
                ]
            },
            {
                "label": "Test",
                "description": "검사 (질병 진단을 위한 의료 검사)",
                "properties": [
                    {
                        "name": "name",
                        "type": "STRING",
                        "description": "검사 이름"
                    }
                ]
            },
            {
                "label": "Treatment",
                "description": "치료법 (질병 치료를 위한 의료 처치)",
                "properties": [
                    {
                        "name": "name",
                        "type": "STRING",
                        "description": "치료법 이름"
                    }
                ]
            },
            {
                "label": "Medication",
                "description": "약물 (질병 치료를 위한 의약품)",
                "properties": [
                    {
                        "name": "name",
                        "type": "STRING",
                        "description": "약물 이름"
                    }
                ]
            },
            {
                "label": "Anatomy",
                "description": "해부학적 구조 (신체 부위 또는 기관)",
                "properties": [
                    {
                        "name": "name",
                        "type": "STRING",
                        "description": "해부학적 구조 이름"
                    }
                ]
            }
        ],
        "relations": [
            {
                "label": "HAS_ANSWER",
                "description": "질문에 대한 답변",
                "source": "Question",
                "target": "Answer"
            },
            {
                "label": "BELONGS_TO",
                "description": "질문이 특정 진료과에 속함",
                "source": "Question",
                "target": "Department"
            },
            {
                "label": "MENTIONS",
                "description": "질문이 엔티티를 언급함",
                "source": "Question",
                "target": "Symptom"
            },
            {
                "label": "MENTIONS",
                "description": "질문이 엔티티를 언급함",
                "source": "Question",
                "target": "Disease"
            },
            {
                "label": "MENTIONS",
                "description": "질문이 엔티티를 언급함",
                "source": "Question",
                "target": "Test"
            },
            {
                "label": "MENTIONS",
                "description": "질문이 엔티티를 언급함",
                "source": "Question",
                "target": "Treatment"
            },
            {
                "label": "MENTIONS",
                "description": "질문이 엔티티를 언급함",
                "source": "Question",
                "target": "Medication"
            },
            {
                "label": "MENTIONS",
                "description": "질문이 엔티티를 언급함",
                "source": "Question",
                "target": "Anatomy"
            },
            {
                "label": "INDICATES",
                "description": "증상이 질병을 나타냄 (추론의 출발점)",
                "source": "Symptom",
                "target": "Disease",
                "properties": [
                    {
                        "name": "evidence",
                        "type": "STRING",
                        "description": "증거 텍스트"
                    },
                    {
                        "name": "qa_id",
                        "type": "INTEGER",
                        "description": "출처 QA ID"
                    }
                ]
            },
            {
                "label": "DIAGNOSES_FOR",
                "description": "검사가 질병을 진단함",
                "source": "Test",
                "target": "Disease",
                "properties": [
                    {
                        "name": "evidence",
                        "type": "STRING",
                        "description": "증거 텍스트"
                    },
                    {
                        "name": "qa_id",
                        "type": "INTEGER",
                        "description": "출처 QA ID"
                    }
                ]
            },
            {
                "label": "TREATS",
                "description": "치료법이 질병을 치료함 (추론의 목표점)",
                "source": "Treatment",
                "target": "Disease",
                "properties": [
                    {
                        "name": "evidence",
                        "type": "STRING",
                        "description": "증거 텍스트"
                    },
                    {
                        "name": "qa_id",
                        "type": "INTEGER",
                        "description": "출처 QA ID"
                    }
                ]
            },
            {
                "label": "TREATS",
                "description": "약물이 질병을 치료함 (추론의 목표점)",
                "source": "Medication",
                "target": "Disease",
                "properties": [
                    {
                        "name": "evidence",
                        "type": "STRING",
                        "description": "증거 텍스트"
                    },
                    {
                        "name": "qa_id",
                        "type": "INTEGER",
                        "description": "출처 QA ID"
                    }
                ]
            },
            {
                "label": "LOCATED_IN",
                "description": "해부학적 구조에 위치한 질병",
                "source": "Anatomy",
                "target": "Disease",
                "properties": [
                    {
                        "name": "evidence",
                        "type": "STRING",
                        "description": "증거 텍스트"
                    },
                    {
                        "name": "qa_id",
                        "type": "INTEGER",
                        "description": "출처 QA ID"
                    }
                ]
            }
        ]
    }
    return schema


def schema_to_text(schema):
    """JSON 스키마를 Text2CypherRetriever가 이해할 수 있는 문자열 형식으로 변환"""
    lines = ["Node properties:"]

    # 엔티티(노드) 정보 추가
    for entity in schema["entities"]:
        props = ", ".join([f"{p['name']}: {p['type']}" for p in entity["properties"]])
        lines.append(f"{entity['label']} {{{props}}}")

    # 관계 속성 추가
    lines.append("\nRelationship properties:")
    for relation in schema["relations"]:
        if "properties" in relation and relation["properties"]:
            props = ", ".join([f"{p['name']}: {p['type']}" for p in relation["properties"]])
            lines.append(f"{relation['label']} {{{props}}}")

    # 관계 정의 추가
    lines.append("\nThe relationships:")
    for relation in schema["relations"]:
        source = relation["source"]
        target = relation["target"]
        label = relation["label"]
        if "properties" in relation and relation["properties"]:
            prop_names = ", ".join([p['name'] for p in relation["properties"]])
            lines.append(f"(:{source})-[:{label} {{{prop_names}}}]->(:{target})")
        else:
            lines.append(f"(:{source})-[:{label}]->(:{target})")

    return "\n".join(lines)


# ============================================
# 예제 쿼리 정의
# ============================================

def get_example_queries():
    examples = [
        # ========================================
        # 패턴 1: 증상 단독 언급 → 포괄적 추론
        # ========================================
        """USER INPUT: '기침이 심해요'
WHAT USER MENTIONED: 증상 하나 (기침)
WHAT SYSTEM INFERS: 가능한 질병들 + 동반되는 다른 증상들 + 치료 옵션
INFERENCE PATH: Symptom → Disease → [Symptom, Treatment, Medication]
QUERY: MATCH (s:Symptom)-[:INDICATES]->(d:Disease)
WHERE s.name CONTAINS '기침' OR s.name CONTAINS '가래'
OPTIONAL MATCH (d)<-[:INDICATES]-(other_s:Symptom)
OPTIONAL MATCH (tr:Treatment)-[:TREATS]->(d)
OPTIONAL MATCH (m:Medication)-[:TREATS]->(d)
RETURN DISTINCT d.name AS disease,
       collect(DISTINCT other_s.name) AS related_symptoms,
       collect(DISTINCT tr.name) AS treatments,
       collect(DISTINCT m.name) AS medications
LIMIT 10""",

        # ========================================
        # 패턴 2: 다중 증상 언급 → 교집합 추론
        # ========================================
        """USER INPUT: '기침도 하고 열도 나요'
WHAT USER MENTIONED: 증상 두 가지 (기침 + 열)
WHAT SYSTEM INFERS: 두 증상의 공통 원인 질병 + 일치도 순위 + 치료 옵션
INFERENCE PATH: [Symptom, Symptom] → Disease (intersection) → [Treatment, Medication]
QUERY: MATCH (s1:Symptom)-[:INDICATES]->(d:Disease)<-[:INDICATES]-(s2:Symptom)
WHERE (s1.name CONTAINS '기침' OR s1.name CONTAINS '가래')
  AND (s2.name CONTAINS '발열' OR s2.name CONTAINS '열')
WITH d, count(DISTINCT s1) + count(DISTINCT s2) AS symptom_match
OPTIONAL MATCH (tr:Treatment)-[:TREATS]->(d)
OPTIONAL MATCH (m:Medication)-[:TREATS]->(d)
RETURN d.name AS disease,
       symptom_match AS matching_symptoms,
       collect(DISTINCT tr.name) AS treatments,
       collect(DISTINCT m.name) AS medications
ORDER BY symptom_match DESC
LIMIT 10""",

        # ========================================
        # 패턴 3: 질병 단독 언급 → 역방향 확장
        # ========================================
        """USER INPUT: '폐렴이에요'
WHAT USER MENTIONED: 질병명 하나 (폐렴)
WHAT SYSTEM INFERS: 전형적 증상들 + 필요한 검사 + 치료법 + 발병 부위
INFERENCE PATH: Disease → [Symptom, Test, Treatment, Medication, Anatomy]
QUERY: MATCH (d:Disease)
WHERE d.name CONTAINS '폐렴'
OPTIONAL MATCH (s:Symptom)-[:INDICATES]->(d)
OPTIONAL MATCH (t:Test)-[:DIAGNOSES_FOR]->(d)
OPTIONAL MATCH (tr:Treatment)-[:TREATS]->(d)
OPTIONAL MATCH (m:Medication)-[:TREATS]->(d)
OPTIONAL MATCH (a:Anatomy)-[:LOCATED_IN]->(d)
RETURN DISTINCT d.name AS disease,
       collect(DISTINCT s.name) AS symptoms,
       collect(DISTINCT t.name) AS diagnostic_tests,
       collect(DISTINCT tr.name) AS treatments,
       collect(DISTINCT m.name) AS medications,
       collect(DISTINCT a.name) AS location
LIMIT 10""",

        # ========================================
        # 패턴 4: 신체 부위 언급 → 관련 지식 탐색
        # ========================================
        """USER INPUT: '골반이 불편해요'
WHAT USER MENTIONED: 신체 부위 하나 (골반)
WHAT SYSTEM INFERS: 해당 부위 질병들 + 증상들 + 진단 검사들
INFERENCE PATH: Anatomy → Disease → [Symptom, Test]
QUERY: MATCH (a:Anatomy)-[:LOCATED_IN]->(d:Disease)
WHERE a.name CONTAINS '골반' OR a.name CONTAINS '골반뼈'
OPTIONAL MATCH (s:Symptom)-[:INDICATES]->(d)
OPTIONAL MATCH (t:Test)-[:DIAGNOSES_FOR]->(d)
RETURN DISTINCT d.name AS disease,
       collect(DISTINCT s.name) AS symptoms,
       collect(DISTINCT t.name) AS tests
LIMIT 10""",

        # ========================================
        # 패턴 5: 치료/약물 언급 → 대상 질병 추론
        # ========================================
        """USER INPUT: '항생제 먹고 있어요'
WHAT USER MENTIONED: 약물 하나 (항생제)
WHAT SYSTEM INFERS: 치료 대상 질병들 + 그 질병의 증상들 + 필요한 검사들
INFERENCE PATH: Medication → Disease → [Symptom, Test]
QUERY: MATCH (m:Medication)-[:TREATS]->(d:Disease)
WHERE m.name CONTAINS '항생제' OR m.name CONTAINS '항균'
OPTIONAL MATCH (s:Symptom)-[:INDICATES]->(d)
OPTIONAL MATCH (t:Test)-[:DIAGNOSES_FOR]->(d)
RETURN DISTINCT d.name AS disease,
       m.name AS medication,
       collect(DISTINCT s.name) AS symptoms,
       collect(DISTINCT t.name) AS tests
LIMIT 10""",
    ]
    return examples


# ============================================
# Neo4j 연결 관리
# ============================================

def create_neo4j_driver():
    uri = os.getenv("NEO4J_URI")
    username = os.getenv("NEO4J_USERNAME")
    password = os.getenv("NEO4J_PASSWORD")

    if not all([uri, username, password]):
        raise ValueError("NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD 환경 변수를 설정해주세요.")

    driver = GraphDatabase.driver(uri, auth=(username, password))

    try:
        driver.verify_connectivity()
        print(f"✓ Neo4j 연결 성공: {uri}")
    except Exception as e:
        print(f"✗ Neo4j 연결 실패: {e}")
        raise

    return driver


def close_neo4j_driver(driver):
    if driver:
        driver.close()


# ============================================
# 추론 특화 Text2Cypher 프롬프트
# ============================================

def get_reasoning_text2cypher_prompt():
    return """
당신은 의료 지식그래프 추론 전문가입니다.

# 핵심 원칙: 그래프 기반 추론 (Graph Reasoning)
주어진 지식 구조를 기반으로 정보를 조회하기 위한 Cypher 쿼리를 생성합니다.
특히 사용자가 A만 물어봤더라도, 그래프 관계를 따라가며 연결된 B, C, D까지 추론해야 합니다.

<knowledge_graph_structure>
{schema}

**핵심 추론 경로들:**
1. Symptom -[INDICATES]-> Disease
   - 증상이 특정 질병을 암시함
   - 역방향: Disease를 통해 다른 증상들도 찾을 수 있음

2. Test -[DIAGNOSES_FOR]-> Disease
   - 검사가 질병을 진단함
   - 역방향: Disease를 알면 필요한 검사를 찾을 수 있음

3. Treatment -[TREATS]-> Disease / Medication -[TREATS]-> Disease
   - 치료법/약물이 질병을 치료함
   - 역방향: Disease를 알면 치료 옵션을 찾을 수 있음

4. Anatomy -[LOCATED_IN]-> Disease
   - 해부학적 구조(신체 부위)에 위치한 질병들
   - 역방향: Disease를 통해 발병 부위를 찾을 수 있음

**다중 홉(Multi-hop) 추론 경로 예시:**
- Symptom → Disease → Treatment (2-hop: 증상에서 치료까지)
- Symptom → Disease → Symptom (2-hop: 한 증상에서 같은 질병의 다른 증상들)
- Symptom → Disease → Test (2-hop: 증상에서 필요한 검사까지)
- Symptom → Disease ← Anatomy (2-hop: 증상에서 발병 부위까지)
- Treatment → Disease → Symptom (2-hop: 치료법에서 대상 질병의 증상들)
- Anatomy → Disease → Symptom, Test, Treatment (2-hop: 신체 부위에서 모든 관련 정보)

이러한 경로들을 조합하여 사용자의 단편적 질문을 종합적 답변으로 확장하세요.
</knowledge_graph_structure>

<reasoning_mindset>

**질문 분석 단계:**
1. 사용자가 명시한 것: 무엇을 직접 언급했는가? (증상? 질병? 약물? 부위?)
2. 사용자가 원하는 것: 명시하지 않았지만 알고 싶어하는 것은? (원인? 치료? 진단?)
3. 추론 경로 설계: 어떤 그래프 경로를 따라가야 하는가?

**추론 전략 결정:**
- 단일 엔티티 언급 → 연결된 모든 관련 정보 수집 (포괄적 탐색)
- 다중 엔티티 언급 → 공통 연결점 찾기 (교집합 추론)
- 명시적 요청 → 특정 경로만 탐색 (타겟 추론)

**쿼리 구조화:**
1. 시작점: 사용자가 언급한 엔티티로부터 시작
2. 확장: OPTIONAL MATCH로 관련 정보들 수집
3. 집계: COLLECT, COUNT로 정보 종합
4. 정렬: ORDER BY로 중요도/빈도 순 정렬
5. 제한: LIMIT로 핵심 정보만 반환 (단, 너무 적지 않게)
</reasoning_mindset>

<example_queries>
{examples}
</example_queries>

<critical_guidelines>
## 필수 지침

1. **항상 OPTIONAL MATCH 활용**: 연결된 정보가 없을 수도 있으므로 OPTIONAL MATCH로 안전하게 수집
2. **다중 홉 경로 설계**: 단일 관계만 보지 말고 2-3 홉 경로를 적극 활용
3. **포괄적 정보 수집**: 사용자가 A만 물어봤어도 B, C, D까지 수집 (예: 증상 → 질병, 검사, 치료, 위치)
4. **COLLECT로 집계**: 여러 관련 정보를 배열로 수집하여 종합 제시
5. **적절한 LIMIT**: 너무 적으면 정보 부족, 너무 많으면 노이즈 (보통 5-10)
6. **COUNT/ORDER BY 활용**: 빈도나 일치도로 정렬하여 중요한 정보부터 제시
7. **WHERE 조건 유연성**: CONTAINS 사용으로 부분 일치 허용
8. **DISTINCT 사용**: 중복 제거로 깔끔한 결과 제공

## 절대 하지 말 것
- 단일 관계만 보는 쿼리 (예: Symptom만 찾고 끝)
- OPTIONAL MATCH 없이 모든 경로를 MATCH로만 구성 (데이터 누락 위험)
- 지나치게 복잡한 쿼리 (가독성 저하)
</critical_guidelines>

<user_query>
{query_text}
</user_query>

위 지식 구조와 추론 전략을 바탕으로, 사용자 질문에 대한 Cypher 쿼리를 생성하세요.
쿼리만 반환하고 다른 설명은 포함하지 마세요.
"""


# ============================================
# GraphRAG 파이프라인 생성
# ============================================

llm = OpenAILLM(
    model_name="gpt-4o",
    model_params={"temperature": 0}
)

def create_graphrag_pipeline(driver, use_schema=True, use_examples=True):

    schema_json = get_medical_schema() if use_schema else None
    schema_text = schema_to_text(schema_json) if schema_json else None
    examples = get_example_queries() if use_examples else None

    # 추론 특화 Text2Cypher 프롬프트 사용
    reasoning_prompt = get_reasoning_text2cypher_prompt()

    # 커스텀 Retriever 사용: Cypher 쿼리를 컨텍스트에 포함 + 추론 특화 프롬프트
    retriever = Text2CypherWithQueryRetriever(
        driver=driver,
        llm=llm,
        neo4j_schema=schema_text,
        examples=examples,
        custom_prompt=reasoning_prompt
    )

    prompt_template = RagTemplate(
        template="""
당신은 의료 지식그래프 추론 전문가입니다.
사용자의 질문에 대해 그래프 추론을 통해 얻은 데이터를 기반으로 정확하고 상세한 답변을 제공하세요.

<retrieval_result>
{context}
</retrieval_result>

<user_query>
{query_text}
</user_query>

<answer_guidelines>
1. Cypher 쿼리 분석: Context에 포함된 Cypher 쿼리를 분석하여 어떤 추론 경로가 사용되었는지 파악하세요.
   - 예: Symptom → Disease (증상에서 질병 추론)
   - 예: Symptom → Disease → Treatment (증상에서 치료법까지 2-hop 추론)
   - 예: Disease ← Symptom (역방향 추론: 질병의 증상들)

2. 추론 과정 설명: 주어진 검색 결과에서 발견한 '추론 과정'을 명확히 설명하세요.

3. 구체적 정보 포함: 검색된 데이터의 구체적인 질병명, 증상, 치료법, 약물명 등을 반드시 포함하세요. 검색결과가 없다면 굳이 언급하지 마세요.

4. 포괄적 답변: 단순한 증상 질문("열이 나요")이라도 관련 질병, 동반 증상, 검사, 치료법까지 종합적으로 설명하세요.

5. 우선순위 제시: 여러 결과가 있을 때 빈도, 중요도, 일치도 등을 고려하여 우선순위를 제시하세요.

6. 의료 안내 필수: 모든 답변 마지막에 "이 정보는 참고용이며, 정확한 진단과 치료를 위해서는 반드시 전문의와 상담이 필요합니다"를 포함하세요.

7. 데이터 기반 답변: 검색된 데이터에 없는 정보는 추측하거나 만들어내지 마세요.

8. 구조화 된 답변: 사용자가 가독성 있게 이해할 수 있도록, 주어진 정보를 명확한 문장과 단락으로 구조화하여 제시하세요.
</answer_guidelines>

답변:
            """,
        expected_inputs=["context", "query_text"]
    )

    rag = GraphRAG(retriever=retriever, llm=llm, prompt_template=prompt_template)
    return rag


def search_with_graphrag(rag, query_text, return_context=False):
    try:
        # GraphRAG 검색 (쿼리 생성 → 실행 → 답변 생성)
        response = rag.search(
            query_text=query_text,
            return_context=return_context,
        )

        return response

    except Exception as e:
        print(f"\n✗ 오류 발생: {e}")
        raise


# ============================================
# 엔티티 샘플 조회
# ============================================

def show_entity_samples(driver, limit=5):
    """지식그래프에 실제로 존재하는 엔티티 샘플들을 출력

    Args:
        driver: Neo4j driver
        limit: 각 엔티티 타입별 출력할 샘플 개수
    """
    entity_types = ["Symptom", "Disease", "Test", "Treatment", "Medication", "Anatomy"]

    print("\n" + "=" * 60)
    print(f"지식그래프에 저장된 엔티티 샘플 (각 타입별 {limit}개)")
    print("=" * 60)

    with driver.session() as session:
        for entity_type in entity_types:
            query = f"MATCH (n:{entity_type}) RETURN n.name AS name LIMIT {limit}"
            try:
                result = session.run(query)
                names = [record["name"] for record in result if record["name"]]

                if names:
                    names_str = " ".join([f"[{name}]" for name in names])
                    print(f"\n[{entity_type}] {names_str}")
                else:
                    print(f"\n[{entity_type}] (데이터 없음)")
            except Exception as e:
                print(f"\n[{entity_type}] (조회 실패: {e})")


# ============================================
# 메인 함수
# ============================================

def main():
    print("\n" + "=" * 60)
    print("의료 지식그래프 기반 Text2Cypher GraphRAG 추론 시스템")
    print("=" * 60)

    driver = create_neo4j_driver()

    rag = create_graphrag_pipeline(driver=driver, use_schema=True, use_examples=True)

    # 사용자 입력 루프
    RETURN_CONTEXT = True

    while True:
        show_entity_samples(driver, limit=5)

        print("\n" + "=" * 60)
        test_query = input("질문: ").strip()

        if not test_query or test_query.lower() in ['quit', 'exit', 'q']:
            print("\n시스템을 종료합니다.")
            break

        response = search_with_graphrag(
            rag,
            test_query,
            return_context=RETURN_CONTEXT,
        )

        if RETURN_CONTEXT and hasattr(response, 'retriever_result'):
            print("\n" + "=" * 60)
            print("생성된 Cypher 쿼리문")
            print("=" * 60)
            print(response.retriever_result.metadata["cypher"])

            print("\n" + "=" * 60)
            print("검색 결과 ")
            print("=" * 60)
            for i in response.retriever_result.items:
                print(i.content)

        print("\n" + "=" * 60)
        print("최종 답변")
        print("=" * 60)
        print(response.answer)

    close_neo4j_driver(driver)

if __name__ == "__main__":
    main()