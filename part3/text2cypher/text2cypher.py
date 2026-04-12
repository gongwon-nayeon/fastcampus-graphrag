import os
from dotenv import load_dotenv
from neo4j import GraphDatabase
from neo4j_graphrag.retrievers import Text2CypherRetriever
from neo4j_graphrag.llm import OpenAILLM
from neo4j_graphrag.generation import RagTemplate, GraphRAG

load_dotenv()


# ============================================
# 타이타닉 지식그래프 스키마 정의
# ============================================

def get_titanic_schema():
    schema = {
        "entities": [
            {
                "label": "Passenger",
                "description": "타이타닉호에 탑승한 승객",
                "properties": [
                    {
                        "name": "PassengerId",
                        "type": "STRING",
                        "description": "승객 고유 식별자"
                    },
                    {
                        "name": "Name",
                        "type": "STRING",
                        "description": "승객의 이름"
                    },
                    {
                        "name": "Sex",
                        "type": "STRING",
                        "description": "승객의 성별 (male/female)"
                    },
                    {
                        "name": "Age",
                        "type": "FLOAT",
                        "description": "승객의 나이"
                    },
                    {
                        "name": "Survived",
                        "type": "INTEGER",
                        "description": "생존 여부 (1: 생존, 0: 사망)"
                    },
                    {
                        "name": "SibSp",
                        "type": "INTEGER",
                        "description": "동승한 형제자매/배우자 수"
                    },
                    {
                        "name": "Parch",
                        "type": "INTEGER",
                        "description": "동승한 부모/자녀 수"
                    },
                    {
                        "name": "Fare",
                        "type": "FLOAT",
                        "description": "지불한 운임 요금"
                    },
                    {
                        "name": "Ticket",
                        "type": "STRING",
                        "description": "티켓 번호"
                    }
                ]
            },
            {
                "label": "PClass",
                "description": "승객 등급 (사회경제적 지위를 나타냄)",
                "properties": [
                    {
                        "name": "Pclass",
                        "type": "INTEGER",
                        "description": "등급 번호 (1/2/3)"
                    },
                    {
                        "name": "ClassName",
                        "type": "STRING",
                        "description": "등급 이름 (1st Class/2nd Class/3rd Class)"
                    },
                    {
                        "name": "SES",
                        "type": "STRING",
                        "description": "사회경제적 지위 (Upper/Middle/Lower)"
                    }
                ]
            },
            {
                "label": "Cabin",
                "description": "승객이 머문 선실",
                "properties": [
                    {
                        "name": "Cabin",
                        "type": "STRING",
                        "description": "선실 번호"
                    }
                ]
            },
            {
                "label": "Port",
                "description": "승객이 탑승한 항구",
                "properties": [
                    {
                        "name": "Port",
                        "type": "STRING",
                        "description": "항구 코드 (S/C/Q)"
                    },
                    {
                        "name": "PortName",
                        "type": "STRING",
                        "description": "항구 이름 (Southampton/Cherbourg/Queenstown)"
                    }
                ]
            }
        ],
        "relations": [
            {
                "label": "TRAVELED_IN",
                "description": "승객이 특정 등급으로 여행함",
                "source": "Passenger",
                "target": "PClass"
            },
            {
                "label": "STAYED_IN",
                "description": "승객이 특정 선실에 머뭄",
                "source": "Passenger",
                "target": "Cabin"
            },
            {
                "label": "EMBARKED_AT",
                "description": "승객이 특정 항구에서 승선함",
                "source": "Passenger",
                "target": "Port"
            },
            {
                "label": "TRAVELED_WITH",
                "description": "같은 티켓으로 함께 여행한 승객들",
                "source": "Passenger",
                "target": "Passenger",
                "properties": [
                    {
                        "name": "Ticket",
                        "type": "STRING",
                        "description": "공유한 티켓 번호"
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
        "USER INPUT: '생존한 승객은 몇 명인가요?' QUERY: MATCH (p:Passenger) WHERE p.Survived = 1 RETURN count(p) AS survivors",
        "USER INPUT: '등급별 생존율은 어떻게 되나요?' QUERY: MATCH (p:Passenger)-[:TRAVELED_IN]->(c:PClass) RETURN c.ClassName, count(p) AS total, sum(p.Survived) AS survived, round(100.0 * sum(p.Survived) / count(p), 2) AS survival_rate ORDER BY c.Pclass",
        "USER INPUT: '여성 승객 중 생존자는 몇 명인가요?' QUERY: MATCH (p:Passenger) WHERE p.Sex = 'female' AND p.Survived = 1 RETURN count(p) AS female_survivors",
        "USER INPUT: '이름에 John이 포함된 승객을 알려주세요' QUERY: MATCH (p:Passenger) WHERE p.Name CONTAINS 'John' RETURN p.Name, p.Age, p.Sex, p.Survived LIMIT 10",
        "USER INPUT: '각 항구에서 탑승한 승객은 몇 명인가요?' QUERY: MATCH (p:Passenger)-[:EMBARKED_AT]->(port:Port) RETURN port.PortName, count(p) AS passengers ORDER BY passengers DESC",
        "USER INPUT: '같은 티켓으로 여행한 승객들을 보여주세요' QUERY: MATCH (p1:Passenger)-[r:TRAVELED_WITH]->(p2:Passenger) RETURN p1.Name, p2.Name, r.Ticket LIMIT 10",
        "USER INPUT: 'C85 선실에 머문 승객은 누구인가요?' QUERY: MATCH (p:Passenger)-[:STAYED_IN]->(c:Cabin) WHERE c.Cabin = 'C85' RETURN p.Name, p.Pclass, p.Survived",
        "USER INPUT: '18세 미만 어린이의 생존율은 어떻게 되나요?' QUERY: MATCH (p:Passenger) WHERE p.Age < 18 AND p.Age IS NOT NULL RETURN count(p) AS total, sum(p.Survived) AS survived, round(100.0 * sum(p.Survived) / count(p), 2) AS survival_rate",
        "USER INPUT: '가족 규모가 생존에 어떤 영향을 미쳤나요?' QUERY: MATCH (p:Passenger) WITH p, p.SibSp + p.Parch AS family_size RETURN CASE WHEN family_size = 0 THEN 'Alone' WHEN family_size <= 3 THEN 'Small' ELSE 'Large' END AS family_category, count(p) AS total, sum(p.Survived) AS survived, round(100.0 * sum(p.Survived) / count(p), 2) AS survival_rate ORDER BY family_category",
        "USER INPUT: '가장 높은 요금을 지불한 승객은 누구인가요?' QUERY: MATCH (p:Passenger) WHERE p.Fare IS NOT NULL RETURN p.Name, p.Fare, p.Pclass ORDER BY p.Fare DESC LIMIT 5"
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
# GraphRAG 파이프라인 생성
# ============================================

llm = OpenAILLM(
    model_name="gpt-4o",
    model_params={"temperature": 0}
)

def create_graphrag_pipeline(driver, use_schema=True, use_examples=True):

    schema_json = get_titanic_schema() if use_schema else None
    schema_text = schema_to_text(schema_json) if schema_json else None
    examples = get_example_queries() if use_examples else None

    retriever = Text2CypherRetriever(
        driver=driver,
        llm=llm,
        neo4j_schema=schema_text,
        examples=examples
    )

    prompt_template = RagTemplate(
        template="""
당신은 타이타닉 데이터베이스 전문 분석가입니다.
사용자의 질문에 대해 검색된 데이터를 기반으로 정확하고 상세한 답변을 제공하세요.

<retrieval_result>
{context}
</retrieval_result>

<user_query>
{query_text}
</user_query>

<answer_guidelines>
1. 검색된 데이터에 포함된 구체적인 숫자, 이름, 비율 등을 반드시 포함하여 답변하세요.
2. 데이터가 여러 건인 경우 주요 내용을 모두 언급하세요.
3. 검색된 데이터에 없는 정보는 추측하거나 만들어내지 마세요.
4. 검색 결과의 원본 텍스트를 활용하되 한국어로 자연스럽게 작성하세요.
5. 필요한 경우 비교, 순위, 통계 등을 명확히 설명하세요.
</answer_guidelines>

답변:
            """,
        expected_inputs=["context", "query_text"]
    )

    rag = GraphRAG(retriever=retriever, llm=llm, prompt_template=prompt_template)
    return rag


def search_with_graphrag(rag, query_text, return_context=False):
    print(f"\n{'=' * 60}")
    print(f"질문: {query_text}")
    print(f"{'=' * 60}")

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
# 메인 함수
# ============================================

def main():
    print("\n" + "=" * 60)
    print("타이타닉 지식그래프 기반 Text2Cypher GraphRAG")
    print("=" * 60)

    driver = create_neo4j_driver()

    rag = create_graphrag_pipeline(driver=driver, use_schema=True, use_examples=True)

    # test_query = "타이타닉 참사에서 생존한 승객은 몇 명인가요?"
    # test_query = "1등급 승객의 생존율은 어떻게 되나요?"
    # test_query = "여성 승객은 총 몇 명이었나요?"
    # test_query = "어느 항구에서 가장 많은 승객이 탑승했나요?"
    # test_query = "남성과 여성의 생존율을 비교해주세요"
    # test_query = "3등급 승객 중 생존자는 몇 명인가요?"
    # test_query = "가장 높은 요금을 지불한 승객은 누구인가요?"
    test_query = "어린이(18세 미만)의 생존율은 얼마인가요?"

    RETURN_CONTEXT = True
    response = search_with_graphrag( # search 메서드
        rag,
        test_query,
        return_context=RETURN_CONTEXT,
    )

    if RETURN_CONTEXT and hasattr(response, 'retriever_result'):
        print("\n" + "=" * 60)
        print("생성된 Cypher 쿼리문")
        print("=" * 60)
        print(response.retriever_result.metadata["cypher"]) # Text2CypherRetriever가 생성한 Cypher 쿼리문 출력(검색 결과의 메타데이터)

        print("\n" + "=" * 60)
        print("검색 결과 ")
        print("=" * 60)
        for i in response.retriever_result.items: # 검색된 데이터들(items)
            print(i.content)

    print("\n" + "=" * 60)
    print("최종 답변")
    print("=" * 60)
    print(response.answer)

    close_neo4j_driver(driver)

if __name__ == "__main__":
    main()