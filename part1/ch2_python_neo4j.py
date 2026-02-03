import os
from neo4j import GraphDatabase, RoutingControl
from dotenv import load_dotenv

load_dotenv()

URI = os.getenv("NEO4J_URI")
USERNAME = os.getenv("NEO4J_USERNAME")
PASSWORD = os.getenv("NEO4J_PASSWORD")

driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))
driver.verify_connectivity()


# -----------------------------------------
# 데이터 조회 (execute_query)
# -----------------------------------------
records, summary, keys = driver.execute_query("""
    MATCH (t:Tag {name: $tagName})<-[:TAGGED]-(q:Question)<-[:ASKED]-(u:User)
    RETURN q.title AS title, u.display_name AS author
    LIMIT 5
    """,
    database_="neo4j",
    tagName="neo4j",
    routing_=RoutingControl.READ # 읽기 전용 쿼리
)

for record in records:
    print(record.data())  # 딕셔너리 형태로 출력
    # print(f"질문 제목: {record['title']}, 작성자: {record['author']}")

print(f"쿼리`{summary.query}`가 반환한 결과 {len(records)} 개는 {summary.result_available_after} ms 만에 반환되었습니다.\n")


# -----------------------------------------
# 데이터 생성
# -----------------------------------------
create_query = """
CREATE (q:Question {
    id: $questionId,
    title: $title,
    body: $body,
    created_at: datetime()
})
RETURN q.id AS new_id, q.title AS new_title
"""

records, summary, keys = driver.execute_query(
    create_query,
    questionId=999999,
    title="질문의 제목입니다.",
    body="질문의 본문입니다.",
    database_="neo4j",
)

if records:
    print(f"ID '{records[0]['new_id']}'인 질문이 생성되었습니다.")
    print(f"생성된 노드 개수: {summary.counters.nodes_created}")

driver.close()


# -----------------------------------------

driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))
driver.verify_connectivity()


# -----------------------------------------
# 데이터 조회 (세션 + 트랜잭션 = execute_read)
# -----------------------------------------
# 트랜잭션 함수 정의
def get_questions(tx, tag_name): # tx : 트랜잭션 객체
    result = tx.run("""
        MATCH (t:Tag {name: $tagName})<-[:TAGGED]-(q:Question)<-[:ASKED]-(u:User)
        RETURN q.title AS title, u.display_name AS author
        LIMIT 5
        """,
        tagName=tag_name
    )

    return [record.data() for record in result]

with driver.session(database="neo4j") as session:
    records = session.execute_read(get_questions, "neo4j")
    for record in records:
        print(record)

driver.close()