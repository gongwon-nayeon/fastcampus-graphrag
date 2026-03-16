import os
import sys
from dotenv import load_dotenv
from neo4j import GraphDatabase

from law2kg import (
    build_interpretation_graph
)


def print_interpretation_statistics(driver):
    """해석례 그래프 통계 출력 (step2용)"""
    print("\n" + "=" * 50)
    print("해석례 그래프 통계")
    print("=" * 50)

    # 노드 수
    stats = [
        ("해석례(LegalInterpretation)", "MATCH (n:LegalInterpretation) RETURN count(n) as cnt"),
        ("질의(Question)", "MATCH (n:Question) RETURN count(n) as cnt"),
        ("회답(Answer)", "MATCH (n:Answer) RETURN count(n) as cnt"),
        ("이유(Reason)", "MATCH (n:Reason) RETURN count(n) as cnt"),
        ("기관(Organization)", "MATCH (n:Organization) RETURN count(n) as cnt"),
    ]

    print("\n노드:")
    for label, query in stats:
        result = driver.execute_query(query, database_="neo4j")
        count = result.records[0]['cnt']
        print(f"  {label}: {count}개")

    # 관계 수
    print("\n관계:")
    rel_stats = [
        ("INTERPRETS", "MATCH ()-[r:INTERPRETS]->() RETURN count(r) as cnt"),
        ("CITES", "MATCH ()-[r:CITES]->() RETURN count(r) as cnt"),
        ("HAS_QUESTION", "MATCH ()-[r:HAS_QUESTION]->() RETURN count(r) as cnt"),
        ("HAS_ANSWER", "MATCH ()-[r:HAS_ANSWER]->() RETURN count(r) as cnt"),
        ("ANSWERED_BY", "MATCH ()-[r:ANSWERED_BY]->() RETURN count(r) as cnt"),
        ("SUPPORTED_BY", "MATCH ()-[r:SUPPORTED_BY]->() RETURN count(r) as cnt"),
        ("REQUESTED", "MATCH ()-[r:REQUESTED]->() RETURN count(r) as cnt"),
    ]

    for label, query in rel_stats:
        result = driver.execute_query(query, database_="neo4j")
        count = result.records[0]['cnt']
        print(f"  {label}: {count}개")

    # CITES 상세 분석
    print("\nCITES 관계 상세:")
    cites_detail_query = """
    MATCH (i:LegalInterpretation)-[r:CITES]->(target)
    WITH labels(target)[0] as target_type, count(r) as cnt
    RETURN target_type, cnt
    ORDER BY cnt DESC
    """
    result = driver.execute_query(cites_detail_query, database_="neo4j")
    for record in result.records:
        print(f"  → {record['target_type']}: {record['cnt']}개")

    # 결론 유형별 통계
    print("\n결론 유형 분포:")
    conclusion_query = """
    MATCH (a:Answer)
    WHERE a.conclusion_result IS NOT NULL
    RETURN a.conclusion_result as result, count(*) as cnt
    ORDER BY cnt DESC
    """
    try:
        result = driver.execute_query(conclusion_query, database_="neo4j")
        for record in result.records:
            print(f"  {record['result']}: {record['cnt']}개")
    except Exception as e:
        print(f"  (결론 통계 없음: {str(e)[:30]}...)")

    print("=" * 50)


if __name__ == "__main__":
    load_dotenv()

    print("=" * 50)
    print("법령 지식 그래프 구축 - (2) 해석례 연결")
    print("=" * 50)

    max_interpretations = 20

    NEO4J_URI = os.getenv('NEO4J_URI')
    NEO4J_USER = os.getenv('NEO4J_USERNAME')
    NEO4J_PASSWORD = os.getenv('NEO4J_PASSWORD')
    LAW_API_KEY = os.getenv('LAW_API_KEY')

    if not NEO4J_URI or not NEO4J_USER or not NEO4J_PASSWORD:
        print("\n오류: Neo4j 연결 정보가 설정되지 않았습니다")
        sys.exit(1)

    if not LAW_API_KEY:
        print("\n오류: LAW_API_KEY가 설정되지 않았습니다")
        sys.exit(1)

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    try:
        driver.verify_connectivity()
        print(f"\nNeo4j 연결 성공: {NEO4J_URI}\n")

        # 해석례 그래프 구축
        print(f"해석례 ~{max_interpretations}개 수집 시작...\n")
        build_interpretation_graph(driver, LAW_API_KEY, max_interpretations=max_interpretations)

        # 통계 출력
        print_interpretation_statistics(driver)

    finally:
        driver.close()
