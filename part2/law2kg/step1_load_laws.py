import os
import sys
from dotenv import load_dotenv
from neo4j import GraphDatabase

from law2kg import (
    setup_neo4j,
    build_law_graph
)


def print_law_statistics(driver):
    """법령 그래프 통계 출력 (step1용)"""
    print("\n" + "=" * 50)
    print("법령 그래프 통계")
    print("=" * 50)

    # 노드 수
    stats = [
        ("법령(Law)", "MATCH (n:Law) RETURN count(n) as cnt"),
        ("조문(Article)", "MATCH (n:Article) RETURN count(n) as cnt"),
        ("항(Paragraph)", "MATCH (n:Paragraph) RETURN count(n) as cnt"),
        ("호(Item)", "MATCH (n:Item) RETURN count(n) as cnt"),
    ]

    print("\n노드:")
    for label, query in stats:
        result = driver.execute_query(query, database_="neo4j")
        count = result.records[0]['cnt']
        print(f"  {label}: {count}개")

    # 관계 수
    print("\n관계:")
    rel_stats = [
        ("HAS_ARTICLE", "MATCH ()-[r:HAS_ARTICLE]->() RETURN count(r) as cnt"),
        ("HAS_PARAGRAPH", "MATCH ()-[r:HAS_PARAGRAPH]->() RETURN count(r) as cnt"),
        ("HAS_ITEM", "MATCH ()-[r:HAS_ITEM]->() RETURN count(r) as cnt"),
        ("NEXT_ARTICLE", "MATCH ()-[r:NEXT_ARTICLE]->() RETURN count(r) as cnt"),
    ]

    for label, query in rel_stats:
        result = driver.execute_query(query, database_="neo4j")
        count = result.records[0]['cnt']
        print(f"  {label}: {count}개")

    # 법령별 조문 수
    print("\n법령별 조문 수 (상위 5개):")
    law_article_query = """
    MATCH (l:Law)-[:HAS_ARTICLE]->(a:Article)
    RETURN l.name as law_name, count(a) as article_count
    ORDER BY article_count DESC
    LIMIT 5
    """
    result = driver.execute_query(law_article_query, database_="neo4j")
    for record in result.records:
        print(f"  {record['law_name']}: {record['article_count']}개")

    print("=" * 50)


if __name__ == "__main__":
    load_dotenv()

    print("=" * 50)
    print("법령 지식 그래프 구축 - (1) 현행법령 적재")
    print("=" * 50)

    max_laws = 10

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

        setup_neo4j(driver)

        print(f"법령 {max_laws}개 적재 시작...\n")
        build_law_graph(driver, LAW_API_KEY, max_laws=max_laws)

        print_law_statistics(driver)

    finally:
        driver.close()
