import os
import sys
from typing import Dict, List, Any

from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()

NEO4J_DATABASE = os.getenv("NEO4J_USERNAME")


# ============================================
# 유틸리티 함수
# ============================================

def create_neo4j_connection():
    """Neo4j 데이터베이스 연결 생성"""
    uri = os.getenv('NEO4J_URI')
    username = os.getenv('NEO4J_USERNAME')
    password = os.getenv('NEO4J_PASSWORD')

    driver = GraphDatabase.driver(uri, auth=(username, password))
    driver.verify_connectivity()
    print(f"✓ Neo4j 연결 성공: {uri}")
    return driver


def verify_gds_available(driver) -> bool:
    """GDS 플러그인 설치 확인"""
    try:
        result = driver.execute_query("RETURN gds.version() AS version", database_=NEO4J_DATABASE)
        version = result.records[0]['version']
        print(f"✓ Neo4j GDS 버전: {version}")
        return True
    except Exception:
        return False


def check_retail_data_exists(driver) -> bool:
    """Retail 데이터 존재 확인"""
    query = """
    MATCH (a:Article)
    RETURN count(a) AS article_count
    LIMIT 1
    """

    result = driver.execute_query(query, database_=NEO4J_DATABASE)

    if not result.records or result.records[0]['article_count'] == 0:
        print("먼저 part2/retail2kg 데이터를 로드해주세요.")
        return False

    count = result.records[0]['article_count']
    print(f"✓ Article 노드 {count:,}개 확인")
    return True


# ============================================
# 데이터 통계 함수
# ============================================

def print_data_statistics(driver):
    """Retail 그래프 통계 출력"""
    print("\n" + "=" * 60)
    print("H&M Retail Graph 통계")
    print("=" * 60)

    # 노드 통계
    node_query = """
    MATCH (n)
    WITH labels(n)[0] AS label, count(*) AS count
    WHERE label IS NOT NULL
    RETURN label, count
    ORDER BY count DESC
    """

    result = driver.execute_query(node_query, database_=NEO4J_DATABASE)

    print("\n노드 통계:")
    print("-" * 60)
    total_nodes = 0
    for record in result.records:
        count = record['count']
        total_nodes += count
        print(f"  {record['label']:20} {count:>10,}개")
    print(f"  {'총합':20} {total_nodes:>10,}개")

    # 관계 통계 (최적화: 관계 타입별로 개별 카운트)
    print("\n관계 통계:")
    print("-" * 60)

    # 알려진 관계 타입들을 개별적으로 빠르게 카운트
    rel_types = [
        'PURCHASED', 'OF_TYPE', 'HAS_COLOUR', 'IN_DEPARTMENT',
        'IN_SECTION', 'IN_GARMENT_GROUP', 'IN_GROUP'
    ]

    total_rels = 0
    for rel_type in rel_types:
        query = f"MATCH ()-[r:{rel_type}]->() RETURN count(r) AS count"
        try:
            result = driver.execute_query(query, database_=NEO4J_DATABASE)
            count = result.records[0]['count']
            if count > 0:
                total_rels += count
                print(f"  {rel_type:20} {count:>10,}개")
        except:
            pass

    print(f"  {'총합':20} {total_rels:>10,}개")

    print("=" * 60)


# ============================================
# Graph Projection
# ============================================

def drop_graph_if_exists(driver, graph_name: str):
    """기존 그래프 제거"""
    try:
        driver.execute_query(
            f"CALL gds.graph.drop('{graph_name}') YIELD graphName",
            database_=NEO4J_DATABASE
        )
        print(f"기존 '{graph_name}' 그래프 제거 완료")
    except:
        print(f"기존 그래프 없음")


def check_purchase_data_size(driver) -> Dict[str, Any]:
    """구매 데이터 규모 확인 (메모리 안전성 체크)"""
    print("\n" + "=" * 60)
    print("구매 데이터 규모 확인")
    print("=" * 60)

    # 전체 PURCHASED 관계 수
    query = "MATCH ()-[r:PURCHASED]->() RETURN count(r) AS total"
    result = driver.execute_query(query, database_=NEO4J_DATABASE)
    total_purchases = result.records[0]['total']

    # 고객 수
    query = "MATCH (c:Customer) RETURN count(c) AS total"
    result = driver.execute_query(query, database_=NEO4J_DATABASE)
    total_customers = result.records[0]['total']

    print(f"\n전체 구매 관계: {total_purchases:,}개")
    print(f"전체 고객 수: {total_customers:,}개")

    if total_purchases > 100_000:
        use_sampling = True
        print("(5회 이상 구매한 고객만 샘플링합니다.)")
        recommended_min_purchases = 5   # 5회 이상 구매 고객
    else:
        use_sampling = False
        recommended_min_purchases = 2

    return {
        'total_purchases': total_purchases,
        'total_customers': total_customers,
        'use_sampling': use_sampling,
        'recommended_min_purchases': recommended_min_purchases
    }


def project_customer_behavior_graph(
    driver,
    graph_name: str = "retailBehaviorGraph",
    min_purchases: int = 10
):
    """
    고객 구매 행동 기반 그래프 프로젝션

    같은 날 구매한 상품들만 연결
    → "함께 구매되는 상품" 정확히 발견
    → 예: 2019-09-25 재킷 + 셔츠 + 바지 구매 → 3개 상품 연결
    """
    print("\n" + "=" * 60)
    print("[1] 고객 구매 행동 Graph Projection")
    print("=" * 60)

    drop_graph_if_exists(driver, graph_name)

    # 활성 고객 수 확인
    active_customer_query = f"""
    MATCH (c:Customer)-[r:PURCHASED]->()
    WITH c, count(r) AS purchaseCount
    WHERE purchaseCount >= {min_purchases}
    RETURN count(c) AS activeCustomers, sum(purchaseCount) AS totalPurchases
    """

    result = driver.execute_query(active_customer_query, database_=NEO4J_DATABASE)
    active_customers = result.records[0]['activeCustomers']
    total_relevant = result.records[0]['totalPurchases']

    print(f"\n샘플링 전략:")
    print(f"  - 최소 구매 횟수: {min_purchases}회 이상 (고활성 고객)")
    print(f"  - 활성 고객 수: {active_customers:,}명")
    print(f"  - 해당 구매 건수: {total_relevant:,}개")

    print(f"\nCO_PURCHASED 관계 생성 중...")

    print(f"  - 기존 CO_PURCHASED 관계 삭제 중...")
    deleted_total = 0
    while True:
        delete_result = driver.execute_query(
            "MATCH ()-[r:CO_PURCHASED]->() WITH r LIMIT 50000 DELETE r RETURN count(r) AS deleted",
            database_=NEO4J_DATABASE
        )
        deleted = delete_result.records[0]['deleted']
        if deleted == 0:
            break
        deleted_total += deleted
        print(f"    - {deleted_total:,}개 삭제 중...")

    if deleted_total > 0:
        print(f"  ✓ 기존 관계 {deleted_total:,}개 삭제 완료")

    print(f"  - 같은 날 구매한 상품 관계 생성 중 (장바구니 분석)...")

    # 1단계: 고활성 고객 ID 수집 (가벼운 쿼리)
    customer_query = f"""
    MATCH (c:Customer)-[:PURCHASED]->(:Article)
    WITH c, count(*) AS purchaseCount
    WHERE purchaseCount >= {min_purchases}
    RETURN elementId(c) AS customerId
    ORDER BY purchaseCount DESC
    LIMIT 10000
    """

    result = driver.execute_query(customer_query, database_=NEO4J_DATABASE)
    customer_ids = [r['customerId'] for r in result.records]
    total_customers = len(customer_ids)
    print(f"  대상 고객: {total_customers:,}명")

    # 2단계: 배치 처리로 관계 생성
    batch_size = 500
    total_created = 0

    for i in range(0, total_customers, batch_size):
        batch_ids = customer_ids[i:i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (total_customers + batch_size - 1) // batch_size

        # 각 고객의 같은 날 구매한 상품들만 연결
        batch_query = """
        UNWIND $customerIds AS customerId
        MATCH (c:Customer)
        WHERE elementId(c) = customerId
        MATCH (c)-[p:PURCHASED]->(a:Article)
        WITH c, p.t_dat AS purchaseDate, collect(a) AS articles
        WHERE size(articles) >= 2
        WITH purchaseDate, articles
        UNWIND articles AS a1
        UNWIND articles AS a2
        WITH a1, a2
        WHERE elementId(a1) < elementId(a2)
        MERGE (a1)-[r:CO_PURCHASED]-(a2)
        ON CREATE SET r.weight = 1
        ON MATCH SET r.weight = r.weight + 1
        RETURN count(*) AS created
        """

        batch_result = driver.execute_query(
            batch_query,
            customerIds=batch_ids,
            database_=NEO4J_DATABASE
        )
        created = batch_result.records[0]['created'] if batch_result.records else 0
        total_created += created

        print(f"  - 배치 {batch_num}/{total_batches} 완료 ({created:,}개 관계 처리)")

    print(f"  ✓ 임시 관계 생성 완료 (총 {total_created:,}개 처리)")

    count_query = "MATCH ()-[r:CO_PURCHASED]->() RETURN count(r) AS total"
    result = driver.execute_query(count_query, database_=NEO4J_DATABASE)
    final_count = result.records[0]['total']
    print(f"\n  ✓ CO_PURCHASED 관계: {final_count:,}개")

    # Native Projection (UNDIRECTED + 가중치 포함)
    query = """
    CALL gds.graph.project(
      'retailBehaviorGraph',
      'Article',
      {
        CO_PURCHASED: {
          orientation: 'UNDIRECTED',
          properties: 'weight'
        }
      }
    )
    YIELD graphName, nodeCount, relationshipCount
    RETURN graphName, nodeCount, relationshipCount
    """

    result = driver.execute_query(query, database_=NEO4J_DATABASE)
    stats = {
        'graphName': result.records[0]['graphName'],
        'nodeCount': result.records[0]['nodeCount'],
        'relationshipCount': result.records[0]['relationshipCount']
    }

    print(f"\n✓ 그래프 '{graph_name}' 프로젝션 완료")
    print(f"  - 노드 수: {stats['nodeCount']:,}")
    print(f"  - 관계 수: {stats['relationshipCount']:,}")
    print(f"  - 관계 타입: UNDIRECTED (무방향)")

    return stats


# ============================================
# Leiden Community Detection
# ============================================

def run_leiden_on_customer_behavior(
    driver,
    graph_name: str = "retailBehaviorGraph",
    write_property: str = "behaviorCommunityId",
    gamma: float = 1.0,
    min_community_size: int = 5
):
    """
    고객 행동 기반 커뮤니티 탐지
    실제 구매 패턴으로 상품을 그룹핑
    """
    print("\n" + "=" * 60)
    print("[2] 고객 행동 기반 커뮤니티 탐지 (Leiden Algorithm)")
    print("=" * 60)
    print(f"\n알고리즘 설정:")
    print(f"  - Resolution (gamma): {gamma}")
    print(f"  - 최소 커뮤니티 크기: {min_community_size}개 상품 이상")
    print(f"  - 가중치 사용: weight")

    query = f"""
    CALL gds.leiden.write('{graph_name}', {{
        writeProperty: '{write_property}',
        includeIntermediateCommunities: false,
        maxLevels: 10,
        tolerance: 0.0001,
        gamma: {gamma},
        minCommunitySize: {min_community_size},
        relationshipWeightProperty: 'weight'
    }})
    """

    result = driver.execute_query(query, database_=NEO4J_DATABASE)
    stats = result.records[0]

    print(f"\n✓ Leiden 알고리즘 실행 완료")
    print(f"  - 탐지된 커뮤니티 수: {stats['communityCount']}")
    print(f"  - 모듈성 점수: {stats['modularity']:.4f}")
    print(f"  - 계층 수준: {stats['ranLevels']}")
    print(f"  - 속성 '{write_property}' 저장 완료")

    return stats

# ============================================
# 커뮤니티 분석
# ============================================

def analyze_behavior_communities(driver):
    """고객 행동 기반 커뮤니티 분석 (PURCHASED 관계)"""
    print("\n" + "=" * 60)
    print("[3] 구매 패턴 커뮤니티 분석")
    print("=" * 60)

    cross_sell_query = """
    MATCH (a:Article)
    WHERE a.behaviorCommunityId IS NOT NULL
    WITH a.behaviorCommunityId AS communityId,
         collect(DISTINCT a.graphical_appearance_name) AS appearances,
         collect(DISTINCT a.perceived_colour_master_name) AS colours,
         collect(a)[..5] AS samples,
         count(a) AS articleCount
    WHERE size([a IN appearances WHERE a IS NOT NULL]) >= 2
    RETURN communityId, appearances, colours, samples, articleCount
    ORDER BY size(appearances) DESC
    LIMIT 5
    """

    result = driver.execute_query(cross_sell_query, database_=NEO4J_DATABASE)

    for record in result.records:
        comm_id = record['communityId']
        appearances = [a for a in record['appearances'] if a]
        colours = [c for c in record['colours'] if c]
        samples = record['samples']
        article_count = record['articleCount']

        print(f"\nCommunity {comm_id}: {article_count:,}개 상품, {len(appearances)}개 스타일 ({', '.join(appearances[:3])})")
        print(f"  주요 색상: {', '.join(colours[:5])}")
        for article in samples[:3]:
            name = article.get('prod_name', 'N/A')
            appearance = article.get('graphical_appearance_name', 'N/A')
            colour = article.get('perceived_colour_master_name', 'N/A')
            print(f"  • {name} ({appearance}, {colour})")


# ============================================
# 메인 함수
# ============================================

def main():
    print("=" * 60)
    print("H&M Retail 고객 구매 행동 기반 커뮤니티 탐지")
    print("=" * 60)

    driver = create_neo4j_connection()

    try:
        # GDS 확인
        if not verify_gds_available(driver):
            sys.exit(1)

        # 데이터 확인
        if not check_retail_data_exists(driver):
            sys.exit(1)

        # 데이터 통계
        print_data_statistics(driver)

        # 구매 데이터 규모 확인
        data_info = check_purchase_data_size(driver)

        project_customer_behavior_graph(
            driver,
            min_purchases=data_info['recommended_min_purchases']
        )

        run_leiden_on_customer_behavior(
            driver,
            gamma=1.0,
            min_community_size=5
        )

        # 커뮤니티 내용 분석
        analyze_behavior_communities(driver)

        print("\n" + "=" * 60)
        print("커뮤니티 탐지 완료!")
        print("=" * 60)
    finally:
        driver.close()


if __name__ == "__main__":
    main()
