import os
import pandas as pd
from pathlib import Path
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()


# ============================================
# 1단계: Neo4j 연결
# ============================================

def create_neo4j_driver(uri, username, password):
    driver = GraphDatabase.driver(uri, auth=(username, password))

    try:
        driver.verify_connectivity()
        print(f"Neo4j 연결 성공: {uri}")
    except Exception as e:
        print(f"Neo4j 연결 실패: {e}")
        raise

    return driver


# ============================================
# 2단계: 데이터베이스 초기화
# ============================================

def clear_database(driver):
    """
    데이터베이스의 모든 노드와 관계를 삭제
    """
    driver.execute_query(
        "MATCH (n) DETACH DELETE n",
        database_="neo4j"
    )
    print("기존 데이터 삭제 완료")


def create_constraints(driver):
    """
    노드 ID의 고유성을 보장하는 제약조건 생성
    """
    constraints = [
        "CREATE CONSTRAINT article_id IF NOT EXISTS FOR (a:Article) REQUIRE a.article_id IS UNIQUE",
        "CREATE CONSTRAINT customer_id IF NOT EXISTS FOR (c:Customer) REQUIRE c.customer_id IS UNIQUE",
        "CREATE CONSTRAINT product_type_no IF NOT EXISTS FOR (p:ProductType) REQUIRE p.product_type_no IS UNIQUE",
        "CREATE CONSTRAINT product_group_name IF NOT EXISTS FOR (p:ProductGroup) REQUIRE p.name IS UNIQUE",
        "CREATE CONSTRAINT colour_code IF NOT EXISTS FOR (c:ColourGroup) REQUIRE c.colour_group_code IS UNIQUE",
        "CREATE CONSTRAINT dept_no IF NOT EXISTS FOR (d:Department) REQUIRE d.department_no IS UNIQUE",
        "CREATE CONSTRAINT section_no IF NOT EXISTS FOR (s:Section) REQUIRE s.section_no IS UNIQUE",
        "CREATE CONSTRAINT garment_no IF NOT EXISTS FOR (g:GarmentGroup) REQUIRE g.garment_group_no IS UNIQUE"
    ]

    for constraint in constraints:
        driver.execute_query(constraint, database_="neo4j")

    print("제약조건 생성 완료")


# ============================================
# 3단계: 노드 생성
# ============================================

def load_article_nodes(driver, csv_path):
    """
    Article 노드를 pandas로 CSV 파일에서 로드하여 배치 삽입
    """
    df = pd.read_csv(csv_path)
    records = df.where(pd.notnull(df), None).to_dict('records')

    query = """
    UNWIND $records AS row
    CREATE (a:Article {
        article_id: toInteger(row.article_id),
        product_code: toInteger(row.product_code),
        prod_name: row.prod_name,
        product_type_no: toInteger(row.product_type_no),
        product_type_name: row.product_type_name,
        product_group_name: row.product_group_name,
        graphical_appearance_no: toInteger(row.graphical_appearance_no),
        graphical_appearance_name: row.graphical_appearance_name,
        colour_group_code: toInteger(row.colour_group_code),
        colour_group_name: row.colour_group_name,
        perceived_colour_value_id: toInteger(row.perceived_colour_value_id),
        perceived_colour_value_name: row.perceived_colour_value_name,
        perceived_colour_master_id: toInteger(row.perceived_colour_master_id),
        perceived_colour_master_name: row.perceived_colour_master_name,
        department_no: toInteger(row.department_no),
        department_name: row.department_name,
        index_code: row.index_code,
        index_name: row.index_name,
        index_group_no: toInteger(row.index_group_no),
        index_group_name: row.index_group_name,
        section_no: toInteger(row.section_no),
        section_name: row.section_name,
        garment_group_no: toInteger(row.garment_group_no),
        garment_group_name: row.garment_group_name,
        detail_desc: row.detail_desc
    })
    """

    summary = driver.execute_query(
        query,
        records=records,
        database_="neo4j"
    ).summary

    count = summary.counters.nodes_created
    print(f"  Article 노드 생성: {count:,}개")


def load_customer_nodes(driver, csv_path):
    """
    Customer 노드를 pandas로 CSV 파일에서 로드하여 배치 삽입
    """
    df = pd.read_csv(csv_path)
    records = df.where(pd.notnull(df), None).to_dict('records')

    query = """
    UNWIND $records AS row
    CREATE (c:Customer {
        customer_id: row.customer_id,
        FN: toFloat(row.FN),
        Active: toFloat(row.Active),
        club_member_status: row.club_member_status,
        fashion_news_frequency: row.fashion_news_frequency,
        age: toInteger(row.age),
        postal_code: row.postal_code
    })
    """

    summary = driver.execute_query(
        query,
        records=records,
        database_="neo4j"
    ).summary

    count = summary.counters.nodes_created
    print(f"  Customer 노드 생성: {count:,}개")


def load_product_type_nodes(driver, csv_path):
    """
    ProductType 노드를 pandas로 CSV 파일에서 로드하여 배치 삽입
    """
    df = pd.read_csv(csv_path)
    records = df.where(pd.notnull(df), None).to_dict('records')

    query = """
    UNWIND $records AS row
    CREATE (p:ProductType {
        product_type_no: toInteger(row.product_type_no),
        product_type_name: row.product_type_name
    })
    """

    summary = driver.execute_query(
        query,
        records=records,
        database_="neo4j"
    ).summary

    count = summary.counters.nodes_created
    print(f"  ProductType 노드 생성: {count:,}개")


def load_product_group_nodes(driver, csv_path):
    """
    ProductGroup 노드를 pandas로 CSV 파일에서 로드하여 배치 삽입
    """
    df = pd.read_csv(csv_path)
    records = df.where(pd.notnull(df), None).to_dict('records')

    query = """
    UNWIND $records AS row
    CREATE (p:ProductGroup {
        name: row.name
    })
    """

    summary = driver.execute_query(
        query,
        records=records,
        database_="neo4j"
    ).summary

    count = summary.counters.nodes_created
    print(f"  ProductGroup 노드 생성: {count:,}개")


def load_colour_group_nodes(driver, csv_path):
    """
    ColourGroup 노드를 pandas로 CSV 파일에서 로드하여 배치 삽입
    """
    df = pd.read_csv(csv_path)
    records = df.where(pd.notnull(df), None).to_dict('records')

    query = """
    UNWIND $records AS row
    CREATE (c:ColourGroup {
        colour_group_code: toInteger(row.colour_group_code),
        colour_group_name: row.colour_group_name
    })
    """

    summary = driver.execute_query(
        query,
        records=records,
        database_="neo4j"
    ).summary

    count = summary.counters.nodes_created
    print(f"  ColourGroup 노드 생성: {count:,}개")


def load_department_nodes(driver, csv_path):
    """
    Department 노드를 pandas로 CSV 파일에서 로드하여 배치 삽입
    """
    df = pd.read_csv(csv_path)
    records = df.where(pd.notnull(df), None).to_dict('records')

    query = """
    UNWIND $records AS row
    CREATE (d:Department {
        department_no: toInteger(row.department_no),
        department_name: row.department_name
    })
    """

    summary = driver.execute_query(
        query,
        records=records,
        database_="neo4j"
    ).summary

    count = summary.counters.nodes_created
    print(f"  Department 노드 생성: {count:,}개")


def load_section_nodes(driver, csv_path):
    """
    Section 노드를 pandas로 CSV 파일에서 로드하여 배치 삽입
    """
    df = pd.read_csv(csv_path)
    records = df.where(pd.notnull(df), None).to_dict('records')

    query = """
    UNWIND $records AS row
    CREATE (s:Section {
        section_no: toInteger(row.section_no),
        section_name: row.section_name
    })
    """

    summary = driver.execute_query(
        query,
        records=records,
        database_="neo4j"
    ).summary

    count = summary.counters.nodes_created
    print(f"  Section 노드 생성: {count:,}개")


def load_garment_group_nodes(driver, csv_path):
    """
    GarmentGroup 노드를 pandas로 CSV 파일에서 로드하여 배치 삽입
    """
    df = pd.read_csv(csv_path)
    records = df.where(pd.notnull(df), None).to_dict('records')

    query = """
    UNWIND $records AS row
    CREATE (g:GarmentGroup {
        garment_group_no: toInteger(row.garment_group_no),
        garment_group_name: row.garment_group_name
    })
    """

    summary = driver.execute_query(
        query,
        records=records,
        database_="neo4j"
    ).summary

    count = summary.counters.nodes_created
    print(f"  GarmentGroup 노드 생성: {count:,}개")


# ============================================
# 4단계: 관계 생성
# ============================================

def create_purchased_relationships(driver, csv_path):
    """
    Customer → Article (PURCHASED) 관계를 pandas로 배치 생성 (청크 단위 처리)
    """
    print("  PURCHASED 관계 생성 중 (대용량 데이터, 청크 단위 처리)...")

    total_count = 0
    chunk_num = 0
    start_time = time.time()

    query = """
    UNWIND $records AS row
    MATCH (c:Customer {customer_id: row.customer_id})
    MATCH (a:Article {article_id: toInteger(row.article_id)})
    CREATE (c)-[:PURCHASED {
        date: date(row.t_dat),
        price: toFloat(row.price),
        sales_channel_id: toInteger(row.sales_channel_id)
    }]->(a)
    """

    # 청크 단위로 읽기 (50,000개씩)
    for chunk in pd.read_csv(csv_path, chunksize=50000):
        chunk_num += 1
        records = chunk.where(pd.notnull(chunk), None).to_dict('records')

        summary = driver.execute_query(
            query,
            records=records,
            database_="neo4j"
        ).summary

        count = summary.counters.relationships_created
        total_count += count

        # 10개 청크마다 진행상황 출력
        if chunk_num % 10 == 0:
            elapsed = time.time() - start_time
            print(f"    처리 중... {total_count:,}개 ({elapsed:.1f}초)")

    elapsed = time.time() - start_time
    print(f"  PURCHASED 관계 생성: {total_count:,}개 ({elapsed:.1f}초)")


def create_of_type_relationships(driver, csv_path):
    """
    Article → ProductType (OF_TYPE) 관계를 pandas로 배치 생성
    """
    df = pd.read_csv(csv_path)
    records = df.where(pd.notnull(df), None).to_dict('records')

    query = """
    UNWIND $records AS row
    MATCH (a:Article {article_id: toInteger(row.article_id)})
    MATCH (p:ProductType {product_type_no: toInteger(row.product_type_no)})
    CREATE (a)-[:OF_TYPE]->(p)
    """

    summary = driver.execute_query(
        query,
        records=records,
        database_="neo4j"
    ).summary

    count = summary.counters.relationships_created
    print(f"  OF_TYPE 관계 생성: {count:,}개")


def create_in_group_relationships(driver, csv_path):
    """
    Article → ProductGroup (IN_GROUP) 관계를 pandas로 배치 생성
    """
    df = pd.read_csv(csv_path)
    records = df.where(pd.notnull(df), None).to_dict('records')

    query = """
    UNWIND $records AS row
    MATCH (a:Article {article_id: toInteger(row.article_id)})
    MATCH (p:ProductGroup {name: row.product_group_name})
    CREATE (a)-[:IN_GROUP]->(p)
    """

    summary = driver.execute_query(
        query,
        records=records,
        database_="neo4j"
    ).summary

    count = summary.counters.relationships_created
    print(f"  IN_GROUP 관계 생성: {count:,}개")


def create_has_colour_relationships(driver, csv_path):
    """
    Article → ColourGroup (HAS_COLOUR) 관계를 pandas로 배치 생성
    """
    df = pd.read_csv(csv_path)
    records = df.where(pd.notnull(df), None).to_dict('records')

    query = """
    UNWIND $records AS row
    MATCH (a:Article {article_id: toInteger(row.article_id)})
    MATCH (c:ColourGroup {colour_group_code: toInteger(row.colour_group_code)})
    CREATE (a)-[:HAS_COLOUR]->(c)
    """

    summary = driver.execute_query(
        query,
        records=records,
        database_="neo4j"
    ).summary

    count = summary.counters.relationships_created
    print(f"  HAS_COLOUR 관계 생성: {count:,}개")


def create_in_department_relationships(driver, csv_path):
    """
    Article → Department (IN_DEPARTMENT) 관계를 pandas로 배치 생성
    """
    df = pd.read_csv(csv_path)
    records = df.where(pd.notnull(df), None).to_dict('records')

    query = """
    UNWIND $records AS row
    MATCH (a:Article {article_id: toInteger(row.article_id)})
    MATCH (d:Department {department_no: toInteger(row.department_no)})
    CREATE (a)-[:IN_DEPARTMENT]->(d)
    """

    summary = driver.execute_query(
        query,
        records=records,
        database_="neo4j"
    ).summary

    count = summary.counters.relationships_created
    print(f"  IN_DEPARTMENT 관계 생성: {count:,}개")


def create_in_section_relationships(driver, csv_path):
    """
    Article → Section (IN_SECTION) 관계를 pandas로 배치 생성
    """
    df = pd.read_csv(csv_path)
    records = df.where(pd.notnull(df), None).to_dict('records')

    query = """
    UNWIND $records AS row
    MATCH (a:Article {article_id: toInteger(row.article_id)})
    MATCH (s:Section {section_no: toInteger(row.section_no)})
    CREATE (a)-[:IN_SECTION]->(s)
    """

    summary = driver.execute_query(
        query,
        records=records,
        database_="neo4j"
    ).summary

    count = summary.counters.relationships_created
    print(f"  IN_SECTION 관계 생성: {count:,}개")


def create_in_garment_group_relationships(driver, csv_path):
    """
    Article → GarmentGroup (IN_GARMENT_GROUP) 관계를 pandas로 배치 생성
    """
    df = pd.read_csv(csv_path)
    records = df.where(pd.notnull(df), None).to_dict('records')

    query = """
    UNWIND $records AS row
    MATCH (a:Article {article_id: toInteger(row.article_id)})
    MATCH (g:GarmentGroup {garment_group_no: toInteger(row.garment_group_no)})
    CREATE (a)-[:IN_GARMENT_GROUP]->(g)
    """

    summary = driver.execute_query(
        query,
        records=records,
        database_="neo4j"
    ).summary

    count = summary.counters.relationships_created
    print(f"  IN_GARMENT_GROUP 관계 생성: {count:,}개")


# ============================================
# 5단계: 통계 확인
# ============================================

def print_graph_statistics(driver):
    # 노드 개수 조회
    node_counts = {
        "Article": driver.execute_query(
            "MATCH (n:Article) RETURN count(n) AS count",
            database_="neo4j"
        ).records[0]["count"],
        "Customer": driver.execute_query(
            "MATCH (n:Customer) RETURN count(n) AS count",
            database_="neo4j"
        ).records[0]["count"],
        "ProductType": driver.execute_query(
            "MATCH (n:ProductType) RETURN count(n) AS count",
            database_="neo4j"
        ).records[0]["count"],
        "ProductGroup": driver.execute_query(
            "MATCH (n:ProductGroup) RETURN count(n) AS count",
            database_="neo4j"
        ).records[0]["count"],
        "ColourGroup": driver.execute_query(
            "MATCH (n:ColourGroup) RETURN count(n) AS count",
            database_="neo4j"
        ).records[0]["count"],
        "Department": driver.execute_query(
            "MATCH (n:Department) RETURN count(n) AS count",
            database_="neo4j"
        ).records[0]["count"],
        "Section": driver.execute_query(
            "MATCH (n:Section) RETURN count(n) AS count",
            database_="neo4j"
        ).records[0]["count"],
        "GarmentGroup": driver.execute_query(
            "MATCH (n:GarmentGroup) RETURN count(n) AS count",
            database_="neo4j"
        ).records[0]["count"]
    }

    # 관계 개수 조회
    rel_counts = {
        "PURCHASED": driver.execute_query(
            "MATCH ()-[r:PURCHASED]->() RETURN count(r) AS count",
            database_="neo4j"
        ).records[0]["count"],
        "OF_TYPE": driver.execute_query(
            "MATCH ()-[r:OF_TYPE]->() RETURN count(r) AS count",
            database_="neo4j"
        ).records[0]["count"],
        "IN_GROUP": driver.execute_query(
            "MATCH ()-[r:IN_GROUP]->() RETURN count(r) AS count",
            database_="neo4j"
        ).records[0]["count"],
        "HAS_COLOUR": driver.execute_query(
            "MATCH ()-[r:HAS_COLOUR]->() RETURN count(r) AS count",
            database_="neo4j"
        ).records[0]["count"],
        "IN_DEPARTMENT": driver.execute_query(
            "MATCH ()-[r:IN_DEPARTMENT]->() RETURN count(r) AS count",
            database_="neo4j"
        ).records[0]["count"],
        "IN_SECTION": driver.execute_query(
            "MATCH ()-[r:IN_SECTION]->() RETURN count(r) AS count",
            database_="neo4j"
        ).records[0]["count"],
        "IN_GARMENT_GROUP": driver.execute_query(
            "MATCH ()-[r:IN_GARMENT_GROUP]->() RETURN count(r) AS count",
            database_="neo4j"
        ).records[0]["count"]
    }

    print("\n" + "=" * 60)
    print("H&M 지식그래프 통계")
    print("=" * 60)

    print("\n1) 노드:")
    for label, count in node_counts.items():
        print(f"  • {label}: {count:,}개")
    total_nodes = sum(node_counts.values())
    print(f"  → 총 {total_nodes:,}개 노드")

    print("\n2) 관계:")
    for rel_type, count in rel_counts.items():
        print(f"  • {rel_type}: {count:,}개")
    total_rels = sum(rel_counts.values())
    print(f"  → 총 {total_rels:,}개 관계")

    print("\n" + "=" * 60)



# ============================================
# 메인
# ============================================

def main():
    print("=" * 50)
    print("H&M 지식그래프 구축")
    print("=" * 50)

    NEO4J_URI = os.getenv("NEO4J_URI")
    NEO4J_USER = os.getenv("NEO4J_USERNAME")
    NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

    if not NEO4J_PASSWORD:
        raise ValueError("NEO4J_PASSWORD 환경변수가 설정되지 않았습니다.")

    # 1. CSV 파일 경로 설정
    output_dir = Path("./output")
    csv_files = {
        "article": output_dir / "nodes_article.csv",
        "customer": output_dir / "nodes_customer.csv",
        "product_type": output_dir / "nodes_product_type.csv",
        "product_group": output_dir / "nodes_product_group.csv",
        "colour_group": output_dir / "nodes_colour_group.csv",
        "department": output_dir / "nodes_department.csv",
        "section": output_dir / "nodes_section.csv",
        "garment_group": output_dir / "nodes_garment_group.csv",
        "rel_purchased": output_dir / "rels_purchased.csv",
        "rel_of_type": output_dir / "rels_of_type.csv",
        "rel_in_group": output_dir / "rels_in_group.csv",
        "rel_has_colour": output_dir / "rels_has_colour.csv",
        "rel_in_department": output_dir / "rels_in_department.csv",
        "rel_in_section": output_dir / "rels_in_section.csv",
        "rel_in_garment_group": output_dir / "rels_in_garment_group.csv"
    }

    # 2. Neo4j 연결
    driver = create_neo4j_driver(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)

    try:
        # 3. 데이터베이스 초기화
        print("\n- 1단계: 데이터베이스 초기화")
        clear_database(driver)
        create_constraints(driver)

        # 4. 노드 생성
        print("\n- 2단계: 노드 생성")
        load_article_nodes(driver, csv_files["article"])
        load_customer_nodes(driver, csv_files["customer"])
        load_product_type_nodes(driver, csv_files["product_type"])
        load_product_group_nodes(driver, csv_files["product_group"])
        load_colour_group_nodes(driver, csv_files["colour_group"])
        load_department_nodes(driver, csv_files["department"])
        load_section_nodes(driver, csv_files["section"])
        load_garment_group_nodes(driver, csv_files["garment_group"])

        # 5. 관계 생성
        print("\n- 3단계: 관계 생성")
        create_purchased_relationships(driver, csv_files["rel_purchased"])
        create_of_type_relationships(driver, csv_files["rel_of_type"])
        create_in_group_relationships(driver, csv_files["rel_in_group"])
        create_has_colour_relationships(driver, csv_files["rel_has_colour"])
        create_in_department_relationships(driver, csv_files["rel_in_department"])
        create_in_section_relationships(driver, csv_files["rel_in_section"])
        create_in_garment_group_relationships(driver, csv_files["rel_in_garment_group"])

        # 6. 통계 출력
        print_graph_statistics(driver)

        print("\nH&M 지식그래프 구축 완료!")

    finally:
        driver.close()


if __name__ == "__main__":
    main()
