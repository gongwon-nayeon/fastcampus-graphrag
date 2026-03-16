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
        "CREATE CONSTRAINT passenger_id IF NOT EXISTS FOR (p:Passenger) REQUIRE p.PassengerId IS UNIQUE",
        "CREATE CONSTRAINT pclass_id IF NOT EXISTS FOR (c:PClass) REQUIRE c.Pclass IS UNIQUE",
        "CREATE CONSTRAINT cabin_id IF NOT EXISTS FOR (c:Cabin) REQUIRE c.Cabin IS UNIQUE",
        "CREATE CONSTRAINT port_id IF NOT EXISTS FOR (p:Port) REQUIRE p.Port IS UNIQUE"
    ]

    for constraint in constraints:
        driver.execute_query(constraint, database_="neo4j")

    print("제약조건 생성 완료")


# ============================================
# 3단계: 노드 생성
# ============================================

def load_passenger_nodes(driver, csv_path):
    """
    Passenger 노드를 pandas로 CSV 파일에서 로드하여 배치 삽입
    """
    df = pd.read_csv(csv_path)

    # pandas DataFrame → dict 리스트로 변환 (NaN → None 처리)
    records = df.where(pd.notnull(df), None).to_dict('records') # [{"PassengerId": "P1", "Name": "John Doe", ...}, {...}, ...]

    query = """
    UNWIND $records AS row
    CREATE (p:Passenger {
        PassengerId: row.PassengerId,
        Name: row.Name,
        Sex: row.Sex,
        Age: row.Age,
        Survived: row.Survived,
        SibSp: row.SibSp,
        Parch: row.Parch,
        Fare: row.Fare,
        Ticket: row.Ticket
    })
    """

    summary = driver.execute_query(
        query,
        records=records,
        database_="neo4j"
    ).summary

    count = summary.counters.nodes_created
    print(f"  Passenger 노드 생성: {count}개")


def load_pclass_nodes(driver, csv_path):
    """
    PClass 노드를 pandas로 CSV 파일에서 로드하여 배치 삽입
    """
    df = pd.read_csv(csv_path)
    records = df.where(pd.notnull(df), None).to_dict('records')

    query = """
    UNWIND $records AS row
    CREATE (c:PClass {
        Pclass: row.Pclass,
        ClassName: row.ClassName,
        SES: row.SES
    })
    """

    summary = driver.execute_query(
        query,
        records=records,
        database_="neo4j"
    ).summary

    count = summary.counters.nodes_created
    print(f"  PClass 노드 생성: {count}개")


def load_cabin_nodes(driver, csv_path):
    """
    Cabin 노드를 pandas로 CSV 파일에서 로드하여 배치 삽입
    """
    df = pd.read_csv(csv_path)
    records = df.where(pd.notnull(df), None).to_dict('records')

    query = """
    UNWIND $records AS row
    CREATE (c:Cabin {
        Cabin: row.Cabin
    })
    """

    summary = driver.execute_query(
        query,
        records=records,
        database_="neo4j"
    ).summary

    count = summary.counters.nodes_created
    print(f"  Cabin 노드 생성: {count}개")


def load_port_nodes(driver, csv_path):
    """
    Port 노드를 pandas로 CSV 파일에서 로드하여 배치 삽입
    """
    df = pd.read_csv(csv_path)
    records = df.where(pd.notnull(df), None).to_dict('records')

    query = """
    UNWIND $records AS row
    CREATE (p:Port {
        Port: row.Port,
        PortName: row.PortName
    })
    """

    summary = driver.execute_query(
        query,
        records=records,
        database_="neo4j"
    ).summary

    count = summary.counters.nodes_created
    print(f"  Port 노드 생성: {count}개")


# ============================================
# 4단계: 관계 생성
# ============================================

def create_traveled_in_relationships(driver, csv_path):
    """
    Passenger → PClass (TRAVELED_IN) 관계를 pandas로 배치 생성
    """
    df = pd.read_csv(csv_path)
    records = df.where(pd.notnull(df), None).to_dict('records')

    query = """
    UNWIND $records AS row
    MATCH (p:Passenger {PassengerId: row.PassengerId})
    MATCH (c:PClass {Pclass: row.Pclass})
    CREATE (p)-[:TRAVELED_IN]->(c)
    """

    summary = driver.execute_query(
        query,
        records=records,
        database_="neo4j"
    ).summary

    count = summary.counters.relationships_created
    print(f"  TRAVELED_IN 관계 생성: {count}개")


def create_stayed_in_relationships(driver, csv_path):
    """
    Passenger → Cabin (STAYED_IN) 관계를 pandas로 배치 생성
    """
    df = pd.read_csv(csv_path)
    records = df.where(pd.notnull(df), None).to_dict('records')

    query = """
    UNWIND $records AS row
    MATCH (p:Passenger {PassengerId: row.PassengerId})
    MATCH (c:Cabin {Cabin: row.Cabin})
    CREATE (p)-[:STAYED_IN]->(c)
    """

    summary = driver.execute_query(
        query,
        records=records,
        database_="neo4j"
    ).summary

    count = summary.counters.relationships_created
    print(f"  STAYED_IN 관계 생성: {count}개")

def create_embarked_at_relationships(driver, csv_path):
    """
    Passenger → Port (EMBARKED_AT) 관계를 pandas로 배치 생성
    """
    df = pd.read_csv(csv_path)
    records = df.where(pd.notnull(df), None).to_dict('records')

    query = """
    UNWIND $records AS row
    MATCH (p:Passenger {PassengerId: row.PassengerId})
    MATCH (port:Port {Port: row.Port})
    CREATE (p)-[:EMBARKED_AT]->(port)
    """

    summary = driver.execute_query(
        query,
        records=records,
        database_="neo4j"
    ).summary

    count = summary.counters.relationships_created
    print(f"  EMBARKED_AT 관계 생성: {count}개")

def create_traveled_with_relationships(driver, csv_path):
    """
    Passenger ↔ Passenger (TRAVELED_WITH) 관계를 pandas로 배치 생성
    """
    # 파일이 존재하는지 확인
    if not Path(csv_path).exists():
        print(f"  TRAVELED_WITH 파일 없음 (모두 단독 여행)")
        return

    df = pd.read_csv(csv_path)
    records = df.where(pd.notnull(df), None).to_dict('records')

    query = """
    UNWIND $records AS row
    MATCH (p1:Passenger {PassengerId: row.PassengerId1})
    MATCH (p2:Passenger {PassengerId: row.PassengerId2})
    CREATE (p1)-[:TRAVELED_WITH {Ticket: row.Ticket}]->(p2)
    """

    summary = driver.execute_query(
        query,
        records=records,
        database_="neo4j"
    ).summary

    count = summary.counters.relationships_created
    print(f"  TRAVELED_WITH 관계 생성: {count}개")


# ============================================
# 5단계: 통계 확인
# ============================================

def print_graph_statistics(driver):
    # 노드 개수 조회
    node_counts = {
        "Passenger": driver.execute_query(
            "MATCH (n:Passenger) RETURN count(n) AS count",
            database_="neo4j"
        ).records[0]["count"],
        "PClass": driver.execute_query(
            "MATCH (n:PClass) RETURN count(n) AS count",
            database_="neo4j"
        ).records[0]["count"],
        "Cabin": driver.execute_query(
            "MATCH (n:Cabin) RETURN count(n) AS count",
            database_="neo4j"
        ).records[0]["count"],
        "Port": driver.execute_query(
            "MATCH (n:Port) RETURN count(n) AS count",
            database_="neo4j"
        ).records[0]["count"]
    }

    # 관계 개수 조회
    rel_counts = {
        "TRAVELED_IN": driver.execute_query(
            "MATCH ()-[r:TRAVELED_IN]->() RETURN count(r) AS count",
            database_="neo4j"
        ).records[0]["count"],
        "STAYED_IN": driver.execute_query(
            "MATCH ()-[r:STAYED_IN]->() RETURN count(r) AS count",
            database_="neo4j"
        ).records[0]["count"],
        "EMBARKED_AT": driver.execute_query(
            "MATCH ()-[r:EMBARKED_AT]->() RETURN count(r) AS count",
            database_="neo4j"
        ).records[0]["count"],
        "TRAVELED_WITH": driver.execute_query(
            "MATCH ()-[r:TRAVELED_WITH]->() RETURN count(r) AS count",
            database_="neo4j"
        ).records[0]["count"]
    }

    print("\n" + "=" * 60)
    print("타이타닉 지식그래프 통계")
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
    print("타이타닉 지식 그래프 추출")
    print("=" * 50)

    NEO4J_URI = os.getenv("NEO4J_URI")
    NEO4J_USER = os.getenv("NEO4J_USERNAME")
    NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

    if not NEO4J_PASSWORD:
        raise ValueError("NEO4J_PASSWORD 환경변수가 설정되지 않았습니다.")

    # 1. CSV 파일 경로 설정
    output_dir = Path("./output")
    csv_files = {
        "passenger": output_dir / "nodes_passenger.csv",
        "pclass": output_dir / "nodes_pclass.csv",
        "cabin": output_dir / "nodes_cabin.csv",
        "port": output_dir / "nodes_port.csv",
        "rel_pclass": output_dir / "rels_passenger_pclass.csv",
        "rel_cabin": output_dir / "rels_passenger_cabin.csv",
        "rel_port": output_dir / "rels_passenger_port.csv",
        "rel_traveled": output_dir / "rels_traveled_with.csv"
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
        load_passenger_nodes(driver, csv_files["passenger"])
        load_pclass_nodes(driver, csv_files["pclass"])
        load_cabin_nodes(driver, csv_files["cabin"])
        load_port_nodes(driver, csv_files["port"])

        # 5. 관계 생성
        print("\n- 3단계: 관계 생성")
        create_traveled_in_relationships(driver, csv_files["rel_pclass"])
        create_stayed_in_relationships(driver, csv_files["rel_cabin"])
        create_embarked_at_relationships(driver, csv_files["rel_port"])
        create_traveled_with_relationships(driver, csv_files["rel_traveled"])

        # 6. 통계 출력
        print_graph_statistics(driver)

        print("\n타이타닉 지식그래프 구축 완료!")

    finally:
        driver.close()


if __name__ == "__main__":
    main()
