import os
import uuid
from typing import List
from dataclasses import dataclass

from dotenv import load_dotenv
from neo4j import GraphDatabase
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
from openai import OpenAI

load_dotenv()


# ============================================
# 데이터 구조
# ============================================

@dataclass
class TextElementData:
    """TextElement 데이터 구조"""
    element_id: str
    toc_id: str
    content: str
    page: int
    toc_title: str = None
    toc_level: int = None


# ============================================
# 클라이언트 생성
# ============================================

def create_neo4j_driver():
    """Neo4j 드라이버 생성"""
    uri = os.getenv("NEO4J_URI")
    username = os.getenv("NEO4J_USERNAME")
    password = os.getenv("NEO4J_PASSWORD")

    if not all([uri, username, password]):
        raise ValueError("NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD 환경 변수를 설정해주세요.")

    driver = GraphDatabase.driver(uri, auth=(username, password))
    driver.verify_connectivity()
    print(f"Neo4j 연결 성공: {uri}")
    return driver


def create_qdrant_client():
    """Qdrant 클라이언트 생성"""
    url = os.getenv("QDRANT_URL")
    api_key = os.getenv("QDRANT_KEY")

    if not url:
        raise ValueError("QDRANT_URL 환경 변수를 설정해주세요.")

    client = QdrantClient(url=url, api_key=api_key)
    print(f"Qdrant 연결 성공: {url}")
    return client


def create_openai_client():
    """OpenAI 클라이언트 생성"""
    api_key = os.getenv("OPENAI_API_KEY")

    if not api_key:
        raise ValueError("OPENAI_API_KEY 환경 변수를 설정해주세요.")

    client = OpenAI(api_key=api_key)
    print(f"OpenAI 클라이언트 생성 완료")
    return client


# ============================================
# Qdrant Collection 관리
# ============================================

def create_qdrant_collection(
    qdrant_client: QdrantClient,
    collection_name: str = os.getenv("QDRANT_COLLECTION", "pdf2kg"),
    vector_size: int = 1536,
    distance: Distance = Distance.COSINE
):
    """
    Qdrant 컬렉션 생성

    Args:
        qdrant_client: Qdrant 클라이언트
        collection_name: 컬렉션 이름
        vector_size: 벡터 차원 (text-embedding-3-small: 1536)
        distance: 거리 측정 방식 (COSINE, EUCLID, DOT)
    """
    collections = qdrant_client.get_collections().collections
    existing = [c.name for c in collections]

    if collection_name in existing:
        print(f"Qdrant 컬렉션 '{collection_name}' 이미 존재")
    else:
        qdrant_client.create_collection(
            collection_name=collection_name,
            vectors_config=VectorParams(
                size=vector_size,
                distance=distance
            )
        )
        print(f"Qdrant 컬렉션 '{collection_name}' 생성 완료")


def delete_qdrant_collection(
    qdrant_client: QdrantClient,
    collection_name: str = os.getenv("QDRANT_COLLECTION", "pdf2kg")
):
    """Qdrant 컬렉션 삭제"""
    qdrant_client.delete_collection(collection_name=collection_name)
    print(f"Qdrant 컬렉션 '{collection_name}' 삭제 완료")


# ============================================
# Neo4j 데이터 조회
# ============================================

def fetch_text_elements_from_neo4j(neo4j_driver):
    """
    Neo4j에서 모든 TextElement 노드와 TOC 정보를 가져오기

    Returns:
        TextElementData 리스트
    """
    query = """
    MATCH (element:TextElement)
    OPTIONAL MATCH (chunk:Chunk)-[:HAS_ELEMENT]->(element)
    OPTIONAL MATCH (toc:TOC)-[:HAS_CHUNK]->(chunk)
    RETURN
        element.element_id AS element_id,
        element.toc_id AS toc_id,
        element.content AS content,
        element.page AS page,
        toc.title AS toc_title,
        toc.level AS toc_level
    ORDER BY element.element_id
    """

    with neo4j_driver.session() as session:
        result = session.run(query)
        elements = [
            TextElementData(
                element_id=record["element_id"],
                toc_id=record["toc_id"],
                content=record["content"] or "",
                page=record["page"] or 0,
                toc_title=record.get("toc_title"),
                toc_level=record.get("toc_level")
            )
            for record in result
            if record["content"]  # content가 있는 것만
        ]

    print(f"✓ Neo4j에서 {len(elements)}개 TextElement 조회 완료")
    return elements


# ============================================
# Qdrant 수집
# ============================================

def ingest_text_elements_to_qdrant(
    elements: List[TextElementData],
    qdrant_client: QdrantClient,
    openai_client: OpenAI,
    collection_name: str = os.getenv("QDRANT_COLLECTION", "pdf2kg"),
    batch_size: int = 10,
    embedding_model: str = "text-embedding-3-small"
):
    """
    TextElement 데이터를 Qdrant에 수집

    Args:
        elements: TextElementData 리스트
        qdrant_client: Qdrant 클라이언트
        openai_client: OpenAI 클라이언트
        collection_name: Qdrant 컬렉션 이름
        batch_size: 배치 크기 (임베딩 생성 속도 제한 고려)
        embedding_model: 임베딩 모델
    """
    print(f"\n{'='*80}")
    print(f"Qdrant 수집 시작: {len(elements)}개 TextElements")
    print(f"{'='*80}\n")

    points = []

    for idx, element in enumerate(elements, 1):
        # 임베딩 생성
        response = openai_client.embeddings.create(
            input=element.content,
            model=embedding_model
        )
        embedding = response.data[0].embedding

        # Qdrant Point 생성
        point = PointStruct(
            id=str(uuid.uuid4()),
            vector=embedding,
            payload={
                "element_id": element.element_id,
                "toc_id": element.toc_id,
                "page": element.page,
                "toc_title": element.toc_title,
                "toc_level": element.toc_level,
                "content": element.content
            }
        )
        points.append(point)

        # 배치 단위로 Qdrant에 업로드
        if len(points) >= batch_size:
            qdrant_client.upsert(
                collection_name=collection_name,
                points=points
            )
            print(f"  [{idx}/{len(elements)}] {len(points)}개 벡터 업로드 완료")
            points = []

    # 남은 벡터 업로드
    if points:
        qdrant_client.upsert(
            collection_name=collection_name,
            points=points
        )
        print(f"  [{len(elements)}/{len(elements)}] {len(points)}개 벡터 업로드 완료")

    print(f"\n{'='*80}")
    print(f"✓ Qdrant 수집 완료: 총 {len(elements)}개 벡터")
    print(f"{'='*80}\n")


# ============================================
# 메인 함수
# ============================================

if __name__ == "__main__":
    import sys

    print("="*80)
    print("Neo4j TextElements → Qdrant 임베딩 수집")
    print("="*80)

    try:
        # 클라이언트 초기화
        neo4j_driver = create_neo4j_driver()
        qdrant_client = create_qdrant_client()
        openai_client = create_openai_client()

        collection_name = os.getenv("QDRANT_COLLECTION", "pdf2kg")

        # Step 1: Qdrant 컬렉션 생성/확인
        print(f"\n[Step 1] Qdrant 컬렉션 '{collection_name}' 확인/생성")
        create_qdrant_collection(qdrant_client, collection_name)

        # 이미 저장된 벡터가 있는지 확인
        collection_info = qdrant_client.get_collection(collection_name)
        points_count = collection_info.points_count

        if points_count > 0:
            print(f"\n이미 {points_count}개의 벡터가 저장되어 있습니다.")
            response = input("기존 데이터를 삭제하고 다시 수집하시겠습니까? (y/N): ").strip().lower()

            if response == 'y':
                delete_qdrant_collection(qdrant_client, collection_name)
                create_qdrant_collection(qdrant_client, collection_name)
            else:
                print("\n수집을 건너뜁니다.")
                neo4j_driver.close()
                sys.exit(0)

        # Step 2: Neo4j에서 TextElement 가져오기
        print(f"\n[Step 2] Neo4j에서 TextElement 조회")
        elements = fetch_text_elements_from_neo4j(neo4j_driver)

        if not elements:
            print("\nNeo4j에 TextElement 데이터가 없습니다.")
            print("먼저 pdf2kg.py를 실행하여 PDF를 Neo4j에 로드하세요.")
            neo4j_driver.close()
            sys.exit(1)

        # Step 3: Qdrant에 임베딩 저장
        print(f"\n[Step 3] Qdrant에 임베딩 저장")
        ingest_text_elements_to_qdrant(
            elements=elements,
            qdrant_client=qdrant_client,
            openai_client=openai_client,
            collection_name=collection_name,
            batch_size=10,
            embedding_model="text-embedding-3-small"
        )
    except Exception as e:
        print(f"\n오류 발생: {e}")
    finally:
        if 'neo4j_driver' in locals():
            neo4j_driver.close()
