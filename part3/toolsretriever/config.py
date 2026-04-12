import os
from dotenv import load_dotenv
import neo4j
from neo4j_graphrag.llm import OpenAILLM
from neo4j_graphrag.embeddings.openai import OpenAIEmbeddings
from neo4j_graphrag.indexes import create_vector_index

# 환경 변수 로드
load_dotenv()


def initialize_connection():
    """Neo4j 연결 및 OpenAI 클라이언트 초기화"""
    uri = os.getenv("NEO4J_URI")
    username = os.getenv("NEO4J_USERNAME", "neo4j")
    password = os.getenv("NEO4J_PASSWORD")

    driver = neo4j.GraphDatabase.driver(uri, auth=(username, password))

    llm = OpenAILLM(
        model_name="gpt-4o",
        model_params={"temperature": 0}
    )

    embedder = OpenAIEmbeddings(model="text-embedding-3-small")

    print(f"Neo4j 연결: {uri}")

    return driver, llm, embedder


def create_vector_indexes(driver):
    """TextElement와 TableElement를 위한 벡터 인덱스 생성"""

    # TextElement 벡터 인덱스
    text_index_name = "text_content_vector_index"
    try:
        create_vector_index(
            driver,
            text_index_name,
            label="TextElement",
            embedding_property="embedding",
            dimensions=1536,
            similarity_fn="cosine",
        )
        print(f"✓ TextElement 벡터 인덱스 생성: {text_index_name}")
    except Exception as e:
        print(f"  TextElement 벡터 인덱스 이미 존재 또는 생성 실패: {e}")

    # TableElement 벡터 인덱스
    table_index_name = "table_content_vector_index"
    try:
        create_vector_index(
            driver,
            table_index_name,
            label="TableElement",
            embedding_property="embedding",
            dimensions=1536,
            similarity_fn="cosine",
        )
        print(f"✓ TableElement 벡터 인덱스 생성: {table_index_name}")
    except Exception as e:
        print(f"  TableElement 벡터 인덱스 이미 존재 또는 생성 실패: {e}")

    return text_index_name, table_index_name
