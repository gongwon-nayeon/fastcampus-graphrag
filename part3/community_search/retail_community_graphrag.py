import os
import sys
from typing import List, Dict, Any, Optional

from dotenv import load_dotenv
from neo4j import GraphDatabase
import openai

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
    print(f"Neo4j 연결 성공: {uri}")
    return driver


def setup_openai():
    """OpenAI API 설정"""
    api_key = os.getenv('OPENAI_API_KEY')

    openai.api_key = api_key
    print("OpenAI API 설정 완료")


def check_community_exists(driver) -> bool:
    """behaviorCommunityId 속성 존재 확인"""
    query = """
    MATCH (a:Article)
    WHERE a.behaviorCommunityId IS NOT NULL
    RETURN count(a) AS count
    LIMIT 1
    """

    result = driver.execute_query(query, database_=NEO4J_DATABASE)

    if not result.records or result.records[0]['count'] == 0:
        print("먼저 community_detection_retail.py를 실행해주세요.")
        return False

    print("behaviorCommunityId 속성 확인 완료")
    return True


# ============================================
# Step 1: 초기 상품 찾기
# ============================================

def extract_search_keywords(question: str) -> List[str]:
    """
    질문에서 검색 키워드 추출

    OpenAI GPT로 질문을 분석하여 H&M 상품 검색에 적합한 키워드 추출
    """
    try:
        response = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": """H&M 상품 검색을 위한 키워드 추출 전문가입니다.
사용자 질문에서 상품명(prod_name)과 매칭될 **구체적인** 영문 키워드를 추출하세요.

실제 H&M 데이터베이스 상품명 예시:
- 후드티/재킷: "Theron", "Mr Harrington w/hood", "Kevin softshell jacket", "Freja Coat", "Pablo coat"
- 청바지: "6DENIM Jeggings", "6DENIM Skinny", "6DENIM+ Jacket oversized"
- 스포츠: "Karin padded sport bra", "Speedy tee", "CINDY tights", "SUPREME RW tights"
- 외관: Solid (단색), Stripe, Lace, Pattern, Denim

중요 규칙:
1. 상품명에 실제로 나타나는 구체적인 단어 사용 (예: "jacket", "coat", "theron", "denim")
2. 너무 일반적인 단어 피하기 (❌ "top", "shirt", "clothes" 등)
3. 한국어를 영어로 정확히 번역 (후드티 → hoodie, theron / 청바지 → denim, jegging)

출력 형식: 쉼표로 구분된 영문 키워드 (최대 3개)
예시:
"스포츠 의류" → sport, bra, tights
"겨울 후드티" → theron, jacket, hoodie
"겨울 코트" → coat, jacket, pablo
"청바지" → denim, jegging, skinny
"운동 레깅스" → tights, cindy, supreme
"드레스" → dress"""
                },
                {
                    "role": "user",
                    "content": f"질문: {question}\n\n검색 키워드:"
                }
            ],
            temperature=0.3,
            max_tokens=50
        )

        keywords_text = response.choices[0].message.content.strip()
        keywords = [k.strip() for k in keywords_text.split(',') if k.strip()]
        return keywords[:3]  # 최대 3개

    except Exception as e:
        print(f"키워드 추출 실패: {e}")


def find_similar_products(
    driver,
    question: str,
    top_k: int = 3,
    min_community_size: int = 50,
    max_community_size: int = 5000
) -> List[Dict[str, Any]]:
    """
    질문과 관련된 상품 찾기 (개선된 버전)

    GPT로 키워드를 추출하고, 유연한 전체 텍스트 검색 수행
    적절한 크기의 커뮤니티만 선택하여 더 정확한 추천 제공

    Args:
        driver: Neo4j driver
        question: 사용자 질문
        top_k: 반환할 상품 개수
        min_community_size: 최소 커뮤니티 크기 (기본: 50)
        max_community_size: 최대 커뮤니티 크기 (기본: 5000)

    Returns:
        찾은 상품 리스트
    """
    print("\n" + "=" * 60)
    print("[Step 1] 질문과 관련된 상품 찾기")
    print("=" * 60)

    # GPT로 검색 키워드 추출
    keywords = extract_search_keywords(question)

    print(f"\n질문: '{question}'")
    print(f"추출된 키워드: {', '.join(keywords)}")

    # 여러 키워드로 검색 시도
    for keyword in keywords:
        # Neo4j 쿼리: prod_name 우선 검색 (가장 정확)
        query = """
        MATCH (a:Article)
        WHERE a.behaviorCommunityId IS NOT NULL
          AND toLower(a.prod_name) CONTAINS toLower($keyword)
        WITH a
        // 커뮤니티 크기 계산
        MATCH (c:Article {behaviorCommunityId: a.behaviorCommunityId})
        WITH a, count(c) AS communitySize
        WHERE communitySize >= $min_size AND communitySize <= $max_size
        RETURN DISTINCT a.article_id AS article_id,
               a.prod_name AS name,
               a.graphical_appearance_name AS appearance,
               a.perceived_colour_master_name AS colour,
               a.behaviorCommunityId AS communityId,
               communitySize
        ORDER BY communitySize ASC
        LIMIT $top_k
        """

        result = driver.execute_query(
            query,
            keyword=keyword,
            top_k=top_k,
            min_size=min_community_size,
            max_size=max_community_size,
            database_=NEO4J_DATABASE
        )

        # prod_name에서 찾으면 바로 반환
        if result.records:
            products = []
            for record in result.records:
                products.append({
                    'article_id': record['article_id'],
                    'name': record['name'],
                    'appearance': record['appearance'],
                    'colour': record['colour'],
                    'communityId': record['communityId'],
                    'communitySize': record['communitySize']
                })

            print(f"'{keyword}' 키워드로 {len(products)}개 상품 발견 (상품명 매칭):")
            for i, prod in enumerate(products, 1):
                appearance = prod['appearance'] or 'N/A'
                colour = prod['colour'] or 'N/A'
                size = prod['communitySize']
                print(f"  {i}. {prod['name']} ({appearance}, {colour}) - Community {prod['communityId']} (크기: {size:,}개)")

            return products

        # prod_name에서 못 찾으면 detail_desc도 검색 (보조)
        fallback_query = """
        MATCH (a:Article)
        WHERE a.behaviorCommunityId IS NOT NULL
          AND (
            toLower(a.detail_desc) CONTAINS toLower($keyword) OR
            toLower(a.graphical_appearance_name) CONTAINS toLower($keyword)
          )
        WITH a
        MATCH (c:Article {behaviorCommunityId: a.behaviorCommunityId})
        WITH a, count(c) AS communitySize
        WHERE communitySize >= $min_size AND communitySize <= $max_size
        RETURN DISTINCT a.article_id AS article_id,
               a.prod_name AS name,
               a.graphical_appearance_name AS appearance,
               a.perceived_colour_master_name AS colour,
               a.behaviorCommunityId AS communityId,
               communitySize
        ORDER BY communitySize ASC
        LIMIT $top_k
        """

        result = driver.execute_query(
            fallback_query,
            keyword=keyword,
            top_k=top_k,
            min_size=min_community_size,
            max_size=max_community_size,
            database_=NEO4J_DATABASE
        )

        if result.records:
            products = []
            for record in result.records:
                products.append({
                    'article_id': record['article_id'],
                    'name': record['name'],
                    'appearance': record['appearance'],
                    'colour': record['colour'],
                    'communityId': record['communityId'],
                    'communitySize': record['communitySize']
                })

            print(f"'{keyword}' 키워드로 {len(products)}개 상품 발견 (설명 매칭):")
            for i, prod in enumerate(products, 1):
                appearance = prod['appearance'] or 'N/A'
                colour = prod['colour'] or 'N/A'
                size = prod['communitySize']
                print(f"  {i}. {prod['name']} ({appearance}, {colour}) - Community {prod['communityId']} (크기: {size:,}개)")

            return products

    return []


# ============================================
# Step 2: 커뮤니티 내 상품 검색
# ============================================

def search_in_community(driver, community_id: int, max_results: int = 20) -> List[Dict[str, Any]]:
    """
    특정 커뮤니티에 속한 상품 검색

    같은 커뮤니티 = 비슷한 카테고리/스타일의 상품들

    Args:
        driver: Neo4j driver
        community_id: 커뮤니티 ID
        max_results: 최대 결과 개수

    Returns:
        커뮤니티 내 상품 리스트
    """
    print("\n" + "=" * 60)
    print(f"[Step 2] Community {community_id} 내부 검색")
    print("=" * 60)

    query = """
    MATCH (a:Article)
    WHERE a.behaviorCommunityId = $community_id
    RETURN a.article_id AS article_id,
           a.prod_name AS name,
           a.graphical_appearance_name AS appearance,
           a.perceived_colour_master_name AS colour,
           a.detail_desc AS description
    LIMIT $max_results
    """

    result = driver.execute_query(
        query,
        community_id=community_id,
        max_results=max_results,
        database_=NEO4J_DATABASE
    )

    products = []
    for record in result.records:
        products.append({
            'article_id': record['article_id'],
            'name': record['name'],
            'appearance': record['appearance'],
            'colour': record['colour'],
            'description': record['description']
        })

    print(f"\nCommunity {community_id}에서 {len(products)}개 상품 발견")

    # 샘플 출력
    print("\n샘플 상품:")
    for i, prod in enumerate(products[:5], 1):
        appearance = prod['appearance'] or 'N/A'
        colour = prod['colour'] or 'N/A'
        print(f"  {i}. {prod['name']} ({appearance}, {colour})")

    return products


# ============================================
# Step 3: 커뮤니티 분석
# ============================================

def analyze_community(driver, community_id: int) -> str:
    """
    커뮤니티의 주요 특징 분석

    어떤 부서, 카테고리, 색상이 많은지 분석하여
    커뮤니티의 특성을 파악

    Args:
        driver: Neo4j driver
        community_id: 커뮤니티 ID

    Returns:
        커뮤니티 분석 문자열
    """
    print("\n" + "=" * 60)
    print(f"[Step 3] Community {community_id} 분석")
    print("=" * 60)

    # 커뮤니티 구성 분석
    query = """
    MATCH (a:Article)
    WHERE a.behaviorCommunityId = $community_id
    WITH
        collect(DISTINCT a.graphical_appearance_name) AS appearances,
        collect(DISTINCT a.perceived_colour_master_name) AS colours,
        count(a) AS total_products
    RETURN appearances, colours, total_products
    """

    result = driver.execute_query(
        query,
        community_id=community_id,
        database_=NEO4J_DATABASE
    )

    if not result.records:
        return f"Community {community_id}: 데이터 없음"

    record = result.records[0]
    appearances = [a for a in record['appearances'] if a][:5]
    colours = [c for c in record['colours'] if c][:5]
    total = record['total_products']

    # 분석 텍스트 구성
    analysis_parts = [f"Community {community_id} 분석 (총 {total:,}개 상품):"]

    if appearances:
        analysis_parts.append(f"\n주요 스타일: {', '.join(appearances)}")

    if colours:
        analysis_parts.append(f"주요 색상: {', '.join(colours)}")

    analysis = "\n".join(analysis_parts)
    print(f"\n{analysis}")

    return analysis


# ============================================
# Step 4: 추천 컨텍스트 구성
# ============================================

def build_recommendation_context(
    question: str,
    initial_products: List[Dict[str, Any]],
    community_products: List[Dict[str, Any]],
    community_analysis: str
) -> str:
    """
    상품 추천을 위한 컨텍스트 구성

    Args:
        question: 사용자 질문
        initial_products: 초기 검색 상품
        community_products: 커뮤니티 내 상품
        community_analysis: 커뮤니티 분석

    Returns:
        포맷된 컨텍스트
    """
    context_parts = [
        "# 커뮤니티 분석",
        community_analysis,
        "",
        "# 검색된 상품",
        ""
    ]

    # 초기 검색 상품
    context_parts.append("## 질문과 직접 관련된 상품:")
    for i, prod in enumerate(initial_products[:3], 1):
        appearance = prod.get('appearance', 'N/A')
        colour = prod.get('colour', 'N/A')
        context_parts.append(f"{i}. {prod['name']} - {appearance}, {colour}")
    context_parts.append("")

    # 커뮤니티 내 추천 상품
    context_parts.append("## 같은 카테고리의 다른 상품 (추천 후보):")
    for i, prod in enumerate(community_products[:10], 1):
        name = prod.get('name', 'N/A')
        appearance = prod.get('appearance', 'N/A')
        colour = prod.get('colour', '')
        if colour:
            context_parts.append(f"{i}. {name} ({appearance}, {colour})")
        else:
            context_parts.append(f"{i}. {name} ({appearance})")

    return "\n".join(context_parts)


# ============================================
# Step 5: LLM 추천 생성
# ============================================

def generate_recommendation(question: str, context: str) -> str:
    """
    OpenAI GPT로 상품 추천 생성

    Args:
        question: 사용자 질문
        context: 상품 컨텍스트

    Returns:
        추천 답변
    """
    print("\n" + "=" * 60)
    print("[Step 4] AI 추천 생성")
    print("=" * 60)

    system_prompt = """당신은 H&M 패션 전문 어드바이저입니다.
커뮤니티 분석 결과와 상품 목록을 바탕으로 고객에게 적합한 상품을 추천해주세요.

추천 답변은 다음 구조로 작성해주세요:

**[메인 추천]**
1. 고객의 질문에 직접 관련된 상품 2-3개를 구체적으로 추천
2. 각 상품의 이름, 특징, 왜 좋은지 간단히 설명
3. 친근하고 도움이 되는 톤으로 작성

**[같은 커뮤니티의 다른 추천]**
4. 메인 추천과 함께 구매하면 좋은 상품이나 같은 카테고리의 다른 스타일 상품 2-3개 소개
5. 직접적으로 관련이 없어도 같은 커뮤니티 내의 상품이라면 언급
6. "함께 보면 좋은 상품", "이런 스타일도 있어요" 등의 표현 사용

커뮤니티 내 다양한 상품을 적극 활용하여 고객에게 더 많은 선택지를 제공하세요.
"""

    user_prompt = f"""고객 질문: {question}

상품 정보:
{context}

위 정보를 바탕으로 고객에게 적합한 상품을 추천해주세요.
"""

    try:
        response = openai.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.7,
            max_tokens=800
        )

        recommendation = response.choices[0].message.content
        print(f"\n추천 생성 완료 ({response.usage.total_tokens} tokens)")
        return recommendation

    except Exception as e:
        print(f"\n추천 생성 실패: {e}")
        return "추천을 생성할 수 없습니다."


# ============================================
# 전체 파이프라인
# ============================================

def retail_graphrag_query(
    driver,
    question: str,
    min_community_size: int = 5,
    max_community_size: int = 5000
) -> Dict[str, Any]:
    """
    커뮤니티 기반 상품 추천 전체 파이프라인

    Args:
        driver: Neo4j driver
        question: 사용자 질문
        min_community_size: 최소 커뮤니티 크기 (기본: 5)
        max_community_size: 최대 커뮤니티 크기 (기본: 5000)

    Returns:
        추천 결과 딕셔너리
    """
    print("\n" + "=" * 70)
    print("Community-based Product Recommendation")
    print("=" * 70)

    # 0. behaviorCommunityId 확인
    if not check_community_exists(driver):
        return {
            'question': question,
            'recommendation': 'behaviorCommunityId 속성이 없습니다. community_detection_retail.py를 먼저 실행해주세요.',
            'communityId': None,
            'products': []
        }

    # Step 1: 관련 상품 찾기 (커뮤니티 크기 필터링 적용)
    initial_products = find_similar_products(
        driver,
        question,
        top_k=3,
        min_community_size=min_community_size,
        max_community_size=max_community_size
    )
    if not initial_products:
        return {
            'question': question,
            'recommendation': f'적절한 크기({min_community_size:,}~{max_community_size:,}개)의 커뮤니티에서 관련 상품을 찾을 수 없습니다. 다른 키워드로 시도하거나 크기 범위를 조정해보세요.',
            'communityId': None,
            'products': []
        }

    # 첫 번째 상품의 커뮤니티 사용
    community_id = initial_products[0]['communityId']
    community_size = initial_products[0]['communitySize']
    print(f"\n선택된 커뮤니티: {community_id} (크기: {community_size:,}개)")

    # Step 2: 커뮤니티 내 상품 검색
    community_products = search_in_community(driver, community_id, max_results=20)

    # Step 3: 커뮤니티 분석
    community_analysis = analyze_community(driver, community_id)

    # Step 4: 컨텍스트 구성
    context = build_recommendation_context(
        question,
        initial_products,
        community_products,
        community_analysis
    )

    # Step 5: 추천 생성
    recommendation = generate_recommendation(question, context)

    return {
        'question': question,
        'recommendation': recommendation,
        'communityId': community_id,
        'communitySize': community_size,
        'products': community_products[:10],
        'context': context
    }


# ============================================
# 결과 출력
# ============================================

def print_recommendation(result: Dict[str, Any]):
    """추천 결과 출력"""
    print("\n" + "=" * 70)
    print("상품 추천 결과")
    print("=" * 70)

    print(f"\n질문: {result['question']}")

    if result.get('communityId'):
        size_info = f" (크기: {result.get('communitySize', 'N/A'):,}개)" if result.get('communitySize') else ""
        print(f"\nCommunity ID: {result['communityId']}{size_info}")

    print(f"\n추천:\n{result['recommendation']}")

    if result.get('products'):
        print(f"\n" + "-" * 70)
        print("추천 상품 목록:")
        print("-" * 70)
        for i, prod in enumerate(result['products'][:5], 1):
            name = prod.get('name', 'N/A')
            appearance = prod.get('appearance', 'N/A')
            colour = prod.get('colour', 'N/A')
            print(f"{i}. {name} ({appearance}, {colour})")


# ============================================
# 메인 함수
# ============================================

def main():
    print("=" * 70)
    print("H&M 커뮤니티 기반 상품 추천 시스템")
    print("=" * 70)

    # 연결 설정
    driver = create_neo4j_connection()
    setup_openai()

    try:
        print("=" * 70)

        while True:
            user_question = input("질문: ").strip()

            if user_question.lower() in ['quit', 'exit', 'q']:
                print("\n프로그램을 종료합니다.")
                break

            if not user_question:
                continue

            result = retail_graphrag_query(driver, user_question)
            print_recommendation(result)

    finally:
        driver.close()

if __name__ == "__main__":
    main()
