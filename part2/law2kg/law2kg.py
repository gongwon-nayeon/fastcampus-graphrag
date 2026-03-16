import os
import re
import json
import textwrap
from typing import Dict, List, Any
from openai import OpenAI

from law_api import fetch_law_list, fetch_law_detail


# ============================================
# Neo4j 제약조건 설정
# ============================================

def setup_neo4j(driver):
    """Neo4j 제약조건 설정"""
    print("Neo4j 제약조건 설정 중...")

    constraints = [
        "CREATE CONSTRAINT law_id_unique IF NOT EXISTS FOR (l:Law) REQUIRE l.law_id IS UNIQUE",
        "CREATE CONSTRAINT article_id_unique IF NOT EXISTS FOR (a:Article) REQUIRE a.article_id IS UNIQUE",
        "CREATE CONSTRAINT org_name_unique IF NOT EXISTS FOR (o:Organization) REQUIRE o.name IS UNIQUE",
        "CREATE CONSTRAINT interpretation_id_unique IF NOT EXISTS FOR (i:LegalInterpretation) REQUIRE i.interpretation_id IS UNIQUE",
        "CREATE CONSTRAINT question_id_unique IF NOT EXISTS FOR (q:Question) REQUIRE q.question_id IS UNIQUE",
        "CREATE CONSTRAINT answer_id_unique IF NOT EXISTS FOR (a:Answer) REQUIRE a.answer_id IS UNIQUE",
        "CREATE CONSTRAINT reason_id_unique IF NOT EXISTS FOR (r:Reason) REQUIRE r.reason_id IS UNIQUE",
    ]

    for constraint in constraints:
        try:
            driver.execute_query(constraint, database_="neo4j")
            print(f"✓ {constraint[:50]}...")
        except Exception as e:
            print(f"ERROR: {str(e)}")

    print("제약조건 설정 완료!\n")


# ============================================
# 유틸리티 함수
# ============================================

def normalize_law_name(name: str) -> str:
    """법령명 정규화 (중복 체크용)

    - 띄어쓰기 제거
    - 특수문자 정규화 (·, ㆍ, ・ → 통일)
    - 소문자 변환
    """
    if not name:
        return ''

    normalized = name.replace('ㆍ', '·').replace('・', '·') # 특수문자 정규화
    normalized = normalized.replace(' ', '') # 띄어쓰기 제거
    normalized = normalized.lower() # 소문자 변환 (영문 포함 시)

    return normalized


def extract_law_citations_with_llm(text: str, interpretation_title: str) -> Dict[str, Any]:
    """LLM으로 해석례 텍스트에서 법령 및 조문 추출 (Structured Output)

    Args:
        text: 질의요지 + 회답 + 이유 합친 텍스트
        interpretation_title: 해석례 제목 (컨텍스트용)

    Returns:
        {
            "primary_law": "도로교통법",
            "cited_articles": ["3", "5", "12"],
            "other_laws": ["주차장법", "장애인복지법"]
        }
    """
    try:
        client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))

        prompt = textwrap.dedent(f"""
        당신은 법령 해석례를 분석하는 전문가입니다.
        주어진 텍스트를 분석하여 다음 키를 가진 JSON 객체를 생성해주세요:

        <output_format>
        1. **primary_law**: 이 해석례가 주로 다루는 법령명 (제목에서 「」 안의 법령)
        2. **cited_articles**: primary_law의 조문·항·호 정보를 상세히 추출 (배열)
        - 각 항목은 객체로 구성: {{"article": "조문번호", "paragraph": "항번호", "item": "호번호"}}
        - 예시:
            * "제4조제1항" → {{"article": "4", "paragraph": "1"}}
            * "제2조제2호" → {{"article": "2", "item": "2"}}
            * "제3조" → {{"article": "3"}}
            * "제5조제3항제2호" → {{"article": "5", "paragraph": "3", "item": "2"}}
        - primary_law와 관련된 조문만 포함
        3. **other_laws**: 본문에 언급된 다른 법령명 리스트 (primary_law 제외)
        </output_format>

        <rules>
        - cited_articles는 반드시 primary_law의 조문만 포함
        - 다른 법령의 조문은 cited_articles에 포함하지 말 것
        - 항·호가 없으면 해당 필드는 생략
        - 법령명은 「」를 제외한 순수 법령명만
        </rules>
        """).strip()

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": f"""
                    다음은 법령 해석례의 내용입니다.

                    <interpretation_title>
                    {interpretation_title}
                    </interpretation_title>

                    <interpretation_text>
                    {text[:3000]}
                    </interpretation_text>

                    위 텍스트에서 primary_law, cited_articles, other_laws를 추출하여 JSON으로 답변해주세요."""
                }
            ],
            response_format={"type": "json_object"},
            temperature=0.1,
            max_tokens=500
        )

        result = json.loads(response.choices[0].message.content)

        return {
            "primary_law": result.get("primary_law", ""),
            "cited_articles": result.get("cited_articles", []),
            "other_laws": result.get("other_laws", [])
        }

    except Exception as e:
        print(f"ERROR: {str(e)}")
        return {
            "primary_law": "",
            "cited_articles": [],
            "other_laws": []
        }


# ============================================
# 조문 추출 함수
# ============================================

def extract_articles_from_law_detail(law_detail: Dict[str, Any]) -> List[Dict[str, Any]]:
    """법령 상세에서 조문 정보 추출"""
    articles = []

    # JSON 구조: 법령 → 조문 → 조문단위
    law_data = law_detail.get('법령', {})
    article_section = law_data.get('조문', {})
    article_list = article_section.get('조문단위', [])

    if isinstance(article_list, dict):
        article_list = [article_list]

    for article in article_list:
        articles.append({
            'number': article.get('조문번호') or article.get('조번호'),
            'title': article.get('조문제목') or article.get('조문가지번호'),
            'content': article.get('조문내용') or article.get('조문'),
            'paragraphs': extract_paragraphs_from_article(article),
        })

    return articles


def extract_paragraphs_from_article(article: Dict[str, Any]) -> List[Dict[str, Any]]:
    """조문에서 항 정보 추출"""
    paragraphs = []

    if '항' in article:
        para_list = article['항']

        # 항이 딕셔너리면 리스트로 변환
        if isinstance(para_list, dict):
            para_list = [para_list]

        for para in para_list:
            # 항이 호 정보만 가진 경우 (제2조처럼)
            if isinstance(para, dict) and '호' in para and '항내용' not in para:
                # 항 없이 바로 호가 있는 경우
                paragraphs.append({
                    'content': '',
                    'items': extract_items_from_value(para.get('호', [])),
                })
            else:
                # 일반적인 항 구조
                paragraphs.append({
                    'content': para.get('항내용') or para.get('내용', ''),
                    'items': extract_items_from_value(para.get('호', [])),
                })

    return paragraphs


def extract_items_from_value(item_value: Any) -> List[Dict[str, Any]]:
    """호 정보 추출 (배열 또는 딕셔너리)"""
    items = []

    if not item_value:
        return items

    item_list = item_value
    if isinstance(item_list, dict):
        item_list = [item_list]

    for item in item_list:
        items.append({
            'content': item.get('호내용') or item.get('내용', ''),
        })

    return items


# ============================================
# 1단계: 현행법령(eflaw) 그래프 구축
# ============================================

def create_law_node(driver, law_data: Dict[str, Any]):
    """법령 노드 생성"""
    cypher = """
    MERGE (l:Law {law_id: $law_id})
    SET l.name = $name,
        l.short_name = $short_name,
        l.category = $category,
        l.promulgation_date = $promulgation_date,
        l.promulgation_number = $promulgation_number,
        l.enforcement_date = $enforcement_date,
        l.authority = $authority,
        l.status = $status,
        l.updated_at = datetime()
    RETURN l
    """

    params = {
        'law_id': law_data.get('법령일련번호') or law_data.get('MST'),
        'name': law_data.get('법령명한글') or law_data.get('법령명'),
        'short_name': law_data.get('법령약칭명'),
        'category': law_data.get('법령구분명') or law_data.get('법령구분'),
        'promulgation_date': law_data.get('공포일자'),
        'promulgation_number': law_data.get('공포번호'),
        'enforcement_date': law_data.get('시행일자'),
        'authority': law_data.get('소관부처명') or law_data.get('소관부처'),
        'status': law_data.get('법령상태') or '시행',
    }

    driver.execute_query(cypher, params, database_="neo4j")


def create_article_structure(driver, law_id: str, articles: List[Dict[str, Any]]):
    """조문 구조 생성 (조문, 항, 호)"""
    if not articles:
        print("    [SKIP] 조문 데이터가 없습니다.")
        return

    print(f"    - 조문 {len(articles)}개에 대한 조-항-호 구조 생성")

    for order, article_data in enumerate(articles, 1):
        article_id = f"{law_id}_{article_data.get('number', order)}"

        # Article 노드 생성
        cypher_article = """
        MERGE (a:Article {article_id: $article_id})
        SET a.number = $number,
            a.title = $title,
            a.content = $content,
            a.law_id = $law_id,
            a.order = $order
        WITH a
        MATCH (l:Law {law_id: $law_id})
        MERGE (l)-[:HAS_ARTICLE {order: $order}]->(a)
        RETURN a
        """

        # content가 dict/list인 경우 JSON 문자열로 변환
        content = article_data.get('content')
        if isinstance(content, (dict, list)):
            content = json.dumps(content, ensure_ascii=False)
        elif content is not None:
            content = str(content)

        params_article = {
            'article_id': article_id,
            'number': article_data.get('number'),
            'title': article_data.get('title'),
            'content': content,
            'law_id': law_id,
            'order': order,
        }

        driver.execute_query(cypher_article, params_article, database_="neo4j")

        # 항(Paragraph) 처리
        paragraphs = article_data.get('paragraphs', [])
        for p_order, para_data in enumerate(paragraphs, 1):
            paragraph_id = f"{article_id}_p{p_order}"

            cypher_para = """
            MERGE (p:Paragraph {paragraph_id: $paragraph_id})
            SET p.number = $number,
                p.content = $content,
                p.article_id = $article_id,
                p.order = $order
            WITH p
            MATCH (a:Article {article_id: $article_id})
            MERGE (a)-[:HAS_PARAGRAPH {order: $order}]->(p)
            RETURN p
            """

            # content가 dict/list인 경우 JSON 문자열로 변환
            para_content = para_data.get('content')
            if isinstance(para_content, (dict, list)):
                para_content = json.dumps(para_content, ensure_ascii=False)
            elif para_content is not None:
                para_content = str(para_content)

            params_para = {
                'paragraph_id': paragraph_id,
                'number': p_order,
                'content': para_content,
                'article_id': article_id,
                'order': p_order,
            }

            driver.execute_query(cypher_para, params_para, database_="neo4j")

            # 호(Item) 처리
            items = para_data.get('items', [])
            for i_order, item_data in enumerate(items, 1):
                item_id = f"{paragraph_id}_i{i_order}"

                cypher_item = """
                MERGE (i:Item {item_id: $item_id})
                SET i.number = $number,
                    i.content = $content,
                    i.paragraph_id = $paragraph_id,
                    i.order = $order
                WITH i
                MATCH (p:Paragraph {paragraph_id: $paragraph_id})
                MERGE (p)-[:HAS_ITEM {order: $order}]->(i)
                RETURN i
                """

                # content가 dict/list인 경우 JSON 문자열로 변환
                item_content = item_data.get('content')
                if isinstance(item_content, (dict, list)):
                    item_content = json.dumps(item_content, ensure_ascii=False)
                elif item_content is not None:
                    item_content = str(item_content)

                params_item = {
                    'item_id': item_id,
                    'number': i_order,
                    'content': item_content,
                    'paragraph_id': paragraph_id,
                    'order': i_order,
                }

                driver.execute_query(cypher_item, params_item, database_="neo4j")

    # NEXT_ARTICLE 관계 생성 (순차적 연결)
    cypher_next = """
    MATCH (l:Law {law_id: $law_id})-[:HAS_ARTICLE]->(a:Article)
    WITH a ORDER BY a.order
    WITH collect(a) as articles
    UNWIND range(0, size(articles)-2) as idx
    WITH articles[idx] as current, articles[idx+1] as next
    MERGE (current)-[:NEXT_ARTICLE]->(next)
    """

    driver.execute_query(cypher_next, {'law_id': law_id}, database_="neo4j")


def build_law_graph(driver, api_key: str, max_laws: int = 10):
    """1단계: 현행법령 그래프 구축"""
    print("=" * 80)
    print("[1단계] 현행법령(eflaw) 그래프 구축 시작")
    print("=" * 80)

    processed_count = 1
    page = 1

    while processed_count < max_laws:
        law_list_response = fetch_law_list(api_key, target='eflaw', display=max_laws, page=page)

        if not law_list_response:
            print("ERROR: 법령 목록을 가져올 수 없습니다.")
            break

        law_list_data = law_list_response.get('LawSearch', {})
        laws = law_list_data.get('law', [])
        if isinstance(laws, dict):
            laws = [laws]

        if not laws:
            print("더 이상 법령이 없습니다.")
            break

        for idx, law_summary in enumerate(laws, 1):
            if processed_count >= max_laws:
                break

            try:
                law_id = law_summary.get('법령일련번호') or law_summary.get('MST')
                law_name = law_summary.get('법령명한글') or law_summary.get('법령명')
                status_code = law_summary.get('현행연혁코드', '')

                if not law_id or not law_name:
                    continue

                if status_code == '연혁':
                    continue

                print(f"[{processed_count}/{max_laws}] 처리 중: {law_name} (ID: {law_id})")

                create_law_node(driver, law_summary)

                law_detail = fetch_law_detail(api_key, law_id, target='eflaw')
                if law_detail:
                    articles = extract_articles_from_law_detail(law_detail)
                    create_article_structure(driver, law_id, articles)

                processed_count += 1

            except Exception as e:
                print(f"ERROR: {str(e)}\n")

        page += 1

    print("=" * 80)
    print(f"[1단계] 현행법령 그래프 구축 완료! (총 {processed_count}개 법령 처리)")
    print("=" * 80)


# ============================================
# 2단계: 법령해석례(expc) 추가
# ============================================

def create_interpretation_node(driver, interp_summary: Dict[str, Any], interp_detail: Dict[str, Any] = None):
    """
    법령해석례 노드 생성

    생성 노드:
    - LegalInterpretation (메타데이터)
    - Question (질의요지)
    - Answer (회답)
    - Reason (이유)
    """
    # 목록 API 데이터
    interp_id = interp_summary.get('법령해석례일련번호') or interp_summary.get('MST')
    title = interp_summary.get('안건명') or interp_summary.get('제목', '')
    case_number = interp_summary.get('안건번호')
    answer_date_str = interp_summary.get('회신일자', '')

    # 상세 API 데이터 (ExpcService)
    question_text = None
    answer_text = None
    reason_text = None

    if interp_detail:
        expc_data = interp_detail.get('ExpcService', {})
        question_text = expc_data.get('질의요지', '')
        answer_text = expc_data.get('회답', '')
        reason_text = expc_data.get('이유', '')

    # 날짜 변환 (YYYYMMDD → datetime) - 안전한 변환
    answer_date_cypher = "null"
    if answer_date_str and len(answer_date_str) >= 8:
        try:
            year = answer_date_str[:4]
            month = answer_date_str[4:6]
            day = answer_date_str[6:8]

            # 숫자인지 검증
            if year.isdigit() and month.isdigit() and day.isdigit():
                y, m, d = int(year), int(month), int(day)
                # 유효 범위 검증
                if 1900 <= y <= 2100 and 1 <= m <= 12 and 1 <= d <= 31:
                    answer_date_cypher = f"datetime('{year}-{month}-{day}')"
        except:
            pass

    # 1. LegalInterpretation 메타 노드 생성
    cypher_interp = f"""
    MERGE (i:LegalInterpretation {{interpretation_id: $interpretation_id}})
    SET i.title = $title,
        i.case_number = $case_number,
        i.answer_date = {answer_date_cypher},
        i.status = 'active',
        i.updated_at = datetime()
    RETURN i
    """

    driver.execute_query(cypher_interp, {
        'interpretation_id': interp_id,
        'title': title,
        'case_number': case_number,
    }, database_="neo4j")

    # 2. Question 노드 생성
    if question_text:
        cypher_question = """
        MATCH (i:LegalInterpretation {interpretation_id: $interpretation_id})
        MERGE (q:Question {question_id: $question_id})
        SET q.text = $text,
            q.updated_at = datetime()
        MERGE (i)-[:HAS_QUESTION]->(q)
        RETURN q
        """

        driver.execute_query(cypher_question, {
            'interpretation_id': interp_id,
            'question_id': f"{interp_id}-Q",
            'text': question_text,
        }, database_="neo4j")

    # 3. Answer 노드 생성 및 Question과 연결
    if answer_text:
        cypher_answer = """
        MATCH (i:LegalInterpretation {interpretation_id: $interpretation_id})
        MERGE (a:Answer {answer_id: $answer_id})
        SET a.text = $text,
            a.updated_at = datetime()
        MERGE (i)-[:HAS_ANSWER]->(a)

        // Question과 Answer 직접 연결
        WITH i, a
        OPTIONAL MATCH (i)-[:HAS_QUESTION]->(q:Question)
        FOREACH (x IN CASE WHEN q IS NOT NULL THEN [1] ELSE [] END |
            MERGE (q)-[:ANSWERED_BY]->(a)
        )
        RETURN a
        """

        driver.execute_query(cypher_answer, {
            'interpretation_id': interp_id,
            'answer_id': f"{interp_id}-A",
            'text': answer_text,
        }, database_="neo4j")

    # 4. Reason 노드 생성
    if reason_text:
        cypher_reason = """
        MATCH (i:LegalInterpretation {interpretation_id: $interpretation_id})
        OPTIONAL MATCH (i)-[:HAS_ANSWER]->(a:Answer)
        MERGE (r:Reason {reason_id: $reason_id})
        SET r.text = $text,
            r.updated_at = datetime()
        WITH i, a, r
        WHERE a IS NOT NULL
        MERGE (a)-[:SUPPORTED_BY]->(r)
        WITH i, r
        WHERE NOT EXISTS((i)-[:HAS_ANSWER]->())
        MERGE (i)-[:HAS_REASON]->(r)
        RETURN r
        """

        driver.execute_query(cypher_reason, {
            'interpretation_id': interp_id,
            'reason_id': f"{interp_id}-R",
            'text': reason_text,
        }, database_="neo4j")

    # LLM으로 인용된 법령 및 조문 추출
    all_text = f"{question_text or ''} {answer_text or ''} {reason_text or ''}"
    citations = extract_law_citations_with_llm(all_text, title)

    return citations



def link_cited_articles(driver, interp_id: str, citations: List[Dict[str, str]]):
    """
    텍스트에서 인용된 조문·항·호와 노드 연결

    예시:
    - "제3조" → Article(3)과 CITES 연결
    - "제4조제1항" → Article(4) + Paragraph(1) 모두 CITES 연결
    - "제2조제2호" → Article(2) + Item(2) 모두 CITES 연결
    - "제5조제3항제2호" → Article(5) + Paragraph(3) + Item(2) 모두 CITES 연결

    Args:
        driver: Neo4j driver
        interp_id: 해석례 ID
        citations: 인용 정보 리스트
                  예: [{'article': '4', 'paragraph': '1'}, {'article': '2', 'item': '2'}]
    """
    if not citations:
        return

    linked_items = []

    for citation in citations:
        article_num = citation.get('article')
        paragraph_num = citation.get('paragraph')
        item_num = citation.get('item')

        if not article_num:
            continue

        # 1. Article 연결
        cypher_article = """
        MATCH (i:LegalInterpretation {interpretation_id: $interp_id})
        MATCH (i)-[:INTERPRETS]->(l:Law)
        MATCH (l)-[:HAS_ARTICLE]->(a:Article)
        WHERE a.number = $article_number
        MERGE (i)-[r:CITES]->(a)
        ON CREATE SET r.created_at = datetime()
        RETURN a.number as target
        """

        try:
            result = driver.execute_query(cypher_article, {
                'interp_id': interp_id,
                'article_number': article_num,
            }, database_="neo4j")

            if result.records:
                link_desc = f"제{article_num}조"

                # 2. Paragraph 연결 (있으면)
                if paragraph_num:
                    cypher_para = """
                    MATCH (i:LegalInterpretation {interpretation_id: $interp_id})
                    MATCH (i)-[:INTERPRETS]->(l:Law)
                    MATCH (l)-[:HAS_ARTICLE]->(a:Article {number: $article_number})
                    MATCH (a)-[:HAS_PARAGRAPH]->(p:Paragraph)
                    WHERE p.number = $paragraph_number
                    MERGE (i)-[r:CITES]->(p)
                    ON CREATE SET r.created_at = datetime()
                    RETURN p.number as target
                    """

                    para_result = driver.execute_query(cypher_para, {
                        'interp_id': interp_id,
                        'article_number': article_num,
                        'paragraph_number': int(paragraph_num),
                    }, database_="neo4j")

                    if para_result.records:
                        link_desc += f"제{paragraph_num}항"

                # 3. Item 연결 (있으면)
                if item_num:
                    # Item은 Paragraph 아래에 있거나, Article 바로 아래에 있을 수 있음
                    if paragraph_num:
                        cypher_item = """
                        MATCH (i:LegalInterpretation {interpretation_id: $interp_id})
                        MATCH (i)-[:INTERPRETS]->(l:Law)
                        MATCH (l)-[:HAS_ARTICLE]->(a:Article {number: $article_number})
                        MATCH (a)-[:HAS_PARAGRAPH]->(p:Paragraph {number: $paragraph_number})
                        MATCH (p)-[:HAS_ITEM]->(item:Item)
                        WHERE item.number = $item_number
                        MERGE (i)-[r:CITES]->(item)
                        ON CREATE SET r.created_at = datetime()
                        RETURN item.number as target
                        """

                        item_result = driver.execute_query(cypher_item, {
                            'interp_id': interp_id,
                            'article_number': article_num,
                            'paragraph_number': int(paragraph_num),
                            'item_number': int(item_num),
                        }, database_="neo4j")
                    else:
                        # Paragraph 없이 Item만 있는 경우 (Article 바로 아래)
                        cypher_item = """
                        MATCH (i:LegalInterpretation {interpretation_id: $interp_id})
                        MATCH (i)-[:INTERPRETS]->(l:Law)
                        MATCH (l)-[:HAS_ARTICLE]->(a:Article {number: $article_number})
                        MATCH (a)-[:HAS_PARAGRAPH]->(p:Paragraph)
                        MATCH (p)-[:HAS_ITEM]->(item:Item)
                        WHERE item.number = $item_number
                        MERGE (i)-[r:CITES]->(item)
                        ON CREATE SET r.created_at = datetime()
                        RETURN item.number as target
                        """

                        item_result = driver.execute_query(cypher_item, {
                            'interp_id': interp_id,
                            'article_number': article_num,
                            'item_number': int(item_num),
                        }, database_="neo4j")

                    if item_result and item_result.records:
                        link_desc += f"제{item_num}호"

                linked_items.append(link_desc)

        except Exception as e:
            print(f"       ERROR: (제{article_num}조): {str(e)[:40]}...")
            continue


def link_organizations(driver, interp_data: Dict[str, Any]):
    """기관 노드 생성 및 해석례 연결"""
    question_org = interp_data.get('질의기관명') or interp_data.get('질의기관')
    answer_org = interp_data.get('회신기관명') or interp_data.get('회신기관')
    interp_id = interp_data.get('법령해석례일련번호') or interp_data.get('MST')

    # 질의기관
    if question_org:
        question_date = interp_data.get('질의일자') or ''
        if question_date:
            cypher = """
            MERGE (o:Organization {name: $org_name})
            SET o.code = $org_code
            WITH o
            MATCH (i:LegalInterpretation {interpretation_id: $interp_id})
            MERGE (o)-[:REQUESTED {date: $date}]->(i)
            """
        else:
            cypher = """
            MERGE (o:Organization {name: $org_name})
            SET o.code = $org_code
            WITH o
            MATCH (i:LegalInterpretation {interpretation_id: $interp_id})
            MERGE (o)-[:REQUESTED]->(i)
            """
        driver.execute_query(cypher, {
            'org_name': question_org,
            'org_code': interp_data.get('질의기관코드'),
            'interp_id': interp_id,
            'date': question_date,
        }, database_="neo4j")

    # 회신기관
    if answer_org:
        answer_date = interp_data.get('회신일자') or ''
        if answer_date:
            cypher = """
            MERGE (o:Organization {name: $org_name})
            SET o.code = $org_code
            WITH o
            MATCH (i:LegalInterpretation {interpretation_id: $interp_id})
            MERGE (i)-[:ANSWERED_BY {date: $date}]->(o)
            """
        else:
            cypher = """
            MERGE (o:Organization {name: $org_name})
            SET o.code = $org_code
            WITH o
            MATCH (i:LegalInterpretation {interpretation_id: $interp_id})
            MERGE (i)-[:ANSWERED_BY]->(o)
            """
        driver.execute_query(cypher, {
            'org_name': answer_org,
            'org_code': interp_data.get('회신기관코드'),
            'interp_id': interp_id,
            'date': answer_date,
        }, database_="neo4j")


def link_to_law(driver, interp_data: Dict[str, Any], interp_detail: Dict[str, Any] = None):
    """해석례를 법령에 연결"""
    interp_id = interp_data.get('법령해석례일련번호') or interp_data.get('MST')

    # 법령명 추출: 해석례 텍스트에서 「법령명」 패턴으로 추출
    law_names = set()

    # 1) 상세 API의 안건명과 본문에서 「법령명」 패턴 추출
    if interp_detail:
        expc_data = interp_detail.get('ExpcService', {})
        text_fields = [
            expc_data.get('안건명', ''),
            expc_data.get('이유', ''),
            expc_data.get('질의요지', ''),
            expc_data.get('회답', '')
        ]

        # 정규식: 「...」 패턴에서 법령명 추출
        law_pattern = r'「([^」]+)」'
        for text in text_fields:
            matches = re.findall(law_pattern, text)
            for match in matches:
                # "동법", "이 법" 등 일반명사 제외
                if len(match) > 5 and not match.startswith('동'):
                    law_names.add(match.strip())

    # 2) 목록 API의 안건명에서도 추출
    title = interp_data.get('안건명', '')
    if title:
        matches = re.findall(r'「([^」]+)」', title)
        for match in matches:
            if len(match) > 5 and not match.startswith('동'):
                law_names.add(match.strip())

    if not law_names:
        print("    - 관련 법령을 찾을 수 없습니다.")
        return

    # 각 법령명으로 매칭 시도
    matched_count = 0
    for law_name in law_names:
        # 법령명 정규화 (특수문자, 띄어쓰기 통일)
        normalized_search = normalize_law_name(law_name)

        # 정규화된 이름으로 매칭 (Cypher에서 직접 정규화는 불가하므로 Python에서 처리)
        # 모든 Law를 가져와서 Python에서 비교
        cypher_get_laws = """
        MATCH (l:Law)
        RETURN l.law_id as law_id, l.name as name, l.short_name as short_name
        """

        result_laws = driver.execute_query(cypher_get_laws, database_="neo4j")

        for law_record in result_laws.records:
            law_id = law_record['law_id']
            db_name = law_record['name'] or ''
            db_short_name = law_record['short_name'] or ''

            # 정규화 비교
            normalized_db_name = normalize_law_name(db_name)
            normalized_db_short = normalize_law_name(db_short_name)

            matched = False

            # 완전 일치만 허용
            if normalized_search == normalized_db_name:
                matched = True
            elif db_short_name and normalized_search == normalized_db_short:
                matched = True

            if matched:
                # 매칭된 법령과 연결
                cypher_link = """
                MATCH (i:LegalInterpretation {interpretation_id: $interp_id})
                MATCH (l:Law {law_id: $law_id})
                MERGE (i)-[r:INTERPRETS]->(l)
                RETURN l.name
                """

                link_result = driver.execute_query(cypher_link, {
                    'interp_id': interp_id,
                    'law_id': law_id,
                }, database_="neo4j")

                if link_result.records:
                    print(f"    - 법령 매칭: {law_name[:30]}")
                    matched_count += 1


def link_cited_laws(driver, interp_id: str, other_laws: List[str]):
    """참조 법령 연결 (other_laws → CITES 관계)

    Args:
        driver: Neo4j driver
        interp_id: 해석례 ID
        other_laws: LLM이 추출한 참조 법령명 리스트
    """
    if not other_laws:
        return

    # 그래프의 모든 법령 목록 조회
    cypher_get_laws = """
    MATCH (l:Law)
    RETURN l.law_id as law_id, l.name as name, l.short_name as short_name
    """

    result_laws = driver.execute_query(cypher_get_laws, database_="neo4j")

    cited_count = 0

    for other_law_name in other_laws:
        # 법령명 정규화
        normalized_search = normalize_law_name(other_law_name)

        # 각 법령과 비교
        for law_record in result_laws.records:
            law_id = law_record['law_id']
            db_name = law_record['name'] or ''
            db_short_name = law_record['short_name'] or ''

            # 정규화 비교
            normalized_db_name = normalize_law_name(db_name)
            normalized_db_short = normalize_law_name(db_short_name)

            # 완전 일치만 허용
            matched = False
            if normalized_search == normalized_db_name:
                matched = True
            elif db_short_name and normalized_search == normalized_db_short:
                matched = True

            if matched:
                # CITES 관계 생성
                cypher_cite = """
                MATCH (i:LegalInterpretation {interpretation_id: $interp_id})
                MATCH (l:Law {law_id: $law_id})
                MERGE (i)-[r:CITES]->(l)
                ON CREATE SET r.created_at = datetime()
                RETURN l.name
                """

                cite_result = driver.execute_query(cypher_cite, {
                    'interp_id': interp_id,
                    'law_id': law_id,
                }, database_="neo4j")

                if cite_result.records:
                    cited_count += 1
                    break  # 매칭되면 다음 법령으로

    if cited_count > 0:
        print(f"       → 참조 법령 연결: {cited_count}개")


def build_interpretation_graph(driver, api_key: str, max_interpretations: int = 50):
    """2단계: 법령해석례 그래프 추가 (적재된 법령 기반)"""
    print("\n" + "=" * 80)
    print("[2단계] 법령해석례(expc) 그래프 구축 시작")
    print("=" * 80)

    # 1단계: 그래프에 적재된 법령 목록 조회
    print("\n[STEP 1] 그래프에서 적재된 법령 목록 조회 중...")
    cypher_laws = """
    MATCH (l:Law)
    RETURN l.name as name, l.short_name as short_name
    ORDER BY l.name
    """

    result = driver.execute_query(cypher_laws, database_="neo4j")
    if not result.records:
        print("ERROR: 그래프에 법령이 없습니다. 먼저 step1_load_laws.py를 실행하세요.")
        return

    law_names = []
    law_name_pairs = {}

    for record in result.records:
        full_name = record['name']
        short_name = record['short_name']

        if full_name:
            law_names.append(full_name)
            if short_name:
                law_name_pairs[full_name] = short_name
        elif short_name:  # 정식명칭이 없으면 약칭 사용
            law_names.append(short_name)

    law_names = list(set(law_names))

    normalized_law_names = {}
    for name in law_names:
        normalized_law_names[normalize_law_name(name)] = name
        # 약칭도 추가
        if name in law_name_pairs:
            normalized_law_names[normalize_law_name(law_name_pairs[name])] = name

    print(f"✓ 적재된 법령: {len(law_names)}개")
    print(f"  {', '.join(law_names[:5])}...\n")

    # 2단계: 각 법령에 대해 해석례 검색
    print(f"[STEP 2] 적재된 법령 기반으로 해석례 검색 중...\n")

    all_interpretations = []
    seen_interp_ids = set()
    seen_search_keywords = set()  # 이미 검색한 키워드 추적 (중복 검색 방지)

    for law_name in law_names:
        if len(all_interpretations) >= max_interpretations:
            break

        # 법령명에서 핵심 키워드 추출 (시행령, 시행규칙 등 제거)
        search_keyword = law_name.replace(' 시행령', '').replace(' 시행규칙', '').replace('시행령', '').replace('시행규칙', '').strip()

        # 너무 짧은 키워드는 스킵
        if len(search_keyword) < 4:
            continue

        # 이미 검색한 키워드는 스킵 (정규화해서 비교)
        normalized_keyword = normalize_law_name(search_keyword)
        if normalized_keyword in seen_search_keywords:
            continue
        seen_search_keywords.add(normalized_keyword)

        print(f"  검색 중: '{search_keyword[:30]}...'")

        # query 파라미터로 해당 법령 관련 해석례만 검색
        display_count = 100  # API 최대값

        expc_list_response = fetch_law_list(
            api_key,
            target='expc',
            display=display_count,
            query=search_keyword
        )

        if not expc_list_response:
            continue

        # JSON 응답에서 해석례 리스트 추출
        expc_list_data = expc_list_response.get('Expc', {})
        interpretations = expc_list_data.get('expc', [])
        if isinstance(interpretations, dict):
            interpretations = [interpretations]

        print(f"    → API 응답: {len(interpretations)}개")

        matched_count = 0
        filtered_count = 0

        # 각 해석례를 검증하면서 수집
        for interp in interpretations:
            if len(all_interpretations) >= max_interpretations:
                break

            interp_id = interp.get('법령해석례일련번호') or interp.get('MST')
            if not interp_id or interp_id in seen_interp_ids:
                filtered_count += 1
                continue

            # 해석례 제목에서 법령명 패턴 추출하여 사전 검증
            title = interp.get('안건명', '')
            extracted_laws = re.findall(r'「([^」]+)」', title)

            # 추출된 법령명이 그래프에 있는지 확인 (완전 일치만)
            has_match = False
            for extracted_law in extracted_laws:
                normalized_extracted = normalize_law_name(extracted_law)
                # 정규화된 이름으로 완전 일치 확인
                if normalized_extracted in normalized_law_names:
                    has_match = True
                    break

            # 매칭되는 경우에만 추가
            if has_match:
                all_interpretations.append(interp)
                seen_interp_ids.add(interp_id)
                matched_count += 1
            else:
                filtered_count += 1

        if matched_count > 0:
            print(f"    → 매칭: {matched_count}개 | 필터링: {filtered_count}개 | 누적: {len(all_interpretations)}개")
        elif filtered_count > 0:
            print(f"    → 매칭: 0개 | 필터링: {filtered_count}개 (관련 없는 해석례)")

    if not all_interpretations:
        print("\nERROR: 적재된 법령과 관련된 해석례를 찾을 수 없습니다.")
        return

    print(f"\n✓ 총 {len(all_interpretations)}개의 관련 해석례를 찾았습니다.\n")
    print("[STEP 3] 해석례 노드 생성 및 법령 연결 중...\n")

    # 3단계: 각 해석례 처리
    for idx, interp_summary in enumerate(all_interpretations, 1):
        try:
            interp_id = interp_summary.get('법령해석례일련번호') or interp_summary.get('MST')
            title = interp_summary.get('안건명') or interp_summary.get('제목')

            if not interp_id:
                print(f"[{idx}/{len(interpretations)}] SKIP: 해석례일련번호가 없습니다.")
                continue

            print(f"[{idx}/{len(all_interpretations)}] 처리 중: {title[:50] if title else 'N/A'}... (ID: {interp_id})")

            # 해석례 상세 조회
            interp_detail = fetch_law_detail(api_key, interp_id, target='expc')

            # 해석례 노드 생성 (상세 정보 포함) - LLM 추출 결과 반환받음
            citations = create_interpretation_node(driver, interp_summary, interp_detail)

            # 기관 노드 생성 및 연결
            link_organizations(driver, interp_summary)

            # 관련 법령 연결 (상세 정보 사용) - INTERPRETS 생성
            link_to_law(driver, interp_summary, interp_detail)

            # LLM으로 추출한 인용 조문 연결 (INTERPRETS 이후 수행)
            if citations and citations.get('cited_articles'):
                cited_articles = citations['cited_articles']
                primary_law = citations.get('primary_law', '')

                # Build display labels for each citation
                cite_labels = []
                for cite in cited_articles:
                    label = f"제{cite['article']}조"
                    if cite.get('paragraph'):
                        label += f"제{cite['paragraph']}항"
                    if cite.get('item'):
                        label += f"제{cite['item']}호"
                    cite_labels.append(label)

                print(f"    → 조항호 상세 추출(LLM): {primary_law} {', '.join(cite_labels)}")

                link_cited_articles(driver, interp_id, cited_articles)

            if citations and citations.get('other_laws'):
                other_laws = citations['other_laws']
                print(f"       참조 법령 추출(LLM): {', '.join(other_laws[:3])}{'...' if len(other_laws) > 3 else ''}")
                link_cited_laws(driver, interp_id, other_laws)

        except Exception as e:
            print(f"ERROR: {str(e)}\n")

    print("=" * 80)
    print("[2단계] 법령해석례 그래프 구축 완료!")
    print("=" * 80)
