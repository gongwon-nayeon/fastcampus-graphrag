# ============================================
# Few-shot 예시
# ============================================

# Cypher 쿼리 생성을 위한 Few-shot 예시 (의료 지식 그래프 도메인)
FEWSHOT_EXAMPLES = [
    "USER INPUT: '기침 증상과 관련된 질병은?' QUERY: MATCH (s:Symptom {name: '기침'})-[:INDICATES]->(d:Disease) RETURN d.name",
    "USER INPUT: '폐렴을 진단하는 검사는?' QUERY: MATCH (t:Test)-[:DIAGNOSES_FOR]->(d:Disease {name: '폐렴'}) RETURN t.name",
    "USER INPUT: '당뇨병 치료 방법을 알려줘' QUERY: MATCH (tr:Treatment)-[:TREATS]->(d:Disease {name: '당뇨병'}) RETURN tr.name LIMIT 5",
    "USER INPUT: '고혈압에 사용하는 약물은?' QUERY: MATCH (m:Medication)-[:TREATS]->(d:Disease {name: '고혈압'}) RETURN m.name",
    "USER INPUT: '두통과 관련된 신체 부위는?' QUERY: MATCH (s:Symptom {name: '두통'})-[:AFFECTS]->(a:Anatomy) RETURN a.name",
    "USER INPUT: '천식 증상에는 무엇이 있나요?' QUERY: MATCH (s:Symptom)-[:INDICATES]->(d:Disease {name: '천식'}) RETURN DISTINCT s.name",

    "USER INPUT: '내과 질문 중 당뇨병 관련 질문들을 보여줘' QUERY: MATCH (q:Question)-[:BELONGS_TO]->(d:Department {name: '내과'}) WHERE q.content CONTAINS '당뇨병' RETURN q.content LIMIT 5",
    "USER INPUT: '응급의학과에서 다루는 질병은?' QUERY: MATCH (q:Question)-[:BELONGS_TO]->(d:Department {name: '응급의학과'}), (q)-[:MENTIONS]->(dis:Disease) RETURN DISTINCT dis.name LIMIT 10",

    "USER INPUT: '폐렴과 관련된 증상과 검사를 함께 보여줘' QUERY: MATCH (d:Disease {name: '폐렴'})<-[:INDICATES]-(s:Symptom), (d)<-[:DIAGNOSES_FOR]-(t:Test) RETURN s.name AS symptom, t.name AS test LIMIT 5",

    "USER INPUT: '가장 많이 언급된 질병 5개는?' QUERY: MATCH (q:Question)-[:MENTIONS]->(d:Disease) RETURN d.name, count(q) AS mention_count ORDER BY mention_count DESC LIMIT 5",
]

FEWSHOT_EXAMPLES_STR = "\n".join(FEWSHOT_EXAMPLES)

# ============================================
# Cypher 쿼리 생성 프롬프트
# ============================================

GENERATE_CYPHER_SYSTEM_TEMPLATE = """당신은 Neo4j 전문가입니다. 입력된 질문에 대해 문법적으로 올바른 Cypher 쿼리를 생성하세요.
백틱이나 다른 것으로 감싸지 마세요. Cypher 쿼리문만 응답하세요!"""

GENERATE_CYPHER_USER_TEMPLATE = """
<schema>
{schema}
</schema>

다음은 질문과 대응하는 Cypher 쿼리의 예시들입니다:
<examples>
{fewshot_examples}
</examples>

<user_question>
{question}
</user_question>
Cypher 쿼리:"""

GENERATE_CYPHER_PROMPTS = [
    ("system", GENERATE_CYPHER_SYSTEM_TEMPLATE),
    ("user", GENERATE_CYPHER_USER_TEMPLATE),
]


# ============================================
# Cypher 쿼리 수정 프롬프트
# ============================================

CORRECT_CYPHER_SYSTEM_TEMPLATE = """당신은 주니어 개발자가 작성한 Cypher 쿼리를 검토하는 Cypher 전문가입니다.
제공된 오류를 기반으로 Cypher 쿼리를 수정해야 합니다.
백틱이나 다른 것으로 감싸지 마세요. Cypher 쿼리문만 응답하세요!"""

CORRECT_CYPHER_USER_TEMPLATE = """잘못된 문법이나 의미를 확인하고 수정된 Cypher 쿼리를 반환하세요.

<schema>
{schema}
</schema>

Cypher 쿼리 작성 외의 다른 질문에는 응답하지 마세요.

질문:
{question}

Cypher 쿼리:
{cypher}

오류:
{errors}

수정된 Cypher 쿼리: """

CORRECT_CYPHER_PROMPTS = [
    ("system", CORRECT_CYPHER_SYSTEM_TEMPLATE),
    ("user", CORRECT_CYPHER_USER_TEMPLATE),
]


# ============================================
# 최종 답변 생성 프롬프트
# ============================================

FINAL_ANSWER_SYSTEM_TEMPLATE = """
당신은 간결하고 정확한 답변을 제공하도록 훈련된 매우 지능적인 의료 지식 어시스턴트입니다.
Neo4j 데이터베이스에서 특정 Cypher 쿼리를 사용하여 검색된 컨텍스트가 제공됩니다.

**중요한 역할:**
1. 컨텍스트가 정상적인 검색 결과인 경우:
   - 제공된 정보를 기반으로 사용자의 질문에 답변하세요
   - 컨텍스트에 명시적으로 언급되지 않는 한 추측하거나 외부 지식을 사용하지 마세요
   - 정보가 부족한 경우 사용자에게 알리고 추가 정보를 제안하세요

2. 컨텍스트가 에러 메시지인 경우:
   - 에러 내용을 사용자가 이해하기 쉽게 설명하세요
   - 질문을 어떻게 수정하면 좋을지 구체적인 제안을 제공하세요
   - 가능한 대안적 질문 방법을 안내하세요
   - 재시도가 많이 발생한 경우, 질문을 더 단순하거나 구체적으로 바꿔보라고 권장하세요

**마크다운 형식 가이드:**
- 주요 내용은 제목(##)과 부제목(###)으로 구조화하세요
- 여러 항목은 목록(-, *)으로 정리하세요
- 중요한 키워드는 **볼드**로 강조하세요
- 기술 용어나 엔티티명은 `백틱`으로 감싸세요
- 단락 간 적절한 줄바꿈을 사용하세요
- 답변 구조: 요약 → 상세 내용 → (필요시) 추가 정보 또는 제안

최종 답변이 명확하고 관련성 있으며 사용자의 질문에 직접적으로 답변하는지 확인하세요.
반드시 한국어로 답변하세요.
"""

FINAL_ANSWER_USER_TEMPLATE = """
다음 Cypher 쿼리를 사용하여 Neo4j 데이터베이스에서 검색을 시도했습니다:
`{cypher_query}`

컨텍스트 (검색 결과 또는 에러 메시지):
{context}

사용자 질문:
<question>
{question}
</question>

최종 답변:
"""

FINAL_ANSWER_PROMPTS = [
    ("system", FINAL_ANSWER_SYSTEM_TEMPLATE),
    ("user", FINAL_ANSWER_USER_TEMPLATE),
]
