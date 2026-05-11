import argparse
import csv
import json
import os
import re
import time
from dataclasses import dataclass, asdict, field

from dotenv import load_dotenv

load_dotenv()

from retriever import create_driver, create_llm, build_retriever, generate_and_execute_cypher


# =============================================
# 데이터 구조
# =============================================

@dataclass
class EvalRecord:
    """단일 질문에 대한 평가 결과"""
    # 입력
    question: str = ""
    question_type: str = ""
    golden_cypher: str = ""
    # 생성 결과
    generated_cypher: str = ""
    # Execution 평가
    is_executable: bool = False
    execution_error: str = ""
    returns_results: bool = False
    result_count: int = 0
    execution_results: list = field(default_factory=list)
    relationship_direction_correct: bool = False
    direction_detail: str = ""
    # LLM-as-a-Judge 평가
    intent_score: int = 0       # 1-5
    completeness_score: int = 0  # 1-5
    correctness_score: int = 0   # 1-5
    llm_judge_reasoning: str = ""
    # 요약 점수
    execution_score: float = 0.0  # 0 or 1
    llm_judge_avg: float = 0.0


# =============================================
# 데이터셋 로드
# =============================================

def load_golden_dataset(csv_path: str, database_filter: str = "recommendations") -> list[dict]:
    """골든 데이터셋에서 특정 database의 레코드만 로드합니다."""
    records = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["database"] == database_filter:
                records.append(row)
    return records


def load_schema(csv_path: str, database_filter: str = "recommendations") -> str:
    """스키마 CSV에서 특정 database의 스키마 텍스트를 로드합니다."""
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["database"] == database_filter:
                return row["schema"]
    raise ValueError(f"Schema not found for database: {database_filter}")


# =============================================
# Execution 평가: 관계 방향 검사
# =============================================

# recommendations DB의 올바른 관계 방향 정의
VALID_RELATIONSHIPS = {
    ("Movie", "IN_GENRE", "Genre"),
    ("User", "RATED", "Movie"),
    ("Actor", "ACTED_IN", "Movie"),
    ("Actor", "DIRECTED", "Movie"),
    ("Director", "DIRECTED", "Movie"),
    ("Director", "ACTED_IN", "Movie"),
    ("Person", "ACTED_IN", "Movie"),
    ("Person", "DIRECTED", "Movie"),
}


def extract_relationships_from_cypher(cypher: str) -> list[tuple[str, str, str]]:
    """Cypher 쿼리에서 관계 패턴 (source_label, rel_type, target_label) 을 추출합니다."""
    relationships = []

    # (var:Label)-[r:REL_TYPE]->(var:Label) 패턴 매칭
    # 정방향: ()-[]->(  )
    forward_pattern = r'\((\w+)?(?::(\w+))?\)\s*-\[(\w+)?(?::(\w+))?\]\s*->\s*\((\w+)?(?::(\w+))?\)'
    for m in re.finditer(forward_pattern, cypher, re.IGNORECASE):
        src_label = m.group(2) or ""
        rel_type = m.group(4) or ""
        tgt_label = m.group(6) or ""
        if rel_type:
            relationships.append((src_label, rel_type, tgt_label))

    # 역방향: ()<-[]-()
    reverse_pattern = r'\((\w+)?(?::(\w+))?\)\s*<-\[(\w+)?(?::(\w+))?\]\s*-\s*\((\w+)?(?::(\w+))?\)'
    for m in re.finditer(reverse_pattern, cypher, re.IGNORECASE):
        src_label = m.group(2) or ""
        rel_type = m.group(4) or ""
        tgt_label = m.group(6) or ""
        if rel_type:
            # 역방향이므로 실제 관계: target -> source
            relationships.append((tgt_label, rel_type, src_label))

    return relationships


def check_relationship_directions(cypher: str) -> tuple[bool, str]:
    """생성된 Cypher의 관계 방향이 스키마와 일치하는지 검사합니다."""
    rels = extract_relationships_from_cypher(cypher)

    if not rels:
        return True, "관계 패턴 없음 (노드 단독 쿼리)"

    issues = []
    for src, rel_type, tgt in rels:
        if not src or not tgt:
            continue  # 라벨이 없으면 검사 불가
        if (src, rel_type, tgt) not in VALID_RELATIONSHIPS:
            # 혹시 역방향으로 존재하는지 확인
            if (tgt, rel_type, src) in VALID_RELATIONSHIPS:
                issues.append(f"방향 역전: (:{src})-[:{rel_type}]->(:{tgt}) → 올바른 방향: (:{tgt})-[:{rel_type}]->(:{src})")
            else:
                issues.append(f"스키마에 없는 관계: (:{src})-[:{rel_type}]->(:{tgt})")

    if issues:
        return False, "; ".join(issues)
    return True, "모든 관계 방향 올바름"


# =============================================
# LLM-as-a-Judge 평가
# =============================================

LLM_JUDGE_SYSTEM_PROMPT = """당신은 Text2Cypher 쿼리 생성 평가 전문가입니다.
사용자의 자연어 질문에 대해 생성된 Cypher 쿼리를 골든(reference) Cypher 쿼리와 비교하여 평가합니다.

<데이터베이스 스키마>
{schema}
</데이터베이스 스키마>

반드시 다음 세 가지 기준으로 평가를 수행하고, JSON 객체로 결과를 반환해야 합니다.
다른 어떤 설명이나 텍스트도 포함하지 말고, JSON 객체만 반환해야 합니다.

<평가 기준>
1. intent_score (1-5): 생성된 쿼리가 사용자의 의도를 얼마나 정확하게 반영하는지 평가합니다.
   - 5: 의도를 완벽하게 반영
   - 4: 약간의 차이는 있지만 대부분 정확함
   - 3: 부분적으로 의도를 반영
   - 2: 의도를 크게 오해함
   - 1: 완전히 잘못된 의도

2. completeness_score (1-5): 필요한 모든 대상, 조건, 범위, 집계 방법이 반영되었는지 평가합니다.
   - 5: 모든 요구사항이 완전히 반영됨
   - 4: 약간의 누락
   - 3: 일부 주요 요소 누락
   - 2: 주요 요소 대부분 누락
   - 1: 거의 반영되지 않음

3. correctness_score (1-5): 불필요한 조건이나 잘못 해석된 부분이 있는지 평가합니다.
   - 5: 불필요하거나 잘못된 부분 없음
   - 4: 매우 작은 불필요한 부분 있음
   - 3: 일부 불필요하거나 잘못된 조건 있음
   - 2: 상당한 잘못된 조건 있음
   - 1: 대부분 잘못됨
</평가 기준>

Return ONLY a JSON object in this exact format:
<출력 형식>
{{"intent_score": <int>, "completeness_score": <int>, "correctness_score": <int>, "reasoning": "<brief explanation in Korean>"}}
</출력 형식>
"""


LLM_JUDGE_USER_PROMPT = """<사용자 질문>
{question}
</사용자 질문>

<정답 Cypher (reference)>
{golden_cypher}
</정답 Cypher>

<생성된 Cypher (평가 대상)>
{generated_cypher}
</생성된 Cypher>

<정답 쿼리 실행 결과>
{golden_returns_results}
</정답 쿼리 실행 결과>

<생성된 쿼리 실행 결과>
{generated_returns_results}
</생성된 쿼리 실행 결과>

Evaluate the generated Cypher query. Return ONLY the JSON object."""


def llm_judge_evaluate(
    llm,
    schema: str,
    question: str,
    golden_cypher: str,
    generated_cypher: str,
    golden_returns_results: bool,
    generated_returns_results: bool,
) -> dict:
    """LLM-as-a-Judge로 생성된 Cypher를 평가합니다."""
    system_msg = LLM_JUDGE_SYSTEM_PROMPT.format(schema=schema)
    user_msg = LLM_JUDGE_USER_PROMPT.format(
        question=question,
        golden_cypher=golden_cypher,
        generated_cypher=generated_cypher,
        golden_returns_results="결과 있음" if golden_returns_results else "결과 없음",
        generated_returns_results="결과 있음" if generated_returns_results else "결과 없음",
    )

    response = llm.invoke(user_msg, system_instruction=system_msg)
    content = response.content.strip()

    # JSON 파싱
    # 코드블럭 제거
    content = re.sub(r"^```(?:json)?\s*", "", content)
    content = re.sub(r"\s*```$", "", content)

    try:
        result = json.loads(content)
    except json.JSONDecodeError:
        # JSON 부분만 추출 시도
        match = re.search(r'\{.*\}', content, re.DOTALL)
        if match:
            result = json.loads(match.group())
        else:
            result = {
                "intent_score": 0,
                "completeness_score": 0,
                "correctness_score": 0,
                "reasoning": f"LLM 응답 파싱 실패: {content[:200]}",
            }

    return result


# =============================================
# 평가 실행
# =============================================

def evaluate_single(
    retriever,
    llm,
    schema: str,
    golden: dict,
) -> EvalRecord:
    """단일 질문에 대해 전체 평가를 수행합니다."""
    record = EvalRecord(
        question=golden["question"],
        question_type=golden["type"],
        golden_cypher=golden["cypher"],
    )

    # 1. Cypher 생성 및 실행 (retriever.get_search_results 사용)
    generated_cypher, results, error = generate_and_execute_cypher(retriever, golden["question"])
    record.generated_cypher = generated_cypher
    record.execution_results = results

    if error:
        record.is_executable = False
        record.execution_error = error
        record.returns_results = False
    else:
        record.is_executable = True
        record.returns_results = len(results) > 0
        record.result_count = len(results)

    # 2. Execution 평가: 관계 방향 검사
    dir_ok, dir_detail = check_relationship_directions(record.generated_cypher)
    record.relationship_direction_correct = dir_ok
    record.direction_detail = dir_detail

    # Execution 점수: 실행 가능 + 결과 있음 + 방향 올바름 → 모두 충족 시 1.0
    exec_factors = [record.is_executable, record.returns_results, record.relationship_direction_correct]
    record.execution_score = sum(exec_factors) / len(exec_factors)

    # 4. LLM-as-a-Judge 평가
    golden_returns = golden.get("returns_results", "True") == "True"
    judge_result = llm_judge_evaluate(
        llm=llm,
        schema=schema,
        question=golden["question"],
        golden_cypher=golden["cypher"],
        generated_cypher=record.generated_cypher,
        golden_returns_results=golden_returns,
        generated_returns_results=record.returns_results,
    )
    record.intent_score = judge_result.get("intent_score", 0)
    record.completeness_score = judge_result.get("completeness_score", 0)
    record.correctness_score = judge_result.get("correctness_score", 0)
    record.llm_judge_reasoning = judge_result.get("reasoning", "")
    record.llm_judge_avg = (
        record.intent_score + record.completeness_score + record.correctness_score
    ) / 3.0

    return record


def print_summary(records: list[EvalRecord]):
    """평가 요약을 출력합니다."""
    total = len(records)
    if total == 0:
        print("평가 대상 없음")
        return

    executable = sum(1 for r in records if r.is_executable)
    has_results = sum(1 for r in records if r.returns_results)
    dir_correct = sum(1 for r in records if r.relationship_direction_correct)
    avg_exec_score = sum(r.execution_score for r in records) / total

    avg_intent = sum(r.intent_score for r in records) / total
    avg_completeness = sum(r.completeness_score for r in records) / total
    avg_correctness = sum(r.correctness_score for r in records) / total
    avg_llm_judge = sum(r.llm_judge_avg for r in records) / total

    # 유형별 통계
    type_stats: dict[str, list[EvalRecord]] = {}
    for r in records:
        type_stats.setdefault(r.question_type, []).append(r)

    print("\n" + "=" * 70)
    print("  TEXT2CYPHER 평가 결과 요약")
    print("=" * 70)
    print(f"\n총 평가 건수: {total}")

    print(f"\n--- Execution 평가 ---")
    print(f"  실행 가능:           {executable}/{total} ({executable/total*100:.1f}%)")
    print(f"  결과 반환:           {has_results}/{total} ({has_results/total*100:.1f}%)")
    print(f"  관계 방향 올바름:    {dir_correct}/{total} ({dir_correct/total*100:.1f}%)")
    print(f"  Execution 평균 점수: {avg_exec_score:.3f}")

    print(f"\n--- LLM-as-a-Judge 평가 (1-5) ---")
    print(f"  의도 반영 (Intent):      {avg_intent:.2f}")
    print(f"  완전성 (Completeness):   {avg_completeness:.2f}")
    print(f"  정확성 (Correctness):    {avg_correctness:.2f}")
    print(f"  LLM Judge 평균:          {avg_llm_judge:.2f}")

    print(f"\n--- 유형별 LLM Judge 평균 ---")
    for qtype, recs in sorted(type_stats.items()):
        t_avg = sum(r.llm_judge_avg for r in recs) / len(recs)
        t_exec = sum(1 for r in recs if r.is_executable)
        print(f"  [{qtype}] 건수={len(recs)}, 실행가능={t_exec}/{len(recs)}, LLM Judge 평균={t_avg:.2f}")

    print("\n" + "=" * 70)


def print_issues(records: list[EvalRecord]):
    """문제가 있는 평가 결과만 상세히 출력합니다."""
    # 문제가 있는 레코드 필터링
    issues = []
    for r in records:
        if not r.is_executable or not r.returns_results or not r.relationship_direction_correct or r.llm_judge_avg < 3.0:
            issues.append(r)

    if not issues:
        print("\n🎉 문제가 있는 항목이 없습니다!")
        return

    print("\n" + "=" * 90)
    print(f"  ⚠️  문제가 있는 항목 ({len(issues)}건)")
    print("=" * 90)

    for idx, r in enumerate(issues, 1):
        # 문제 유형 태그 생성
        tags = []
        if not r.is_executable:
            tags.append("❌ 실행 실패")
        if r.is_executable and not r.returns_results:
            tags.append("⚠️  빈 결과")
        if not r.relationship_direction_correct:
            tags.append("🔀 관계 방향 오류")
        if r.llm_judge_avg < 3.0:
            tags.append(f"📉 낮은 점수 ({r.llm_judge_avg:.1f}/5)")

        print(f"\n[{idx}] {' | '.join(tags)}")
        print(f"📝 질문: {r.question}")
        print(f"🏷️  유형: {r.question_type}")

        if not r.is_executable:
            print(f"❌ 에러: {r.execution_error[:150]}{'...' if len(r.execution_error) > 150 else ''}")

        if not r.relationship_direction_correct:
            print(f"🔀 방향 문제: {r.direction_detail}")

        if r.llm_judge_avg < 3.0:
            print(f"📊 LLM Judge 점수: Intent={r.intent_score}, Complete={r.completeness_score}, Correct={r.correctness_score}")
            print(f"💭 판단 근거: {r.llm_judge_reasoning[:200]}{'...' if len(r.llm_judge_reasoning) > 200 else ''}")

        # Golden vs Generated 비교 (문제가 있는 경우만)
        print(f"\n   Golden Cypher:")
        for line in r.golden_cypher.split('\n'):
            print(f"   │ {line}")

        print(f"\n   Generated Cypher:")
        if r.generated_cypher:
            for line in r.generated_cypher.split('\n'):
                print(f"   │ {line}")
        else:
            print(f"   │ (생성 실패)")

        # 실행 결과 출력
        print(f"\n   실행 결과 ({r.result_count}건):")
        if r.execution_results:
            max_display = 3  # 최대 3건만 표시
            for idx, result in enumerate(r.execution_results[:max_display], 1):
                # 결과를 읽기 쉽게 포맷팅
                result_str = ", ".join([f"{k}={v}" for k, v in result.items()])
                print(f"   │ [{idx}] {result_str}")
        elif r.is_executable:
            print(f"   │ (빈 결과)")
        else:
            print(f"   │ (실행 실패로 결과 없음)")

    print(f"\n총 {len(issues)}건의 문제 발견 (전체 {len(records)}건 중 {len(issues)/len(records)*100:.1f}%)")
    print("=" * 90)


def save_results(records: list[EvalRecord], output_path: str):
    """평가 결과를 CSV로 저장합니다."""
    if not records:
        return
    fieldnames = list(asdict(records[0]).keys())
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in records:
            row = asdict(r)
            # 멀티라인 문자열 처리
            for k, v in row.items():
                if isinstance(v, str):
                    row[k] = v.replace("\n", "\\n")
                elif isinstance(v, list):
                    # execution_results는 JSON으로 직렬화
                    row[k] = json.dumps(v, ensure_ascii=False)
            writer.writerow(row)
    print(f"\n결과 저장 완료: {output_path}")


# =============================================
# 메인 함수
# =============================================

def main():
    parser = argparse.ArgumentParser(description="Text2Cypher 평가")
    parser.add_argument("--limit", type=int, default=0, help="평가할 최대 건수 (0=전체)")
    parser.add_argument("--output", type=str, default="eval_results.csv", help="결과 CSV 파일 경로")
    parser.add_argument("--dataset", type=str, default="text2cypher_gpt4o.csv", help="골든 데이터셋 CSV 경로")
    parser.add_argument("--schema-csv", type=str, default="text2cypher_schemas.csv", help="스키마 CSV 경로")
    parser.add_argument(
        "--dataset-db",
        type=str,
        default=os.getenv("DATABASE_NAME", "recommendations"),
        help="골든 데이터셋 필터링용 database 이름 (기본값: .env의 DATABASE_NAME 또는 'recommendations')"
    )
    parser.add_argument("--neo4j-db", type=str, default="neo4j", help="Neo4j 실행 대상 데이터베이스명")
    args = parser.parse_args()

    # 데이터셋, 스키마 로드
    script_dir = os.path.dirname(os.path.abspath(__file__))
    dataset_path = os.path.join(script_dir, args.dataset)
    schema_csv_path = os.path.join(script_dir, args.schema_csv)

    print("[1/5] 골든 데이터셋 로드 중...")
    golden_data = load_golden_dataset(dataset_path, args.dataset_db)
    if args.limit > 0:
        golden_data = golden_data[:args.limit]
    print(f"  → {len(golden_data)}건 로드 완료 (database={args.dataset_db})")

    print("[2/5] 스키마 로드 중...")
    schema = load_schema(schema_csv_path, args.dataset_db)
    print(f"  → 스키마 로드 완료 ({len(schema)} chars)")

    # Neo4j 연결 + Retriever 생성
    print("[3/5] Neo4j 연결 및 Retriever 생성 중...")
    driver = create_driver()
    llm = create_llm()
    retriever = build_retriever(driver, llm, neo4j_schema=schema)
    print("  → Retriever 생성 완료")

    # 평가 실행
    print(f"[4/5] 평가 실행 중... ({len(golden_data)}건)")
    results: list[EvalRecord] = []
    for i, golden in enumerate(golden_data):
        print(f"  [{i+1}/{len(golden_data)}] {golden['question']}", end="", flush=True)
        start_t = time.time()
        record = evaluate_single(retriever, llm, schema, golden)
        elapsed = time.time() - start_t
        status = "✓" if record.is_executable else "✗"
        print(f" {status} ({elapsed:.1f}s, exec={record.execution_score:.2f}, judge={record.llm_judge_avg:.2f})")
        results.append(record)

    # 결과 출력 + 저장
    print("[5/5] 결과 정리 중...")
    print_summary(results)
    print_issues(results)

    output_path = os.path.join(script_dir, args.output)
    save_results(results, output_path)

    driver.close()


if __name__ == "__main__":
    main()
