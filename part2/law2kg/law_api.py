import os
import json
import requests
from typing import Dict, Any, Optional
from dotenv import load_dotenv


# ============================================
# API 엔드포인트
# ============================================

BASE_URL_SEARCH = "http://www.law.go.kr/DRF/lawSearch.do" # 목록 조회
BASE_URL_SERVICE = "http://www.law.go.kr/DRF/lawService.do" # 본문 조회
# target=eflaw : 현행법령
# target=expc : 해석례


# ============================================
# API 호출 함수
# ============================================

def fetch_law_list(
    api_key: str,
    target: str = "eflaw",
    display: int = 100,
    page: int = 1,
    query: Optional[str] = None
) -> Dict[str, Any]:
    """법령 목록 조회

    Args:
        api_key: 법제처 API 인증키
        target: 조회 대상 ('eflaw': 현행법령, 'expc': 법령해석례)
        display: 조회 개수 (기본: 100)
        page: 페이지 번호 (기본: 1)
        query: 검색 키워드 (선택)

    Returns:
        API 응답 JSON (딕셔너리)
    """
    params = {
        'OC': api_key,
        'target': target,
        'display': display,
        'page': page,
        'type': 'JSON'
    }

    if query:
        params['query'] = query

    try:
        response = requests.get(BASE_URL_SEARCH, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"ERROR: 법령 목록 조회 실패 - {str(e)}")
        return {}
    except json.JSONDecodeError as e:
        print(f"ERROR: JSON 파싱 실패 - {str(e)}")
        return {}


def fetch_law_detail(
    api_key: str,
    law_id: str,
    target: str = "eflaw"
) -> Dict[str, Any]:
    """법령 상세 조회

    Args:
        api_key: 법제처 API 인증키
        law_id: 법령일련번호 (MST) 또는 해석례일련번호 (ID)
        target: 조회 대상 ('eflaw': 현행법령, 'expc': 법령해석례)

    Returns:
        API 응답 JSON (딕셔너리)
    """
    # eflaw는 MST 파라미터, expc는 ID 파라미터 사용
    id_param = 'ID' if target == 'expc' else 'MST'

    params = {
        'OC': api_key,
        'target': target,
        id_param: law_id,
        'type': 'JSON'
    }

    try:
        response = requests.get(BASE_URL_SERVICE, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"ERROR: 법령 상세 조회 실패 (ID: {law_id}) - {str(e)}")
        return {}
    except json.JSONDecodeError as e:
        print(f"ERROR: JSON 파싱 실패 - {str(e)}")
        return {}


# ============================================
# API 테스트 및 샘플 데이터 생성
# ============================================

def test_api_connection(save_samples: bool = True) -> bool:
    print("=" * 80)
    print("법제처 Open API 연결 테스트")
    print("=" * 80)

    load_dotenv()
    api_key = os.getenv('LAW_API_KEY')

    if not api_key:
        print("\nERROR: LAW_API_KEY가 설정되지 않았습니다.")
        print("\n.env 파일에 다음과 같이 설정하세요:")
        print("  LAW_API_KEY=이메일ID")
        print("\n예: fastcampus@gmail.com → LAW_API_KEY=fastcampus")
        return False

    print(f"\n✓ OC 인증키: {api_key}")

    # 1. 현행법령 테스트
    print("\n" + "-" * 80)
    print("[1] 현행법령(eflaw) 테스트")
    print("-" * 80)

    eflaw_list = fetch_law_list(api_key, target='eflaw', display=3)

    print(eflaw_list)

    if eflaw_list:
        law_data = eflaw_list.get('LawSearch', {})
        laws = law_data.get('law', [])
        total = law_data.get('totalCnt', 0)

        print(f"✓ 성공: 총 {total}건 중 {len(laws)}건 조회")

        if save_samples:
            with open('eflaw_list_sample.json', 'w', encoding='utf-8') as f:
                json.dump(eflaw_list, f, ensure_ascii=False, indent=2)
            print(f"  → 저장: eflaw_list_sample.json")

        # 상세 조회 (첫 번째 법령)
        if laws:
            law_id = laws[0].get('법령일련번호')
            law_name = laws[0].get('법령명한글', 'N/A')[:30]

            print(f"\n  첫 번째 법령: {law_name}... (ID: {law_id})")

            eflaw_detail = fetch_law_detail(api_key, law_id, target='eflaw')

            if eflaw_detail and save_samples:
                with open('eflaw_detail_sample.json', 'w', encoding='utf-8') as f:
                    json.dump(eflaw_detail, f, ensure_ascii=False, indent=2)
                print(f"  → 상세 저장: eflaw_detail_sample.json")
    else:
        print("실패: 현행법령 조회 불가")
        return False

    # 2. 법령해석례 테스트
    print("\n" + "-" * 80)
    print("[2] 법령해석례(expc) 테스트")
    print("-" * 80)

    expc_list = fetch_law_list(api_key, target='expc', display=3)

    if expc_list:
        expc_data = expc_list.get('Expc', {})
        cases = expc_data.get('expc', [])
        total = expc_data.get('totalCnt', 0)

        print(f"✓ 성공: 총 {total}건 중 {len(cases)}건 조회")

        if save_samples:
            with open('expc_list_sample.json', 'w', encoding='utf-8') as f:
                json.dump(expc_list, f, ensure_ascii=False, indent=2)
            print(f"  → 저장: expc_list_sample.json")

        # 상세 조회 (첫 번째 해석례)
        if cases:
            case_id = cases[0].get('법령해석례일련번호')
            case_name = cases[0].get('안건명', 'N/A')[:30]

            print(f"\n  첫 번째 해석례: {case_name}... (ID: {case_id})")

            expc_detail = fetch_law_detail(api_key, case_id, target='expc')

            if expc_detail and save_samples:
                with open('expc_detail_sample.json', 'w', encoding='utf-8') as f:
                    json.dump(expc_detail, f, ensure_ascii=False, indent=2)
                print(f"  → 상세 저장: expc_detail_sample.json")
    else:
        print("실패: 법령해석례 조회 불가")
        return False

    # 결과 요약
    print("\n" + "=" * 80)
    print("테스트 완료!")
    print("=" * 80)

    if save_samples:
        print("\n생성된 샘플 파일:")
        print("  • eflaw_list_sample.json    - 현행법령 목록")
        print("  • eflaw_detail_sample.json  - 현행법령 상세")
        print("  • expc_list_sample.json     - 법령해석례 목록")
        print("  • expc_detail_sample.json   - 법령해석례 상세")

    print("=" * 80)
    return True

def main():
    test_api_connection(save_samples=True)


if __name__ == "__main__":
    main()
