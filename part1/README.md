# Neo4j Python 드라이버 사용하기

**Part 1. GraphDB와 Neo4j 핵심 기초**

- Chapter 02. neo4j 사용하기

    - 📒 Clip 05. [실습] 파이썬에서 neo4j 연결하기

> Neo4j Python 드라이버로 그래프 데이터베이스에 연결하고, 데이터를 생성/조회하는 실습입니다.

## 실습 순서

### 1. 패키지 설치

Python 3.13

```bash
# uv 설치
# Windows (PowerShell)
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"

# macOS / Linux
curl -LsSf https://astral.sh/uv/install.sh | sh
```

```bash
# 방법 1: uv sync 사용 (권장)
uv sync
.venv\Scripts\activate
```

또는

```bash
# 방법 2: requirements.txt 사용
uv venv
.venv\Scripts\activate
uv pip install -r requirements.txt
```

### 2. Neo4j 데이터베이스 준비

- [Neo4j Aura](https://neo4j.com/product/auradb/) 무료 티어 인스턴스 생성
- 데이터베이스 생성 후 URI, username, password 확인(credentials.txt)

### 3. 환경변수 설정

`.env.example` 파일을 `.env`로 복사하고 본인의 정보로 수정:

```bash
cp .env.example .env
```

### 4. 실행

```bash
python ch2_python_neo4j.py
```

## execute_query 의 routing_ 파라미터 이해하기

`routing_` 파라미터는 Neo4j 클러스터 환경에서 쿼리를 어느 서버로 보낼지 결정하는 역할

| 값 (`RoutingControl`) | 설명 | 주요 사용 예 |
|----------------------|------|--------------|
| **`WRITE`** (기본값) | 요청을 **리더(Leader) 서버**로 라우팅 | 데이터 생성·수정·삭제 (`CREATE`, `MERGE`, `SET`) |
| **`READ`** | 요청을 **팔로워(Follower)** 또는 **읽기 전용 복제본(Read Replica)** 으로 라우팅 | 데이터 조회 (`MATCH`, `RETURN`) |


* **부하 분산**: 조회 업무를 팔로워 서버로 분산시켜 리더 서버의 병목 현상을 방지
* **데이터 무결성**: 모든 쓰기 작업은 리더 노드에서 처리되어야 클러스터 전체에 안정적으로 동기화
* **성능 최적화**: 대규모 트래픽 상황에서 읽기 전용 인스턴스를 활용해 응답 속도 향상
