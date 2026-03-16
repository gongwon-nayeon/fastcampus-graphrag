# CSV 데이터를 지식그래프로 표현하기

**Part 2. 지식그래프 구축 실전**

- Chapter 02. 지식그래프 구축하기

    - 📒 Clip 02. [프로젝트] CSV/Excel 데이터를 지식그래프로 변환하기

> csv 파일을 지식그래프로 표현하는 실습입니다.


## 타이타닉 데이터셋 설명

이 실습에서는 [타이타닉 호 승객 데이터](https://www.kaggle.com/c/titanic/data)를 사용하여 지식그래프를 구축합니다.

### 데이터 변수 설명

| 변수명 | 설명 | 값 |
|--------|------|-----|
| **survival** | 생존 여부 | 0 = 사망, 1 = 생존 |
| **pclass** | 객실 등급 | 1 = 1등급, 2 = 2등급, 3 = 3등급 |
| **sex** | 성별 | male, female |
| **age** | 나이 (년) | 소수점: 1세 미만, xx.5: 추정 나이 |
| **sibsp** | 함께 탑승한 형제자매/배우자 수 | 정수 |
| **parch** | 함께 탑승한 부모/자녀 수 | 정수 |
| **ticket** | 티켓 번호 | 문자열 |
| **fare** | 티켓 요금 | 실수 |
| **cabin** | 객실 번호 | 문자열 |
| **embarked** | 승선 항구 | C = Cherbourg, Q = Queenstown, S = Southampton |

### 주요 변수 상세 설명

**pclass (객실 등급)**
- 사회경제적 지위(SES)의 대리 지표로 사용됩니다
  - 1등급 = 상류층 (Upper)
  - 2등급 = 중산층 (Middle)
  - 3등급 = 하류층 (Lower)

**age (나이)**
- 1세 미만의 경우 소수점으로 표현됩니다
- 추정된 나이는 xx.5 형태로 표현됩니다

**sibsp (형제자매/배우자)**
  - 형제자매(Sibling) = 남자형제, 여자형제, 의붓형제, 의붓자매
  - 배우자(Spouse) = 남편, 아내 (약혼자나 정부는 제외됨)

**parch (부모/자녀)**
  - 부모(Parent) = 어머니, 아버지
  - 자녀(Child) = 딸, 아들, 의붓딸, 의붓아들
  - 일부 어린이는 유모와만 여행했기 때문에 parch=0으로 표시됩니다


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

**Jupyter Notebook 사용시 커널 등록:**

```bash
.venv\Scripts\python.exe -m ipykernel install --user --name=csv2kg --display-name="csv2kg"
```

커널 등록 후 VS Code를 리로드하면 노트북에서 "csv2kg" 커널을 선택할 수 있습니다.

### 2. Neo4j 데이터베이스 및 LLM API 준비

- 데이터베이스 생성 후 URI, username, password 확인(credentials.txt)


### 3. 환경변수 설정

`.env.example` 파일을 `.env`로 복사하고 본인의 정보로 수정:

```bash
cp .env.example .env
```


### 4. CSV 파일 생성

`titanic.ipynb` 노트북을 열어서 순서대로 실행:

1. 타이타닉 데이터 로드
2. 데이터 분석 (티켓/그룹 분석)
3. 노드 CSV 생성 (Passenger, PClass, Cabin, Port)
4. 관계 CSV 생성 (TRAVELED_IN, STAYED_IN, EMBARKED_AT, TRAVELED_WITH)
5. `output/` 폴더에 CSV 파일 저장

생성되는 파일:
- `nodes_passenger.csv` - 승객 정보
- `nodes_pclass.csv` - 객실 등급 (1st/2nd/3rd)
- `nodes_cabin.csv` - 객실 번호
- `nodes_port.csv` - 승선 항구
- `rels_passenger_pclass.csv` - 승객-등급 관계
- `rels_passenger_cabin.csv` - 승객-객실 관계
- `rels_passenger_port.csv` - 승객-항구 관계
- `rels_traveled_with.csv` - 같은 티켓 그룹 관계


### 5. Neo4j에 데이터 적재

생성된 CSV 파일을 Neo4j 데이터베이스에 적재:

```bash
python csv2kg.py
```

---

## 대안: LOAD CSV 쿼리 방식

참고자료: [https://neo4j.com/docs/getting-started/data-import/csv-import/](https://neo4j.com/docs/getting-started/data-import/csv-import/)

Neo4j에서는 `LOAD CSV` 문법을 사용하여 CSV 파일을 직접 읽어올 수도 있습니다.

#### Neo4j Desktop 설정

1. Neo4j Desktop에서 개별 인스턴스 우측 상단 ... 클릭
2. Open > Instance folder 열기
3. "import" 폴더에 csv 파일 넣기

#### LOAD CSV 쿼리 예시

```cypher
// 1. 기존 데이터 삭제
MATCH (n) DETACH DELETE n;

// 2. 제약조건 생성
CREATE CONSTRAINT IF NOT EXISTS FOR (p:Passenger) REQUIRE p.PassengerId IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (c:PClass) REQUIRE c.Pclass IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (c:Cabin) REQUIRE c.Cabin IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (p:Port) REQUIRE p.Port IS UNIQUE;

// 3. 노드 생성 - Passenger
LOAD CSV WITH HEADERS FROM 'file:///nodes_passenger.csv' AS row
CREATE (p:Passenger {
    PassengerId: row.PassengerId,
    Name: row.Name,
    Sex: row.Sex,
    Age: toFloat(row.Age),
    Survived: toInteger(row.Survived),
    SibSp: toInteger(row.SibSp),
    Parch: toInteger(row.Parch),
    Fare: toFloat(row.Fare),
    Ticket: row.Ticket
});

// 4. 노드 생성 - PClass
LOAD CSV WITH HEADERS FROM 'file:///nodes_pclass.csv' AS row
CREATE (c:PClass {
    Pclass: toInteger(row.Pclass),
    ClassName: row.ClassName,
    SES: row.SES
});

// 5. 노드 생성 - Cabin
LOAD CSV WITH HEADERS FROM 'file:///nodes_cabin.csv' AS row
CREATE (c:Cabin {
    Cabin: row.Cabin
});

// 6. 노드 생성 - Port
LOAD CSV WITH HEADERS FROM 'file:///nodes_port.csv' AS row
CREATE (p:Port {
    Port: row.Port,
    PortName: row.PortName
});

// 7. 관계 생성 - TRAVELED_IN
LOAD CSV WITH HEADERS FROM 'file:///rels_passenger_pclass.csv' AS row
MATCH (p:Passenger {PassengerId: row.PassengerId})
MATCH (c:PClass {Pclass: toInteger(row.Pclass)})
CREATE (p)-[:TRAVELED_IN]->(c);

// 8. 관계 생성 - STAYED_IN
LOAD CSV WITH HEADERS FROM 'file:///rels_passenger_cabin.csv' AS row
MATCH (p:Passenger {PassengerId: row.PassengerId})
MATCH (c:Cabin {Cabin: row.Cabin})
CREATE (p)-[:STAYED_IN]->(c);

// 9. 관계 생성 - EMBARKED_AT
LOAD CSV WITH HEADERS FROM 'file:///rels_passenger_port.csv' AS row
MATCH (p:Passenger {PassengerId: row.PassengerId})
MATCH (port:Port {Port: row.Port})
CREATE (p)-[:EMBARKED_AT]->(port);

// 10. 관계 생성 - TRAVELED_WITH
LOAD CSV WITH HEADERS FROM 'file:///rels_traveled_with.csv' AS row
MATCH (p1:Passenger {PassengerId: row.PassengerId1})
MATCH (p2:Passenger {PassengerId: row.PassengerId2})
CREATE (p1)-[:TRAVELED_WITH {Ticket: row.Ticket}]->(p2);
```

**테스트 쿼리:**

```cypher
// 전체 그래프 구조 확인
MATCH p=()-[]->()
RETURN p

// 1등급 승객 조회
MATCH (p:Passenger)-[t:TRAVELED_IN]->(c:PClass)
WHERE c.Pclass = 1
RETURN p,t,c
LIMIT 20

// 같은 티켓으로 여행한 그룹
MATCH (p1:Passenger)-[t:TRAVELED_WITH]-(p2:Passenger)
RETURN p1,t,p2

// S 항구에서 승선한 승객 조회
MATCH path=(p:Passenger)-[:EMBARKED_AT]->(port:Port)
WHERE port.Port = 'S'
RETURN path
LIMIT 20
```
