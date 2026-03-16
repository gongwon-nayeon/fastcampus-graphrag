# 의료 지식 데이터를 지식그래프로 변환하기

**Part 2. 지식그래프 구축 실전**

- Chapter 02. 지식그래프 구축하기
    - 📒 Clip 08. [프로젝트] 의료 지식 데이터를 지식그래프로 변환하기

> 의료 지식 데이터의 구조를 분석하고 LLM을 사용하여 지식그래프로 표현하는 실습입니다.

---

## 데이터 준비

### 데이터 다운로드

AI Hub에서 "필수의료 의학지식 데이터" 다운로드:
- URL: https://aihub.or.kr/aihubdata/data/view.do?currMenu=115&topMenu=100&dataSetSn=71875
- 다운로드 후 `09.필수의료 의학지식 데이터` 폴더에 압축 파일 저장

### 데이터 압축 해제

다운로드한 데이터는 다음 경로에 있습니다:
```
09.필수의료 의학지식 데이터/3.개방데이터/1.데이터/Training/02.라벨링데이터/
├── TL_내과.zip
├── TL_산부인과.zip
├── TL_소아청소년과.zip
└── TL_응급의학과.zip
```

**Windows PowerShell에서 압축 해제:**

```powershell
# medical2kg 폴더로 이동
cd "part2/medical2kg"

# 압축 파일 경로 설정
$basePath = "09.필수의료 의학지식 데이터\3.개방데이터\1.데이터\Training\02.라벨링데이터"
$outputPath = "data"

# 출력 폴더 생성
New-Item -ItemType Directory -Force -Path $outputPath

# 각 zip 파일 압축 해제
Expand-Archive -Path "$basePath\TL_내과.zip" -DestinationPath "$outputPath\내과" -Force
Expand-Archive -Path "$basePath\TL_산부인과.zip" -DestinationPath "$outputPath\산부인과" -Force
Expand-Archive -Path "$basePath\TL_소아청소년과.zip" -DestinationPath "$outputPath\소아청소년과" -Force
Expand-Archive -Path "$basePath\TL_응급의학과.zip" -DestinationPath "$outputPath\응급의학과" -Force
```

**macOS / Linux 터미널에서 압축 해제:**

```bash
# medical2kg 폴더로 이동
cd "part2/medical2kg"

# 압축 파일 경로 설정
BASE_PATH="09.필수의료 의학지식 데이터/3.개방데이터/1.데이터/Training/02.라벨링데이터"
OUTPUT_PATH="data"

# 출력 폴더 생성
mkdir -p "$OUTPUT_PATH"

# 각 zip 파일 압축 해제
unzip -o "$BASE_PATH/TL_내과.zip" -d "$OUTPUT_PATH/내과"
unzip -o "$BASE_PATH/TL_산부인과.zip" -d "$OUTPUT_PATH/산부인과"
unzip -o "$BASE_PATH/TL_소아청소년과.zip" -d "$OUTPUT_PATH/소아청소년과"
unzip -o "$BASE_PATH/TL_응급의학과.zip" -d "$OUTPUT_PATH/응급의학과"
```

**또는 Python으로 압축 해제:**

```python
import zipfile
from pathlib import Path

base_path = Path("09.필수의료 의학지식 데이터/3.개방데이터/1.데이터/Training/02.라벨링데이터")
output_path = Path("data")
output_path.mkdir(exist_ok=True)

departments = ["내과", "산부인과", "소아청소년과", "응급의학과"]

for dept in departments:
    zip_file = base_path / f"TL_{dept}.zip"
    if zip_file.exists():
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(output_path / dept)
        print(f"✓ {dept} 압축 해제 완료")
```

---

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
.venv\Scripts\python.exe -m ipykernel install --user --name=medical2kg --display-name="medical2kg"
```


### 2. 환경변수 설정

```bash
cp .env.example .env
```

```bash
# Neo4j 연결 정보
NEO4J_URI=bolt://localhost:7687
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=your_password_here

# OpenAI API 키 (Stage 2에서 사용)
OPENAI_API_KEY=sk-your_openai_api_key_here
```

---

## 실행 방법

### EDA 노트북

```bash
jupyter notebook medical_dataset.ipynb
```

### 지식그래프 구축 파이프라인

```bash
python medical2kg.py
```

**출력:**
- `output/domain_schemas.json`: 진료과별 스키마 정의

### Step 1: QA 그래프 구축 (QA Graph Construction)

**생성되는 관계:**
- `(Question)-[:HAS_ANSWER]->(Answer)`
- `(Question)-[:BELONGS_TO]->(Department)`
---

### Step 2: 엔티티 및 관계 통합 추출 (Entity & Relationship Extraction)

**생성되는 관계:**
- `(Question)-[:MENTIONS]->(Entity)`: 질문에서 엔티티 언급
- `(Symptom)-[:INDICATES]->(Disease)`: 증상 → 질병
...

**출력:**
- `output/extracted_graph.json`: 모든 QA의 엔티티와 관계
