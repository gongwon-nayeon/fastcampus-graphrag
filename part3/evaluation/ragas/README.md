# RAGAS

**Part 3. GraphRAG 핵심 패턴과 평가**

- Chapter 02. GraphRAG 평가하기
    - 📒 Clip 02. [프로젝트] RAGAS로 GraphRAG 평가하기

> RAGAS를 활용하여 GraphRAG의 검색과 답변 결과를 Faithfulness, Answer Relevancy, Context Recall, Context Precision, Answer Correctness 평가 기준에 맞춰 평가(evaluation)합니다.

- 충실성 (Faithfulness)
- 질문-답변 관련성 (Relevancy)
- 정보 재현율 (Recall)
- 검색 정확도 (Precision)
- 정답 일치도 (Correctness)

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
.venv\Scripts\python.exe -m ipykernel install --user --name=ragas --display-name="ragas"
```


### 2. 환경변수 설정

```bash
copy .env.example .env
```

