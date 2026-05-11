# 도구 기반 AI Agent

**Part 4. GraphRAG 에이전트와 챗봇 실전**

- Chapter 01. GraphRAG Agent 구축하기
    - 📒 Clip 02. [실습] 도구 기반 Agent 구현하기

> 랭체인의 표준 에이전트 형식을 `create_agent`를 통해 생성하고, 도구 기반 에이전트를 실습합니다.


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
.venv\Scripts\python.exe -m ipykernel install --user --name=createagent --display-name="createagent"
```


### 2. 환경변수 설정

```bash
cp .env.example .env
```

```bash
OPENAI_API_KEY=sk-your_openai_api_key_here
```
