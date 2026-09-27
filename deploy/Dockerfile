# tpa-data-plane API 镜像（构建上下文 = 本仓库根）
FROM python:3.12-slim
WORKDIR /app
ENV DP_RUN_DIR=/data/runs PYTHONUNBUFFERED=1
COPY deploy/api-requirements.txt deploy/api-requirements.txt
RUN pip install --no-cache-dir -r deploy/api-requirements.txt
COPY app/ app/
COPY serve/ serve/
COPY tools/ tools/
RUN useradd -m dpuser && useradd -m ffuser 2>/dev/null; true
EXPOSE 8100
CMD ["uvicorn", "serve.api:app", "--host", "0.0.0.0", "--port", "8100"]
