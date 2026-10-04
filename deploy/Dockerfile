# 【DEPRECATED · DP-02 退役标注（2026-10-04）】裁决 15 归位：问答面已退役，功能将由
# 主平台问数链重建承接；本镜像与 serve 入口保留为参考标本，勿再演进
# （serve/api.py 全部问答路由恒 410 Gone）。线上 8.163.33.135/dp/ 停服＝Owner 手工项。
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
