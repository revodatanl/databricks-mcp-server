FROM ghcr.io/astral-sh/uv:bookworm-slim
WORKDIR /app

RUN apt-get update && apt-get install -y ca-certificates

ADD . /app
RUN uv sync

CMD ["uv", "run", "databricks-mcp"]