FROM python:3.12-slim AS builder

RUN pip install --no-cache-dir uv

WORKDIR /app
COPY pyproject.toml ./
COPY etl ./etl
COPY sql ./sql

RUN uv pip install --system --no-cache .

FROM python:3.12-slim

RUN groupadd --system app && useradd --system --gid app --home-dir /app app

WORKDIR /app
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY etl ./etl
COPY sql ./sql

USER app
EXPOSE 8080

CMD ["python", "-m", "etl.main"]
