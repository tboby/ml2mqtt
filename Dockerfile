FROM ubuntu:24.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        libgomp1 \
        libstdc++6 \
        python3 \
        python3-venv \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:0.7.17 /uv /uvx /usr/local/bin/
COPY ml2mqtt/pyproject.toml /app/ml2mqtt/pyproject.toml
COPY ml2mqtt/uv.lock /app/ml2mqtt/uv.lock
RUN uv sync --project /app/ml2mqtt --python /usr/bin/python3 --no-dev --frozen

COPY . /app

FROM ubuntu/python:3.12-24.04_stable

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app/ml2mqtt

COPY --from=builder /usr/lib/*-linux-gnu/libgomp.so.1* /usr/lib/
COPY --from=builder /usr/lib/*-linux-gnu/libstdc++.so.6* /usr/lib/
COPY --from=builder /app /app

VOLUME ["/data"]
EXPOSE 5000

ENTRYPOINT ["/app/ml2mqtt/.venv/bin/waitress-serve"]
CMD ["--host=0.0.0.0", "--port=5000", "app:app"]
