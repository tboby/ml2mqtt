FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:0.7.17 /uv /uvx /bin/
COPY ml2mqtt/pyproject.toml /app/ml2mqtt/pyproject.toml
COPY ml2mqtt/uv.lock /app/ml2mqtt/uv.lock
RUN uv sync --project /app/ml2mqtt --no-dev --frozen

COPY . /app

WORKDIR /app/ml2mqtt

VOLUME ["/data"]
EXPOSE 5000

CMD ["/app/ml2mqtt/.venv/bin/waitress-serve", "--host=0.0.0.0", "--port=5000", "app:app"]
