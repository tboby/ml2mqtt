FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:0.7.17 /uv /uvx /bin/
COPY pyproject.toml /app/pyproject.toml
COPY uv.lock /app/uv.lock
RUN uv sync --project /app --no-dev --frozen

COPY . .

VOLUME ["/data"]
EXPOSE 5000

CMD ["/app/.venv/bin/waitress-serve", "--host=0.0.0.0", "--port=5000", "app:app"]
