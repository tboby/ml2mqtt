FROM mcr.microsoft.com/devcontainers/python:1-3.11-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/home/vscode/.local/share/ml2mqtt-venv

COPY --from=ghcr.io/astral-sh/uv:0.7.17 /uv /uvx /usr/local/bin/

RUN mkdir -p /tmp/ml2mqtt /home/vscode/.local/share && \
    chown -R vscode:vscode /tmp/ml2mqtt /home/vscode/.local/share

COPY --chown=vscode:vscode ml2mqtt/pyproject.toml /tmp/ml2mqtt/pyproject.toml
COPY --chown=vscode:vscode ml2mqtt/uv.lock /tmp/ml2mqtt/uv.lock

USER vscode
RUN cd /tmp/ml2mqtt && uv sync --frozen
RUN rm -rf /tmp/ml2mqtt
