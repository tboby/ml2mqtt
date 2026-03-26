# AGENTS.md

This file is for coding agents working in `D:\Local\Git\Third Party\ml2mqtt-hass`.
It captures the commands, conventions, and repo-specific gotchas discovered from the current codebase.

## Project Layout

- Repo root contains Home Assistant add-on packaging plus a standalone Docker workflow.
- Main Python app lives in `ml2mqtt/`.
- Flask entrypoint is `ml2mqtt/app.py`.
- Core runtime modules include `Config.py`, `MqttClient.py`, `ModelManager.py`, `ModelService.py`, and `ModelStore.py`.
- Route handlers are under `ml2mqtt/routes/`.
- ML model code lives in `ml2mqtt/classifiers/`.
- Preprocessors and postprocessors live in `ml2mqtt/preprocessors/` and `ml2mqtt/postprocessors/`.
- Templates and static assets live in `ml2mqtt/templates/` and `ml2mqtt/static/`.
- Root `Dockerfile` builds the production/standalone container.
- Root `docker-compose.yml` is the easiest full-stack local run path.

## Agent Rules Sources

- No `.cursor/rules/` directory was found.
- No `.cursorrules` file was found.
- No `.github/copilot-instructions.md` file was found.
- If any of those files are added later, update this document and follow them as higher-priority repo guidance.

## Environment And Tooling

- Python requirement is `>=3.11` from `ml2mqtt/pyproject.toml`.
- Dependencies are managed with `uv`.
- The project is configured as `tool.uv.package = false`, so treat it as an app repo, not a distributable package.
- There is no configured formatter or linter in `pyproject.toml` today.
- A `mypy` override section exists, but `mypy` is not installed by default in the current env.
- The repo has a checked-in `.venv/` under `ml2mqtt/`; avoid relying on that layout in new docs or scripts.

## Setup Commands

Run these from the repo root unless noted otherwise.

```bash
cd ml2mqtt
uv sync
```

- Install/sync dependencies: `cd ml2mqtt && uv sync`
- Run the app directly: `cd ml2mqtt && uv run python app.py`
- Run the app with explicit config via env: `cd ml2mqtt && ML2MQTT_CONFIG_FILE=settings.json uv run python app.py`
- Start the local Home Assistant devcontainer stack: open `.devcontainer/devcontainer.json` in a devcontainer-capable editor
- The devcontainer auto-starts ML2MQTT via `.devcontainer/ensure-ml2mqtt-running.sh`; for manual startup use `bash .devcontainer/start-ml2mqtt.sh`
- Home Assistant in the devcontainer workflow is exposed at `http://localhost:18123`; use `http://workspace:5000` inside Home Assistant for the ML2MQTT app URL
- The default devcontainer ML2MQTT startup is stable single-process mode; use `ML2MQTT_DEBUG=true bash .devcontainer/start-ml2mqtt.sh` only when you explicitly want Flask debug reloads
- The devcontainer workspace image preinstalls `uv` and the locked Python dependencies during image build; rebuild the devcontainer after changing `ml2mqtt/pyproject.toml` or `ml2mqtt/uv.lock`
- Production-style local serve inside container: `docker compose up --build`
- Validate compose config: `docker compose config`
- Build the container manually: `docker build -t ml2mqtt .`

## Config Expectations

- `app.py` fails fast if no config source is present.
- Valid config sources are:
- `ML2MQTT_CONFIG_FILE`
- `MQTT_SERVER` plus optional related env vars
- `/data/options.json` for Home Assistant
- `ml2mqtt/settings.json`
- For local dev, copy `ml2mqtt/settings.example.json` to `ml2mqtt/settings.json` or set MQTT env vars.

## Build / Lint / Test Commands

Use these exact commands as the first choices.

- Dependency sync: `cd ml2mqtt && uv sync`
- Devcontainer compose validation: `docker compose -f .devcontainer/docker-compose.yml config`
- App smoke run: `cd ml2mqtt && uv run python app.py`
- Docker build: `docker build -t ml2mqtt .`
- Docker compose run: `docker compose up --build`
- Compose validation: `docker compose config`
- Type-check script used by repo: `cd ml2mqtt && uv run mypy app.py --check-untyped-defs`

## Test Commands

There is no `pytest` suite configured.
Current tests use `unittest` and legacy `*Test.py` files.

- Discover all tests: `cd ml2mqtt && uv run python -m unittest discover -p "*Test.py"`
- Run one module from app root: `cd ml2mqtt && uv run python -m unittest ModelStoreTest`
- Run classifier test with import workaround: `cd ml2mqtt/classifiers && PYTHONPATH=".." uv run python -m unittest RandomForestTest`
- Run a single test method: `cd ml2mqtt/classifiers && PYTHONPATH=".." uv run python -m unittest RandomForestTest.TestRandomForest.test_simple_prediction`

## Current Test Reality

- `uv run python -m unittest discover -p "*Test.py"` currently fails.
- `ModelStoreTest.py` imports `SkillStore`, which does not exist in the repo.
- `RandomForestTest.py` needs `PYTHONPATH=".."` because imports are not package-relative.
- `RandomForestTest.TestRandomForest.test_simple_prediction` still fails after that because the test passes plain dicts where `RandomForest.populateDataframe()` expects `ModelObservation`-like objects.
- Treat existing tests as stale unless you update them as part of your change.
- If you add new tests, prefer making them runnable from `cd ml2mqtt && uv run python -m unittest ...` without `PYTHONPATH` hacks.

## Import Conventions

- Preserve the repo's current import style unless you are doing a broad cleanup.
- Top-level modules inside `ml2mqtt/` usually import siblings without package prefixes, e.g. `from ModelStore import ModelStore`.
- Subpackages often use relative imports for local base classes, e.g. `from .base import BasePreprocessor`.
- Standard-library imports usually appear before third-party imports, but ordering is not strictly enforced.
- Avoid introducing mixed import patterns within the same file.
- If you refactor imports, do it consistently across the affected module group.

## Formatting Conventions

- Use 4-space indentation.
- Keep code ASCII unless the file already needs Unicode.
- Follow existing spacing and quote style in the touched file instead of reformatting unrelated lines.
- The repo currently mixes compact legacy formatting and newer typed code; preserve local style when editing.
- Do not introduce formatter-only churn.
- Keep Jinja templates and CSS aligned with the existing naming and structure.

## Naming Conventions

- Module filenames at the app root often use PascalCase, e.g. `ModelStore.py`, `ModelService.py`, `Config.py`.
- Subpackage modules are usually snake_case, e.g. `only_diff.py`, `type_caster.py`, `model_routes.py`.
- Classes use PascalCase.
- Functions and methods usually use camelCase in legacy modules, e.g. `getModelSettings`, `predictLabel`, `setMqttTopic`.
- Private/internal attributes usually use a leading underscore, e.g. `_modelstore`, `_pipeline`, `_modelsDir`.
- Constants are uppercase with underscores, e.g. `DISABLED_LABEL`.
- When adding code to a file, match that file's naming style rather than forcing PEP 8 renames.

## Types And Data Modeling

- Prefer adding type hints in new or significantly edited Python code.
- The repo already uses `TypedDict`, dataclasses, and explicit return annotations in many newer paths.
- Use `Dict[str, Any]`, `List[...]`, and `Optional[...]` where the surrounding file already uses typing-module generics.
- Reuse existing data structures such as `ModelObservation`, `EntityKey`, `ProcessorEntry`, `RandomForestParams`, and `KNNParams` instead of inventing parallel shapes.
- Be careful with the distinction between raw dict payloads and `ModelObservation` objects.
- Preserve SQLite-backed persistence contracts in `ModelStore.py` when changing stored values.

## Error Handling And Logging

- Prefer fail-fast config validation, as seen in `Config.py`.
- For request handlers, return JSON errors with meaningful HTTP status codes rather than swallowing failures.
- For background/runtime paths, log warnings or exceptions with context.
- Existing code often uses `logger.warning(...)`, `logger.error(...)`, and `logger.exception(...)`; follow that pattern.
- Avoid broad silent `except` blocks.
- If catching a broad exception, either return a clear error response or log enough context to debug the issue.
- Do not remove defensive checks around MQTT payload decoding, JSON parsing, path validation, or missing request data.

## Flask And Routing Conventions

- Routes are registered through initializer functions such as `init_model_routes()` and `init_log_routes()`.
- Keep route additions inside the relevant blueprint module.
- Match existing response style: HTML routes return `render_template(...)` or redirects; API-style routes return `jsonify(...)` plus status codes.
- Preserve section-based edit views in `routes/model_routes.py` rather than splitting behavior arbitrarily.
- Validate request payloads before mutating model state.

## Data, Files, And Persistence

- Model databases are stored under the configured data path, typically `data/models/` locally or `/data/models/` in containers.
- `settings.json` is ignored by git; do not commit local secrets.
- `.db`, `.pyc`, and `.venv/` are ignored; avoid adding generated artifacts anyway.
- Be careful not to break Home Assistant add-on assumptions while changing standalone Docker behavior.

## When Editing

- Make focused changes.
- Do not rename modules or convert naming styles repo-wide unless the task explicitly calls for cleanup.
- Avoid touching unrelated legacy formatting.
- If you fix tests, update this document if the recommended commands change.
- If you add linting, formatting, or CI commands, document exact single-file and single-test invocations here.
