# CLAUDE.md

## Context
- Learning project for data engineering. Overall vision and data model: see `README.md`.
- The user is new to these concepts: they want to understand every choice, not just get working code.

## Environment
- Windows, PowerShell. Give test commands in PowerShell syntax.
- Python 3.12 managed by uv. Always go through `uv run` (e.g. `uv run python ...`, `uv run pytest`).
- Lint and format: `uv run ruff check .` and `uv run ruff format --check .` must pass.
- Spark needs Java, which only the Docker image has: run Spark code and the full test suite with `docker compose run --rm pipeline uv run ...` (e.g. `uv run pytest`). On Windows, `uv run pytest` skips the Spark tests.

## Working rules
- One step at a time. Wait for the user's "ok" before moving to the next one.
- Before coding a step, explain in a few lines what we will do and why.
- Keep changes small: if a step grows, propose splitting it.
- NEVER run `git add`, `git commit`, `git push` or any git command that modifies the repo. The user tests and commits themselves.
- Work on a dedicated branch, never directly on `master`.
- One commit = one atomic step, testable on its own, message in Conventional Commits format (`feat:`, `fix:`, `docs:`, `test:`, `chore:`...).
- End of step: give the test command, the expected result and the proposed commit message, then stop.
- If the user wants to write a part themselves: give hints, not the full solution.
- No over-engineering: no dependency or abstraction we do not use yet.

## Language
- All documentation, code, comments and commit messages in English.
- Replies to the user in French, concise, as bullet points.
