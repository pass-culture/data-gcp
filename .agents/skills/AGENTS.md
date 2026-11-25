# AGENTS.md: skills

Shared agent skills: one folder per skill, with a `SKILL.md` and optional `scripts/` and `references/`.

## Scripts

Scripts in `<skill>/scripts/` are read by humans in review. Keep them clear:

- **Python** by default. Use shell only when Python would be clearly worse, e.g. a short pipeline kept inline in `SKILL.md`.
- **Standard library only**, with a PEP 723 header, called as `uv run --script "$SKILL_DIR/scripts/<script>.py"`:

  ```python
  # /// script
  # requires-python = ">=3.11"
  # ///
  ```

- **Module docstring**: purpose, usage lines, and a sample output.
- **Typed and documented**: every function has type hints and a docstring.
  Use dataclasses (or aliases such as `Report = dict[str, Any]` for external payloads) rather than bare `dict`.
- **Doctests** on every pure function, with realistic inputs (a trimmed API payload, a lockfile excerpt...).
- Keep I/O (`git`, `gh`, `uv`, files) in small functions without doctests, and the logic in pure functions; `main()` only wires them.

Examples: `vulnerability-fix/scripts/audit.py`, `vulnerability-fix/scripts/lock_diff.py`, `code-scanning-triage/scripts/alerts.py`.

Validate from the repo root:

```bash
uv run python -m doctest -v .agents/skills/<skill>/scripts/<script>.py
make ruff_check
```
