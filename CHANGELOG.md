# v1.2.1 on April 21, 2026
- Add `pr_checks_typing` workflow and `tools/typing_gate.py` enforcing typing discipline as a ratchet: pyright strict config audit, `pyrightconfig.json` ban, `pyright.include` identity check, regex escape-hatch ratchet, AST-based `typing.Any`-reference ratchet (covers bare `Any`, `typing.Any`, `t.Any`, aliased imports, and module-level assignment-alias chains), pyright total-error-count ratchet, `filesAnalyzed` ratchet, and a base-vs-head budget-source ratchet that blocks weakening of the oracle in the same PR it gates.
- Add `[tool.pyright]` strict configuration to `pyproject.toml` with the full `report*` matrix set to `error`.
- Add `[tool.ruff]` configuration to `pyproject.toml` selecting `E/F/I/UP/RUF/BLE/ANN`.
- Add `.github/typing_budget.json` as the committed baseline oracle (zero escape hatches, 1213 pyright-strict errors on 35 files at introduction).
- Bump `project.requires-python` to `>=3.11` to align with `tomllib` usage, `pyright.pythonVersion`, `ruff.target-version`, and CI.
- Bump `pr_checks_codeql.yml` Python from `3.10` to `3.11` to match the above.
