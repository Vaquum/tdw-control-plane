# v1.6.3 on April 25, 2026
- Make the two daily Binance Origo source-template schedules launch partitioned catch-up runs instead of non-partitioned empty-config runs.
- Keep the daily spot and futures schedules enabled while bounding automated catch-up with the existing daily backfill limits and skipping loud when manual backfill is required.

# v1.6.2 on April 23, 2026
- Rename the two Origo source-template schedules to `daily_binance_spot_pipeline_schedule` and `daily_binance_futures_pipeline_schedule` so both surfaces include the source prefix explicitly.
- Keep the existing spot and futures source-template jobs unchanged; this slice only renames the Dagster schedule definitions and their registration surface.

# v1.6.1 on April 23, 2026
- Add the repo-root `AGENTS.md` governance file with the operator-specified ten-law workflow contract and `zero-bang` approval authority.
- Add `tests/tools/test_agents_contract.py` and extend `pr_checks_ruleset` so the checked-in `AGENTS.md` file identity and workflow coverage are mechanically enforced in CI.

# v1.6.0 on April 23, 2026
- Complete the Binance futures Origo data-source template on top of `binance_daily_futures_trades` by adding the single-source `binance_futures_klines` projection and the shared `aligned_1m_exchange` futures path.
- Rename the generic spot schedule to `daily_spot_pipeline_schedule`, add `daily_futures_pipeline_schedule`, and wire `refresh_binance_futures_data_source_job` so spot and futures source-template automation follow the same naming law.
- Add checked-in real Binance futures daily fixtures for both the headerless and headered source shapes, plus fixture-backed futures row/schema/idempotency tests that prove `aligned_1m_exchange` can hold both `binance_spot` and `binance_futures`.

# v1.5.1 on April 23, 2026
- Correct the Origo Binance spot projection contract so `binance_spot_klines` and `aligned_1m_exchange` match the TDW 1-minute kline schema instead of the exchange-native 12-column shape.
- Replace the single-source and aligned refresh SQL so both tables materialize the TDW analytics columns (`mean`, `std`, `median`, `iqr`, maker/liquidity fields) from raw spot trades.
- Add a checked-in TDW contract fixture and replace the old exchange-native row tests with fixture-backed schema and row-parity tests for both projection tables.

# v1.5.0 on April 23, 2026
- Complete the first Binance spot Origo data-source template on top of `binance_daily_spot_trades` by adding the single-source `binance_spot_klines` projection and the shared `aligned_1m_exchange` projection layer.
- Replace the old raw-only daily Origo schedule target with `refresh_binance_spot_data_source_job`, which materializes the raw daily insert plus both projection layers for the same partition.
- Add end-to-end `tests/origo_source_native/test_origo_binance_spot_data_source_template.py` proofs for table-name contracts, exact schema, exact Binance-derived 1-minute rows, aligned dataset-source rows, and same-partition rerun idempotency.

# v1.4.1 on April 22, 2026
- Sync `tests/fixtures/github/ruleset_live_unexpected_field.json` to the current 9-context protected-check set on `main`, including `pr_checks_lint` and `pr_checks_tests`.
- Add `test_unexpected_field_fixture_preserves_required_contexts` so `pr_checks_ruleset` fails if that negative fixture ever drifts from the checked-in ruleset snapshot's required-status list.

# v1.4.0 on April 22, 2026
- Move the runtime image to Python `3.11.12` so Docker matches the package and CI interpreter contract.
- Replace the Origo path with a daily-source-native Binance spot trades template: idempotent `create_origo_database`, idempotent `create_binance_daily_spot_trades_table_origo`, and fail-loud `insert_daily_binance_spot_trades_to_origo`.
- Rename the Origo raw table surface to `binance_daily_spot_trades`, add the companion `binance_daily_spot_trades_ingestion` ledger, preserve source timestamps with `DateTime64(6)`, and record Dagster run metadata plus source checksums/counts per ingested daily file.
- Add `.github/workflows/pr_checks_tests.yml`, the ClickHouse-backed `tests/origo_source_native` suite, fixture-backed Binance daily archives plus `.CHECKSUM` files, and the checked-in ruleset snapshot change that requires `pr_checks_tests` on `main`.

# v1.3.3 on April 22, 2026
- Add `.github/workflows/pr_checks_lint.yml` so `tools` and `tests/tools` gain a required fail-loud Ruff gate on `main`, pinned to Ruff `0.15.11`.
- Extend `pr_checks_ruleset` with `tests/tools/test_lint_ci_contract.py` and a pinned Ruff install so the lint gate itself is mechanically protected by required CI.
- Remove dead Ruff ignores `ANN101` and `ANN102`, replace broad-exception handling in `tools/slice_gate.py` and `tools/typing_gate.py` with explicit fail-loud setup/read handling, and replace the remaining `RUF005` list concatenations in `tools/fail_loud_gate.py`.

# v1.3.2 on April 22, 2026
- Add `.github/rulesets/main.json`, `tools/ruleset_gate.py`, `pr_checks_ruleset`, and fixture-backed ruleset drift tests so `main` can ratchet its required PR-path contexts against a checked-in snapshot.
- Fix `tools/cc_gate.py` so linked-issue title lookup failures raise a hard setup error instead of silently skipping Conventional Commits validation.
- Remove the post-merge CHANGELOG automation workflow and `scripts/update_changelog.py` because `pr_checks_version` is now the authoritative pre-merge version/changelog gate.

# v1.3.1 on April 21, 2026
- Add `pr_checks_fail_loud` workflow and `tools/fail_loud_gate.py` ratcheting seven silent-fallback categories in the package: `bare_except`, `empty_pass`, `empty_ellipsis`, `empty_return_none`, `empty_continue_break`, `contextlib_suppress`, `errors_ignore_kwarg`. Base-vs-head protection so the budget cannot be weakened in the same PR that gates against it.
- Add `.github/fail_loud_budget.json` as the committed baseline oracle (`bare_except=4`, `empty_pass=6`, `empty_continue_break=1`, all others zero on 35 production files at introduction).
- Add `pr_checks_version` workflow and `tools/version_gate.py` enforcing six rules on every PR: pyproject.toml differs, `[project].version` advances by strict `MAJOR.MINOR.PATCH` (prerelease and build-metadata forms rejected outright, since the gate compares as integer triples and real semver precedence would be misrepresented), CHANGELOG.md differs, CHANGELOG's first `# v<X.Y.Z>` header matches the new version, the top version section has at least one non-empty non-header line of content before the next version header (so a header-only trail is rejected), and the bump level meets the minimum required by the PR's Conventional Commits type (`type!` → major, `feat` → minor, anything else → patch).
- `contextlib.suppress` detection resolves module-alias chains to a fixed point (`import contextlib as cl; mod = cl; sup = mod.suppress; sup2 = sup`) so any re-binding path to `contextlib.suppress` is counted. Same fixed-point technique already used by `typing_gate.py` for `typing.Any`.

# v1.3.0 on April 21, 2026
- Add `.github/ISSUE_TEMPLATE/slice.yml` — slice issue template. Eleven sections each carrying a blockquoted `> **Significance.**` paragraph that survives into the filed issue body.
- Add `pr_checks_slice` workflow and `tools/slice_gate.py` enforcing the PR↔slice-issue contract as eight deterministic rules: exactly one `Closes/Fixes/Resolves #N` reference, the reference resolves in the repo, resolves to an issue (not another PR), issue is OPEN, issue has the `slice` label, PR title byte-equals issue title, issue body contains every full multi-line Significance blockquote from the template verbatim (extracted at gate runtime so template and validator cannot drift apart), PR diff ⊆ issue `## Surfaces`, PR diff ∩ issue `## Out of Scope` = ∅.
- Add `pr_checks_slice_on_issue` workflow — stale-state recovery on `issues` events (edited, labeled, unlabeled, closed, reopened, deleted). Finds linked open PRs by scanning every open PR body with the same closing-keyword regex rule 1 uses, re-runs the slice gate against current state, and posts a fresh `pr_checks_slice` check-run to each PR's head SHA via the Checks API. Branch protection uses the latest check-run per name per SHA, so an issue change that breaks any rule invalidates the required check within seconds.
- Add `pr_checks_cc` workflow and `tools/cc_gate.py` enforcing Conventional Commits v1.0.0 on three surfaces: PR title, linked-issue title, and every non-merge commit in the PR range. Allowed types: `feat | fix | docs | style | refactor | perf | test | build | ci | chore | revert`.
- Use GitHub REST `pulls/:num/files` with `--paginate` for all PR file enumeration (previously `gh pr view --json files`, which caps at 100). Both slice workflows cross-check the enumerated count against the PR object's `changedFiles` field and hard-fail if they disagree, so scope rules cannot silently under-enforce on large PRs.
- Ensure branch-protection `Protect-Main` ruleset requires `pr_checks_slice` and `pr_checks_cc` in addition to `PR Checks CodeQL (python)` and `pr_checks_typing`.

# v1.2.1 on April 21, 2026
- Add `pr_checks_typing` workflow and `tools/typing_gate.py` enforcing typing discipline as a ratchet: pyright strict config audit, `pyrightconfig.json` ban, `pyright.include` identity check, regex escape-hatch ratchet, AST-based `typing.Any`-reference ratchet (covers bare `Any`, `typing.Any`, `t.Any`, aliased imports, and module-level assignment-alias chains), pyright total-error-count ratchet, `filesAnalyzed` ratchet, and a base-vs-head budget-source ratchet that blocks weakening of the oracle in the same PR it gates.
- Add `[tool.pyright]` strict configuration to `pyproject.toml` with the full `report*` matrix set to `error`.
- Add `[tool.ruff]` configuration to `pyproject.toml` selecting `E/F/I/UP/RUF/BLE/ANN`.
- Add `.github/typing_budget.json` as the committed baseline oracle (zero escape hatches, 1213 pyright-strict errors on 35 files at introduction).
- Bump `project.requires-python` to `>=3.11` to align with `tomllib` usage, `pyright.pythonVersion`, `ruff.target-version`, and CI.
- Bump `pr_checks_codeql.yml` Python from `3.10` to `3.11` to match the above.
