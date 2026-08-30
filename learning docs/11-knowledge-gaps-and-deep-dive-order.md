# 11 — Knowledge Gaps & Recommended Deep-Dive Order

## 11.1 Why this file exists

A reverse-engineered curriculum can only answer what the repository's code and
docs *show*. Everything else is tagged `[A]` (assumption) or `[I]` (inference)
in the other docs. This file collects the genuine unknowns and converts them
into a reading order that closes them — code first, then the required external
theory.

## 11.2 Gaps, ordered by how much they matter

| # | Gap | Tag | Closes how |
| --- | --- | --- | --- |
| G1 | The **true, honest statistical guarantee** the estimator delivers (vs the docs' claim) | `[I]` | Read Vovk/CRC (Vovk, Gammerman, Shafer *Algorithmic Learning in a Random World* ch. on conformity) + *Conformal Risk Control* (Angelopoulos et al.); then re-read `calibration.py:143-204` |
| G2 | **Sort-order tie handling & small-n behavior** of `compute_threshold` | `[I]` | `src/tests/test_calibration.py` edge cases; read them, add manual probes for n in [1, 5, 20], ties in score |
| G3 | **Exploration → calibration propagation** details (the `explored` flag path end-to-end) | `[I]` | Trace `GateAction.EXPLORE_SHADOW` from `gatekeeper.py` → `DecisionRecord.explored` → joiner → calibrator filter; read `test_integration_week1` exploration-rate test |
| G4 | **Drift detection thresholds** — what magnitudes trip `detect_drift`, and how the pipeline *reacts* (keeps old threshold?) | `[I]` | Read `pipeline.py` drift branch + `calibration.py` `detect_drift`, then write a drift-slide test |
| G5 | **`policies` table vs YAML artifacts** object model (both exist; serve different guarantees) | `[E]` shoulders | `policy.py` `load_policy_history`/`save_policy`/`rollback_policy`; run `test_policy` |
| G6 | **The real OneDrive sync constraint** that motivated WAL-off | `[A]` | Author context — cannot verify from repo; noted as the design D-3 rationale in `database.py:20-22` |
| G7 | **Deployment model** (multi-replica reload triggers, cron/ops integration) | `[A]` | `docs/RUNBOOK.md` + `policy_latest.yaml` protocol; the artifact-on-disk design implies an external reloader |
| G8 | **Benchmark budgets** (1M/s aspirational consumer target) — doc vs code | `[E]` | `src/docs/BENCHMARKS.md` + consumer stress scripts; re-measure on target hardware |
| G9 | **Python-version fallbacks** (blake3-uuid6) — only 3.14 path exercised | `[E]` | Run test suite on 3.10/3.12 in CI; read `utils.py` fallback branches |
| G10 | **Median/edge semantics of `joined_records` for calibration parity** ("balanced groups" claim) | `[I]` | Read group-balancing in `calibration.py`; add a parity assertion test |
| G11 | **Why two outcome collectors** (durable + in-memory) and what "source" states mean | `[I]` | `outcomes.py` `OutcomeSource` + tests; the in-memory one is a test seam |
| G12 | **Rollback semantics / policy history** — operator workflow | `[E]` mostly | `policy.py:382-399` `rollback_policy` + design doc section |

## 11.3 Deep-dive order (the recommended reading path)

Each step pairs a *code surface* with the *external theory* that makes the code
legible.

1. **Conformal prediction basics** (30–60 min, no code)
   — read: *A Gentle Introduction to Conformal Prediction* (Vovk et al. ch.1)
   → *then* `calibration.py` non-conformity/prefix-risk with fresh eyes.
2. **Risk control** (60–90 min)
   — read: *Conformal Risk Control* paper + *Split Conformal Risk Control*
   → *then* `compute_threshold` + `min_sample_size` rationale + G1.
3. **SQLite durability + journaling** (30 min)
   — read: SQLite docs on journal modes, autocommit, `BUSY` retry
   → read `database.py:252-281` + `493-502` + `20-22` + G6.
4. **Threading & lock-free structures** (45 min)
   — read: the ring buffer + consumer + `test_storage_pipeline` to see the
   actual concurrency contract, MED-3 included.
5. **The gatekeeper policy protocol** (30 min)
   — read: `policy.py` artifact schema + `gatekeeper.reload` + a real artifact
   from `data/policies/` (`policy_*.yaml`).
6. **Performance engineering** (45 min)
   — read: `src/docs/BENCHMARKS.md`, then re-run the latency benchmarks and
   profile `evaluate()` with `py-spy`/`cProfile`; understand why numpy avoids
   the hot path (it’s only in calibration).
7. **The design documents fully** (60 min)
   — read: `DESIGN.md`, `ARCHITECTURE_WEEK1.md`; re-read `09` and `07` — you now
   have the vocabulary to argue with them.
8. **The gaps you care about** — pick from §11.2 for your use case, not all.

## 11.4 Suggested self-check at each stage

| After stage | You should be able to |
| --- | --- |
| 1–2 | Derive `q_hat` for a hand-computed example; state the honest guarantee |
| 3–4 | Explain the exact crash-loss window and why autocommit+batch-tx coexist |
| 5 | Describe one policy artifact field-for-field; simulate a rollback |
| 6 | Say where the microseconds go; justify telemetry-on/off budgets |
| 7 | Re-answer `09`’s MED list with your own proposed fixes |
| 8 | Write a one-page "v2 design delta" and defend the seams you’d keep |