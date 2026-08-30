#!/usr/bin/env python3
"""Calibration demo: learn thresholds from outcome data, then serve with them.

The full control loop in one script:

1. Phase A -- serve 1000 requests against an *empty* (fail-closed) policy.
   Every request escalates, because the gatekeeper does not trust anything
   yet. All 1000 decisions are persisted.
2. Log 500 simulated outcomes (is the small model's answer good?) using the
   batch API.
3. Run calibration. For each context that has enough outcomes, Split
   Conformal Risk Control picks a ``q_hat``: delegate only when the model's
   confidence is high enough that the calibrated failure risk stays under
   ``target_alpha``. A schema-1.0 policy artifact is generated and hot-reloaded
   into the live gatekeeper -- no restart.
4. Phase B -- serve another 500 requests *with* the new policy and compare.

Run from the repo root:

    python src/examples/calibration_demo.py
"""

from __future__ import annotations

import random
import sys
import tempfile
from collections import Counter
from pathlib import Path

# Make `src/` importable when the script is run directly (not installed).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from decision_ledger import (
    CalibrationPipeline,
    DecisionLedger,
    PolicyGenerator,
    make_context_hash,
)

PHASE_A_REQUESTS = 1000
OUTCOMES_TO_LOG = 500
PHASE_B_REQUESTS = 500

CTX_ROUTE = make_context_hash("qwen-7b", "routing")
CTX_JUDGE = make_context_hash("qwen-7b", "judging")
CTX_SUMMARY = make_context_hash("qwen-7b", "summarization")
CTX_HASHES = [CTX_ROUTE, CTX_JUDGE, CTX_SUMMARY]

# The small model is right when its confidence clears this bar (plus 2%
# label noise). Calibration must rediscover essentially this line.
CORRECTNESS_BAR = 0.6
NOISE = 0.02


def empty_policy(policies_dir: Path) -> str:
    """Write an empty schema-1.0 artifact so all artifacts live in `tmp`.

    A policy with zero contexts behaves exactly like no policy (fail-closed),
    but it pins the ledger's policy directory to the temp workspace so the
    calibration artifact from step 3 lands next to it.

    The version is pinned to an old timestamp: a fresh placeholder each run
    and a calibration artifact share the same second, so a dynamic version
    would collide inside ``generate_policy``.
    """
    return PolicyGenerator(str(policies_dir)).generate_policy(
        {}, policy_version="20200101-000000"
    )


def phase_a(ledger: DecisionLedger, rng: random.Random) -> Counter:
    """Serve without any trust: every request must escalate."""
    print(
        f"\n=== Phase A: {PHASE_A_REQUESTS} requests, empty (fail-closed) policy ===\n"
    )
    actions: Counter = Counter()
    for _ in range(PHASE_A_REQUESTS):
        ctx_hash = CTX_HASHES[_ % len(CTX_HASHES)]
        confidence = rng.random()
        action = ledger.evaluate(ctx_hash, confidence=confidence, decision_type="route")
        actions[action] += 1
    # Make the decisions durable so we can attach outcomes to them.
    ledger.consumer.drain_now()
    print(f"actions: {dict(actions)}  (all ESCALATE: no calibrated context yet)")
    print(f"decisions on disk: {ledger.stats()['total_decisions']}")
    return actions


def log_outcomes(ledger: DecisionLedger, rng: random.Random) -> None:
    """Attach simulated quality labels to every other decision, in bulk."""
    print(f"\n=== Step: log {OUTCOMES_TO_LOG} simulated outcomes (batch) ===\n")
    rows = ledger.database.execute_query(
        "SELECT decision_id, model_confidence FROM decisions"
    )

    records = []
    for index, row in enumerate(rows):
        if index % 2 == 1:  # label half of the decisions
            continue
        # Simulated quality: correct above the bar, with 2% label noise.
        correct = 1.0 if row["model_confidence"] >= CORRECTNESS_BAR else 0.0
        if rng.random() < NOISE:
            correct = 1.0 - correct
        records.append(
            {
                "decision_id": row["decision_id"],
                "outcome_value": correct,
                "source": "human",  # a human annotated the model's answer
                "metadata": {"task": "labeling", "labeler": "demo-rater"},
            }
        )

    before = ledger.stats()
    print(f"before logging: outcomes={before['total_outcomes']}")
    outcome_ids = ledger.outcome_collector.log_outcomes_batch(records)
    print(f"batch-logged    outcomes={len(outcome_ids)}  (one transaction)")
    print(f"after logging:  outcomes={ledger.stats()['total_outcomes']}")

    ledger.database.joiner.join_decisions_and_outcomes()
    joined = ledger.stats()
    print(
        f"after join:     decisions={joined['total_decisions']} "
        f"match_rate={joined['join_rate']:.0%}"
    )


def run_calibration(ledger: DecisionLedger) -> str:
    """Recalibrate every context, publish a policy, hot-reload the gate."""
    print("\n=== Step: run calibration ===\n")
    pipeline = CalibrationPipeline(
        ledger.database,
        ledger.gatekeeper,
        ledger.policy_generator,
        target_alpha=0.05,
    )

    policy_file = pipeline.run_calibration()
    print(f"policy artifact: {policy_file}")

    print("\nper-context thresholds (q_hat) now loaded into the gatekeeper:")
    for ctx_hash, cfg in ledger.gatekeeper.policy.items():
        threshold = f"{cfg.q_hat:.3f}" if cfg.q_hat is not None else "None (draining)"
        print(
            f"  {ctx_hash.hex()[:16]}... samples={cfg.current_sample_size:3d}"
            f"  q_hat={threshold:<16} active={cfg.is_active}"
        )

    stats = pipeline.get_calibration_stats()
    q_hats = stats["q_hat_distribution"]["q_hats"]
    print(
        f"\ncalibration summary: {stats['contexts_calibrated']} contexts, "
        f"{len(q_hats)} activated, {stats['q_hat_distribution']['draining_count']} draining"
    )
    print(f"drift flagged for: {stats['drift']['drifted_contexts']}")
    print("  (unexplored contexts are flagged as drift until shadow samples arrive)")
    return policy_file


def phase_b(ledger: DecisionLedger, rng: random.Random) -> Counter:
    """Serve again with the freshly calibrated policy in the gate."""
    print(
        f"\n=== Phase B: {PHASE_B_REQUESTS} requests with the calibrated policy ===\n"
    )
    actions: Counter = Counter()
    for _ in range(PHASE_B_REQUESTS):
        ctx_hash = CTX_HASHES[_ % len(CTX_HASHES)]
        confidence = rng.random()
        action = ledger.evaluate(ctx_hash, confidence=confidence, decision_type="route")
        actions[action] += 1
    ledger.consumer.drain_now()
    print(f"actions: {dict(actions)}")
    return actions


def compare(before: Counter, after: Counter) -> None:
    """Show the control-loop payoff side by side."""
    print("\n=== Before vs after calibration ===\n")
    print(f"{'action':<16} {'before':>10} {'after':>10} {'before %':>9} {'after %':>9}")
    for action in ["DELEGATE", "ESCALATE", "EXPLORE_SHADOW"]:
        b, a = before.get(action, 0), after.get(action, 0)
        bloom = 100.0 * b / PHASE_A_REQUESTS
        aloom = 100.0 * a / PHASE_B_REQUESTS
        print(f"{action:<16} {b:>10} {a:>10} {bloom:>8.1f}% {aloom:>8.1f}%")

    delegated = after.get("DELEGATE", 0)
    print(
        f"\nthe gate now trusts the small model on {delegated} of "
        f"{PHASE_B_REQUESTS} requests, providing fast-path delegation "
        "instead of escalating everything."
    )


def main() -> int:
    """Run the full learn -> serve -> learn-again loop in a temp workspace."""
    with tempfile.TemporaryDirectory(
        prefix="decision_ledger_calibration_"
    ) as workspace:
        workspace_path = Path(workspace)

        # Empty policy pins the artifact directory to the temp workspace.
        policy_file = empty_policy(workspace_path / "policies")

        ledger = DecisionLedger(
            db_path=str(workspace_path / "ledger.db"),
            policy_file=policy_file,
            auto_start_consumer=False,  # we drain explicitly between phases
        )
        rng = random.Random(2026)
        try:
            # Phase A: no trust yet.
            before = phase_a(ledger, rng)

            # Label half of the decisions.
            log_outcomes(ledger, rng)

            # Learn thresholds and hot-reload the gate.
            run_calibration(ledger)

            # Phase B: trust, but only where it is calibrated.
            after = phase_b(ledger, rng)

            compare(before, after)
        finally:
            ledger.shutdown()
        print("\ncleaned up (temp workspace deleted).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
