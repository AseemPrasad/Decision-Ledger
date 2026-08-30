#!/usr/bin/env python3
"""Basic serving: route real-time requests through a calibrated gate.

This is the happy-path entry example. It:

1. builds a calibrated policy *offline* from synthetic statistics (no
   database needed) via :class:`PolicyGenerator`,
2. loads that policy into a :class:`DecisionLedger`,
3. serves 1000 simulated model requests across three contexts (routing,
   judging, summarizing), wrapping each in ``evaluate()``,

The gatekeeper answers every request in fast-path microseconds and every
decision is transparently pushed into the telemetry ring buffer for the
background consumer to flush to SQLite. Because the contexts are active,
all three actions are exercised: ``DELEGATE`` (confidence high enough),
``ESCALATE`` (fail-closed when confidence misses the calibrated threshold)
and ``EXPLORE_SHADOW`` (the ~2% stratified counterfactual bandit).

Run from the repo root:

    python src/examples/basic_serving.py
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
    CalibrationResult,
    DecisionLedger,
    PolicyGenerator,
    make_context_hash,
)

TOTAL_REQUESTS = 1000
STATS_EVERY = 100

# Three contexts we serve in production. Each is a deterministic 128-bit
# hash of a (model, task) factor set -- same inputs, same hash, every time.
CTX_ROUTE = make_context_hash("qwen-7b", "routing")
CTX_JUDGE = make_context_hash("qwen-7b", "judging")
CTX_SUMMARY = make_context_hash("qwen-7b", "summarization")

CTX_HASHES = [CTX_ROUTE, CTX_JUDGE, CTX_SUMMARY]


def build_policy(policies_dir: Path) -> str:
    """Synthesize a calibrated policy artifact for the three contexts.

    In a real deployment this artifact is produced by the calibration
    pipeline (see ``calibration_demo.py``). Here we hand the pipeline's
    output -- a ``context_hash -> CalibrationResult`` mapping -- straight to
    :meth:`PolicyGenerator.generate_policy`, which serializes a schema-1.0
    YAML artifact and points ``policy_latest.yaml`` at it.

    ``q_hat`` is the per-context harmful-action threshold: the gatekeeper
    delegates only when ``1 - confidence <= q_hat``, i.e. when the model is
    confident enough that the calibrated failure risk stays acceptable.
    """
    generator = PolicyGenerator(str(policies_dir))
    results = {
        # Routing is well calibrated: allow delegation at confidence >= 0.70.
        CTX_ROUTE: CalibrationResult(
            q_hat=0.30,
            sample_size=240,
            coverage_lower_bound=0.95,
            achieved_empirical_risk=0.04,
            min_observed_loss=0.0,
            max_observed_loss=1.0,
        ),
        # Judging is riskier: the bar is higher (confidence >= 0.80).
        CTX_JUDGE: CalibrationResult(
            q_hat=0.20,
            sample_size=210,
            coverage_lower_bound=0.95,
            achieved_empirical_risk=0.03,
            min_observed_loss=0.0,
            max_observed_loss=1.0,
        ),
        # Summarization has the strictest bar (confidence >= 0.90).
        CTX_SUMMARY: CalibrationResult(
            q_hat=0.10,
            sample_size=260,
            coverage_lower_bound=0.95,
            achieved_empirical_risk=0.02,
            min_observed_loss=0.0,
            max_observed_loss=1.0,
        ),
    }

    print("\n=== Step 1: synthesize a calibrated policy (offline) ===\n")
    print(f"{'context':<20} {'q_hat':<8} {'samples':<8}  delegates when confident>=")
    for ctx_hash, result in results.items():
        print(
            f"{ctx_hash.hex()[:16] + '...':<20} "
            f"{result.q_hat:<8.2f} {result.sample_size:<8}  "
            f"{1.0 - result.q_hat:.2f}"
        )

    policy_file = generator.generate_policy(results)
    print(f"\nartifact written to: {policy_file}")
    return policy_file


def serve(ledger: DecisionLedger, rng: random.Random) -> Counter:
    """Serve ``TOTAL_REQUESTS`` requests and report live stats every 100."""
    print(f"\n=== Step 2: serve {TOTAL_REQUESTS} requests " "(stats every 100) ===\n")

    actions: Counter = Counter()
    for request_number in range(1, TOTAL_REQUESTS + 1):
        # Simulate one request: pick a context and a model confidence.
        ctx_hash = rng.choice(CTX_HASHES)
        confidence = rng.random()

        # The one-line API. The gatekeeper decides, logs telemetry, returns
        # one of DELEGATE / ESCALATE / EXPLORE_SHADOW.
        action = ledger.evaluate(ctx_hash, confidence=confidence, decision_type="route")
        actions[action] += 1

        if request_number % STATS_EVERY == 0:
            stats = ledger.stats()
            print(
                f"  request {request_number:4d}: "
                f"actions={dict(actions)}  "
                f"ring_buffer_fill={stats['ring_buffer_fill']:.2%}  "
                f"decisions_on_disk={stats['total_decisions']}"
            )

    print("\n...serving done; flushing the consumer so counts are durable...")
    ledger.consumer.drain_now()
    return actions


def main() -> int:
    """Run the demo end-to-end inside a throwaway temp directory."""
    # A TemporaryDirectory keeps the ledger self-contained: the SQLite file,
    # policy artifacts and the final *_stats.json are deleted on exit, so the
    # example leaves no trace in the repo.
    with tempfile.TemporaryDirectory(prefix="decision_ledger_basic_") as workspace:
        workspace_path = Path(workspace)

        # Step 1: offline policy generation (no database involved).
        policy_file = build_policy(workspace_path / "policies")

        # Step 2: construct the ledger. `policy_file=` primes the fail-closed
        # gatekeeper with the artifact we just generated.
        print("\n=== Step 2 (cont.): construct the ledger ===\n")
        ledger = DecisionLedger(
            db_path=str(workspace_path / "ledger.db"),
            policy_file=policy_file,
            auto_start_consumer=True,  # background thread flushes to SQLite
        )
        try:
            print(f"database:      {workspace_path / 'ledger.db'}")
            print(f"policy fields: {len(ledger.gatekeeper.policy)} contexts")
            for ctx_hash, cfg in ledger.gatekeeper.policy.items():
                print(
                    f"  {ctx_hash.hex()[:16]}... q_hat={cfg.q_hat}"
                    f" active={cfg.is_active}"
                )

            # Step 3: serve requests.
            actions = serve(ledger, random.Random(1234))

            # Step 4: final report.
            print("\n=== Final report ===\n")
            stats = ledger.stats()
            print(f"total decisions on disk: {stats['total_decisions']}")
            print(f"decisions by action:     {dict(stats['decisions_by_action'])}")
            print(
                f"consumer flushed:        {stats['consumer']['total_records_flushed']}"
            )
            print(f"dropped records:         {stats['dropped_records']}")
            print("\nactions observed this run:")
            for action, count in sorted(actions.items()):
                share = 100.0 * count / TOTAL_REQUESTS
                print(f"  {action:<16} {count:4d}  ({share:4.1f}%)")

            # Demo requirement: all three actions must show up. With the 2%
            # exploration rate ~20 requests are shadow-sampled; delegation
            # needs confidence above 1 - q_hat, which random confidence hits
            # frequently.
            expected = {"DELEGATE", "ESCALATE", "EXPLORE_SHADOW"}
            missing = expected - set(actions)
            if missing:
                print(f"\nNOTE: actions not observed: {sorted(missing)}")
            else:
                print("\nall three actions observed: PASS")
        finally:
            # Always shut down: flush, snapshot, close, stop threads.
            ledger.shutdown()
        print("\ncleaned up (temp workspace deleted).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
