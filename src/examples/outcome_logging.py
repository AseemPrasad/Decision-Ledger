#!/usr/bin/env python3
"""Outcome logging: attach independent quality labels to decisions.

The ledger is only as good as its outcome data -- you cannot calibrate
without labels. This example shows the full outcome surface:

1. recording single outcomes per source (human review, task metric, user
   report, model verification),
2. bulk-loading outcomes in one transaction (``log_outcomes_batch``),
3. querying outcomes back by id, by decision, and in bulk,
4. joining decisions<->outcomes and inspecting matched / unmatched rows,

plus the two errors newcomers will hit most (unknown source, out-of-range
value) and how the ledger rejects them.

Run from the repo root:

    python src/examples/outcome_logging.py
"""

from __future__ import annotations

import json
import random
import sys
import tempfile
from collections import Counter
from pathlib import Path

# Make `src/` importable when the script is run directly (not installed).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from decision_ledger import DecisionLedger, make_context_hash
from decision_ledger.outcomes import (
    InvalidOutcomeSourceError,
    InvalidOutcomeValueError,
)

CTX_ROUTE = make_context_hash("qwen-7b", "routing")
CTX_JUDGE = make_context_hash("qwen-7b", "judging")
CTX_HASHES = [CTX_ROUTE, CTX_JUDGE]

NUM_DECISIONS = 8


def seed_decisions(ledger: DecisionLedger, rng: random.Random) -> list:
    """Serve a handful of decisions and return their durable decision ids."""
    for _ in range(NUM_DECISIONS):
        ctx_hash = CTX_HASHES[_ % len(CTX_HASHES)]
        ledger.evaluate(ctx_hash, confidence=rng.random(), decision_type="route")
    ledger.consumer.drain_now()

    rows = ledger.database.execute_query(
        "SELECT decision_id, context_hash, action_taken FROM decisions"
    )
    return [
        {
            "decision_id": row["decision_id"],
            "context_hash": row["context_hash"],
            "action": row["action_taken"],
        }
        for row in rows
    ]


def show_row(label: str, row: dict) -> None:
    """Print one outcome/joined row compactly."""
    print(f"  {label:<36} {json.dumps(row, default=str)}")


def main() -> int:
    """Run the outcome-logging tour inside a throwaway temp directory."""
    with tempfile.TemporaryDirectory(prefix="decision_ledger_outcomes_") as workspace:
        ledger = DecisionLedger(
            db_path=str(Path(workspace) / "ledger.db"),
            auto_start_consumer=False,
        )
        rng = random.Random(7)
        try:
            decisions = seed_decisions(ledger, rng)
            print(
                f"\nseeded {len(decisions)} decisions "
                f"(actions: {Counter(d['action'] for d in decisions)})\n"
            )

            # ----------------------------------------------------------- #
            print("=== 1. Single outcomes, one per source ===\n")
            # Each source is an enum: human, task_metric, user_report,
            # model_verification. MODEL_VERIFICATION outcomes are never used
            # for calibration (the verifier is another model, not independent).
            single = [
                (0, 1.0, "human", {"reviewer": "alice", "verdict": "ok"}),
                (1, 0.0, "human", {"reviewer": "bob", "verdict": "wrong"}),
                (2, 1.0, "task_metric", {"metric": "exact_match"}),
                (
                    3,
                    0.0,
                    "user_report",
                    {"user": "u-42", "feedback": "the route was wrong"},
                ),
                (4, 1.0, "model_verification", {"verifier": "judge-v2"}),
            ]
            outcome_ids = []
            for index, value, source, metadata in single:
                outcome_id = ledger.log_outcome(
                    decisions[index]["decision_id"],
                    value,
                    outcome_source=source,
                    metadata=metadata,
                )
                outcome_ids.append(outcome_id)
                print(
                    f"  logged outcome for decision[{index}] via {source:<20} "
                    f"-> {outcome_id}"
                )

            # ----------------------------------------------------------- #
            print("\n=== 2. Batch outcomes (one transaction) ===\n")
            batch_records = [
                {
                    "decision_id": decisions[5]["decision_id"],
                    "outcome_value": 1.0,
                    "source": "human",
                    "metadata": {"batch": "b2"},
                },
                {
                    "decision_id": decisions[6]["decision_id"],
                    "outcome_value": 1.0,
                    "source": "task_metric",
                    "metadata": {"metric": "accuracy@1"},
                },
                {
                    "decision_id": decisions[7]["decision_id"],
                    "outcome_value": 0.0,
                    "source": "human",
                    "metadata": {"batch": "b2"},
                },
            ]
            batch_ids = ledger.outcome_collector.log_outcomes_batch(batch_records)
            print(f"logged {len(batch_ids)} outcomes in one transaction:")
            outcome_ids.extend(batch_ids)
            for outcome_id in batch_ids:
                print(f"  -> {outcome_id}")

            print(f"\ntotal outcomes now: {ledger.stats()['total_outcomes']}")

            # ----------------------------------------------------------- #
            print("\n=== 3. Querying outcomes ===\n")
            show_row(
                "get_outcome(first)",
                ledger.outcome_collector.get_outcome(outcome_ids[0]),
            )
            show_row(
                "get_outcomes_for_decision(0)",
                ledger.outcome_collector.get_outcomes_for_decision(
                    decisions[0]["decision_id"]
                ),
            )
            all_outcomes = ledger.database.get_outcomes()
            print(f"\n  database.get_outcomes() -> {len(all_outcomes)} rows, e.g.:")
            show_row("row[0]", all_outcomes[0])

            # ----------------------------------------------------------- #
            print("\n=== 4. Join matching ===\n")
            # All 8 decisions received exactly one outcome above, so the join
            # is complete. One extra decision is left unlabeled on purpose:
            # its joined row will carry outcome_value=None (unmatched).
            print("  adding one unlabeled decision to demonstrate an unmatched row")
            ledger.evaluate(CTX_ROUTE, confidence=0.6, decision_type="route")
            ledger.consumer.drain_now()

            inserted = ledger.database.joiner.join_decisions_and_outcomes()
            stats = ledger.stats()
            print(f"joined rows inserted: {inserted}")
            print(
                f"decisions={stats['total_decisions']} "
                f"outcomes={stats['total_outcomes']} "
                f"match_rate={stats['join_rate']:.0%}"
            )

            print("\njoined records (action_taken / outcome_value):")
            joined = ledger.database.get_joined_records()
            for row in joined:
                value = row["outcome_value"]
                show_row(
                    f"{row['decision_id'][:8]} ({row['action_taken']})",
                    {
                        "action_taken": row["action_taken"],
                        "outcome_value": value,
                        "matched": value is not None,
                    },
                )

            # ----------------------------------------------------------- #
            print("\n=== 5. Validation: what the ledger refuses ===\n")
            # DecisionLedger.log_outcome logs a stack trace before re-raising
            # bad input. Silence the package's loggers for this expected-failure
            # demo so the tracebacks don't bury the lesson.
            import logging

            saved_levels = {
                name: logging.getLogger(name).getEffectiveLevel()
                for name in logging.root.manager.loggerDict
                if name == "decision_ledger"
                or name.startswith("decision_ledger.")
            }
            for name in saved_levels:
                logging.getLogger(name).setLevel(logging.CRITICAL)
            try:
                try:
                    ledger.log_outcome(decisions[0]["decision_id"], 0.5, "alien_source")
                except InvalidOutcomeSourceError as exc:
                    print(f"  unknown source rejected: {exc}")

                try:
                    ledger.log_outcome(decisions[0]["decision_id"], 1.5)
                except InvalidOutcomeValueError as exc:
                    print(f"  out-of-range value rejected: {exc}")
            finally:
                for name, level in saved_levels.items():
                    logging.getLogger(name).setLevel(level)
        finally:
            ledger.shutdown()
        print("\ncleaned up (temp workspace deleted).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
