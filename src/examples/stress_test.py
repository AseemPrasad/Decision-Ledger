#!/usr/bin/env python3
"""Stress test: 10k decisions and how fast the hot path really is.

Serves 10,000 requests through :meth:`DecisionLedger.evaluate` while the
background batch consumer stream-flushes decisions to SQLite, then reports:

* latency percentiles of the whole evaluate() hot path (p50 / p99 / p99.9),
* sustained throughput (decisions/second),
* consumer effectiveness -- records processed, dropped, flush counts, and
  how quickly the ring buffer drains behind the serving loop,
* a durability check: all 10,000 decisions actually landed in SQLite.

The serving loop is deliberately trivial (drain _records_ to a memory ring
buffer) so the numbers reflect the ledger's constant-factor cost rather than
the (much larger) cost of the model that produced ``confidence``.

Run from the repo root:

    python src/examples/stress_test.py
"""

from __future__ import annotations

import random
import statistics
import sys
import tempfile
import time
from pathlib import Path

# Make `src/` importable when the script is run directly (not installed).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from decision_ledger import DecisionLedger, make_context_hash

TARGET_DECISIONS = 10_000
CONTEXT = make_context_hash("qwen-7b", "routing")
CONVERGE_TIMEOUT_S = 60


def percentile(sorted_times: list, pct: float) -> float:
    """Nearest-rank percentile of a pre-sorted sequence of microseconds."""
    if not sorted_times:
        return 0.0
    index = min(len(sorted_times) - 1, int(pct / 100.0 * len(sorted_times)))
    return sorted_times[index]


def main() -> int:
    """Run the serving + ingestion stress loop in a throwaway workspace."""
    with tempfile.TemporaryDirectory(prefix="decision_ledger_stress_") as workspace:
        # The consumer runs live in the background (flush up to every second).
        ledger = DecisionLedger(
            db_path=str(Path(workspace) / "ledger.db"),
            auto_start_consumer=True,
            flush_interval=1.0,
        )
        rng = random.Random(99)
        try:
            print(f"\n=== Serving {TARGET_DECISIONS} decisions ===\n")

            latencies_us: list = []
            start = time.perf_counter()
            for request_number in range(1, TARGET_DECISIONS + 1):
                t0 = time.perf_counter()
                ledger.evaluate(CONTEXT, confidence=rng.random(), decision_type="route")
                latencies_us.append((time.perf_counter() - t0) * 1e6)

                if request_number % 2_500 == 0:
                    metrics = ledger.consumer.get_metrics()
                    print(
                        f"  {request_number:6d} served | ring_size={ledger.ring_buffer.size():6d}"
                        f" | flushed={metrics['total_records_flushed']:6d}"
                    )
            elapsed_s = time.perf_counter() - start

            # ----------------------------------------------------------- #
            print("\n=== Latency (evaluate() hot path) ===\n")
            sorted_micros = sorted(latencies_us)
            mean_us = statistics.fmean(latencies_us)
            print(f"  p50   = {percentile(sorted_micros, 50):8.1f} us")
            print(f"  p99   = {percentile(sorted_micros, 99):8.1f} us")
            print(f"  p99.9 = {percentile(sorted_micros, 99.9):8.1f} us")
            print(f"  mean  = {mean_us:8.1f} us")
            print(f"  throughput: {TARGET_DECISIONS / elapsed_s:10.0f} decisions/s")

            # ----------------------------------------------------------- #
            print("\n=== Consumer effectiveness (drain while serving) ===\n")
            polls = []
            deadline = time.monotonic() + CONVERGE_TIMEOUT_S
            while time.monotonic() < deadline:
                on_disk = ledger.database.execute_query(
                    "SELECT COUNT(*) FROM decisions"
                )[0][0]
                polls.append((on_disk, ledger.ring_buffer.size()))
                if on_disk >= TARGET_DECISIONS:
                    break
                time.sleep(0.25)

            ledger.consumer.drain_now()  # empty the remainder synchronously

            for count, ring_size in polls:
                print(
                    f"  poll: decisions_on_disk={count:6d} ring_buffer={ring_size:6d}"
                )

            metrics = ledger.consumer.get_metrics()
            print(
                f"\n  records processed by consumer: {metrics['total_records_processed']}"
            )
            print(
                f"  records flushed to SQLite:     {metrics['total_records_flushed']}"
            )
            print(
                f"  records dropped (should be 0): {metrics['total_records_dropped']}"
            )
            print(f"  flush count:                   {metrics['total_flushes']}")
            print(f"  avg flush time:                {metrics['avg_flush_time_ms']} ms")
            print(f"  records still buffered:        {metrics['backlog_records']}")

            # ----------------------------------------------------------- #
            print("\n=== Durability check ===\n")
            final = ledger.database.execute_query("SELECT COUNT(*) FROM decisions")[0][
                0
            ]
            dropped = ledger.ring_buffer.dropped_count
            print(f"decisions persisted: {final} / {TARGET_DECISIONS}")
            print(f"ring buffer drops:   {dropped}")
            drop_metric = ledger.consumer.get_metrics()
            print(f"consumer backlog (end):         {drop_metric['backlog_records']}")
            if final == TARGET_DECISIONS and dropped == 0:
                print("all decisions persisted with zero drops: PASS")
            else:
                print("data loss detected: FAIL")
                return 1
        finally:
            ledger.shutdown()
        print("\ncleaned up (temp workspace deleted).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
