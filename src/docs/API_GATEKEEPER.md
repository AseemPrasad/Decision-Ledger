# Gatekeeper API Reference

The gatekeeper is the hot-path trust gate. Everything here is public surface
from `decision_ledger.gatekeeper` and is re-exported from the package root.

```python
from decision_ledger import Gatekeeper, GateAction, DecisionType, CalibrationContext
```

## `class Gatekeeper(policy=None, policy_file=None, exploration_rate=0.02, telemetry=None)`

Dataclass. Decides whether a small model may act for a given context.

| Parameter | Type | Default | Meaning |
| --- | --- | --- | --- |
| `policy` | `dict[bytes, CalibrationContext]` | `{}` | Calibrated context snapshot keyed by 16-byte context hash. Build with `policy_from_results(...).contexts` or `policy_from_dict(load_policy(...)).contexts`. Defaults to an empty (fail-closed) policy. |
| `policy_file` | `str \| None` | `None` | Optional schema-1.0 artifact YAML loaded at construction. Takes precedence over `policy`. |
| `exploration_rate` | `float` | `0.02` | Epsilon for stratified counterfactual sampling. Must satisfy `0.0 <= rate <= 1.0` (raises `ValueError` otherwise); `0.0` disables exploration. |
| `telemetry` | `RingBuffer \| None` | `None` | Optional ring buffer. When attached, every `evaluate` appends a `DecisionRecord` with the measured decision latency. Off by default to keep the hot path allocation-free. |

```python
gk = Gatekeeper(policy.contexts, exploration_rate=0.02, telemetry=buffer)
```

## `evaluate(context_hash: bytes, model_confidence: float, decision_type: str) -> GateAction`

Perform one delegation decision.

- **`context_hash`** — 16-byte context reference (from `utils.context_hash`).
  A `str` here is a bug, not a feature; pass bytes.
- **`model_confidence`** — the small model's self-reported confidence. Values
  `< 0` and `NaN` warn and clamp to `0.0`; values `> 1` silently clamp to `1.0`.
- **`decision_type`** — a name from `"route"`, `"judge"`, `"speculate"`,
  `"mutate"`, `"summarize"`, `"abstain"`. Unknown names warn and are treated
  as `"route"`.

Decision logic, in order (all fail-closed branches return `ESCALATE`):

1. Unknown context (not in `policy`) → `ESCALATE`.
2. `context.is_active` is `False` → `ESCALATE`.
3. `context.current_sample_size < context.min_sample_size` → `ESCALATE`
   (insufficient statistical power).
4. `context.q_hat` is `None` → `ESCALATE`.
5. Exploration triggers (see below) → `EXPLORE_SHADOW`.
6. `1 - model_confidence <= context.q_hat` → `DELEGATE`.
7. Otherwise → `ESCALATE`.

Returns one of `GateAction.DELEGATE`, `GateAction.ESCALATE`,
`GateAction.EXPLORE_SHADOW`.

```python
action = gk.evaluate(ctx_hash, 0.92, "route")
if action == GateAction.DELEGATE:
    result = small_model.generate(query)
elif action == GateAction.ESCALATE:
    result = frontier_model.generate(query)
else:  # EXPLORE_SHADOW
    small = small_model.generate(query)      # log the counterfactual
    result = frontier_model.generate(query)  # serve the frontier answer
```

### Exploration cadence

Exploration is deterministic. Eligible call `n` (active context, enough data,
valid `q_hat`) explores when `n % int(1.0 / exploration_rate) == 0`. With the
default `0.02`, exactly one call in fifty is shadowed — reproducible across
runs, and the observed rate equals the parameter.

## `reload_policy(new_policy: dict[bytes, CalibrationContext]) -> None`

Thread-safe, ~atomic policy swap under the same `RLock` as `evaluate`.

- Evaluations already in flight finish against the **old** snapshot; all
  subsequent evaluations observe the **new** dict.
- Template for the calibration loop:

```python
from decision_ledger.policy import load_policy, policy_from_dict

gk.reload_policy(policy_from_dict(load_policy("policies/policy-v1.yaml")).contexts)
```

## `from_policy_file(policy_file: str, exploration_rate=0.02) -> Gatekeeper` *(classmethod)*

Build a gatekeeper from a schema-1.0 artifact YAML:

```python
gk = Gatekeeper.from_policy_file("policies/policy_20260830-153000.yaml")
```

Loads the artifact, converts it with `convert_policy_dict_to_contexts`, logs
`[Loaded policy: version=<version>, contexts=<n>]`, and raises
`FileNotFoundError` (missing) or `PolicyValidationError` (invalid artifact).

## `reload_policy_from_file(policy_file: str) -> None`

Reload a policy from a schema-1.0 artifact YAML, swapping atomically. Logs the
version transition: `[Reloaded policy: <old> -> <new>]`.

## `convert_policy_dict_to_contexts(policy_dict: dict) -> dict[bytes, CalibrationContext]` *(staticmethod)*

Convert a validated artifact dict into gatekeeper contexts: `context_ref` hex
decodes to the 16-byte context hash, `min_sample_size` is global
`min_sample_size_default`, and `is_active` is `True` only for `ACTIVE`
contexts. `DRAINING` / `REVOKED` contexts are present but inactive (fail
closed).

## `get_metrics() -> dict[str, Any]`

Consistent snapshot of counters (copied under the lock, safe to hold). Shape:

```python
{
    "delegate": 1221,            # int
    "escalate": 734,             # int
    "explore": 45,               # int
    "total": 2000,               # delegate + escalate + explore
    "escalation_rate": 0.367,    # float (0.0 if no calls)
    "exploration_rate": 0.0225,  # float (0.0 if no calls)
    "per_decision_type": {
        "route": {"calls": 2000, "escalations": 734, "escalation_rate": 0.367},
        "judge": {"calls": 0, "escalations": 0, "escalation_rate": 0.0},
        # ... one entry per DecisionType
    },
}
```

## `enum DecisionType(IntEnum)`

`ROUTE = 0`, `JUDGE = 1`, `SPECULATE = 2`, `MUTATE = 3`, `SUMMARIZE = 4`,
`ABSTAIN = 5`. Mirrors the strings accepted by `evaluate` — e.g.
`DecisionType.ROUTE` corresponds to `"route"`. Useful for switch/ternary
against the string keys in metrics.

## `enum GateAction(IntEnum)`

`DELEGATE = 0`, `ESCALATE = 1`, `EXPLORE_SHADOW = 2`. Values are stable, so
you can persist them, but prefer `.name` for readable records — the telemetry
stores the name (`"DELEGATE"`, `"ESCALATE"`, `"EXPLORE_SHADOW"`), not the int.

## `class CalibrationContext(context_hash, q_hat=None, min_sample_size=100, current_sample_size=0, is_active=False)`

Per-context operating envelope (produced by the calibration pipeline; you
build one manually when hand-rolling a policy).

| Field | Type | Meaning |
| --- | --- | --- |
| `context_hash` | `bytes` | 16-byte BLAKE3 (truncated) context reference. |
| `q_hat` | `float \| None` | Conformal non-conformity threshold. `None` stays on the escalate path. |
| `min_sample_size` | `int` | Samples required before delegation is allowed. |
| `current_sample_size` | `int` | Sample count tracked by calibration. |
| `is_active` | `bool` | Whether the context may delegate. |
| `has_enough_data` | `bool` | Property: `current_sample_size >= min_sample_size`. |

## Constructing a gatekeeper from scratch

The normal path is calibration output:

```python
from decision_ledger import context_hash, policy_from_results, Gatekeeper
from decision_ledger.calibration import CalibrationResult

ctx = context_hash("route", prompt_template="...", model_id="qwen-7b")
result = CalibrationResult(
    q_hat=0.18, sample_size=5000,
    coverage_lower_bound=0.972, achieved_empirical_risk=0.024,
)
policy = policy_from_results({ctx: result}, version_id=1, min_sample_size=500)
gk = Gatekeeper(policy.contexts)
```

## Common mistakes

- **Passing a hex `str` instead of `bytes` for `context_hash`.** A string key
  will never match the byte keys in `policy`, so you get silent `ESCALATE`
  forever. Use the `bytes` from `context_hash()`, or `bytes.fromhex(hex_str)`.
- **Attaching telemetry and never draining.** The buffer is bounded; once
  full at ≥95% it starts dropping and counting. Attach a `BatchConsumer`
  (or call `pop_batch`) or the audit trail will shed records under load.
- **Expecting `get_metrics()["explore"]` to equal buffer contents.** Metrics
  are gatekeeper counters; buffer accounting lives on the `RingBuffer`
  (`total_pushed`, `dropped_count`). They agree only while nothing is dropped.
- **Reusing a gatekeeper across contexts with a stale policy.** Remember
  `reload_policy` after each calibration so deployments aren't judging traffic
  against an old `q_hat`.
- **Forgetting that unknown `decision_type` strings become `"route"`.** Typos
  like `"routing"` do not raise — they silently route-treat. Validate type
  names against `DecisionType` when you control the caller.
- **Wrapping `evaluate` itself in coarse locks or I/O.** The call is
  microseconds by design; holding it while writing to disk serializes your
  whole serving loop. Do I/O in the consumer thread.