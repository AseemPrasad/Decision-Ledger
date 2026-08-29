# Code Style Guide

The repo is machine-fixed by **black** and sanity-checked by **mypy** on every
`src/` check; the rest of this guide is for the judgment black can't make.
Read [README.md](../README.md)'s Development section for the exact commands.

## Formatting: `black`

- Black is **pinned at `line-length = 88`** in `pyproject.toml`
  (`[tool.black]`; `target-version = ["py311"]`). The whole tree is formatted
  at 88 — do not reformat to a different width, and do not hand-wrap lines
  black would unwrap.
- Run `black .` before finishing a change; `black --check .` in CI.
- Want a different width? Change `line-length` in `[tool.black]` and re-run
  `black .` once — don't mix styles commit-to-commit.

## Type hints everywhere

- All public functions and methods carry full annotations; dataclass fields
  are annotated; no bare `-> None` omissions.
- `mypy src` must stay clean. Config (`[tool.mypy]` in `pyproject.toml`):
  - `disallow_untyped_defs = true`, `disallow_any_generics = true` — no
    implicit `Any` in signatures; containers are parameterized
    (`dict[bytes, CalibrationContext]`, not `dict`).
  - `ignore_missing_imports = true` — third-party packages without stubs
    (e.g. `blake3`, `uuid6`) are tolerated.
  - tests are checked more leniently (`tests.*` override).
- Use `from __future__ import annotations` at the top of every module for
  forward references and `Optional[T]`/`T | None` written freely (the project
  supports Python ≥ 3.9 at runtime).

## Docstrings: Google style

Modules, classes, and public functions get docstrings. Style:

```python
def make_context_hash(
    model_id: str,
    task_type: str,
    prompt_template_version: str = "default",
) -> bytes:
    """Single-line summary: what it does, imperative mood.

    A paragraph that explains the *why* when it isn't obvious — e.g. why a
    parameter exists or which guarantee the behavior upholds.

    Example:
        >>> make_context_hash("qwen-7b", "routing")

    Args:
        model_id: model name and version.
        task_type: control-plane task, e.g. ``"routing"``.

    Returns:
        bytes: 16-byte BLAKE3 digest.
    """
```

Rules of thumb:

- First line: imperative verb + the one thing it does ("Decide whether…",
  "Drain up to N records…").
- `Args:`/`Returns:`/`Raises:` sections only when there is something to say —
  obvious self-documenting members can skip them.
- Cross-reference with Sphinx-style roles where useful:
  `:class:`~.telemetry.RingBuffer``, `:func:`context_hash``.
- Docstring **examples should be real**: values they show must be what the
  code actually returns (see `utils.py`)
- Do **not** belabor the obvious in comments — see below.

## Comments

- Prefer a good docstring to an explanatory comment.
- Use comments only for the non-obvious: invariants, thread-safety
  assumptions, performance decisions, or why-a-value (e.g. "call n explores
  when … so results are reproducible").
- Never leave commented-out code behind.

## Meaningful names

- Names say *what*, not *how*: `dropped_count`, `pop_batch`,
  `fill_level()`, `has_enough_data`, `escalation_rate`.
- Domain vocabulary over generic terms: `q_hat` (not `threshold`),
  `non_conformity` (not `score`), `action_taken` (not `result`).
- Singletons/constants are `UPPER_SNAKE`; module levels only.

## Logging, not `print()`

- Library code (`src/decision_ledger/`) **never uses `print`**. Use the
  module logger:

  ```python
  logger = logging.getLogger(__name__)
  logger.warning("unknown decision_type %r; treating as %r", dt, fallback)
  ```

  Note the **lazy formatting**: pass arguments to the logger, never
  f-strings—the message line is only formatted if the level is enabled.
- Warning/critical thresholds carry context (which buffer, which tier, which
  context hash) so logs are actionable on their own.
- `print` is acceptable only in `src/examples/*` demo scripts and `main()`.
- Operators configure logging once via `utils.setup_logging(level, file)` —
  never reconfigure inside library functions.

## Data models

- Value objects are `@dataclass`; immutable records are
  `@dataclass(frozen=True)`.
- Hot-path records additionally use `slots=True` (`DecisionRecord`) — field
  `__dict__` is sacrificed for ~15 µs/1000 cheaper teardown.
- Enums share one vocabulary between the string API and the numeric one:
  `DecisionType`/`GateAction` are `IntEnum` with stable values; `.name`
  strings are what the telemetry persists.

## Imports

- Order: standard library, third party, local; each block alphabetized.
- Import only what you use (`disallow_any_generics` makes unused `Any`
  imports obvious).
- Relative imports within the package (`from .telemetry import RingBuffer`).

## Tests

- pytest, discovered under `src/tests` (configured testpaths + pythonpath).
- One test file per module; a new behavior gets a focused test, an existing
  test stays green.
- Mark slow/expensive scenarios `@pytest.mark.slow` and micro‑benchmarks
  `@pytest.mark.benchmark` (markers declared in `pyproject.toml`).
- Coverage runs exclude benchmarks (`-k "not benchmark"`) — the tracer
  inflates sub-microsecond hot paths. Keep `src/decision_ledger` north of
  90%, and the hot modules (telemetry, gatekeeper, utils) at 100%.
- Prefer deterministic fixtures and seeded/derived pseudo-random values (the
  integration suite uses `_confidence(i)` rather than `random`) so runs are
  reproducible.