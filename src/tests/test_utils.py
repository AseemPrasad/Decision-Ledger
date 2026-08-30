"""Tests for the shared utilities: context hashing, UUIDv7, timestamps,
logging configuration, and input validation.

Covers the ``utils.py`` deliverable spec:

* ``make_context_hash`` is deterministic, 16 bytes, and sensitive to every
  context factor;
* ``generate_uuidv7`` returns 36-character time-ordered UUIDv7 strings;
* ``now_us``/``now_ns`` agree with the wall clock;
* ``setup_logging`` configures console +/- file output idempotently;
* ``validate_confidence`` clamps and warns, ``validate_context_hash`` enforces
  the exact 128-bit length.
"""

from __future__ import annotations

import logging
import re
import time
import uuid

import pytest

from decision_ledger.utils import (
    generate_uuidv7,
    make_context_hash,
    now_ns,
    now_us,
    setup_logging,
    validate_confidence,
    validate_context_hash,
)

KNOWN_DIGEST = "1f810209b58ae19f185af097ca9cf646"


@pytest.fixture(autouse=True)
def _clean_logger_state():
    """Restore the ``decision_ledger`` logger after every test in this module.

    ``setup_logging`` attaches handlers and disables propagation; without
    this, later tests that rely on captured logs (and must see records reach
    the root handler) would silently lose them.
    """
    yield
    log = logging.getLogger("decision_ledger")
    for handler in list(log.handlers):
        handler.close()
        log.removeHandler(handler)
    log.setLevel(logging.NOTSET)
    log.propagate = True


# --- make_context_hash ------------------------------------------------------


def test_make_context_hash_deterministic_and_16_bytes():
    a = make_context_hash("qwen-7b", "routing")
    b = make_context_hash("qwen-7b", "routing")
    assert a == b
    assert isinstance(a, bytes)
    assert len(a) == 16


def test_make_context_hash_pinned_digest():
    # Regression pin: same inputs must produce the same digest on any machine.
    assert make_context_hash("qwen-7b", "routing").hex() == KNOWN_DIGEST


@pytest.mark.parametrize(
    "kw",
    [
        {"model_id": "qwen-1.5b", "task_type": "routing"},
        {"model_id": "qwen-7b", "task_type": "judge"},
        {
            "model_id": "qwen-7b",
            "task_type": "routing",
            "prompt_template_version": "v2",
        },
        {"model_id": "qwen-7b", "task_type": "routing", "quantization": "fp16"},
        {
            "model_id": "qwen-7b",
            "task_type": "routing",
            "adapter_config": "lora-143",
        },
    ],
)
def test_make_context_hash_factor_sensitivity(kw):
    base = make_context_hash("qwen-7b", "routing")
    assert make_context_hash(**kw) != base


def test_make_context_hash_factor_boundaries_cannot_collide():
    assert make_context_hash("a", "bc") != make_context_hash("ab", "c")


# --- generate_uuidv7 --------------------------------------------------------


def test_generate_uuidv7_shape_and_version():
    pattern = r"[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"
    for _ in range(200):
        value = generate_uuidv7()
        assert len(value) == 36
        assert re.fullmatch(pattern, value), value
        assert str(uuid.UUID(value)) == value


def test_generate_uuidv7_time_ordered():
    stamps = [int(generate_uuidv7().replace("-", "")[:12], 16) for _ in range(2000)]
    assert stamps == sorted(stamps), "timestamps out of order"
    time.sleep(0.003)  # cross a millisecond boundary
    assert int(generate_uuidv7().replace("-", "")[:12], 16) > stamps[-1]


# --- timestamps -------------------------------------------------------------


def test_now_us_and_now_ns_sanity():
    a_ns, a_us = now_ns(), now_us()
    assert a_ns > 0 and a_us > 0
    assert now_ns() >= a_ns
    assert abs(now_us() - time.time_ns() // 1000) < 100_000


# --- setup_logging ----------------------------------------------------------


def test_setup_logging_console_only():
    log = setup_logging("WARNING")
    assert log is logging.getLogger("decision_ledger")
    assert log.level == logging.WARNING
    assert any(isinstance(h, logging.StreamHandler) for h in log.handlers)
    assert not any(isinstance(h, logging.FileHandler) for h in log.handlers)
    assert log.propagate is False


def test_setup_logging_file_output(tmp_path):
    log_path = tmp_path / "ledger.log"
    log = setup_logging("INFO", log_file=str(log_path))
    log.info("hello decision ledger ctx=%s", "abc")

    for handler in list(log.handlers):
        handler.close()
        log.removeHandler(handler)

    lines = log_path.read_text().splitlines()
    assert lines, "no lines written to the log file"
    assert re.search(
        r"\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}\] \[INFO\] " r"hello decision ledger ctx=abc",
        lines[-1],
    )


def test_setup_logging_idempotent(tmp_path):
    setup_logging("INFO", log_file=str(tmp_path / "first.log"))
    log = setup_logging("DEBUG", log_file=str(tmp_path / "second.log"))
    assert log.level == logging.DEBUG
    assert len(log.handlers) == 2  # one console + one file, never stacked
    assert sum(isinstance(h, logging.FileHandler) for h in log.handlers) == 1


def test_setup_logging_unknown_level_defaults_to_info():
    assert setup_logging("VERBOSE_NOT_A_LEVEL").level == logging.INFO


# --- validation helpers -----------------------------------------------------


def test_validate_confidence_passthrough():
    assert validate_confidence(0.5) == 0.5
    assert validate_confidence(0.0) == 0.0
    assert validate_confidence(1.0) == 1.0
    assert validate_confidence(0.99) == pytest.approx(0.99)


def test_validate_confidence_clamps_high(caplog):
    assert validate_confidence(1.7) == 1.0
    assert "clamped" in caplog.text


def test_validate_confidence_clamps_low(caplog):
    assert validate_confidence(-0.4) == 0.0
    assert "clamped" in caplog.text


@pytest.mark.parametrize("bad", ["high", None, True, object()])
def test_validate_confidence_rejects_non_numbers(bad):
    with pytest.raises(TypeError):
        validate_confidence(bad)


def test_validate_context_hash_accepts_16_bytes():
    assert validate_context_hash(b"\x01" * 16) is True


@pytest.mark.parametrize(
    "bad",
    [b"\x01" * 15, b"\x01" * 17, b"", "a" * 16, None, 16],
)
def test_validate_context_hash_rejects(bad):
    assert validate_context_hash(bad) is False
