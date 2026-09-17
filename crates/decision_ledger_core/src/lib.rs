use pyo3::prelude::*;

mod gatekeeper;
mod hashing;

use gatekeeper::PyGatekeeper;
use hashing::make_context_hash_rust;

/// Decision Ledger Core Rust Extension Module.
#[pymodule]
fn decision_ledger_core(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(make_context_hash_rust, m)?)?;
    m.add_class::<PyGatekeeper>()?;
    Ok(())
}
