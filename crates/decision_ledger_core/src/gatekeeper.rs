use parking_lot::RwLock;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct RustCalibrationContext {
    pub context_hash: [u8; 16],
    pub q_hat: Option<f64>,
    pub min_sample_size: usize,
    pub current_sample_size: usize,
    pub is_active: bool,
}

#[pyclass]
pub struct PyGatekeeper {
    policy: Arc<RwLock<HashMap<[u8; 16], RustCalibrationContext>>>,
    exploration_rate: f64,
    call_count: AtomicU64,
    delegate_count: AtomicU64,
    escalate_count: AtomicU64,
    explore_count: AtomicU64,
}

#[pymethods]
impl PyGatekeeper {
    #[new]
    #[pyo3(signature = (exploration_rate = 0.02))]
    pub fn new(exploration_rate: f64) -> PyResult<Self> {
        if !(0.0..=1.0).contains(&exploration_rate) {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "exploration_rate must be within [0.0, 1.0]",
            ));
        }

        Ok(Self {
            policy: Arc::new(RwLock::new(HashMap::new())),
            exploration_rate,
            call_count: AtomicU64::new(0),
            delegate_count: AtomicU64::new(0),
            escalate_count: AtomicU64::new(0),
            explore_count: AtomicU64::new(0),
        })
    }

    #[pyo3(signature = (context_hash, q_hat, min_sample_size, current_sample_size, is_active))]
    pub fn set_context(
        &self,
        context_hash: &[u8],
        q_hat: Option<f64>,
        min_sample_size: usize,
        current_sample_size: usize,
        is_active: bool,
    ) -> PyResult<()> {
        if context_hash.len() != 16 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "context_hash must be exactly 16 bytes",
            ));
        }

        let mut hash_arr = [0u8; 16];
        hash_arr.copy_from_slice(context_hash);

        let context = RustCalibrationContext {
            context_hash: hash_arr,
            q_hat,
            min_sample_size,
            current_sample_size,
            is_active,
        };

        self.policy.write().insert(hash_arr, context);
        Ok(())
    }

    pub fn clear_policy(&self) {
        self.policy.write().clear();
    }

    /// Hot-path evaluation with GIL released for true multi-threaded CPU execution.
    pub fn evaluate(
        &self,
        py: Python<'_>,
        context_hash: &[u8],
        model_confidence: f64,
    ) -> PyResult<u8> {
        if context_hash.len() != 16 {
            return Ok(1); // ESCALATE
        }

        let mut hash_arr = [0u8; 16];
        hash_arr.copy_from_slice(context_hash);

        let confidence = if model_confidence.is_nan() || model_confidence < 0.0 {
            0.0
        } else if model_confidence > 1.0 {
            1.0
        } else {
            model_confidence
        };

        // Release Python GIL during read lock and non-conformity comparison
        let action = py.allow_threads(|| {
            let policy_guard = self.policy.read();

            let context = match policy_guard.get(&hash_arr) {
                Some(ctx) => ctx,
                None => {
                    self.escalate_count.fetch_add(1, Ordering::Relaxed);
                    return 1; // ESCALATE
                }
            };

            if !context.is_active || context.current_sample_size < context.min_sample_size {
                self.escalate_count.fetch_add(1, Ordering::Relaxed);
                return 1; // ESCALATE
            }

            let q_hat = match context.q_hat {
                Some(val) => val,
                None => {
                    self.escalate_count.fetch_add(1, Ordering::Relaxed);
                    return 1; // ESCALATE
                }
            };

            // Exploration check
            if self.exploration_rate > 0.0 {
                let current_call = self.call_count.fetch_add(1, Ordering::Relaxed) + 1;
                let period = (1.0 / self.exploration_rate) as u64;
                if period > 0 && (current_call % period == 0) {
                    self.explore_count.fetch_add(1, Ordering::Relaxed);
                    return 2; // EXPLORE_SHADOW
                }
            }

            let non_conformity = 1.0 - confidence;
            if non_conformity <= q_hat {
                self.delegate_count.fetch_add(1, Ordering::Relaxed);
                0 // DELEGATE
            } else {
                self.escalate_count.fetch_add(1, Ordering::Relaxed);
                1 // ESCALATE
            }
        });

        Ok(action)
    }

    pub fn stats(&self) -> (u64, u64, u64, u64) {
        (
            self.call_count.load(Ordering::Relaxed),
            self.delegate_count.load(Ordering::Relaxed),
            self.escalate_count.load(Ordering::Relaxed),
            self.explore_count.load(Ordering::Relaxed),
        )
    }
}
