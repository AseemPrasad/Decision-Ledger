use blake3::Hasher;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

/// Fast BLAKE3 context hash implementation in Rust.
/// Returns a 16-byte digest as PyBytes.
#[pyfunction]
#[pyo3(signature = (model_id, task_type, prompt_template_version="default", quantization="int8", adapter_config=""))]
pub fn make_context_hash_rust<'py>(
    py: Python<'py>,
    model_id: &str,
    task_type: &str,
    prompt_template_version: &str,
    quantization: &str,
    adapter_config: &str,
) -> PyResult<&'py PyBytes> {
    let mut hasher = Hasher::new();
    let tuple_repr = format!(
        "('{}', '{}', '{}', '{}', '{}')",
        model_id, task_type, prompt_template_version, quantization, adapter_config
    );
    hasher.update(tuple_repr.as_bytes());

    let mut output = [0u8; 16];
    let mut output_reader = hasher.finalize_xof();
    output_reader.fill(&mut output);

    Ok(PyBytes::new(py, &output))
}
