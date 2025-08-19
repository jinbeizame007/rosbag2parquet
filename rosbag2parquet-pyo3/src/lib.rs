use std::path::PathBuf;

use camino::Utf8PathBuf;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use rosbag2parquet::{self, Config, TopicFilter};

#[pyfunction]
#[pyo3(signature = (path, output_dir=None, topics=None))]
fn convert(
    py: Python<'_>,
    path: PathBuf,
    output_dir: Option<PathBuf>,
    topics: Option<Vec<String>>,
) -> PyResult<()> {
    let utf8_path = Utf8PathBuf::try_from(path)
        .map_err(|e| PyValueError::new_err(format!("Invalid UTF-8 path: {e}")))?;

    let filter = match topics {
        Some(topic_list) => TopicFilter::include(topic_list),
        None => TopicFilter::all(),
    };

    py.allow_threads(|| {
        let config = Config::new(
            filter,
            output_dir.map(|p| Utf8PathBuf::try_from(p).unwrap()),
        );
        rosbag2parquet::rosbag2parquet(&utf8_path, config);
        Ok(())
    })
}

#[pymodule]
fn _rosbag2parquet(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(convert, m)?)?;
    Ok(())
}
