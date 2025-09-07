use std::path::PathBuf;

use camino::Utf8PathBuf;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use rosbag2parquet::{self, Config};

#[pyfunction]
#[pyo3(signature = (path, output_dir=None, topics=None, exclude=None, start_time=None, end_time=None))]
fn convert(
    py: Python<'_>,
    path: PathBuf,
    output_dir: Option<PathBuf>,
    topics: Option<Vec<String>>,
    exclude: Option<Vec<String>>,
    start_time: Option<u64>,
    end_time: Option<u64>,
) -> PyResult<()> {
    let utf8_path = Utf8PathBuf::try_from(path)
        .map_err(|e| PyValueError::new_err(format!("Invalid UTF-8 path: {e}")))?;

    let topics_set = topics.map(|v| v.into_iter().collect());
    let exclude_set = exclude.map(|v| v.into_iter().collect());

    py.allow_threads(|| {
        let output_dir_utf8 = match output_dir {
            Some(p) => Some(
                Utf8PathBuf::try_from(p)
                    .map_err(|e| PyValueError::new_err(format!("Invalid UTF-8 path: {e}")))?,
            ),
            None => None,
        };

        let config = Config::default()
            .set_include_topic_names(topics_set)
            .set_exclude_topic_names(exclude_set)
            .set_start_time(start_time)
            .set_end_time(end_time)
            .set_output_dir(output_dir_utf8);
        match rosbag2parquet::rosbag2parquet(&utf8_path, config) {
            Ok(()) => Ok(()),
            Err(e) => Err(PyValueError::new_err(format!(
                "rosbag2parquet failed: {}",
                e
            ))),
        }
    })
}

#[pymodule]
fn _rosbag2parquet(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(convert, m)?)?;
    Ok(())
}
