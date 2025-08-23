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

    let topics_set = match topics.is_some() {
        true => Some(topics.unwrap().into_iter().collect()),
        false => None,
    };
    let exclude_set = match exclude.is_some() {
        true => Some(exclude.unwrap().into_iter().collect()),
        false => None,
    };

    py.allow_threads(|| {
        let config = Config::default()
            .set_include_topic_names(topics_set)
            .set_exclude_topic_names(exclude_set)
            .set_start_time(start_time)
            .set_end_time(end_time)
            .set_output_dir(output_dir.map(|p| Utf8PathBuf::try_from(p).unwrap()));
        rosbag2parquet::rosbag2parquet(&utf8_path, config);
        Ok(())
    })
}

#[pymodule]
fn _rosbag2parquet(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(convert, m)?)?;
    Ok(())
}
