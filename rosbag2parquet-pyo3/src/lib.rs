use std::path::PathBuf;

use camino::Utf8PathBuf;
use pyo3::exceptions::{PyIOError, PyValueError};
use pyo3::prelude::*;
use rosbag2parquet::{self, TopicFilter};

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
        if let Some(output) = output_dir {
            let utf8_output = Utf8PathBuf::try_from(output)
                .map_err(|e| PyValueError::new_err(format!("Invalid UTF-8 output path: {e}")))?;

            let record_batches = rosbag2parquet::rosbag2record_batches(&utf8_path, filter)
                .map_err(|e| PyIOError::new_err(format!("Failed to read MCAP file: {e}")))?;

            rosbag2parquet::write_record_batches_to_parquet(record_batches, utf8_output);
        } else {
            rosbag2parquet::rosbag2parquet(&utf8_path, filter);
        }
        Ok(())
    })
}

#[pymodule]
fn _rosbag2parquet(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(convert, m)?)?;
    Ok(())
}
