pub mod arrow;
pub mod cdr;
pub mod config;
pub mod error;
pub mod ros;

use arrow_array::RecordBatch;
use camino::{Utf8Path, Utf8PathBuf};
use mcap::MessageStream;
use memmap2::Mmap;
use rayon::prelude::*;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use walkdir::WalkDir;

use crate::arrow::ArrowSchemaBuilder;
use crate::arrow::CdrArrowParser;
use crate::config::CompressionSetting;
use crate::ros::extract_message_type;
use crate::ros::CdrRosParser;
use crate::ros::Message;
pub use error::{Result, Rosbag2ParquetError};

pub use cdr::Endianness;
pub use config::{Config, MessageFilter};
pub use ros::{BaseValue, FieldValue, PrimitiveValue};

pub fn rosbag2parquet<P: AsRef<Utf8Path>>(paths: &[P], config: Config) -> Result<()> {
    config.validate()?;

    if paths.is_empty() {
        return Err(Rosbag2ParquetError::ConfigError {
            message: "At least one path is required.".to_string(),
        });
    }

    let results = if paths.len() == 1 && paths[0].as_ref().is_dir() {
        rosbag2parquet_parallel_from_dir(&paths[0], config)
    } else {
        rosbag2parquet_parallel(paths, config)
    }?;

    if let Some(err) = results.into_iter().find_map(|r| r.err()) {
        return Err(err);
    }
    Ok(())
}

pub fn rosbag2parquet_parallel_from_dir<P: AsRef<Utf8Path>>(
    dir: &P,
    config: Config,
) -> Result<Vec<Result<()>>> {
    config.validate()?;
    let paths = find_rosbags_recursive(dir.as_ref())?;
    rosbag2parquet_parallel(&paths, config)
}

fn find_rosbags_recursive(path: &Utf8Path) -> Result<Vec<Utf8PathBuf>> {
    let mut rosbag_paths = Vec::new();

    for entry in WalkDir::new(path).into_iter().filter_map(|e| e.ok()) {
        let entry_path = Utf8PathBuf::from_path_buf(entry.into_path()).map_err(|p| {
            Rosbag2ParquetError::ConfigError {
                message: format!("Invalid UTF-8 path found: {:?}", p),
            }
        })?;

        if entry_path.is_file() && entry_path.extension() == Some("mcap") {
            rosbag_paths.push(entry_path);
        }
    }

    Ok(rosbag_paths)
}

pub fn rosbag2parquet_parallel<P: AsRef<Utf8Path>>(
    paths: &[P],
    config: Config,
) -> Result<Vec<Result<()>>> {
    for path in paths {
        if path.as_ref().is_dir() {
            return Err(Rosbag2ParquetError::ConfigError {
                message: "Paths should be files if multiple paths are provided".to_string(),
            });
        }
    }

    config.validate()?;
    let root_output_dir = config
        .output_dir()
        .cloned()
        .unwrap_or_else(|| Utf8PathBuf::from("parquet"));

    if let Some(threads) = config.threads() {
        rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build_global()
            .map_err(|e| Rosbag2ParquetError::ConfigError {
                message: format!("Failed to build thread pool: {e}"),
            })?;
    }

    let path_bufs: Vec<Utf8PathBuf> = paths.iter().map(|p| p.as_ref().to_path_buf()).collect();

    let results = path_bufs
        .par_iter()
        .map(|path| {
            let mut single_rosbag_config = config.clone();
            let dir = root_output_dir.clone().join(path.file_stem().unwrap());

            single_rosbag_config = single_rosbag_config.set_output_dir(Some(dir));
            rosbag2parquet_single(path, single_rosbag_config)
        })
        .collect::<Vec<_>>();
    Ok(results)
}

pub fn rosbag2parquet_single<P: AsRef<Utf8Path>>(path: &P, config: Config) -> Result<()> {
    config.validate()?;
    let record_batches = rosbag2record_batches(path, config.message_filter())?;
    let output_dir = config.output_dir().cloned().unwrap_or_else(|| {
        path.as_ref()
            .parent()
            .map(|p| p.join("parquet"))
            .unwrap_or_else(|| Utf8PathBuf::from("parquet"))
    });
    let compression = config.compression();
    write_record_batches_to_parquet_with_options(record_batches, &output_dir, compression)?;
    Ok(())
}

pub fn rosbag2record_batches<P: AsRef<Utf8Path>>(
    path: &P,
    message_filter: &MessageFilter,
) -> Result<HashMap<String, RecordBatch>> {
    let mcap_file = read_mcap(path)?;

    let mut type_registry = HashMap::new();
    let mut topic_name_type_table = HashMap::new();
    let mut skipped_topic_names = HashSet::new();
    let message_stream =
        MessageStream::new(&mcap_file).map_err(|e| Rosbag2ParquetError::ConfigError {
            message: format!("Failed to create message stream: {}", e),
        })?;

    for (index, message_result) in message_stream.enumerate() {
        let message = message_result.map_err(|e| Rosbag2ParquetError::ParseError {
            topic: "unknown".to_string(),
            index,
            message: format!("Failed to read message: {}", e),
        })?;
        let Some(schema) = &message.channel.schema else {
            continue;
        };

        let topic_name = &message.channel.topic;
        if !message_filter.matches(&message) {
            continue;
        }
        if schema.data.is_empty() {
            if !skipped_topic_names.contains(topic_name) {
                info_topic_skipped(topic_name);
                skipped_topic_names.insert(topic_name.clone());
            }
            continue;
        }

        let type_name = extract_message_type(&schema.name).to_string();
        topic_name_type_table.insert(topic_name.clone(), type_name.clone());
        type_registry
            .entry(type_name)
            .or_insert_with(|| schema.data.clone());
    }

    let mut msg_definition_table = HashMap::new();
    for (type_name, schema_data) in &type_registry {
        let schema_text = std::str::from_utf8(schema_data)?;
        let sections = ros::parse_schema_sections(type_name, schema_text);
        ros::parse_msg_definition_from_schema_section(&sections, &mut msg_definition_table)?;
    }

    let mut schemas = ArrowSchemaBuilder::new(&msg_definition_table).build_all()?;
    let mut parser =
        CdrArrowParser::new(&topic_name_type_table, &msg_definition_table, &mut schemas).map_err(
            |e| Rosbag2ParquetError::SchemaError {
                type_name: "unknown".to_string(),
                message: e.to_string(),
            },
        )?;
    let message_stream =
        MessageStream::new(&mcap_file).map_err(|e| Rosbag2ParquetError::ConfigError {
            message: format!("Failed to create message stream: {}", e),
        })?;
    for (index, message_result) in message_stream.enumerate() {
        let message = message_result.map_err(|e| Rosbag2ParquetError::ParseError {
            topic: "unknown".to_string(),
            index,
            message: format!("Failed to read message: {}", e),
        })?;

        if !message_filter.matches(&message) {
            continue;
        }

        let Some(schema) = &message.channel.schema else {
            continue;
        };

        let topic_name = &message.channel.topic;
        if schema.data.is_empty() || !topic_name_type_table.contains_key(topic_name) {
            continue;
        }
        if let Err(e) = parser.parse(topic_name, &message.data, message.log_time as i64) {
            warn_parse_failure(index, topic_name, &e);
            continue;
        }
    }

    let record_batches = parser
        .finish()
        .map_err(|e| Rosbag2ParquetError::SchemaError {
            type_name: "unknown".to_string(),
            message: e.to_string(),
        })?;

    Ok(record_batches)
}

fn read_mcap<P: AsRef<Utf8Path>>(path: P) -> Result<Mmap> {
    let fd = fs::File::open(path.as_ref()).map_err(|e| {
        Rosbag2ParquetError::Io(std::io::Error::new(
            e.kind(),
            format!("Couldn't open MCap file '{}': {}", path.as_ref(), e),
        ))
    })?;
    unsafe { Mmap::map(&fd) }.map_err(|e| {
        Rosbag2ParquetError::Io(std::io::Error::new(
            e.kind(),
            format!("Couldn't map MCap file '{}': {}", path.as_ref(), e),
        ))
    })
}

pub fn write_record_batches_to_parquet<P: AsRef<Utf8Path>>(
    record_batches: HashMap<String, RecordBatch>,
    root_dir_path: P,
) -> Result<()> {
    write_record_batches_to_parquet_with_options(
        record_batches,
        root_dir_path,
        CompressionSetting::new(parquet::basic::Compression::SNAPPY, None),
    )
}

pub fn write_record_batches_to_parquet_with_options<P: AsRef<Utf8Path>>(
    record_batches: HashMap<String, RecordBatch>,
    root_dir_path: P,
    compression: CompressionSetting,
) -> Result<()> {
    if !root_dir_path.as_ref().exists() {
        fs::create_dir_all(root_dir_path.as_ref())?;
    }

    for (name, record_batch) in record_batches {
        let rel = topic_to_rel_file(&name)?;
        let path = Utf8PathBuf::from(root_dir_path.as_ref()).join(rel);
        let dir_path = path
            .parent()
            .ok_or_else(|| Rosbag2ParquetError::ConfigError {
                message: format!("Invalid path: {}", path),
            })?;
        if !dir_path.exists() {
            fs::create_dir_all(dir_path)?;
        }

        let file = std::fs::File::create(&path)?;
        let props = parquet::file::properties::WriterProperties::builder()
            .set_compression(compression.kind())
            .build();
        let mut writer = parquet::arrow::arrow_writer::ArrowWriter::try_new(
            file,
            record_batch.schema(),
            Some(props),
        )?;
        writer.write(&record_batch)?;
        writer.close()?;
    }
    Ok(())
}

pub fn rosbag2ros_msg_values<P: AsRef<Utf8Path>>(path: P) -> Result<Vec<Message>> {
    let mcap_file = read_mcap(path)?;

    let mut type_registry = HashMap::new();
    let message_stream =
        MessageStream::new(&mcap_file).map_err(|e| Rosbag2ParquetError::ConfigError {
            message: format!("Failed to create message stream: {}", e),
        })?;

    for (index, message_result) in message_stream.enumerate() {
        let message = message_result.map_err(|e| Rosbag2ParquetError::ParseError {
            topic: "unknown".to_string(),
            index,
            message: format!("Failed to read message: {}", e),
        })?;

        if let Some(schema) = &message.channel.schema {
            let type_name = extract_message_type(&schema.name).to_string();
            type_registry
                .entry(type_name)
                .or_insert_with(|| schema.data.clone());
        }
    }

    let mut msg_definition_table = HashMap::new();
    for (type_name, schema_data) in &type_registry {
        let schema_text = std::str::from_utf8(schema_data)?;
        let sections = ros::parse_schema_sections(type_name, schema_text);
        ros::parse_msg_definition_from_schema_section(&sections, &mut msg_definition_table)?;
    }

    let mut parsed_messages = Vec::new();
    let message_stream =
        MessageStream::new(&mcap_file).map_err(|e| Rosbag2ParquetError::ConfigError {
            message: format!("Failed to create message stream: {}", e),
        })?;

    let mut cdr_ros_parser = CdrRosParser::new(&msg_definition_table);
    for (index, message_result) in message_stream.enumerate() {
        let message = message_result.map_err(|e| Rosbag2ParquetError::ParseError {
            topic: "unknown".to_string(),
            index,
            message: format!("Failed to read message: {}", e),
        })?;

        if let Some(schema) = &message.channel.schema {
            let type_name = extract_message_type(&schema.name).to_string();
            let parsed_message = cdr_ros_parser
                .parse(&type_name, &message.data)
                .map_err(|e| Rosbag2ParquetError::ParseError {
                    topic: "unknown".to_string(),
                    index,
                    message: format!("Failed to parse message with type {}: {}", type_name, e),
                })?;
            parsed_messages.push(parsed_message);
        }
    }

    Ok(parsed_messages)
}

fn info_topic_skipped(topic_name: &str) {
    println!("{topic_name} topic is skipped because it has no schema text.");
}

fn warn_parse_failure(index: usize, topic_name: &str, err: &dyn std::error::Error) {
    eprintln!(
        "Warning: Failed to parse message {} on topic {}: {}",
        index, topic_name, err
    );
}

fn topic_to_rel_file(topic_name: &str) -> Result<String> {
    let trimmed = topic_name.trim_start_matches('/');
    let mut parts: Vec<&str> = Vec::new();
    for seg in trimmed.split('/') {
        if seg.is_empty() {
            continue;
        }
        if seg == "." || seg == ".." {
            return Err(Rosbag2ParquetError::ConfigError {
                message: format!("invalid topic segment: {}", seg),
            });
        }
        parts.push(seg);
    }
    let rel = if parts.is_empty() {
        "topic".to_string()
    } else {
        parts.join("/")
    };
    Ok(format!("{}.parquet", rel))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use ::arrow::datatypes::Float64Type;
    use arrow_array::{
        FixedSizeListArray, Float64Array, Int32Array, ListArray, StringArray, StructArray,
        UInt32Array,
    };

    use super::*;
    use crate::ros::test_helpers::*;

    #[test]
    fn test_rosbag2record_batches_invalid_file() {
        let result = rosbag2record_batches(&"nonexistent.mcap", &MessageFilter::default());
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("Couldn't open MCap file"));
    }

    #[test]
    fn test_write_record_batches_invalid_path() {
        use std::collections::HashMap;
        let empty_batches: HashMap<String, RecordBatch> = HashMap::new();
        let result = write_record_batches_to_parquet(empty_batches, "/invalid\0path/");
        assert!(result.is_err());
    }

    fn assert_struct_field_equals<T>(struct_array: &StructArray, field_name: &str, expected: T)
    where
        T: arrow_array::Array,
    {
        let column = struct_array.column_by_name(field_name).unwrap_or_else(|| {
            panic!(
                "Column '{}' not found in struct. Available columns: {:?}",
                field_name,
                struct_array
                    .fields()
                    .iter()
                    .map(|f| f.name())
                    .collect::<Vec<_>>()
            )
        });
        assert_eq!(
            *column.as_ref(),
            *Arc::new(expected).as_ref(),
            "Field '{field_name}' values don't match"
        );
    }

    fn assert_column_equals<T>(batch: &RecordBatch, column_name: &str, expected: T)
    where
        T: arrow_array::Array,
    {
        let column = batch.column_by_name(column_name).unwrap_or_else(|| {
            panic!(
                "Column '{}' not found in batch. Available columns: {:?}",
                column_name,
                batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name())
                    .collect::<Vec<_>>()
            )
        });
        assert_eq!(
            *column.as_ref(),
            *Arc::new(expected).as_ref(),
            "Column '{column_name}' values don't match"
        );
    }

    fn assert_fixed_size_list_equals(
        batch: &RecordBatch,
        column_name: &str,
        expected: FixedSizeListArray,
    ) {
        let column = batch.column_by_name(column_name).unwrap_or_else(|| {
            panic!(
                "Column '{}' not found in batch. Available columns: {:?}",
                column_name,
                batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name())
                    .collect::<Vec<_>>()
            )
        });
        let actual = column
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap_or_else(|| {
                panic!(
                    "Column '{}' is not a FixedSizeListArray, actual type: {:?}",
                    column_name,
                    column.data_type()
                )
            });
        assert_eq!(
            actual, &expected,
            "FixedSizeListArray '{column_name}' values don't match"
        );
    }

    fn assert_list_equals(batch: &RecordBatch, column_name: &str, expected: ListArray) {
        let column = batch.column_by_name(column_name).unwrap_or_else(|| {
            panic!(
                "Column '{}' not found in batch. Available columns: {:?}",
                column_name,
                batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name())
                    .collect::<Vec<_>>()
            )
        });
        let actual = column
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap_or_else(|| {
                panic!(
                    "Column '{}' is not a ListArray, actual type: {:?}",
                    column_name,
                    column.data_type()
                )
            });
        assert_eq!(
            actual, &expected,
            "ListArray '{column_name}' values don't match"
        );
    }

    #[test]
    fn test_rosbag2ros_msg_values() {
        let test_path = "../testdata/base_msgs/base_msgs_0.mcap";
        let ros_msg_values = rosbag2ros_msg_values(test_path)
            .expect("Failed to parse ROS messages from test MCAP file");

        let mut expected_ros_msg_values = Vec::new();

        let linear_msg_value = create_vector3_message(1.2, 0.0, 0.0);
        let angular_msg_value = create_vector3_message(0.0, 0.0, -0.6);
        let twist_msg_value = Message {
            name: "Twist".to_string(),
            value: vec![
                ros::Field::new(
                    "linear".to_string(),
                    FieldValue::Base(BaseValue::Complex(linear_msg_value)),
                ),
                ros::Field::new(
                    "angular".to_string(),
                    FieldValue::Base(BaseValue::Complex(angular_msg_value)),
                ),
            ],
        };
        expected_ros_msg_values.push(twist_msg_value);

        expected_ros_msg_values.push(create_vector3_message(1.1, 2.2, 3.3));
        expected_ros_msg_values.push(create_string_message("Hello, World!"));

        assert_eq!(ros_msg_values.len(), expected_ros_msg_values.len());
        assert_eq!(ros_msg_values, expected_ros_msg_values);
    }

    #[test]
    fn test_topic_to_rel_file_reject_parent() {
        let r = topic_to_rel_file("/one//shot/../imu");
        assert!(r.is_err());
    }

    #[test]
    fn test_topic_to_rel_file_keep_chars() {
        let s = topic_to_rel_file("/foo:bar*<baz>").unwrap();
        assert_eq!(s, "foo:bar*<baz>.parquet");
    }

    #[test]
    fn test_topic_to_rel_file_empty() {
        let s = topic_to_rel_file("").unwrap();
        assert_eq!(s, "topic.parquet");
    }

    #[test]
    fn test_rosbag2ros_msg_values_array() {
        let test_path = "../testdata/array_msgs/array_msgs_0.mcap";
        let ros_msg_values = rosbag2ros_msg_values(test_path)
            .expect("Failed to parse ROS array messages from test MCAP file");

        let mut expected_ros_msg_values = Vec::new();

        let imu_time_msg_value = create_time_message(0, 0);
        let imu_header_msg_value = create_header_message(imu_time_msg_value, "imu_link");
        let imu_orientation_msg_value = create_quaternion_message(0.0, 0.0, 0.0, 1.0);

        let imu_msg_value = Message {
            name: "Imu".to_string(),
            value: vec![
                ros::Field::new(
                    "header".to_string(),
                    FieldValue::Base(BaseValue::Complex(imu_header_msg_value)),
                ),
                ros::Field::new(
                    "orientation".to_string(),
                    FieldValue::Base(BaseValue::Complex(imu_orientation_msg_value)),
                ),
                ros::Field::new(
                    "orientation_covariance".to_string(),
                    create_float64_array(vec![0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]),
                ),
                ros::Field::new(
                    "angular_velocity".to_string(),
                    FieldValue::Base(BaseValue::Complex(create_vector3_message(0.1, 0.2, 0.3))),
                ),
                ros::Field::new(
                    "angular_velocity_covariance".to_string(),
                    create_float64_array(vec![0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1]),
                ),
                ros::Field::new(
                    "linear_acceleration".to_string(),
                    FieldValue::Base(BaseValue::Complex(create_vector3_message(1.0, 2.0, 3.0))),
                ),
                ros::Field::new(
                    "linear_acceleration_covariance".to_string(),
                    create_float64_array(vec![0.1, 0.0, 0.0, 0.0, 0.1, 0.0, 0.0, 0.0, 0.1]),
                ),
            ],
        };
        expected_ros_msg_values.push(imu_msg_value);

        let joint_state_time_msg_value = create_time_message(0, 0);
        let joint_state_header_msg_value = create_header_message(joint_state_time_msg_value, "");

        let joint_state_msg_value = Message {
            name: "JointState".to_string(),
            value: vec![
                ros::Field::new(
                    "header".to_string(),
                    FieldValue::Base(BaseValue::Complex(joint_state_header_msg_value)),
                ),
                ros::Field::new(
                    "name".to_string(),
                    create_string_sequence(vec!["joint1", "joint2", "joint3"]),
                ),
                ros::Field::new(
                    "position".to_string(),
                    create_float64_sequence(vec![1.5, -0.5, 0.8]),
                ),
                ros::Field::new(
                    "velocity".to_string(),
                    create_float64_sequence(vec![0.1, 0.2, 0.3]),
                ),
                ros::Field::new(
                    "effort".to_string(),
                    create_float64_sequence(vec![10.1, 10.2, 10.3]),
                ),
            ],
        };
        expected_ros_msg_values.push(joint_state_msg_value);

        assert_eq!(ros_msg_values.len(), expected_ros_msg_values.len());
        assert_eq!(ros_msg_values[0], expected_ros_msg_values[0]);
    }

    #[test]
    fn test_cdr_arrow_parser() {
        let test_path = "../testdata/base_msgs/base_msgs_0.mcap";
        let record_batches = rosbag2record_batches(&test_path, &MessageFilter::default()).unwrap();

        let twist_batch = record_batches
            .get("/one_shot/twist")
            .expect("Twist message type not found in record batches");
        let linear_array = twist_batch
            .column_by_name("linear")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        assert_struct_field_equals(linear_array, "x", Float64Array::from(vec![1.2]));
        assert_struct_field_equals(linear_array, "y", Float64Array::from(vec![0.0]));
        assert_struct_field_equals(linear_array, "z", Float64Array::from(vec![0.0]));

        let angular_array = twist_batch
            .column_by_name("angular")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        assert_struct_field_equals(angular_array, "x", Float64Array::from(vec![0.0]));
        assert_struct_field_equals(angular_array, "y", Float64Array::from(vec![0.0]));
        assert_struct_field_equals(angular_array, "z", Float64Array::from(vec![-0.6]));

        let vector3_batch = record_batches
            .get("/one_shot/vector3")
            .expect("Vector3 message type not found in record batches");
        assert_column_equals(vector3_batch, "x", Float64Array::from(vec![1.1]));
        assert_column_equals(vector3_batch, "y", Float64Array::from(vec![2.2]));
        assert_column_equals(vector3_batch, "z", Float64Array::from(vec![3.3]));

        let string_batch = record_batches
            .get("/one_shot/string")
            .expect("String message type not found in record batches");
        assert_column_equals(
            string_batch,
            "data",
            StringArray::from(vec!["Hello, World!"]),
        );

        write_record_batches_to_parquet(record_batches, "../testdata/base_msgs/parquet")
            .expect("Failed to write Parquet files in test");
    }

    #[test]
    fn test_cdr_arrow_parser_array() {
        let test_path = "../testdata/array_msgs/array_msgs_0.mcap";
        let record_batches = rosbag2record_batches(&test_path, &MessageFilter::default()).unwrap();

        let imu_batch = record_batches.get("/one_shot/imu").unwrap();

        let header_array = imu_batch
            .column_by_name("header")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let stamp_array = header_array
            .column_by_name("stamp")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        assert_struct_field_equals(stamp_array, "sec", Int32Array::from(vec![0]));
        assert_struct_field_equals(stamp_array, "nanosec", UInt32Array::from(vec![0]));
        assert_struct_field_equals(
            header_array,
            "frame_id",
            StringArray::from(vec!["imu_link"]),
        );

        let orientation_array = imu_batch
            .column_by_name("orientation")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_struct_field_equals(orientation_array, "x", Float64Array::from(vec![0.0]));
        assert_struct_field_equals(orientation_array, "y", Float64Array::from(vec![0.0]));
        assert_struct_field_equals(orientation_array, "z", Float64Array::from(vec![0.0]));
        assert_struct_field_equals(orientation_array, "w", Float64Array::from(vec![1.0]));

        let expected_orientation_covariance_array =
            FixedSizeListArray::from_iter_primitive::<Float64Type, _, _>(
                vec![Some(vec![
                    Some(0.1),
                    Some(0.2),
                    Some(0.3),
                    Some(0.4),
                    Some(0.5),
                    Some(0.6),
                    Some(0.7),
                    Some(0.8),
                    Some(0.9),
                ])],
                9,
            );
        assert_fixed_size_list_equals(
            imu_batch,
            "orientation_covariance",
            expected_orientation_covariance_array,
        );

        let angular_velocity_array = imu_batch
            .column_by_name("angular_velocity")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_struct_field_equals(angular_velocity_array, "x", Float64Array::from(vec![0.1]));
        assert_struct_field_equals(angular_velocity_array, "y", Float64Array::from(vec![0.2]));
        assert_struct_field_equals(angular_velocity_array, "z", Float64Array::from(vec![0.3]));

        let expected_angular_velocity_covariance_array =
            FixedSizeListArray::from_iter_primitive::<Float64Type, _, _>(
                vec![Some(vec![
                    Some(0.9),
                    Some(0.8),
                    Some(0.7),
                    Some(0.6),
                    Some(0.5),
                    Some(0.4),
                    Some(0.3),
                    Some(0.2),
                    Some(0.1),
                ])],
                9,
            );
        assert_fixed_size_list_equals(
            imu_batch,
            "angular_velocity_covariance",
            expected_angular_velocity_covariance_array,
        );

        let linear_acceleration_array = imu_batch
            .column_by_name("linear_acceleration")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_struct_field_equals(
            linear_acceleration_array,
            "x",
            Float64Array::from(vec![1.0]),
        );
        assert_struct_field_equals(
            linear_acceleration_array,
            "y",
            Float64Array::from(vec![2.0]),
        );
        assert_struct_field_equals(
            linear_acceleration_array,
            "z",
            Float64Array::from(vec![3.0]),
        );

        let expected_linear_acceleration_covariance_array =
            FixedSizeListArray::from_iter_primitive::<Float64Type, _, _>(
                vec![Some(vec![
                    Some(0.1),
                    Some(0.0),
                    Some(0.0),
                    Some(0.0),
                    Some(0.1),
                    Some(0.0),
                    Some(0.0),
                    Some(0.0),
                    Some(0.1),
                ])],
                9,
            );
        assert_fixed_size_list_equals(
            imu_batch,
            "linear_acceleration_covariance",
            expected_linear_acceleration_covariance_array,
        );

        let joint_state_batch = record_batches.get("/one_shot/joint_state").unwrap();
        let header_array = joint_state_batch
            .column_by_name("header")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let stamp_array = header_array
            .column_by_name("stamp")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        assert_struct_field_equals(stamp_array, "sec", Int32Array::from(vec![0]));
        assert_struct_field_equals(stamp_array, "nanosec", UInt32Array::from(vec![0]));
        assert_struct_field_equals(header_array, "frame_id", StringArray::from(vec![""]));

        let name_array = joint_state_batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let expected_name_array = Arc::new(StringArray::from(vec!["joint1", "joint2", "joint3"]));
        assert_eq!(
            *name_array
                .value(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap(),
            *expected_name_array.as_ref()
        );

        let expected_position_array = ListArray::from_iter_primitive::<Float64Type, _, _>(vec![
            Some(vec![Some(1.5), Some(-0.5), Some(0.8)]),
        ]);
        assert_list_equals(joint_state_batch, "position", expected_position_array);

        let expected_velocity_array = ListArray::from_iter_primitive::<Float64Type, _, _>(vec![
            Some(vec![Some(0.1), Some(0.2), Some(0.3)]),
        ]);
        assert_list_equals(joint_state_batch, "velocity", expected_velocity_array);

        let expected_effort_array = ListArray::from_iter_primitive::<Float64Type, _, _>(vec![
            Some(vec![Some(10.1), Some(10.2), Some(10.3)]),
        ]);
        assert_list_equals(joint_state_batch, "effort", expected_effort_array);

        write_record_batches_to_parquet(record_batches, "../testdata/array_msgs/parquet")
            .expect("Failed to write Parquet files in test");
    }

    #[ignore]
    #[test]
    fn test_record_batch_builder_large() {
        let mut topic_names = HashSet::new();
        topic_names.insert("/livox/imu".to_string());
        let test_path = "../testdata/r3live/hku_park_00_0.mcap";
        let _record_batches = rosbag2record_batches(&test_path, &MessageFilter::default()).unwrap();
    }

    #[test]
    fn test_topic_filter_include() {
        let test_path = "../testdata/base_msgs/base_msgs_0.mcap";
        let mut message_filter = MessageFilter::default();
        message_filter
            .set_include_topic_names(Some(HashSet::from(["/one_shot/vector3".to_string()])));
        let record_batches = rosbag2record_batches(&test_path, &message_filter).unwrap();

        assert!(record_batches.contains_key("/one_shot/vector3"));
        assert!(!record_batches.contains_key("/one_shot/twist"));
        assert!(!record_batches.contains_key("/one_shot/string"));
    }

    #[test]
    fn test_topic_filter_exclude() {
        let test_path = "../testdata/base_msgs/base_msgs_0.mcap";
        let mut message_filter = MessageFilter::default();
        message_filter
            .set_exclude_topic_names(Some(HashSet::from(["/one_shot/vector3".to_string()])));
        let record_batches = rosbag2record_batches(&test_path, &message_filter).unwrap();

        assert!(!record_batches.contains_key("/one_shot/vector3"));
        assert!(record_batches.contains_key("/one_shot/twist"));
        assert!(record_batches.contains_key("/one_shot/string"));
    }

    #[test]
    fn test_topic_filter_all() {
        let test_path = "../testdata/base_msgs/base_msgs_0.mcap";
        let record_batches = rosbag2record_batches(&test_path, &MessageFilter::default()).unwrap();

        assert!(record_batches.contains_key("/one_shot/vector3"));
        assert!(record_batches.contains_key("/one_shot/twist"));
        assert!(record_batches.contains_key("/one_shot/string"));
    }
}
