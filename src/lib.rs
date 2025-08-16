pub mod arrow;
pub mod cdr;
pub mod core;
pub mod ros;

use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;

use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use camino::Utf8Path;
use mcap::MessageStream;
use memmap2::Mmap;

use crate::arrow::ArrowSchemaBuilder;
use crate::arrow::CdrArrowParser;
use crate::ros::extract_message_type;
use crate::ros::CdrRosParser;
use crate::ros::Message;

pub use cdr::Endianness;
pub use ros::{BaseValue, FieldValue, PrimitiveValue};

pub fn rosbag2parquet<P: AsRef<Utf8Path>>(path: P) -> Result<()> {
    let mmap = read_mcap(path)?;
    let message_stream = MessageStream::new(&mmap).context("Failed to create message stream")?;

    for (index, message_result) in message_stream.enumerate() {
        let message = message_result.with_context(|| format!("Failed to read message {index}"))?;

        if let Some(schema) = &message.channel.schema {
            let _schema_name = schema.name.clone();
            let schema_data = schema.data.clone();

            let _schema_text = std::str::from_utf8(&schema_data)?;
            // println!("Schema: {} ({})", schema_name, schema_text);
            // println!();
            // println!();
        }
    }

    Ok(())
}

pub fn rosbag2ros_msg_values<P: AsRef<Utf8Path>>(path: P) -> Result<Vec<Message>> {
    let mcap_file = read_mcap(path)?;

    // First, collect all schema data
    let mut type_registry = HashMap::new();
    let message_stream =
        MessageStream::new(&mcap_file).context("Failed to create message stream")?;

    let max_message_count = 10;
    let mut message_count = 0;
    for (index, message_result) in message_stream.enumerate() {
        let message = message_result.with_context(|| format!("Failed to read message {index}"))?;

        if let Some(schema) = &message.channel.schema {
            let type_name = extract_message_type(&schema.name).to_string();
            type_registry
                .entry(type_name)
                .or_insert_with(|| schema.data.clone());

            message_count += 1;
            if message_count >= max_message_count {
                break;
            }
        }
    }

    // Build message definition table from collected schemas
    let mut msg_definition_table = HashMap::new();
    for (type_name, schema_data) in &type_registry {
        let schema_text = std::str::from_utf8(schema_data)?;
        let sections = ros::parse_schema_sections(type_name, schema_text);
        ros::parse_msg_definition_from_schema_section(&sections, &mut msg_definition_table);
    }

    // Process messages
    let mut parsed_messages = Vec::new();
    let message_stream =
        MessageStream::new(&mcap_file).context("Failed to create message stream")?;

    message_count = 0;
    let mut cdr_ros_parser = CdrRosParser::new(&msg_definition_table);
    for (index, message_result) in message_stream.enumerate() {
        let message = message_result.with_context(|| format!("Failed to read message {index}"))?;

        if let Some(schema) = &message.channel.schema {
            let type_name = extract_message_type(&schema.name).to_string();
            let parsed_message = cdr_ros_parser.parse(&type_name, &message.data);
            parsed_messages.push(parsed_message);

            message_count += 1;
            if message_count >= max_message_count {
                break;
            }
        }
    }

    Ok(parsed_messages)
}

pub fn rosbag2record_batches<P: AsRef<Utf8Path>>(
    path: P,
    topic_filter: Option<HashSet<String>>,
) -> Result<HashMap<String, RecordBatch>> {
    let mcap_file = read_mcap(path)?;

    // First, collect all schema data
    let mut type_registry = HashMap::new();
    let message_stream =
        MessageStream::new(&mcap_file).context("Failed to create message stream")?;

    for (index, message_result) in message_stream.enumerate() {
        let message = message_result.with_context(|| format!("Failed to read message {index}"))?;

        if let Some(schema) = &message.channel.schema {
            if let Some(ref filter) = topic_filter {
                if !filter.contains(&schema.name) {
                    continue;
                }
            }

            let type_name = extract_message_type(&schema.name).to_string();
            type_registry
                .entry(type_name)
                .or_insert_with(|| schema.data.clone());
        }
    }

    // Build message definition table from collected schemas
    let mut msg_definition_table = HashMap::new();
    for (type_name, schema_data) in &type_registry {
        let schema_text = std::str::from_utf8(schema_data)?;
        let sections = ros::parse_schema_sections(type_name, schema_text);
        ros::parse_msg_definition_from_schema_section(&sections, &mut msg_definition_table);
    }

    // Process messages
    let message_stream =
        MessageStream::new(&mcap_file).context("Failed to create message stream")?;

    let converter = ArrowSchemaBuilder::new(&msg_definition_table);
    let mut schemas = converter.build_all()?;

    let mut parser = CdrArrowParser::new(&msg_definition_table, &mut schemas);
    for (index, message_result) in message_stream.enumerate() {
        let message = message_result.with_context(|| format!("Failed to read message {index}"))?;

        if let Some(schema) = &message.channel.schema {
            if let Some(ref filter) = topic_filter {
                if !filter.contains(&schema.name) {
                    continue;
                }
            }

            let type_name = extract_message_type(&schema.name).to_string();
            parser.parse(type_name, &message.data);
        }
    }

    let record_batches = parser.finish();

    Ok(record_batches)
}

fn read_mcap<P: AsRef<Utf8Path>>(path: P) -> Result<Mmap> {
    let fd = fs::File::open(path.as_ref()).context("Couldn't open MCap file")?;
    unsafe { Mmap::map(&fd) }.context("Couldn't map MCap file")
}

pub fn write_record_batches_to_parquet<P: AsRef<Utf8Path>>(
    record_batches: HashMap<String, RecordBatch>,
    path: P,
) {
    let path = path.as_ref().join("parquet");
    if !path.exists() {
        fs::create_dir_all(&path).unwrap();
    }

    for (name, record_batch) in record_batches {
        let file = std::fs::File::create(format!("{}/{}.parquet", &path, name)).unwrap();
        let props = parquet::file::properties::WriterProperties::builder()
            .set_compression(parquet::basic::Compression::SNAPPY)
            .build();
        let mut writer = parquet::arrow::arrow_writer::ArrowWriter::try_new(
            file,
            record_batch.schema(),
            Some(props),
        )
        .unwrap();
        writer.write(&record_batch).expect("Writing batch failed");
        writer.close().unwrap();
    }
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

    // Helper function to assert struct field equality with better error messages
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

    // Helper function to assert column equality with better error messages
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

    // Helper function to assert FixedSizeListArray equality with better error messages
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

    // Helper function to assert ListArray equality with better error messages
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
        let test_path = "rosbags/non_array_msgs/non_array_msgs_0.mcap";
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
    fn test_rosbag2ros_msg_values_array() {
        let test_path = "rosbags/array_msgs/array_msgs_0.mcap";
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

        // assert_eq!(ros_msg_values.len(), expected_ros_msg_values.len());
        assert_eq!(ros_msg_values[0], expected_ros_msg_values[0]);
    }

    #[test]
    fn test_cdr_arrow_parser() {
        let test_path = "rosbags/non_array_msgs/non_array_msgs_0.mcap";
        let record_batches = rosbag2record_batches(test_path, None).unwrap();

        println!("keys: {:?}", record_batches.keys());

        let twist_batch = record_batches
            .get("Twist")
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
            .get("Vector3")
            .expect("Vector3 message type not found in record batches");
        assert_column_equals(vector3_batch, "x", Float64Array::from(vec![1.1]));
        assert_column_equals(vector3_batch, "y", Float64Array::from(vec![2.2]));
        assert_column_equals(vector3_batch, "z", Float64Array::from(vec![3.3]));

        let string_batch = record_batches
            .get("String")
            .expect("String message type not found in record batches");
        assert_column_equals(
            string_batch,
            "data",
            StringArray::from(vec!["Hello, World!"]),
        );

        write_record_batches_to_parquet(record_batches, "rosbags/non_array_msgs");
    }

    #[test]
    fn test_cdr_arrow_parser_array() {
        let test_path = "rosbags/array_msgs/array_msgs_0.mcap";
        let record_batches = rosbag2record_batches(test_path, None).unwrap();

        let imu_batch = record_batches.get("Imu").unwrap();

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

        // ********** JointState **********

        let joint_state_batch = record_batches.get("JointState").unwrap();
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

        write_record_batches_to_parquet(record_batches, "rosbags/array_msgs");
    }

    #[test]
    fn test_record_batch_builder_large() {
        let mut topic_names = HashSet::new();
        topic_names.insert("sensor_msgs/msg/JointState".to_string());
        // topic_names.insert("geometry_msgs/msg/QuaternionStamped".to_string());
        // topic_names.insert("sensor_msgs/msg/PointCloud2".to_string());
        // topic_names.insert("geometry_msgs/msg/PointStamped".to_string());
        // topic_names.insert("std_msgs/msg/String".to_string());
        // topic_names.insert("std_msgs/msg/Float64".to_string());
        // topic_names.insert("geometry_msgs/msg/TwistStamped".to_string());
        // topic_names.insert("sensor_msgs/msg/Image".to_string());
        // topic_names.insert("visualization_msgs/msg/MarkerArray".to_string());
        // topic_names.insert("nav_msgs/msg/OccupancyGrid".to_string());
        // topic_names.insert("diagnostic_msgs/msg/DiagnosticArray".to_string());
        // topic_names.insert("diagnostic_msgs/msg/DiagnosticStatus".to_string());
        // topic_names.insert("tf2_msgs/msg/TFMessage".to_string());

        let test_path = "rosbags/large2/large2.mcap";
        let _record_batches = rosbag2record_batches(test_path, Some(topic_names)).unwrap();
        // let record_batches =
        //     rosbag2record_batches_with_topic_names(test_path, topic_names).unwrap();
        // write_record_batches_to_parquet(record_batches, "rosbags/large2");
    }
}
