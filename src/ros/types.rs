//! ROS2 message type definitions

use std::collections::HashMap;

use nom::{
    branch::alt,
    bytes::complete::tag,
    combinator::{map, recognize},
    multi::many0,
    sequence::pair,
    IResult, Parser,
};

use super::core::{extract_message_type, is_constant_line, identifier};

#[derive(Clone, Debug, PartialEq)]
pub struct MessageDefinition<'a> {
    pub name: &'a str,
    pub fields: Vec<FieldDefinition<'a>>,
}

impl<'a> MessageDefinition<'a> {
    pub fn new(name: &'a str, fields: Vec<FieldDefinition<'a>>) -> MessageDefinition<'a> {
        MessageDefinition { name, fields }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct FieldDefinition<'a> {
    pub data_type: FieldType,
    pub name: &'a str,
}

impl<'a> FieldDefinition<'a> {
    pub fn new(data_type: FieldType, name: &'a str) -> FieldDefinition<'a> {
        FieldDefinition { data_type, name }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum FieldType {
    Base(BaseType),
    Array { data_type: BaseType, length: u32 },
    Sequence(BaseType),
}

#[derive(Clone, Debug, PartialEq)]
pub enum BaseType {
    Primitive(Primitive),
    Complex(String),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Primitive {
    Bool,
    Byte,
    Char,
    Float32,
    Float64,
    Int8,
    UInt8,
    Int16,
    UInt16,
    Int32,
    UInt32,
    Int64,
    UInt64,
    String,
}

// Runtime data types are now in data.rs

#[derive(Debug, Clone)]
pub struct SchemaSection<'a> {
    pub type_name: &'a str,
    pub content: &'a str,
}

pub fn parse_schema_sections<'a>(
    schema_name: &'a str,
    schema_text: &'a str,
) -> Vec<SchemaSection<'a>> {
    let mut sections = Vec::new();

    let delimiter =
        "================================================================================";

    let raw_sections: Vec<&str> = schema_text
        .split(delimiter)
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();

    for (index, raw_section) in raw_sections.iter().enumerate() {
        let (type_name, content) = if index == 0 {
            (schema_name, *raw_section)
        } else {
            let type_name = raw_section.split_whitespace().nth(1).unwrap_or("");
            let first_newline = raw_section.find('\n').unwrap();
            let content_without_first_line = &raw_section[first_newline + 1..];
            (type_name, content_without_first_line)
        };

        let schema_section = SchemaSection { type_name, content };
        sections.push(schema_section);
    }

    sections
}

pub fn parse_msg_definition_from_schema_section<'a>(
    schema_sections: &[SchemaSection<'a>],
    msg_definition_table: &mut HashMap<&'a str, MessageDefinition<'a>>,
) {
    for schema_section in schema_sections.iter().rev() {
        // Use the short name as the key
        let short_name = extract_message_type(schema_section.type_name);

        if msg_definition_table.contains_key(short_name) {
            continue;
        }

        let mut fields = Vec::new();
        for line in schema_section.content.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with("#") || is_constant_line(trimmed) {
                continue;
            }

            let data_type = line
                .split_whitespace()
                .next()
                .unwrap_or_else(|| {
                    panic!(
                        "Could not extract data type from line: '{}' in section: '{}'",
                        line, schema_section.type_name
                    )
                })
                .rsplit("/")
                .next()
                .unwrap_or_else(|| {
                    panic!(
                        "Could not extract type name from data type: '{}' in section: '{}'",
                        line, schema_section.type_name
                    )
                });
            let name = line.split_whitespace().nth(1).unwrap_or_else(|| {
                panic!(
                    "Could not extract field name from line: '{}' in section: '{}'",
                    line, schema_section.type_name
                )
            });

            let data_type = ros_data_type(data_type)
                .unwrap_or_else(|err| {
                    panic!(
                        "Failed to parse data type '{}' in section '{}': {}",
                        data_type, schema_section.type_name, err
                    )
                })
                .1;
            let field = FieldDefinition::new(data_type, name);
            fields.push(field);
        }

        let msg_definition = MessageDefinition::new(short_name, fields);
        msg_definition_table.insert(short_name, msg_definition);
    }
}


pub fn ros_data_type(input: &str) -> IResult<&str, FieldType> {
    if input.ends_with("[]") {
        let (rest, data_type) = non_array_ros_data_type(input.split_at(input.len() - 2).0)?;
        return Ok((rest, FieldType::Sequence(data_type)));
    }

    if input.ends_with("]") {
        let data_type_and_length = input
            .split_at(input.len() - 1)
            .0
            .split('[')
            .collect::<Vec<&str>>();
        let (rest, data_type) = non_array_ros_data_type(data_type_and_length[0])?;
        let length = data_type_and_length[1].parse::<u32>().unwrap();
        return Ok((rest, FieldType::Array { data_type, length }));
    }

    let (rest, data_type) = non_array_ros_data_type(input)?;
    Ok((rest, FieldType::Base(data_type)))
}

pub fn non_array_ros_data_type(input: &str) -> IResult<&str, BaseType> {
    if let Ok((rest, prim)) = primitive_type(input) {
        return Ok((rest, BaseType::Primitive(prim)));
    }

    // Otherwise, parse as a complex type (package/type format)
    let mut parser = map(
        recognize(pair(identifier, many0(pair(tag("/"), identifier)))),
        |full_type: &str| BaseType::Complex(full_type.to_string()),
    );
    parser.parse(input)
}

pub fn primitive_type(input: &str) -> IResult<&str, Primitive> {
    let mut parser = alt((
        map(tag("bool"), |_| Primitive::Bool),
        map(tag("byte"), |_| Primitive::Byte),
        map(tag("char"), |_| Primitive::Char),
        map(tag("float32"), |_| Primitive::Float32),
        map(tag("float64"), |_| Primitive::Float64),
        map(tag("int8"), |_| Primitive::Int8),
        map(tag("uint8"), |_| Primitive::UInt8),
        map(tag("int16"), |_| Primitive::Int16),
        map(tag("uint16"), |_| Primitive::UInt16),
        map(tag("int32"), |_| Primitive::Int32),
        map(tag("uint32"), |_| Primitive::UInt32),
        map(tag("int64"), |_| Primitive::Int64),
        map(tag("uint64"), |_| Primitive::UInt64),
        map(tag("string"), |_| Primitive::String),
    ));
    parser.parse(input)
}

/// ROSの識別子（フィールド名、パッケージ名など）を認識する
/// 仕様: [a-zA-Z]で始まり、英数字とアンダースコアが続く [2]

#[cfg(test)]
pub mod test_helpers {
    use super::*;
    use super::super::data::{Message, Field, FieldValue, BaseValue, PrimitiveValue};

    // Helper builder for creating field definitions
    pub struct FieldDefBuilder;

    impl FieldDefBuilder {
        pub fn primitive(data_type: Primitive, name: &'static str) -> FieldDefinition<'static> {
            FieldDefinition::new(FieldType::Base(BaseType::Primitive(data_type)), name)
        }

        pub fn complex(type_name: &str, name: &'static str) -> FieldDefinition<'static> {
            FieldDefinition::new(
                FieldType::Base(BaseType::Complex(type_name.to_string())),
                name,
            )
        }

        pub fn sequence(data_type: Primitive, name: &'static str) -> FieldDefinition<'static> {
            FieldDefinition::new(FieldType::Sequence(BaseType::Primitive(data_type)), name)
        }

        pub fn array(
            data_type: Primitive,
            length: u32,
            name: &'static str,
        ) -> FieldDefinition<'static> {
            FieldDefinition::new(
                FieldType::Array {
                    data_type: BaseType::Primitive(data_type),
                    length,
                },
                name,
            )
        }
    }

    // MessageDefinition helper functions using the builder
    pub fn create_vector3_definition() -> MessageDefinition<'static> {
        MessageDefinition::new(
            "Vector3",
            vec![
                FieldDefBuilder::primitive(Primitive::Float64, "x"),
                FieldDefBuilder::primitive(Primitive::Float64, "y"),
                FieldDefBuilder::primitive(Primitive::Float64, "z"),
            ],
        )
    }

    pub fn create_time_definition() -> MessageDefinition<'static> {
        MessageDefinition::new(
            "Time",
            vec![
                FieldDefBuilder::primitive(Primitive::Int32, "sec"),
                FieldDefBuilder::primitive(Primitive::UInt32, "nanosec"),
            ],
        )
    }

    pub fn create_header_definition() -> MessageDefinition<'static> {
        MessageDefinition::new(
            "Header",
            vec![
                FieldDefBuilder::complex("Time", "stamp"),
                FieldDefBuilder::primitive(Primitive::String, "frame_id"),
            ],
        )
    }

    pub fn create_twist_definition() -> MessageDefinition<'static> {
        MessageDefinition::new(
            "Twist",
            vec![
                FieldDefBuilder::complex("Vector3", "linear"),
                FieldDefBuilder::complex("Vector3", "angular"),
            ],
        )
    }

    pub fn create_twist_stamped_definition() -> MessageDefinition<'static> {
        MessageDefinition::new(
            "TwistStamped",
            vec![
                FieldDefBuilder::complex("Header", "header"),
                FieldDefBuilder::complex("Twist", "twist"),
            ],
        )
    }

    pub fn create_joint_state_definition() -> MessageDefinition<'static> {
        MessageDefinition::new(
            "JointState",
            vec![
                FieldDefBuilder::complex("Header", "header"),
                FieldDefBuilder::sequence(Primitive::String, "name"),
                FieldDefBuilder::sequence(Primitive::Float64, "position"),
                FieldDefBuilder::sequence(Primitive::Float64, "velocity"),
                FieldDefBuilder::sequence(Primitive::Float64, "effort"),
            ],
        )
    }

    // Helper builder for creating messages and field values
    pub struct MessageBuilder {
        name: String,
        fields: Vec<Field>,
    }

    impl MessageBuilder {
        pub fn new(name: &str) -> Self {
            Self {
                name: name.to_string(),
                fields: Vec::new(),
            }
        }

        pub fn add_primitive<T>(mut self, field_name: &str, value: T) -> Self
        where
            PrimitiveValue: From<T>,
        {
            self.fields.push(Field::new(
                field_name.to_string(),
                FieldValue::Base(BaseValue::Primitive(value.into())),
            ));
            self
        }

        pub fn add_complex(mut self, field_name: &str, message: Message) -> Self {
            self.fields.push(Field::new(
                field_name.to_string(),
                FieldValue::Base(BaseValue::Complex(message)),
            ));
            self
        }

        pub fn build(self) -> Message {
            Message {
                name: self.name,
                value: self.fields,
            }
        }
    }

    // Implement From traits for common primitive conversions
    impl From<f64> for PrimitiveValue {
        fn from(val: f64) -> Self {
            PrimitiveValue::Float64(val)
        }
    }

    impl From<f32> for PrimitiveValue {
        fn from(val: f32) -> Self {
            PrimitiveValue::Float32(val)
        }
    }

    impl From<i32> for PrimitiveValue {
        fn from(val: i32) -> Self {
            PrimitiveValue::Int32(val)
        }
    }

    impl From<u32> for PrimitiveValue {
        fn from(val: u32) -> Self {
            PrimitiveValue::UInt32(val)
        }
    }

    impl From<String> for PrimitiveValue {
        fn from(val: String) -> Self {
            PrimitiveValue::String(val)
        }
    }

    impl From<&str> for PrimitiveValue {
        fn from(val: &str) -> Self {
            PrimitiveValue::String(val.to_string())
        }
    }

    // Message value creation helper functions using the builder
    pub fn create_vector3_message(x: f64, y: f64, z: f64) -> Message {
        MessageBuilder::new("Vector3")
            .add_primitive("x", x)
            .add_primitive("y", y)
            .add_primitive("z", z)
            .build()
    }

    pub fn create_time_message(sec: i32, nanosec: u32) -> Message {
        MessageBuilder::new("Time")
            .add_primitive("sec", sec)
            .add_primitive("nanosec", nanosec)
            .build()
    }

    pub fn create_header_message(time_msg: Message, frame_id: &str) -> Message {
        MessageBuilder::new("Header")
            .add_complex("stamp", time_msg)
            .add_primitive("frame_id", frame_id)
            .build()
    }

    pub fn create_quaternion_message(x: f64, y: f64, z: f64, w: f64) -> Message {
        MessageBuilder::new("Quaternion")
            .add_primitive("x", x)
            .add_primitive("y", y)
            .add_primitive("z", z)
            .add_primitive("w", w)
            .build()
    }

    pub fn create_string_message(data: &str) -> Message {
        MessageBuilder::new("String")
            .add_primitive("data", data)
            .build()
    }

    // FieldValue creation helper functions
    pub fn create_float64_array(values: Vec<f64>) -> FieldValue {
        FieldValue::Array(
            values
                .into_iter()
                .map(|v| BaseValue::Primitive(PrimitiveValue::Float64(v)))
                .collect(),
        )
    }

    pub fn create_string_sequence(values: Vec<&str>) -> FieldValue {
        FieldValue::Sequence(
            values
                .into_iter()
                .map(|v| BaseValue::Primitive(PrimitiveValue::String(v.to_string())))
                .collect(),
        )
    }

    pub fn create_float64_sequence(values: Vec<f64>) -> FieldValue {
        FieldValue::Sequence(
            values
                .into_iter()
                .map(|v| BaseValue::Primitive(PrimitiveValue::Float64(v)))
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ros::test_helpers::*;

    #[test]
    fn test_ros_data_type() {
        let input = "float64";
        let (rest, data_type) = ros_data_type(input).unwrap();
        assert_eq!(rest, "");
        assert_eq!(
            data_type,
            FieldType::Base(BaseType::Primitive(Primitive::Float64))
        );

        let input = "sensor_msgs/msg/Temperature";
        let (rest, data_type) = ros_data_type(input).unwrap();
        assert_eq!(rest, "");
        assert_eq!(
            data_type,
            FieldType::Base(BaseType::Complex("sensor_msgs/msg/Temperature".to_string(),))
        );
    }

    #[test]
    fn test_parse_schema_sections_single_section() {
        let schema_name = "geometry_msgs/msg/Vector3";
        let schema_text = include_str!("../../testdata/schema/vector3d.txt");
        let sections = parse_schema_sections(schema_name, schema_text);
        assert_eq!(sections.len(), 1);
        assert_eq!(sections[0].type_name, schema_name);

        // ファイル全体の内容（コメント含む）を期待値とする
        assert_eq!(sections[0].content, schema_text.trim());
    }

    #[test]
    fn test_parse_schema_sections_multiple_sections() {
        let schema_name = "sensor_msgs/msg/JointState";
        let schema_text = include_str!("../../testdata/schema/joint_state.txt");

        let sections = parse_schema_sections(schema_name, schema_text);

        assert_eq!(sections.len(), 3);
        assert_eq!(sections[0].type_name, schema_name);

        // 最初のセクションの内容（コメント含む）を期待値とする
        let expected_content = concat!(
            "# This is a message that holds data to describe the state of a set of torque controlled joints.\n",
            "#\n",
            "# The state of each joint (revolute or prismatic) is defined by:\n",
            "#  * the position of the joint (rad or m),\n",
            "#  * the velocity of the joint (rad/s or m/s) and\n",
            "#  * the effort that is applied in the joint (Nm or N).\n",
            "#\n",
            "# Each joint is uniquely identified by its name\n",
            "# The header specifies the time at which the joint states were recorded. All the joint states\n",
            "# in one message have to be recorded at the same time.\n",
            "#\n",
            "# This message consists of a multiple arrays, one for each part of the joint state.\n",
            "# The goal is to make each of the fields optional. When e.g. your joints have no\n",
            "# effort associated with them, you can leave the effort array empty.\n",
            "#\n",
            "# All arrays in this message should have the same size, or be empty.\n",
            "# This is the only way to uniquely associate the joint name with the correct\n",
            "# states.\n",
            "\n",
            "std_msgs/Header header\n",
            "\n",
            "string[] name\n",
            "float64[] position\n",
            "float64[] velocity\n",
            "float64[] effort"
        );
        assert_eq!(sections[0].content, expected_content);

        let expected_type_name = "std_msgs/Header";
        let expected_content = concat!(
            "# Standard metadata for higher-level stamped data types.\n",
            "# This is generally used to communicate timestamped data\n",
            "# in a particular coordinate frame.\n",
            "\n",
            "# Two-integer timestamp that is expressed as seconds and nanoseconds.\n",
            "builtin_interfaces/Time stamp\n",
            "\n",
            "# Transform frame with which this data is associated.\n",
            "string frame_id"
        );
        assert_eq!(sections[1].type_name, expected_type_name);
        assert_eq!(sections[1].content, expected_content);

        let expected_type_name = "builtin_interfaces/Time";
        let expected_content = concat!(
            "# This message communicates ROS Time defined here:\n",
            "# https://design.ros2.org/articles/clock_and_time.html\n",
            "\n",
            "# The seconds component, valid over all int32 values.\n",
            "int32 sec\n",
            "\n",
            "# The nanoseconds component, valid in the range [0, 10e9).\n",
            "uint32 nanosec"
        );
        assert_eq!(sections[2].type_name, expected_type_name);
        assert_eq!(sections[2].content, expected_content);
    }

    #[test]
    fn test_parse_msg_definition_from_schema_section_single_section() {
        let schema_name = "geometry_msgs/msg/Vector3";
        let schema_text = include_str!("../../testdata/schema/vector3d.txt");
        let sections = parse_schema_sections(schema_name, schema_text);
        let mut msg_definition_table = HashMap::new();
        parse_msg_definition_from_schema_section(&sections, &mut msg_definition_table);

        assert_eq!(msg_definition_table.len(), 1);
        assert_eq!(
            msg_definition_table.get("Vector3"),
            Some(&create_vector3_definition())
        );
    }

    #[test]
    fn test_parse_msg_definition_from_schema_section_multiple_sections() {
        let schema_name = "geometry_msgs/msg/TwistStamped";
        let schema_text = include_str!("../../testdata/schema/twist_stamped.txt");
        let sections = parse_schema_sections(schema_name, schema_text);
        let mut msg_definition_table = HashMap::new();
        parse_msg_definition_from_schema_section(&sections, &mut msg_definition_table);

        let mut expected_msg_definition_table = HashMap::new();

        let time_msg_definition = create_time_definition();
        expected_msg_definition_table.insert("Time", time_msg_definition.clone());

        let header_msg_definition = create_header_definition();
        expected_msg_definition_table.insert("Header", header_msg_definition.clone());

        let vector3d_msg_definition = create_vector3_definition();
        expected_msg_definition_table.insert("Vector3", vector3d_msg_definition.clone());

        let twist_msg_definition = create_twist_definition();
        expected_msg_definition_table.insert("Twist", twist_msg_definition.clone());

        let twist_stamped_msg_definition = create_twist_stamped_definition();
        expected_msg_definition_table.insert("TwistStamped", twist_stamped_msg_definition.clone());

        assert_eq!(
            msg_definition_table.len(),
            expected_msg_definition_table.len()
        );
        assert_eq!(
            msg_definition_table
                .keys()
                .cloned()
                .collect::<std::collections::HashSet<_>>(),
            expected_msg_definition_table
                .keys()
                .cloned()
                .collect::<std::collections::HashSet<_>>()
        );
        assert_eq!(
            msg_definition_table.get("Time"),
            expected_msg_definition_table.get("Time")
        );
        assert_eq!(
            msg_definition_table.get("Header"),
            expected_msg_definition_table.get("Header")
        );
        assert_eq!(
            msg_definition_table.get("Vector3"),
            expected_msg_definition_table.get("Vector3")
        );
        assert_eq!(
            msg_definition_table.get("Twist"),
            expected_msg_definition_table.get("Twist")
        );
        assert_eq!(
            msg_definition_table.get("TwistStamped"),
            expected_msg_definition_table.get("TwistStamped")
        );
    }

    #[test]
    fn test_parse_msg_definition_from_schema_section_joint_state() {
        let schema_name = "sensor_msgs/msg/JointState";
        let schema_text = include_str!("../../testdata/schema/joint_state.txt");
        let sections = parse_schema_sections(schema_name, schema_text);
        let mut msg_definition_table = HashMap::new();
        parse_msg_definition_from_schema_section(&sections, &mut msg_definition_table);

        let mut expected_msg_definition_table = HashMap::new();

        let time_msg_definition = create_time_definition();
        expected_msg_definition_table.insert("Time", time_msg_definition.clone());

        let header_msg_definition = create_header_definition();
        expected_msg_definition_table.insert("Header", header_msg_definition.clone());

        let joint_state_msg_definition = create_joint_state_definition();
        expected_msg_definition_table.insert("JointState", joint_state_msg_definition.clone());

        assert_eq!(
            msg_definition_table.len(),
            expected_msg_definition_table.len()
        );
        assert_eq!(msg_definition_table, expected_msg_definition_table);
    }
}
