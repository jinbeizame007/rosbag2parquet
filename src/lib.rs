use std::collections::HashMap;
use std::fs;

use anyhow::{Context, Result};
use byteorder::{BigEndian, ByteOrder, LittleEndian};
use camino::Utf8Path;
use mcap::MessageStream;
use memmap2::Mmap;
use nom::{
    branch::alt,
    bytes::complete::tag,
    character::complete::{alpha1, alphanumeric1},
    combinator::{map, recognize},
    multi::many0,
    sequence::pair,
    IResult, Parser,
};

#[derive(Clone, Debug, PartialEq)]
pub struct RosMsgDefinition<'a> {
    pub name: &'a str,
    pub fields: Vec<RosField<'a>>,
}

impl<'a> RosMsgDefinition<'a> {
    pub fn new(name: &'a str, fields: Vec<RosField<'a>>) -> RosMsgDefinition<'a> {
        RosMsgDefinition {
            name: name.rsplit("/").next().unwrap(),
            fields,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct RosMsgValue {
    pub name: String,
    pub value: Vec<RosFieldValue>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RosField<'a> {
    pub data_type: RosDataType,
    pub name: &'a str,
}

impl<'a> RosField<'a> {
    pub fn new(data_type: RosDataType, name: &'a str) -> RosField<'a> {
        RosField {
            data_type,
            name: name.rsplit("/").next().unwrap(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct RosFieldValue {
    pub name: String,
    pub value: RosDataValue,
}

impl RosFieldValue {
    pub fn new(name: String, value: RosDataValue) -> RosFieldValue {
        RosFieldValue { name, value }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum RosDataType {
    NonArray(NonArrayRosDataType),
    StaticArray {
        data_type: NonArrayRosDataType,
        length: u32,
    },
    DynamicArray(NonArrayRosDataType),
}

#[derive(Clone, Debug, PartialEq)]
pub enum NonArrayRosDataType {
    Primitive(Primitive),
    Complex(String),
}

#[derive(Clone, Debug, PartialEq)]
pub enum RosDataValue {
    NonArray(NonArrayRosDataValue),
    StaticArray(StaticArrayRosDataValue),
    DynamicArray(DynamicArrayRosDataValue),
}

#[derive(Clone, Debug, PartialEq)]
pub struct DynamicArrayRosDataValue {
    pub values: Vec<NonArrayRosDataValue>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StaticArrayRosDataValue {
    pub values: Vec<NonArrayRosDataValue>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum NonArrayRosDataValue {
    Primitive(PrimitiveValue),
    Complex(RosMsgValue),
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

#[derive(Debug, Clone, PartialEq)]
pub enum PrimitiveValue {
    Bool(bool),
    Byte(u8),
    Char(char),
    Float32(f32),
    Float64(f64),
    Int8(i8),
    UInt8(u8),
    Int16(i16),
    UInt16(u16),
    Int32(i32),
    UInt32(u32),
    Int64(i64),
    UInt64(u64),
    String(String),
}

impl PrimitiveValue {
    pub fn from_bytes(
        data: &[u8],
        primitive_type: &Primitive,
        endianess: &Endianess,
    ) -> PrimitiveValue {
        match primitive_type {
            Primitive::Bool => PrimitiveValue::Bool(data[0] == 0x01),
            Primitive::Byte => {
                todo!()
            }
            Primitive::Char => {
                todo!()
            }
            Primitive::Float32 => {
                let value = match endianess {
                    Endianess::BigEndian => BigEndian::read_f32(data),
                    Endianess::LittleEndian => LittleEndian::read_f32(data),
                };
                PrimitiveValue::Float32(value)
            }
            Primitive::Float64 => {
                let value = match endianess {
                    Endianess::BigEndian => BigEndian::read_f64(data),
                    Endianess::LittleEndian => LittleEndian::read_f64(data),
                };
                PrimitiveValue::Float64(value)
            }
            Primitive::Int8 => {
                todo!()
            }
            Primitive::UInt8 => {
                todo!()
            }
            Primitive::Int16 => {
                let value = match endianess {
                    Endianess::BigEndian => BigEndian::read_i16(data),
                    Endianess::LittleEndian => LittleEndian::read_i16(data),
                };
                PrimitiveValue::Int16(value)
            }
            Primitive::UInt16 => {
                let value = match endianess {
                    Endianess::BigEndian => BigEndian::read_u16(data),
                    Endianess::LittleEndian => LittleEndian::read_u16(data),
                };
                PrimitiveValue::UInt16(value)
            }
            Primitive::Int32 => {
                let value = match endianess {
                    Endianess::BigEndian => BigEndian::read_i32(data),
                    Endianess::LittleEndian => LittleEndian::read_i32(data),
                };
                PrimitiveValue::Int32(value)
            }
            Primitive::UInt32 => {
                let value = match endianess {
                    Endianess::BigEndian => BigEndian::read_u32(data),
                    Endianess::LittleEndian => LittleEndian::read_u32(data),
                };
                PrimitiveValue::UInt32(value)
            }
            Primitive::Int64 => {
                let value = match endianess {
                    Endianess::BigEndian => BigEndian::read_i64(data),
                    Endianess::LittleEndian => LittleEndian::read_i64(data),
                };
                PrimitiveValue::Int64(value)
            }
            Primitive::UInt64 => {
                let value = match endianess {
                    Endianess::BigEndian => BigEndian::read_u64(data),
                    Endianess::LittleEndian => LittleEndian::read_u64(data),
                };
                PrimitiveValue::UInt64(value)
            }
            Primitive::String => match endianess {
                Endianess::BigEndian => {
                    let byte_length = BigEndian::read_u32(&data[0..4]);
                    let bytes = &data[4..4 + byte_length as usize];
                    let bytes_without_null = match bytes.split_last() {
                        None => bytes,
                        Some((_null_char, contents)) => contents,
                    };
                    let value = std::str::from_utf8(bytes_without_null).unwrap();
                    PrimitiveValue::String(value.to_string())
                }
                Endianess::LittleEndian => {
                    let byte_length = LittleEndian::read_u32(&data[0..4]);
                    let bytes = &data[4..4 + byte_length as usize];
                    let bytes_without_null = match bytes.split_last() {
                        None => bytes,
                        Some((_null_char, contents)) => contents,
                    };
                    let value = std::str::from_utf8(bytes_without_null).unwrap();
                    PrimitiveValue::String(value.to_string())
                }
            },
        }
    }
}

fn rosbag2parquet<P: AsRef<Utf8Path>>(path: P) -> Result<()> {
    let mmap = read_mcap(path)?;
    let message_stream = MessageStream::new(&mmap).context("Failed to create message stream")?;

    for (index, message_result) in message_stream.enumerate() {
        let message =
            message_result.with_context(|| format!("Failed to read message {}", index))?;

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

fn rosbag2ros_msg_values<P: AsRef<Utf8Path>>(path: P) -> Result<Vec<RosMsgValue>> {
    let mmap = read_mcap(path)?;

    // First, collect all schema data
    let mut schema_map = HashMap::new();
    let message_stream = MessageStream::new(&mmap).context("Failed to create message stream")?;

    for (index, message_result) in message_stream.enumerate() {
        let message =
            message_result.with_context(|| format!("Failed to read message {}", index))?;

        if let Some(schema) = &message.channel.schema {
            let schema_name = schema.name.rsplit("/").next().unwrap().to_string();
            if !schema_map.contains_key(&schema_name) {
                schema_map.insert(schema_name, schema.data.clone());
            }
        }
    }

    // Build message definition table from collected schemas
    let mut msg_definition_table = HashMap::new();
    for (schema_name, schema_data) in &schema_map {
        let schema_text = std::str::from_utf8(schema_data)?;
        let sections = parse_schema_sections(schema_name, schema_text);
        parse_msg_definition_from_schema_section(&sections, &mut msg_definition_table);
    }

    // Process messages
    let mut ros_msg_values = Vec::new();
    let message_stream = MessageStream::new(&mmap).context("Failed to create message stream")?;

    let mut cdr_deserializer = CdrDeserializer::new(&msg_definition_table);
    for (index, message_result) in message_stream.enumerate() {
        let message =
            message_result.with_context(|| format!("Failed to read message {}", index))?;

        if let Some(schema) = &message.channel.schema {
            let schema_name = schema.name.rsplit("/").next().unwrap().to_string();
            let ros_msg_value = cdr_deserializer.parse(&schema_name, &message.data);
            ros_msg_values.push(ros_msg_value);
        }
    }

    Ok(ros_msg_values)
}

fn read_mcap<P: AsRef<Utf8Path>>(path: P) -> Result<Mmap> {
    let fd = fs::File::open(path.as_ref()).context("Couldn't open MCap file")?;
    unsafe { Mmap::map(&fd) }.context("Couldn't map MCap file")
}

fn parse_msg_definition(schema_text: &str) -> Result<()> {
    // let mut fields = Vec::new();
    // let mut constants = Vec::new();

    for (_line_num, line) in schema_text.lines().enumerate() {
        let trimmed = line.trim();

        if trimmed.is_empty() || trimmed.starts_with("#") {
            continue;
        }

        if trimmed.starts_with("float64") {}
    }

    Ok(())
}

#[derive(Debug, Clone)]
pub struct SchemaSection<'a> {
    pub type_name: &'a str,
    pub content: &'a str,
}

fn parse_schema_sections<'a>(schema_name: &'a str, schema_text: &'a str) -> Vec<SchemaSection<'a>> {
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

fn parse_msg_definition_from_schema_section<'a>(
    schema_sections: &[SchemaSection<'a>],
    msg_definition_table: &mut HashMap<&'a str, RosMsgDefinition<'a>>,
) {
    for schema_section in schema_sections.iter().rev() {
        // Use the short name as the key
        let short_name = schema_section.type_name.rsplit("/").next().unwrap();

        if msg_definition_table.contains_key(short_name) {
            continue;
        }

        let mut fields = Vec::new();
        for line in schema_section.content.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with("#") {
                continue;
            }

            let data_type = line
                .split_whitespace()
                .next()
                .unwrap()
                .rsplit("/")
                .next()
                .unwrap();
            let name = line.split_whitespace().nth(1).unwrap();

            let data_type = ros_data_type(data_type).unwrap().1;
            let field = RosField::new(data_type, name);
            fields.push(field);
        }

        let msg_definition = RosMsgDefinition::new(schema_section.type_name, fields);
        msg_definition_table.insert(short_name, msg_definition);
    }
}

// *******************************
// ************  CDR  ************
// *******************************

#[derive(Copy, Clone, PartialEq, Debug)]
pub enum Endianess {
    BigEndian,
    LittleEndian,
}

pub struct CdrDeserializer<'a> {
    position: usize,
    msg_definition_table: &'a HashMap<&'a str, RosMsgDefinition<'a>>,
    endianess: Endianess,
}

impl<'a> CdrDeserializer<'a> {
    pub fn new(msg_definition_table: &'a HashMap<&'a str, RosMsgDefinition<'a>>) -> Self {
        Self {
            position: 0,
            msg_definition_table,
            endianess: Endianess::BigEndian,
        }
    }

    fn align_to(&mut self, count: usize) {
        let modulo = (self.position - 4) % count;
        if modulo != 0 {
            self.position += count - modulo;
        }
    }

    fn next_bytes<'b>(&mut self, data: &'b [u8], count: usize) -> &'b [u8] {
        self.position += count;
        &data[self.position - count..self.position]
    }

    fn parse<'b>(&mut self, name: &str, data: &[u8]) -> RosMsgValue {
        self.endianess = if data[1] == 0x00 {
            Endianess::BigEndian
        } else {
            Endianess::LittleEndian
        };

        self.position = 4;

        self.parse_without_header(name, data)
    }

    fn parse_without_header(&mut self, name: &str, data: &[u8]) -> RosMsgValue {
        let mut value = RosMsgValue {
            name: name.to_string(),
            value: Vec::new(),
        };

        for field in self.msg_definition_table.get(name).unwrap().fields.iter() {
            let field_value = self.parse_field(field, data);
            value.value.push(field_value);
        }

        value
    }

    fn parse_field(&mut self, field: &'a RosField<'a>, data: &[u8]) -> RosFieldValue {
        let value = match &field.data_type {
            RosDataType::DynamicArray(non_array_data_type) => {
                RosDataValue::DynamicArray(self.parse_dynamic_array(non_array_data_type, data))
            }
            RosDataType::StaticArray { data_type, length } => {
                RosDataValue::StaticArray(self.parse_static_array(data_type, length, data))
            }
            RosDataType::NonArray(non_array_data_type) => match non_array_data_type {
                NonArrayRosDataType::Primitive(prim) => RosDataValue::NonArray(
                    NonArrayRosDataValue::Primitive(self.parse_primitive(prim, data)),
                ),
                NonArrayRosDataType::Complex(name) => RosDataValue::NonArray(
                    NonArrayRosDataValue::Complex(self.parse_complex(name, data)),
                ),
            },
        };

        RosFieldValue {
            name: field.name.to_string(),
            value,
        }
    }

    fn parse_static_array(
        &mut self,
        non_array_data_type: &NonArrayRosDataType,
        length: &u32,
        data: &[u8],
    ) -> StaticArrayRosDataValue {
        let mut values = Vec::new();
        for _ in 0..*length {
            let value = self.parse_non_array_data_value(non_array_data_type, data);
            values.push(value);
        }
        StaticArrayRosDataValue { values }
    }

    fn parse_dynamic_array(
        &mut self,
        non_array_data_type: &NonArrayRosDataType,
        data: &[u8],
    ) -> DynamicArrayRosDataValue {
        self.align_to(4);
        let header = self.next_bytes(data, 4);
        let length = match self.endianess {
            Endianess::BigEndian => BigEndian::read_u32(header),
            Endianess::LittleEndian => LittleEndian::read_u32(header),
        };
        let mut values = Vec::new();
        for _ in 0..length {
            let value = self.parse_non_array_data_value(non_array_data_type, data);
            values.push(value);
        }
        DynamicArrayRosDataValue { values }
    }

    fn parse_non_array_data_value(
        &mut self,
        non_array_data_type: &NonArrayRosDataType,
        data: &[u8],
    ) -> NonArrayRosDataValue {
        match non_array_data_type {
            NonArrayRosDataType::Primitive(prim) => {
                NonArrayRosDataValue::Primitive(self.parse_primitive(prim, data))
            }
            NonArrayRosDataType::Complex(name) => {
                NonArrayRosDataValue::Complex(self.parse_complex(name, data))
            }
        }
    }

    fn parse_primitive(&mut self, prim: &Primitive, data: &[u8]) -> PrimitiveValue {
        let endianess = self.endianess;
        match prim {
            Primitive::Bool => {
                let bytes = self.next_bytes(data, 1);
                PrimitiveValue::from_bytes(bytes, prim, &endianess)
            }
            Primitive::Byte => {
                todo!()
            }
            Primitive::Char => {
                todo!()
            }
            Primitive::Float32 => {
                self.align_to(4);
                let bytes = self.next_bytes(data, 4);
                PrimitiveValue::from_bytes(bytes, prim, &endianess)
            }
            Primitive::Float64 => {
                self.align_to(8);
                let bytes = self.next_bytes(data, 8);
                PrimitiveValue::from_bytes(bytes, prim, &endianess)
            }
            Primitive::Int8 | Primitive::UInt8 => {
                let bytes = self.next_bytes(data, 1);
                PrimitiveValue::from_bytes(bytes, prim, &endianess)
            }
            Primitive::Int16 | Primitive::UInt16 => {
                self.align_to(2);
                let bytes = self.next_bytes(data, 2);
                PrimitiveValue::from_bytes(bytes, prim, &endianess)
            }
            Primitive::Int32 | Primitive::UInt32 => {
                self.align_to(4);
                let bytes = self.next_bytes(data, 4);
                PrimitiveValue::from_bytes(bytes, prim, &endianess)
            }
            Primitive::Int64 | Primitive::UInt64 => {
                self.align_to(8);
                let bytes = self.next_bytes(data, 8);
                PrimitiveValue::from_bytes(bytes, prim, &endianess)
            }
            Primitive::String => {
                self.align_to(4);
                let header = self.next_bytes(data, 4);
                let byte_length = match endianess {
                    Endianess::BigEndian => BigEndian::read_u32(header),
                    Endianess::LittleEndian => LittleEndian::read_u32(header),
                };
                let bytes = self.next_bytes(data, byte_length as usize);
                let bytes_without_null = match bytes.split_last() {
                    None => bytes,
                    Some((_null_char, contents)) => contents,
                };
                let value = std::str::from_utf8(bytes_without_null).unwrap();
                PrimitiveValue::String(value.to_string())
            }
        }
    }

    fn parse_complex(&mut self, name: &str, data: &[u8]) -> RosMsgValue {
        let msg_definition = self.msg_definition_table.get(name).unwrap();
        let mut ros_msg_value = RosMsgValue {
            name: name.to_string(),
            value: Vec::new(),
        };

        for field in msg_definition.fields.iter() {
            let field_value = self.parse_field(field, data);
            ros_msg_value.value.push(field_value);
        }

        ros_msg_value
    }
}

fn ros_data_type(input: &str) -> IResult<&str, RosDataType> {
    if input.ends_with("[]") {
        let (rest, data_type) = non_array_ros_data_type(input.split_at(input.len() - 2).0)?;
        return Ok((rest, RosDataType::DynamicArray(data_type)));
    }

    if input.ends_with("]") {
        let data_type_and_length = input
            .split_at(input.len() - 1)
            .0
            .split('[')
            .collect::<Vec<&str>>();
        let (rest, data_type) = non_array_ros_data_type(data_type_and_length[0])?;
        let length = data_type_and_length[1].parse::<u32>().unwrap();
        return Ok((rest, RosDataType::StaticArray { data_type, length }));
    }

    let (rest, data_type) = non_array_ros_data_type(input)?;
    Ok((rest, RosDataType::NonArray(data_type)))
}

fn non_array_ros_data_type(input: &str) -> IResult<&str, NonArrayRosDataType> {
    if let Ok((rest, prim)) = primitive_type(input) {
        return Ok((rest, NonArrayRosDataType::Primitive(prim)));
    }

    // Otherwise, parse as a complex type (package/type format)
    let mut parser = map(
        recognize(pair(identifier, many0(pair(tag("/"), identifier)))),
        |full_type: &str| NonArrayRosDataType::Complex(full_type.to_string()),
    );
    parser.parse(input)
}

fn primitive_type(input: &str) -> IResult<&str, Primitive> {
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
fn identifier(input: &str) -> IResult<&str, &str> {
    let mut parser = recognize(pair(alpha1, many0(alt((alphanumeric1, tag("_"))))));
    parser.parse(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ros_data_type() {
        let input = "float64";
        let (rest, data_type) = ros_data_type(input).unwrap();
        assert_eq!(rest, "");
        assert_eq!(
            data_type,
            RosDataType::NonArray(NonArrayRosDataType::Primitive(Primitive::Float64))
        );

        let input = "sensor_msgs/msg/Temperature";
        let (rest, data_type) = ros_data_type(input).unwrap();
        assert_eq!(rest, "");
        assert_eq!(
            data_type,
            RosDataType::NonArray(NonArrayRosDataType::Complex(
                "sensor_msgs/msg/Temperature".to_string(),
            ))
        );
    }

    #[test]
    fn test_parse_schema_sections_single_section() {
        let schema_name = "geometry_msgs/msg/Vector3";
        let schema_text = include_str!("../testdata/schema/vector3d.txt");
        let sections = parse_schema_sections(schema_name, schema_text);
        assert_eq!(sections.len(), 1);
        assert_eq!(sections[0].type_name, schema_name);

        // ファイル全体の内容（コメント含む）を期待値とする
        assert_eq!(sections[0].content, schema_text.trim());
    }

    #[test]
    fn test_parse_schema_sections_multiple_sections() {
        let schema_name = "sensor_msgs/msg/JointState";
        let schema_text = include_str!("../testdata/schema/joint_state.txt");

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
        let schema_text = include_str!("../testdata/schema/vector3d.txt");
        let sections = parse_schema_sections(schema_name, schema_text);
        let mut msg_definition_table = HashMap::new();
        parse_msg_definition_from_schema_section(&sections, &mut msg_definition_table);

        assert_eq!(msg_definition_table.len(), 1);
        assert_eq!(
            msg_definition_table.get("Vector3"),
            Some(&RosMsgDefinition::new(
                schema_name,
                vec![
                    RosField::new(
                        RosDataType::NonArray(NonArrayRosDataType::Primitive(Primitive::Float64)),
                        "x",
                    ),
                    RosField::new(
                        RosDataType::NonArray(NonArrayRosDataType::Primitive(Primitive::Float64)),
                        "y",
                    ),
                    RosField::new(
                        RosDataType::NonArray(NonArrayRosDataType::Primitive(Primitive::Float64)),
                        "z",
                    ),
                ],
            ))
        );
    }

    #[test]
    fn test_parse_msg_definition_from_schema_section_multiple_sections() {
        let schema_name = "geometry_msgs/msg/TwistStamped";
        let schema_text = include_str!("../testdata/schema/twist_stamped.txt");
        let sections = parse_schema_sections(schema_name, schema_text);
        let mut msg_definition_table = HashMap::new();
        parse_msg_definition_from_schema_section(&sections, &mut msg_definition_table);

        let mut expected_msg_definition_table = HashMap::new();

        let time_msg_definition = RosMsgDefinition::new(
            "builtin_interfaces/Time",
            vec![
                RosField::new(
                    RosDataType::NonArray(NonArrayRosDataType::Primitive(Primitive::Int32)),
                    "sec",
                ),
                RosField::new(
                    RosDataType::NonArray(NonArrayRosDataType::Primitive(Primitive::UInt32)),
                    "nanosec",
                ),
            ],
        );
        expected_msg_definition_table.insert("Time", time_msg_definition.clone());

        let header_msg_definition = RosMsgDefinition::new(
            "std_msgs/Header",
            vec![
                RosField::new(
                    RosDataType::NonArray(NonArrayRosDataType::Complex("Time".to_string())),
                    "stamp",
                ),
                RosField::new(
                    RosDataType::NonArray(NonArrayRosDataType::Primitive(Primitive::String)),
                    "frame_id",
                ),
            ],
        );
        expected_msg_definition_table.insert("Header", header_msg_definition.clone());

        let vector3d_msg_definition = RosMsgDefinition::new(
            "geometry_msgs/Vector3",
            vec![
                RosField::new(
                    RosDataType::NonArray(NonArrayRosDataType::Primitive(Primitive::Float64)),
                    "x",
                ),
                RosField::new(
                    RosDataType::NonArray(NonArrayRosDataType::Primitive(Primitive::Float64)),
                    "y",
                ),
                RosField::new(
                    RosDataType::NonArray(NonArrayRosDataType::Primitive(Primitive::Float64)),
                    "z",
                ),
            ],
        );
        expected_msg_definition_table.insert("Vector3", vector3d_msg_definition.clone());

        let twist_msg_definition = RosMsgDefinition::new(
            "geometry_msgs/Twist",
            vec![
                RosField::new(
                    RosDataType::NonArray(NonArrayRosDataType::Complex("Vector3".to_string())),
                    "linear",
                ),
                RosField::new(
                    RosDataType::NonArray(NonArrayRosDataType::Complex("Vector3".to_string())),
                    "angular",
                ),
            ],
        );
        expected_msg_definition_table.insert("Twist", twist_msg_definition.clone());

        let twist_stamped_msg_definition = RosMsgDefinition::new(
            "geometry_msgs/msg/TwistStamped",
            vec![
                RosField::new(
                    RosDataType::NonArray(NonArrayRosDataType::Complex("Header".to_string())),
                    "header",
                ),
                RosField::new(
                    RosDataType::NonArray(NonArrayRosDataType::Complex("Twist".to_string())),
                    "twist",
                ),
            ],
        );
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
        let schema_text = include_str!("../testdata/schema/joint_state.txt");
        let sections = parse_schema_sections(schema_name, schema_text);
        let mut msg_definition_table = HashMap::new();
        parse_msg_definition_from_schema_section(&sections, &mut msg_definition_table);

        let mut expected_msg_definition_table = HashMap::new();

        let time_msg_definition = RosMsgDefinition::new(
            "builtin_interfaces/Time",
            vec![
                RosField::new(
                    RosDataType::NonArray(NonArrayRosDataType::Primitive(Primitive::Int32)),
                    "sec",
                ),
                RosField::new(
                    RosDataType::NonArray(NonArrayRosDataType::Primitive(Primitive::UInt32)),
                    "nanosec",
                ),
            ],
        );
        expected_msg_definition_table.insert("Time", time_msg_definition.clone());

        let header_msg_definition = RosMsgDefinition::new(
            "std_msgs/Header",
            vec![
                RosField::new(
                    RosDataType::NonArray(NonArrayRosDataType::Complex("Time".to_string())),
                    "stamp",
                ),
                RosField::new(
                    RosDataType::NonArray(NonArrayRosDataType::Primitive(Primitive::String)),
                    "frame_id",
                ),
            ],
        );
        expected_msg_definition_table.insert("Header", header_msg_definition.clone());

        let joint_state_msg_definition = RosMsgDefinition::new(
            "sensor_msgs/msg/JointState",
            vec![
                RosField::new(
                    RosDataType::NonArray(NonArrayRosDataType::Complex("Header".to_string())),
                    "header",
                ),
                RosField::new(
                    RosDataType::DynamicArray(NonArrayRosDataType::Primitive(Primitive::String)),
                    "name",
                ),
                RosField::new(
                    RosDataType::DynamicArray(NonArrayRosDataType::Primitive(Primitive::Float64)),
                    "position",
                ),
                RosField::new(
                    RosDataType::DynamicArray(NonArrayRosDataType::Primitive(Primitive::Float64)),
                    "velocity",
                ),
                RosField::new(
                    RosDataType::DynamicArray(NonArrayRosDataType::Primitive(Primitive::Float64)),
                    "effort",
                ),
            ],
        );
        expected_msg_definition_table.insert("JointState", joint_state_msg_definition.clone());

        assert_eq!(
            msg_definition_table.len(),
            expected_msg_definition_table.len()
        );
        assert_eq!(msg_definition_table, expected_msg_definition_table);
    }

    #[test]
    fn test_cdr_deserializer_vector3d() {
        let vector3d_msg_definition = RosMsgDefinition::new(
            "geometry_msgs/Vector3",
            vec![
                RosField::new(
                    RosDataType::NonArray(NonArrayRosDataType::Primitive(Primitive::Float64)),
                    "x",
                ),
                RosField::new(
                    RosDataType::NonArray(NonArrayRosDataType::Primitive(Primitive::Float64)),
                    "y",
                ),
                RosField::new(
                    RosDataType::NonArray(NonArrayRosDataType::Primitive(Primitive::Float64)),
                    "z",
                ),
            ],
        );
        let vector3d_msg_definition_table = HashMap::from([("Vector3", vector3d_msg_definition)]);

        let header: Vec<u8> = vec![0x00, 0x00, 0x00, 0x00];
        let mut data_x: Vec<u8> = vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        let mut data_y: Vec<u8> = vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        let mut data_z: Vec<u8> = vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        BigEndian::write_f64(&mut data_x, 1.0);
        BigEndian::write_f64(&mut data_y, 2.0);
        BigEndian::write_f64(&mut data_z, 3.0);

        let data: Vec<u8> = [
            header.clone(),
            data_x.clone(),
            data_y.clone(),
            data_z.clone(),
        ]
        .concat();

        let mut cdr_deserializer = CdrDeserializer::new(&vector3d_msg_definition_table);
        let value = cdr_deserializer.parse("Vector3", &data);
        assert_eq!(
            value,
            RosMsgValue {
                name: "Vector3".to_string(),
                value: vec![
                    RosFieldValue::new(
                        "x".to_string(),
                        RosDataValue::NonArray(NonArrayRosDataValue::Primitive(
                            PrimitiveValue::Float64(1.0)
                        ))
                    ),
                    RosFieldValue::new(
                        "y".to_string(),
                        RosDataValue::NonArray(NonArrayRosDataValue::Primitive(
                            PrimitiveValue::Float64(2.0)
                        ))
                    ),
                    RosFieldValue::new(
                        "z".to_string(),
                        RosDataValue::NonArray(NonArrayRosDataValue::Primitive(
                            PrimitiveValue::Float64(3.0)
                        ))
                    ),
                ]
            }
        );
    }

    #[test]
    fn test_rosbag2ros_msg_values() {
        let test_path = "rosbags/non_array_msgs/non_array_msgs_0.mcap";
        let ros_msg_values = rosbag2ros_msg_values(test_path).unwrap();

        let mut expected_ros_msg_values = Vec::new();

        let linear_msg_value = RosMsgValue {
            name: "Vector3".to_string(),
            value: vec![
                RosFieldValue::new(
                    "x".to_string(),
                    RosDataValue::NonArray(NonArrayRosDataValue::Primitive(
                        PrimitiveValue::Float64(1.2),
                    )),
                ),
                RosFieldValue::new(
                    "y".to_string(),
                    RosDataValue::NonArray(NonArrayRosDataValue::Primitive(
                        PrimitiveValue::Float64(0.0),
                    )),
                ),
                RosFieldValue::new(
                    "z".to_string(),
                    RosDataValue::NonArray(NonArrayRosDataValue::Primitive(
                        PrimitiveValue::Float64(0.0),
                    )),
                ),
            ],
        };
        let angular_msg_value = RosMsgValue {
            name: "Vector3".to_string(),
            value: vec![
                RosFieldValue::new(
                    "x".to_string(),
                    RosDataValue::NonArray(NonArrayRosDataValue::Primitive(
                        PrimitiveValue::Float64(0.0),
                    )),
                ),
                RosFieldValue::new(
                    "y".to_string(),
                    RosDataValue::NonArray(NonArrayRosDataValue::Primitive(
                        PrimitiveValue::Float64(0.0),
                    )),
                ),
                RosFieldValue::new(
                    "z".to_string(),
                    RosDataValue::NonArray(NonArrayRosDataValue::Primitive(
                        PrimitiveValue::Float64(-0.6),
                    )),
                ),
            ],
        };
        let twist_msg_value = RosMsgValue {
            name: "Twist".to_string(),
            value: vec![
                RosFieldValue::new(
                    "linear".to_string(),
                    RosDataValue::NonArray(NonArrayRosDataValue::Complex(linear_msg_value)),
                ),
                RosFieldValue::new(
                    "angular".to_string(),
                    RosDataValue::NonArray(NonArrayRosDataValue::Complex(angular_msg_value)),
                ),
            ],
        };
        expected_ros_msg_values.push(twist_msg_value);

        let vector3d_msg_value = RosMsgValue {
            name: "Vector3".to_string(),
            value: vec![
                RosFieldValue::new(
                    "x".to_string(),
                    RosDataValue::NonArray(NonArrayRosDataValue::Primitive(
                        PrimitiveValue::Float64(1.1),
                    )),
                ),
                RosFieldValue::new(
                    "y".to_string(),
                    RosDataValue::NonArray(NonArrayRosDataValue::Primitive(
                        PrimitiveValue::Float64(2.2),
                    )),
                ),
                RosFieldValue::new(
                    "z".to_string(),
                    RosDataValue::NonArray(NonArrayRosDataValue::Primitive(
                        PrimitiveValue::Float64(3.3),
                    )),
                ),
            ],
        };
        expected_ros_msg_values.push(vector3d_msg_value);

        let string_msg_value = RosMsgValue {
            name: "String".to_string(),
            value: vec![RosFieldValue::new(
                "data".to_string(),
                RosDataValue::NonArray(NonArrayRosDataValue::Primitive(PrimitiveValue::String(
                    "Hello, World!".to_string(),
                ))),
            )],
        };
        expected_ros_msg_values.push(string_msg_value);

        assert_eq!(ros_msg_values.len(), expected_ros_msg_values.len());
        assert_eq!(ros_msg_values, expected_ros_msg_values);
    }

    #[test]
    fn test_rosbag2ros_msg_values_array() {
        let test_path = "rosbags/array_msgs/array_msgs_0.mcap";
        let ros_msg_values = rosbag2ros_msg_values(test_path).unwrap();

        let mut expected_ros_msg_values = Vec::new();

        let imu_time_msg_value = RosMsgValue {
            name: "Time".to_string(),
            value: vec![
                RosFieldValue::new(
                    "sec".to_string(),
                    RosDataValue::NonArray(NonArrayRosDataValue::Primitive(PrimitiveValue::Int32(
                        0,
                    ))),
                ),
                RosFieldValue::new(
                    "nanosec".to_string(),
                    RosDataValue::NonArray(NonArrayRosDataValue::Primitive(
                        PrimitiveValue::UInt32(0),
                    )),
                ),
            ],
        };

        let imu_header_msg_value = RosMsgValue {
            name: "Header".to_string(),
            value: vec![
                RosFieldValue::new(
                    "stamp".to_string(),
                    RosDataValue::NonArray(NonArrayRosDataValue::Complex(imu_time_msg_value)),
                ),
                RosFieldValue::new(
                    "frame_id".to_string(),
                    RosDataValue::NonArray(NonArrayRosDataValue::Primitive(
                        PrimitiveValue::String("imu_link".to_string()),
                    )),
                ),
            ],
        };

        let imu_orientation_msg_value = RosMsgValue {
            name: "Quaternion".to_string(),
            value: vec![
                RosFieldValue::new(
                    "x".to_string(),
                    RosDataValue::NonArray(NonArrayRosDataValue::Primitive(
                        PrimitiveValue::Float64(0.0),
                    )),
                ),
                RosFieldValue::new(
                    "y".to_string(),
                    RosDataValue::NonArray(NonArrayRosDataValue::Primitive(
                        PrimitiveValue::Float64(0.0),
                    )),
                ),
                RosFieldValue::new(
                    "z".to_string(),
                    RosDataValue::NonArray(NonArrayRosDataValue::Primitive(
                        PrimitiveValue::Float64(0.0),
                    )),
                ),
                RosFieldValue::new(
                    "w".to_string(),
                    RosDataValue::NonArray(NonArrayRosDataValue::Primitive(
                        PrimitiveValue::Float64(1.0),
                    )),
                ),
            ],
        };

        let imu_msg_value = RosMsgValue {
            name: "Imu".to_string(),
            value: vec![
                RosFieldValue::new(
                    "header".to_string(),
                    RosDataValue::NonArray(NonArrayRosDataValue::Complex(imu_header_msg_value)),
                ),
                RosFieldValue::new(
                    "orientation".to_string(),
                    RosDataValue::NonArray(NonArrayRosDataValue::Complex(
                        imu_orientation_msg_value,
                    )),
                ),
                RosFieldValue::new(
                    "orientation_covariance".to_string(),
                    RosDataValue::StaticArray(StaticArrayRosDataValue {
                        values: vec![
                            NonArrayRosDataValue::Primitive(PrimitiveValue::Float64(0.1)),
                            NonArrayRosDataValue::Primitive(PrimitiveValue::Float64(0.2)),
                            NonArrayRosDataValue::Primitive(PrimitiveValue::Float64(0.3)),
                            NonArrayRosDataValue::Primitive(PrimitiveValue::Float64(0.4)),
                            NonArrayRosDataValue::Primitive(PrimitiveValue::Float64(0.5)),
                            NonArrayRosDataValue::Primitive(PrimitiveValue::Float64(0.6)),
                            NonArrayRosDataValue::Primitive(PrimitiveValue::Float64(0.7)),
                            NonArrayRosDataValue::Primitive(PrimitiveValue::Float64(0.8)),
                            NonArrayRosDataValue::Primitive(PrimitiveValue::Float64(0.9)),
                        ],
                    }),
                ),
                RosFieldValue::new(
                    "angular_velocity".to_string(),
                    RosDataValue::NonArray(NonArrayRosDataValue::Complex(RosMsgValue {
                        name: "Vector3".to_string(),
                        value: vec![
                            RosFieldValue::new(
                                "x".to_string(),
                                RosDataValue::NonArray(NonArrayRosDataValue::Primitive(
                                    PrimitiveValue::Float64(0.1),
                                )),
                            ),
                            RosFieldValue::new(
                                "y".to_string(),
                                RosDataValue::NonArray(NonArrayRosDataValue::Primitive(
                                    PrimitiveValue::Float64(0.2),
                                )),
                            ),
                            RosFieldValue::new(
                                "z".to_string(),
                                RosDataValue::NonArray(NonArrayRosDataValue::Primitive(
                                    PrimitiveValue::Float64(0.3),
                                )),
                            ),
                        ],
                    })),
                ),
                RosFieldValue::new(
                    "angular_velocity_covariance".to_string(),
                    RosDataValue::StaticArray(StaticArrayRosDataValue {
                        values: vec![
                            NonArrayRosDataValue::Primitive(PrimitiveValue::Float64(0.9)),
                            NonArrayRosDataValue::Primitive(PrimitiveValue::Float64(0.8)),
                            NonArrayRosDataValue::Primitive(PrimitiveValue::Float64(0.7)),
                            NonArrayRosDataValue::Primitive(PrimitiveValue::Float64(0.6)),
                            NonArrayRosDataValue::Primitive(PrimitiveValue::Float64(0.5)),
                            NonArrayRosDataValue::Primitive(PrimitiveValue::Float64(0.4)),
                            NonArrayRosDataValue::Primitive(PrimitiveValue::Float64(0.3)),
                            NonArrayRosDataValue::Primitive(PrimitiveValue::Float64(0.2)),
                            NonArrayRosDataValue::Primitive(PrimitiveValue::Float64(0.1)),
                        ],
                    }),
                ),
                RosFieldValue::new(
                    "linear_acceleration".to_string(),
                    RosDataValue::NonArray(NonArrayRosDataValue::Complex(RosMsgValue {
                        name: "Vector3".to_string(),
                        value: vec![
                            RosFieldValue::new(
                                "x".to_string(),
                                RosDataValue::NonArray(NonArrayRosDataValue::Primitive(
                                    PrimitiveValue::Float64(1.0),
                                )),
                            ),
                            RosFieldValue::new(
                                "y".to_string(),
                                RosDataValue::NonArray(NonArrayRosDataValue::Primitive(
                                    PrimitiveValue::Float64(2.0),
                                )),
                            ),
                            RosFieldValue::new(
                                "z".to_string(),
                                RosDataValue::NonArray(NonArrayRosDataValue::Primitive(
                                    PrimitiveValue::Float64(3.0),
                                )),
                            ),
                        ],
                    })),
                ),
                RosFieldValue::new(
                    "linear_acceleration_covariance".to_string(),
                    RosDataValue::StaticArray(StaticArrayRosDataValue {
                        values: vec![
                            NonArrayRosDataValue::Primitive(PrimitiveValue::Float64(0.1)),
                            NonArrayRosDataValue::Primitive(PrimitiveValue::Float64(0.0)),
                            NonArrayRosDataValue::Primitive(PrimitiveValue::Float64(0.0)),
                            NonArrayRosDataValue::Primitive(PrimitiveValue::Float64(0.0)),
                            NonArrayRosDataValue::Primitive(PrimitiveValue::Float64(0.1)),
                            NonArrayRosDataValue::Primitive(PrimitiveValue::Float64(0.0)),
                            NonArrayRosDataValue::Primitive(PrimitiveValue::Float64(0.0)),
                            NonArrayRosDataValue::Primitive(PrimitiveValue::Float64(0.0)),
                            NonArrayRosDataValue::Primitive(PrimitiveValue::Float64(0.1)),
                        ],
                    }),
                ),
            ],
        };
        expected_ros_msg_values.push(imu_msg_value);

        let joint_state_time_msg_value = RosMsgValue {
            name: "Time".to_string(),
            value: vec![
                RosFieldValue::new(
                    "sec".to_string(),
                    RosDataValue::NonArray(NonArrayRosDataValue::Primitive(PrimitiveValue::Int32(
                        0,
                    ))),
                ),
                RosFieldValue::new(
                    "nanosec".to_string(),
                    RosDataValue::NonArray(NonArrayRosDataValue::Primitive(
                        PrimitiveValue::UInt32(0),
                    )),
                ),
            ],
        };

        let joint_state_header_msg_value = RosMsgValue {
            name: "Header".to_string(),
            value: vec![
                RosFieldValue::new(
                    "stamp".to_string(),
                    RosDataValue::NonArray(NonArrayRosDataValue::Complex(
                        joint_state_time_msg_value,
                    )),
                ),
                RosFieldValue::new(
                    "frame_id".to_string(),
                    RosDataValue::NonArray(NonArrayRosDataValue::Primitive(
                        PrimitiveValue::String("".to_string()),
                    )),
                ),
            ],
        };

        let joint_state_msg_value = RosMsgValue {
            name: "JointState".to_string(),
            value: vec![
                RosFieldValue::new(
                    "header".to_string(),
                    RosDataValue::NonArray(NonArrayRosDataValue::Complex(
                        joint_state_header_msg_value,
                    )),
                ),
                RosFieldValue::new(
                    "name".to_string(),
                    RosDataValue::DynamicArray(DynamicArrayRosDataValue {
                        values: vec![
                            NonArrayRosDataValue::Primitive(PrimitiveValue::String(
                                "joint1".to_string(),
                            )),
                            NonArrayRosDataValue::Primitive(PrimitiveValue::String(
                                "joint2".to_string(),
                            )),
                            NonArrayRosDataValue::Primitive(PrimitiveValue::String(
                                "joint3".to_string(),
                            )),
                        ],
                    }),
                ),
                RosFieldValue::new(
                    "position".to_string(),
                    RosDataValue::DynamicArray(DynamicArrayRosDataValue {
                        values: vec![
                            NonArrayRosDataValue::Primitive(PrimitiveValue::Float64(1.5)),
                            NonArrayRosDataValue::Primitive(PrimitiveValue::Float64(-0.5)),
                            NonArrayRosDataValue::Primitive(PrimitiveValue::Float64(0.8)),
                        ],
                    }),
                ),
                RosFieldValue::new(
                    "velocity".to_string(),
                    RosDataValue::DynamicArray(DynamicArrayRosDataValue {
                        values: vec![
                            NonArrayRosDataValue::Primitive(PrimitiveValue::Float64(0.1)),
                            NonArrayRosDataValue::Primitive(PrimitiveValue::Float64(0.2)),
                            NonArrayRosDataValue::Primitive(PrimitiveValue::Float64(0.3)),
                        ],
                    }),
                ),
                RosFieldValue::new(
                    "effort".to_string(),
                    RosDataValue::DynamicArray(DynamicArrayRosDataValue {
                        values: vec![
                            NonArrayRosDataValue::Primitive(PrimitiveValue::Float64(10.1)),
                            NonArrayRosDataValue::Primitive(PrimitiveValue::Float64(10.2)),
                            NonArrayRosDataValue::Primitive(PrimitiveValue::Float64(10.3)),
                        ],
                    }),
                ),
            ],
        };
        expected_ros_msg_values.push(joint_state_msg_value);

        // assert_eq!(ros_msg_values.len(), expected_ros_msg_values.len());
        assert_eq!(ros_msg_values[0], expected_ros_msg_values[0]);
    }
}
