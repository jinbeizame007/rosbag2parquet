use std::fs;

use anyhow::{Context, Result};
use camino::Utf8Path;
use mcap::MessageStream;
use memmap2::Mmap;
use nom::{
    branch::alt,
    bytes::complete::{tag, take_while1},
    character::complete::{alpha1, alphanumeric1},
    combinator::{map, opt, recognize},
    multi::many0,
    sequence::{pair, preceded, terminated},
    IResult, Parser,
};

mod create_test_data;

pub struct RosMsgDefinition {
    pub name: String,
    pub fields: Vec<RosField>,
}

pub struct RosField {
    pub name: String,
    pub data_type: RosDataType,
}

#[derive(Debug, PartialEq)]
pub enum RosDataType {
    Primitive(Primitive),
    Complex(String),
}

#[derive(Debug, PartialEq)]
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
    String { upper_bound: Option<usize> },
}

fn rosbag2parquet<P: AsRef<Utf8Path>>(path: P) -> Result<()> {
    let mmap = read_mcap(path)?;
    let message_stream = MessageStream::new(&mmap).context("Failed to create message stream")?;

    for (index, message_result) in message_stream.enumerate() {
        let message =
            message_result.with_context(|| format!("Failed to read message {}", index))?;

        if let Some(schema) = &message.channel.schema {
            let schema_name = schema.name.clone();
            let schema_data = schema.data.clone();

            let schema_text = std::str::from_utf8(&schema_data)?;
            println!("Schema: {} ({})", schema_name, schema_text);
            println!();
            println!();
        }
    }

    Ok(())
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

    for (index, raw_section) in raw_sections.iter().enumerate().rev() {
        let (type_name, content) = if index == 0 {
            (schema_name, *raw_section)
        } else {
            let type_name = raw_section.split_whitespace().next().unwrap();
            let first_newline = raw_section.find('\n').unwrap();
            let content_without_first_line = &raw_section[first_newline + 1..];
            (type_name, content_without_first_line)
        };

        let schema_section = SchemaSection { type_name, content };
        sections.push(schema_section);
    }

    sections
}

// fn field_definition(input: &str) -> IResult<&str, RosField> {
//     map(separated_pair(ros_data_type, sp, identifier))
// }

fn ros_data_type(input: &str) -> IResult<&str, RosDataType> {
    // First try to parse as a primitive type
    if let Ok((rest, prim)) = primitive_type(input) {
        return Ok((rest, RosDataType::Primitive(prim)));
    }

    // Otherwise, parse as a complex type (package/type format)
    let mut parser = map(
        recognize(pair(identifier, many0(pair(tag("/"), identifier)))),
        |full_type: &str| RosDataType::Complex(full_type.to_string()),
    );
    parser.parse(input)
}

fn number(input: &str) -> IResult<&str, &str> {
    take_while1(|c: char| c.is_ascii_digit())(input)
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
        // "string" と "string<=N" の両方を処理
        map(
            pair(tag("string"), opt(preceded(tag("<="), number))),
            |(_, bound)| Primitive::String {
                upper_bound: bound.and_then(|s| s.parse().ok()),
            },
        ),
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
    use crate::create_test_data::create_test_mcap_file;

    #[test]
    fn test_read_mcap() {
        // Create a test MCAP file
        let test_path = "testdata/test_read.mcap";
        create_test_mcap_file(test_path).unwrap();

        let mmap = read_mcap(test_path).unwrap();
        assert!(!mmap.is_empty());

        // Cleanup
        std::fs::remove_file(test_path).unwrap();
    }

    #[test]
    fn test_rosbag2parquet() {
        // Create a test MCAP file
        let test_path = "testdata/large.mcap";
        // create_test_mcap_file(test_path).unwrap();

        rosbag2parquet(test_path).unwrap();

        // Cleanup
        // std::fs::remove_file(test_path).unwrap();
    }

    #[test]
    fn test_ros_data_type() {
        let input = "float64";
        let (rest, data_type) = ros_data_type(input).unwrap();
        assert_eq!(rest, "");
        assert_eq!(data_type, RosDataType::Primitive(Primitive::Float64));

        let input = "sensor_msgs/msg/Temperature";
        let (rest, data_type) = ros_data_type(input).unwrap();
        assert_eq!(rest, "");
        assert_eq!(
            data_type,
            RosDataType::Complex("sensor_msgs/msg/Temperature".to_string())
        );
    }
}
