pub mod cdr;
pub mod ros;

use std::borrow::Cow;
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{
    ArrayBuilder, BooleanBuilder, FixedSizeListBuilder, Float32Builder, Float64Builder,
    Int16Builder, Int32Builder, Int64Builder, Int8Builder, ListBuilder, StringBuilder,
    StructBuilder, UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder,
};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::record_batch::RecordBatch;
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

use crate::cdr::CdrDeserializer;
use crate::ros::{
    BaseType, FieldDefinition, FieldType, Message, MessageDefinition, Primitive, SchemaSection,
};

pub use cdr::Endianness;
pub use ros::*;

pub fn rosbag2parquet<P: AsRef<Utf8Path>>(path: P) -> Result<()> {
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

fn extract_message_type(full_type_name: &str) -> &str {
    full_type_name.rsplit('/').next().unwrap_or(full_type_name)
}

pub fn rosbag2ros_msg_values<P: AsRef<Utf8Path>>(path: P) -> Result<Vec<Message>> {
    let mcap_file = read_mcap(path)?;

    // First, collect all schema data
    let mut type_registry = HashMap::new();
    let message_stream =
        MessageStream::new(&mcap_file).context("Failed to create message stream")?;

    for (index, message_result) in message_stream.enumerate() {
        let message =
            message_result.with_context(|| format!("Failed to read message {}", index))?;

        if let Some(schema) = &message.channel.schema {
            let type_name = extract_message_type(&schema.name).to_string();
            if !type_registry.contains_key(&type_name) {
                type_registry.insert(type_name, schema.data.clone());
            }
        }
    }

    // Build message definition table from collected schemas
    let mut msg_definition_table = HashMap::new();
    for (type_name, schema_data) in &type_registry {
        let schema_text = std::str::from_utf8(schema_data)?;
        let sections = parse_schema_sections(type_name, schema_text);
        parse_msg_definition_from_schema_section(&sections, &mut msg_definition_table);
    }

    // Process messages
    let mut parsed_messages = Vec::new();
    let message_stream =
        MessageStream::new(&mcap_file).context("Failed to create message stream")?;

    let mut cdr_deserializer = CdrDeserializer::new(&msg_definition_table);
    for (index, message_result) in message_stream.enumerate() {
        let message =
            message_result.with_context(|| format!("Failed to read message {}", index))?;

        if let Some(schema) = &message.channel.schema {
            let type_name = extract_message_type(&schema.name).to_string();
            let parsed_message = cdr_deserializer.parse(&type_name, &message.data);
            parsed_messages.push(parsed_message);
        }
    }

    Ok(parsed_messages)
}

pub fn rosbag2record_batches<P: AsRef<Utf8Path>>(path: P) -> Result<Vec<RecordBatch>> {
    let mcap_file = read_mcap(path)?;

    // First, collect all schema data
    let mut type_registry = HashMap::new();
    let message_stream =
        MessageStream::new(&mcap_file).context("Failed to create message stream")?;

    for (index, message_result) in message_stream.enumerate() {
        let message =
            message_result.with_context(|| format!("Failed to read message {}", index))?;

        if let Some(schema) = &message.channel.schema {
            let type_name = extract_message_type(&schema.name).to_string();
            if !type_registry.contains_key(&type_name) {
                type_registry.insert(type_name, schema.data.clone());
            }
        }
    }

    // Build message definition table from collected schemas
    let mut msg_definition_table = HashMap::new();
    for (type_name, schema_data) in &type_registry {
        let schema_text = std::str::from_utf8(schema_data)?;
        let sections = parse_schema_sections(type_name, schema_text);
        parse_msg_definition_from_schema_section(&sections, &mut msg_definition_table);
    }

    // Process messages
    let mut parsed_messages = HashMap::new();
    let message_stream =
        MessageStream::new(&mcap_file).context("Failed to create message stream")?;

    let mut cdr_deserializer = CdrDeserializer::new(&msg_definition_table);
    for (index, message_result) in message_stream.enumerate() {
        let message =
            message_result.with_context(|| format!("Failed to read message {}", index))?;

        if let Some(schema) = &message.channel.schema {
            let type_name = extract_message_type(&schema.name).to_string();
            let parsed_message = cdr_deserializer.parse(&type_name, &message.data);

            if !parsed_messages.contains_key(&type_name) {
                parsed_messages.insert(type_name, vec![parsed_message]);
            } else {
                parsed_messages
                    .get_mut(&type_name)
                    .unwrap()
                    .push(parsed_message);
            }
        }
    }

    let converter = MessageDefinitionToArrowSchemaConverter::new(&msg_definition_table);
    let schemas = converter.convert_all();

    let mut record_batches = Vec::new();
    for (name, ros_messages) in parsed_messages.iter() {
        let record_batch_builder = RecordBatchBuilder::new(&schemas, ros_messages);
        let record_batch = record_batch_builder.build(name);
        record_batches.push(record_batch);
    }

    Ok(record_batches)
}

fn read_mcap<P: AsRef<Utf8Path>>(path: P) -> Result<Mmap> {
    let fd = fs::File::open(path.as_ref()).context("Couldn't open MCap file")?;
    unsafe { Mmap::map(&fd) }.context("Couldn't map MCap file")
}

pub fn parse_msg_definition(schema_text: &str) -> Result<()> {
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

fn non_array_ros_data_type(input: &str) -> IResult<&str, BaseType> {
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

struct MessageDefinitionToArrowSchemaConverter<'a> {
    message_definition_table: &'a HashMap<&'a str, MessageDefinition<'a>>,
}

impl<'a> MessageDefinitionToArrowSchemaConverter<'a> {
    pub fn new(message_definition_table: &'a HashMap<&'a str, MessageDefinition<'a>>) -> Self {
        Self {
            message_definition_table,
        }
    }

    pub fn convert(&self, name: &str) -> Arc<Schema> {
        let message_definition = self.message_definition_table.get(name).unwrap();
        let fields: Vec<Field> = message_definition
            .fields
            .iter()
            .map(|field| self.ros_field_to_arrow_field(field))
            .collect();
        Arc::new(Schema::new(fields))
    }

    pub fn convert_all(&self) -> HashMap<&'a str, Arc<Schema>> {
        let mut schemas = HashMap::new();
        for (name, message_definition) in self.message_definition_table.iter() {
            schemas.insert(*name, self.convert(name).clone());
        }
        schemas
    }

    fn ros_field_to_arrow_field(&self, field: &FieldDefinition<'a>) -> Field {
        let arrow_type = match &field.data_type {
            FieldType::Base(base_type) => self.ros_base_type_to_arrow_data_type(base_type),
            FieldType::Array { data_type, length } => {
                self.ros_array_type_to_arrow_data_type(data_type, *length)
            }
            FieldType::Sequence(data_type) => self.ros_sequence_type_to_arrow_data_type(data_type),
        };

        Field::new(field.name, arrow_type, true)
    }

    fn ros_array_type_to_arrow_data_type(&self, data_type: &BaseType, length: u32) -> DataType {
        let base_arrow_type = self.ros_base_type_to_arrow_data_type(data_type);
        let array_arrow_type = DataType::FixedSizeList(
            Arc::new(Field::new("item", base_arrow_type, true)),
            length as i32,
        );
        array_arrow_type
    }

    fn ros_sequence_type_to_arrow_data_type(&self, data_type: &BaseType) -> DataType {
        let base_arrow_type = self.ros_base_type_to_arrow_data_type(data_type);
        let sequence_arrow_type =
            DataType::List(Arc::new(Field::new("item", base_arrow_type, true)));
        sequence_arrow_type
    }

    fn ros_base_type_to_arrow_data_type(&self, base_type: &BaseType) -> DataType {
        match base_type {
            BaseType::Primitive(primitive) => self.ros_primitive_to_arrow_data_type(primitive),
            BaseType::Complex(name) => {
                let message_definition = self.message_definition_table.get(name.as_str()).unwrap();
                let fields = message_definition
                    .fields
                    .iter()
                    .map(|field| self.ros_field_to_arrow_field(field))
                    .collect();
                DataType::Struct(fields)
            }
        }
    }

    fn ros_primitive_to_arrow_data_type(&self, primitive: &Primitive) -> DataType {
        match primitive {
            Primitive::Bool => DataType::Boolean,
            Primitive::Byte => DataType::UInt8,
            Primitive::Char => DataType::UInt8,
            Primitive::Float32 => DataType::Float32,
            Primitive::Float64 => DataType::Float64,
            Primitive::Int8 => DataType::Int8,
            Primitive::UInt8 => DataType::UInt8,
            Primitive::Int16 => DataType::Int16,
            Primitive::UInt16 => DataType::UInt16,
            Primitive::Int32 => DataType::Int32,
            Primitive::UInt32 => DataType::UInt32,
            Primitive::Int64 => DataType::Int64,
            Primitive::UInt64 => DataType::UInt64,
            Primitive::String => DataType::Utf8,
        }
    }

    fn ros_complex_type_to_arrow_data_type(&self, name: &str) -> DataType {
        let message_definition = self.message_definition_table.get(name).unwrap();
        let fields = message_definition
            .fields
            .iter()
            .map(|field| self.ros_field_to_arrow_field(field))
            .collect();
        DataType::Struct(fields)
    }
}

pub struct RecordBatchBuilder<'a> {
    schemas: &'a HashMap<&'a str, Arc<Schema>>,
    messages: &'a Vec<Message>,
}

impl<'a> RecordBatchBuilder<'a> {
    pub fn new(schemas: &'a HashMap<&'a str, Arc<Schema>>, messages: &'a Vec<Message>) -> Self {
        Self { schemas, messages }
    }

    pub fn build(&self, name: &str) -> RecordBatch {
        let mut builders = self.schemas[name]
            .fields()
            .iter()
            .map(|field| -> Box<dyn ArrayBuilder> {
                match field.data_type() {
                    DataType::Boolean => Box::new(BooleanBuilder::new()),
                    DataType::UInt8 => Box::new(UInt8Builder::new()),
                    DataType::UInt16 => Box::new(UInt16Builder::new()),
                    DataType::UInt32 => Box::new(UInt32Builder::new()),
                    DataType::UInt64 => Box::new(UInt64Builder::new()),
                    DataType::Int8 => Box::new(Int8Builder::new()),
                    DataType::Int16 => Box::new(Int16Builder::new()),
                    DataType::Int32 => Box::new(Int32Builder::new()),
                    DataType::Int64 => Box::new(Int64Builder::new()),
                    DataType::Float32 => Box::new(Float32Builder::new()),
                    DataType::Float64 => Box::new(Float64Builder::new()),
                    DataType::Utf8 => Box::new(StringBuilder::new()),
                    DataType::Struct(fields) => self.build_struct_builder(fields),
                    _ => unreachable!(),
                }
            })
            .collect::<Vec<_>>();

        for message in self.messages.iter() {
            for (i, field) in message.value.iter().enumerate() {
                let builder = &mut builders[i];
                self.append_value(builder, &field.value);
            }
        }

        let arrays = builders
            .iter_mut()
            .map(|builder| builder.finish())
            .collect::<Vec<_>>();

        let record_batch = RecordBatch::try_new(self.schemas[name].clone(), arrays).unwrap();
        record_batch
    }

    pub fn build_struct_builder(&self, fields: &Fields) -> Box<dyn ArrayBuilder> {
        let field_builders = fields
            .iter()
            .map(|field| -> Box<dyn ArrayBuilder> {
                match field.data_type() {
                    DataType::Boolean => Box::new(BooleanBuilder::new()),
                    DataType::UInt8 => Box::new(UInt8Builder::new()),
                    DataType::UInt16 => Box::new(UInt16Builder::new()),
                    DataType::UInt32 => Box::new(UInt32Builder::new()),
                    DataType::UInt64 => Box::new(UInt64Builder::new()),
                    DataType::Int8 => Box::new(Int8Builder::new()),
                    DataType::Int16 => Box::new(Int16Builder::new()),
                    DataType::Int32 => Box::new(Int32Builder::new()),
                    DataType::Int64 => Box::new(Int64Builder::new()),
                    DataType::Float32 => Box::new(Float32Builder::new()),
                    DataType::Float64 => Box::new(Float64Builder::new()),
                    DataType::Utf8 => Box::new(StringBuilder::new()),
                    DataType::Struct(sub_fields) => self.build_struct_builder(sub_fields),
                    _ => unreachable!(),
                }
            })
            .collect();
        Box::new(StructBuilder::new(fields.clone(), field_builders))
    }

    pub fn append_value(&self, builder: &mut dyn ArrayBuilder, value: &FieldValue) {
        match value {
            FieldValue::Base(base_value) => match base_value {
                BaseValue::Primitive(primitive) => self.append_primitive(builder, primitive),
                BaseValue::Complex(complex) => {
                    let struct_builder = builder
                        .as_any_mut()
                        .downcast_mut::<StructBuilder>()
                        .unwrap();
                    self.append_complex(struct_builder, complex);
                }
            },
            FieldValue::Array(array) => self.append_array(builder, array),
            FieldValue::Sequence(sequence) => self.append_sequence(builder, sequence),
        }
    }

    pub fn append_array(&self, builder: &mut dyn ArrayBuilder, value: &[BaseValue]) {
        match &value[0] {
            BaseValue::Primitive(primitive) => match primitive {
                PrimitiveValue::Bool(_) => {
                    let boolean_array_builder = builder
                        .as_any_mut()
                        .downcast_mut::<FixedSizeListBuilder<BooleanBuilder>>()
                        .unwrap();
                    for base_value in value.iter() {
                        match base_value {
                            BaseValue::Primitive(PrimitiveValue::Bool(b)) => {
                                boolean_array_builder.values().append_value(*b);
                            }
                            _ => unreachable!(),
                        }
                    }
                    boolean_array_builder.append(true);
                }
                PrimitiveValue::Byte(value) => {
                    todo!()
                }
                PrimitiveValue::Char(value) => {
                    todo!()
                }
                PrimitiveValue::Float32(_) => {
                    let float32_array_builder = builder
                        .as_any_mut()
                        .downcast_mut::<FixedSizeListBuilder<Float32Builder>>()
                        .unwrap();
                    for base_value in value.iter() {
                        match base_value {
                            BaseValue::Primitive(PrimitiveValue::Float32(f)) => {
                                float32_array_builder.values().append_value(*f);
                            }
                            _ => unreachable!(),
                        }
                    }
                    float32_array_builder.append(true);
                }
                PrimitiveValue::Float64(_) => {
                    let float64_array_builder = builder
                        .as_any_mut()
                        .downcast_mut::<FixedSizeListBuilder<Float64Builder>>()
                        .unwrap();
                    for base_value in value.iter() {
                        match base_value {
                            BaseValue::Primitive(PrimitiveValue::Float64(f)) => {
                                float64_array_builder.values().append_value(*f);
                            }
                            _ => unreachable!(),
                        }
                    }
                    float64_array_builder.append(true);
                }
                PrimitiveValue::Int8(_) => {
                    todo!()
                }
                PrimitiveValue::UInt8(_) => {
                    todo!()
                }
                PrimitiveValue::Int16(_) => {
                    todo!()
                }
                PrimitiveValue::UInt16(_) => {
                    todo!()
                }
                PrimitiveValue::Int32(_) => {
                    todo!()
                }
                PrimitiveValue::UInt32(_) => {
                    todo!()
                }
                PrimitiveValue::Int64(_) => {
                    let int64_array_builder = builder
                        .as_any_mut()
                        .downcast_mut::<FixedSizeListBuilder<Int64Builder>>()
                        .unwrap();
                    for base_value in value.iter() {
                        match base_value {
                            BaseValue::Primitive(PrimitiveValue::Int64(i)) => {
                                int64_array_builder.values().append_value(*i);
                            }
                            _ => unreachable!(),
                        }
                    }
                    int64_array_builder.append(true);
                }
                PrimitiveValue::UInt64(_) => {
                    let uint64_array_builder = builder
                        .as_any_mut()
                        .downcast_mut::<FixedSizeListBuilder<UInt64Builder>>()
                        .unwrap();
                    for base_value in value.iter() {
                        match base_value {
                            BaseValue::Primitive(PrimitiveValue::UInt64(u)) => {
                                uint64_array_builder.values().append_value(*u);
                            }
                            _ => unreachable!(),
                        }
                    }
                    uint64_array_builder.append(true);
                }
                PrimitiveValue::String(_) => {
                    let string_array_builder = builder
                        .as_any_mut()
                        .downcast_mut::<ListBuilder<StringBuilder>>()
                        .unwrap();
                    for base_value in value.iter() {
                        match base_value {
                            BaseValue::Primitive(PrimitiveValue::String(s)) => {
                                string_array_builder.values().append_value(s);
                            }
                            _ => unreachable!(),
                        }
                    }
                    string_array_builder.append(true);
                }
            },
            BaseValue::Complex(complex) => {
                let struct_array_builder = builder
                    .as_any_mut()
                    .downcast_mut::<ListBuilder<StructBuilder>>()
                    .unwrap();
                for complex_value in value.iter() {
                    match complex_value {
                        BaseValue::Complex(complex) => {
                            let substruct_builder = struct_array_builder
                                .values()
                                .as_any_mut()
                                .downcast_mut::<StructBuilder>()
                                .unwrap();
                            self.append_complex(substruct_builder, complex);
                        }
                        _ => unreachable!(),
                    }
                }
                struct_array_builder.append(true);
            }
        }
    }

    pub fn append_sequence(&self, builder: &mut dyn ArrayBuilder, value: &[BaseValue]) {
        match &value[0] {
            BaseValue::Primitive(primitive) => match primitive {
                PrimitiveValue::Bool(_) => {
                    let boolean_array_builder = builder
                        .as_any_mut()
                        .downcast_mut::<ListBuilder<BooleanBuilder>>()
                        .unwrap();
                    for base_value in value.iter() {
                        match base_value {
                            BaseValue::Primitive(PrimitiveValue::Bool(b)) => {
                                boolean_array_builder.values().append_value(*b);
                            }
                            _ => unreachable!(),
                        }
                    }
                }
                PrimitiveValue::Byte(value) => {
                    todo!()
                }
                PrimitiveValue::Char(value) => {
                    todo!()
                }
                PrimitiveValue::Float32(_) => {
                    let float32_array_builder = builder
                        .as_any_mut()
                        .downcast_mut::<ListBuilder<Float32Builder>>()
                        .unwrap();
                    for base_value in value.iter() {
                        match base_value {
                            BaseValue::Primitive(PrimitiveValue::Float32(f)) => {
                                float32_array_builder.values().append_value(*f);
                            }
                            _ => unreachable!(),
                        }
                    }
                }
                PrimitiveValue::Float64(_) => {
                    let float64_array_builder = builder
                        .as_any_mut()
                        .downcast_mut::<ListBuilder<Float64Builder>>()
                        .unwrap();
                    for base_value in value.iter() {
                        match base_value {
                            BaseValue::Primitive(PrimitiveValue::Float64(f)) => {
                                float64_array_builder.values().append_value(*f);
                            }
                            _ => unreachable!(),
                        }
                    }
                }
                PrimitiveValue::Int8(_) => {
                    todo!()
                }
                PrimitiveValue::UInt8(_) => {
                    todo!()
                }
                PrimitiveValue::Int16(_) => {
                    todo!()
                }
                PrimitiveValue::UInt16(_) => {
                    todo!()
                }
                PrimitiveValue::Int32(_) => {
                    todo!()
                }
                PrimitiveValue::UInt32(_) => {
                    todo!()
                }
                PrimitiveValue::Int64(_) => {
                    let int64_array_builder = builder
                        .as_any_mut()
                        .downcast_mut::<ListBuilder<Int64Builder>>()
                        .unwrap();
                    for base_value in value.iter() {
                        match base_value {
                            BaseValue::Primitive(PrimitiveValue::Int64(i)) => {
                                int64_array_builder.values().append_value(*i);
                            }
                            _ => unreachable!(),
                        }
                    }
                }
                PrimitiveValue::UInt64(_) => {
                    let uint64_array_builder = builder
                        .as_any_mut()
                        .downcast_mut::<ListBuilder<UInt64Builder>>()
                        .unwrap();
                    for base_value in value.iter() {
                        match base_value {
                            BaseValue::Primitive(PrimitiveValue::UInt64(u)) => {
                                uint64_array_builder.values().append_value(*u);
                            }
                            _ => unreachable!(),
                        }
                    }
                }
                PrimitiveValue::String(_) => {
                    let string_array_builder = builder
                        .as_any_mut()
                        .downcast_mut::<ListBuilder<StringBuilder>>()
                        .unwrap();
                    for base_value in value.iter() {
                        match base_value {
                            BaseValue::Primitive(PrimitiveValue::String(s)) => {
                                string_array_builder.values().append_value(s);
                            }
                            _ => unreachable!(),
                        }
                    }
                }
            },
            BaseValue::Complex(complex) => {
                let struct_builder = builder
                    .as_any_mut()
                    .downcast_mut::<StructBuilder>()
                    .unwrap();
                self.append_complex(struct_builder, complex);
            }
        }
    }

    pub fn append_complex(&self, struct_builder: &mut StructBuilder, message: &Message) {
        for (i, field_builder) in struct_builder.field_builders_mut().iter_mut().enumerate() {
            let field = &message.value[i];
            match &field.value {
                FieldValue::Base(base_value) => match base_value {
                    BaseValue::Primitive(primitive) => {
                        self.append_primitive(field_builder, &primitive);
                    }
                    BaseValue::Complex(complex) => {
                        let substruct_builder = field_builder
                            .as_any_mut()
                            .downcast_mut::<StructBuilder>()
                            .unwrap();
                        self.append_complex(substruct_builder, &complex);
                    }
                },
                FieldValue::Array(array) => {
                    self.append_array(field_builder, array);
                }
                FieldValue::Sequence(sequence) => {
                    self.append_sequence(field_builder, sequence);
                }
            }
        }

        struct_builder.append(true);
    }

    pub fn append_primitive(&self, builder: &mut dyn ArrayBuilder, value: &PrimitiveValue) {
        match value {
            PrimitiveValue::Bool(value) => {
                let boolean_builder = builder
                    .as_any_mut()
                    .downcast_mut::<BooleanBuilder>()
                    .unwrap();
                boolean_builder.append_value(*value);
            }
            PrimitiveValue::Byte(value) => {
                let uint8_builder = builder.as_any_mut().downcast_mut::<UInt8Builder>().unwrap();
                uint8_builder.append_value(*value);
            }
            PrimitiveValue::Char(value) => {
                todo!()
            }
            PrimitiveValue::Float32(value) => {
                let float32_builder = builder
                    .as_any_mut()
                    .downcast_mut::<Float32Builder>()
                    .unwrap();
                float32_builder.append_value(*value);
            }
            PrimitiveValue::Float64(value) => {
                let float64_builder = builder
                    .as_any_mut()
                    .downcast_mut::<Float64Builder>()
                    .unwrap();
                float64_builder.append_value(*value);
            }
            PrimitiveValue::Int8(value) => {
                let int8_builder = builder.as_any_mut().downcast_mut::<Int8Builder>().unwrap();
                int8_builder.append_value(*value);
            }
            PrimitiveValue::UInt8(value) => {
                let uint8_builder = builder.as_any_mut().downcast_mut::<UInt8Builder>().unwrap();
                uint8_builder.append_value(*value);
            }
            PrimitiveValue::Int16(value) => {
                let int16_builder = builder.as_any_mut().downcast_mut::<Int16Builder>().unwrap();
                int16_builder.append_value(*value);
            }
            PrimitiveValue::UInt16(value) => {
                let uint16_builder = builder
                    .as_any_mut()
                    .downcast_mut::<UInt16Builder>()
                    .unwrap();
                uint16_builder.append_value(*value);
            }
            PrimitiveValue::Int32(value) => {
                let int32_builder = builder.as_any_mut().downcast_mut::<Int32Builder>().unwrap();
                int32_builder.append_value(*value);
            }
            PrimitiveValue::UInt32(value) => {
                let uint32_builder = builder
                    .as_any_mut()
                    .downcast_mut::<UInt32Builder>()
                    .unwrap();
                uint32_builder.append_value(*value);
            }
            PrimitiveValue::UInt64(value) => {
                let uint64_builder = builder
                    .as_any_mut()
                    .downcast_mut::<UInt64Builder>()
                    .unwrap();
                uint64_builder.append_value(*value);
            }
            PrimitiveValue::Int64(value) => {
                let int64_builder = builder.as_any_mut().downcast_mut::<Int64Builder>().unwrap();
                int64_builder.append_value(*value);
            }
            PrimitiveValue::String(value) => {
                let string_builder = builder
                    .as_any_mut()
                    .downcast_mut::<StringBuilder>()
                    .unwrap();
                string_builder.append_value(value);
            }
        }
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
            Some(&create_vector3_definition())
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
        let schema_text = include_str!("../testdata/schema/joint_state.txt");
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

    #[test]
    fn test_rosbag2ros_msg_values() {
        let test_path = "rosbags/non_array_msgs/non_array_msgs_0.mcap";
        let ros_msg_values = rosbag2ros_msg_values(test_path).unwrap();

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
        let ros_msg_values = rosbag2ros_msg_values(test_path).unwrap();

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
    fn test_record_batch_builder() {
        let test_path = "rosbags/non_array_msgs/non_array_msgs_0.mcap";
        let record_batches = rosbag2record_batches(test_path).unwrap();
    }
}
