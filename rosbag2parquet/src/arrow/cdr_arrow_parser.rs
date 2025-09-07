use std::collections::HashMap;
use std::sync::Arc;

use crate::error::{Result, Rosbag2ParquetError};
use arrow::array::{
    ArrayBuilder, BooleanBuilder, FixedSizeListBuilder, Float32Builder, Float64Builder,
    Int16Builder, Int32Builder, Int64Builder, Int8Builder, StringBuilder, StructBuilder,
    TimestampNanosecondBuilder, UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder,
};
use arrow::datatypes::Schema;
use arrow_array::RecordBatch;

use crate::arrow::core::{
    create_array_builder, downcast_fixed_size_list_builder, downcast_list_builder,
    impl_parse_array_typed, impl_parse_primitive_typed, impl_parse_sequence_typed,
};
use crate::cdr::CdrDeserializer;
use crate::ros::{BaseType, FieldDefinition, FieldType, MessageDefinition, Primitive};

pub struct CdrArrowParser<'a> {
    array_builders_table: HashMap<String, Vec<Box<dyn ArrayBuilder>>>,
    topic_name_type_table: &'a HashMap<String, String>,
    msg_definition_table: &'a HashMap<&'a str, MessageDefinition<'a>>,
    schemas: &'a mut HashMap<&'a str, Arc<Schema>>,
}

impl<'a> CdrArrowParser<'a> {
    pub fn new(
        topic_name_type_table: &'a HashMap<String, String>,
        msg_definition_table: &'a HashMap<&'a str, MessageDefinition<'a>>,
        schemas: &'a mut HashMap<&'a str, Arc<Schema>>,
    ) -> Result<Self> {
        let array_builders_table = topic_name_type_table
            .iter()
            .map(|(topic_name, type_name)| {
                let schema = schemas.get(type_name.as_str()).ok_or_else(|| {
                    Rosbag2ParquetError::SchemaError {
                        type_name: type_name.clone(),
                        message: format!(
                            "Schema not found during initialization (topic: {})",
                            topic_name
                        ),
                    }
                })?;
                Ok((
                    topic_name.to_string(),
                    schema
                        .fields()
                        .iter()
                        .map(|field| create_array_builder(field.data_type()))
                        .collect::<Vec<Box<dyn ArrayBuilder>>>(),
                ))
            })
            .collect::<Result<HashMap<String, Vec<Box<dyn ArrayBuilder>>>>>()?;

        Ok(Self {
            array_builders_table,
            topic_name_type_table,
            msg_definition_table,
            schemas,
        })
    }

    pub fn parse(&mut self, topic_name: &str, data: &[u8], timestamp_ns: i64) -> Result<()> {
        let type_name = self.topic_name_type_table.get(topic_name).ok_or_else(|| {
            Rosbag2ParquetError::TopicNotFound {
                topic: topic_name.to_owned(),
            }
        })?;
        let mut single_message_parser = SingleMessageCdrArrowParser::new(
            &mut self.array_builders_table,
            self.msg_definition_table,
            topic_name,
            type_name.as_str(),
            data,
            timestamp_ns,
        );
        single_message_parser.parse()
    }

    pub fn finish(&mut self) -> Result<HashMap<String, RecordBatch>> {
        let mut batches = HashMap::new();
        let keys = self
            .array_builders_table
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for name in keys {
            let type_name = self.topic_name_type_table.get(&name).ok_or_else(|| {
                Rosbag2ParquetError::TypeNotFound {
                    type_name: "unknown".to_string(),
                    topic: name.clone(),
                }
            })?;
            let schema = self.schemas.get(type_name.as_str()).ok_or_else(|| {
                Rosbag2ParquetError::SchemaError {
                    type_name: type_name.clone(),
                    message: "Schema not found".to_string(),
                }
            })?;
            let mut builders = self.array_builders_table.remove(&name).ok_or_else(|| {
                Rosbag2ParquetError::SchemaError {
                    type_name: type_name.clone(),
                    message: format!("Array builders not found for topic: {}", name),
                }
            })?;
            let built_array = builders
                .iter_mut()
                .map(|builder| builder.finish())
                .collect::<Vec<_>>();
            let batch = RecordBatch::try_new(schema.clone(), built_array).map_err(|e| {
                Rosbag2ParquetError::SchemaError {
                    type_name: type_name.clone(),
                    message: format!("Failed to create RecordBatch for topic {}: {}", name, e),
                }
            })?;
            batches.insert(name.to_string(), batch);
        }

        Ok(batches)
    }
}

pub struct SingleMessageCdrArrowParser<'a> {
    array_builders_table: &'a mut HashMap<String, Vec<Box<dyn ArrayBuilder>>>,
    msg_definition_table: &'a HashMap<&'a str, MessageDefinition<'a>>,
    cdr_deserializer: CdrDeserializer<'a>,
    topic_name: String,
    type_name: &'a str,
    timestamp_ns: i64,
}

impl<'a> SingleMessageCdrArrowParser<'a> {
    pub fn new(
        array_builders_table: &'a mut HashMap<String, Vec<Box<dyn ArrayBuilder>>>,
        msg_definition_table: &'a HashMap<&'a str, MessageDefinition<'a>>,
        topic_name: &str,
        type_name: &'a str,
        data: &'a [u8],
        timestamp_ns: i64,
    ) -> Self {
        Self {
            array_builders_table,
            msg_definition_table,
            cdr_deserializer: CdrDeserializer::new(data),
            topic_name: topic_name.to_owned(),
            type_name,
            timestamp_ns,
        }
    }

    impl_parse_sequence_typed! {
        bool => BooleanBuilder => bool,
        f32 => Float32Builder => f32,
        f64 => Float64Builder => f64,
        i8 => Int8Builder => i8,
        u8 => UInt8Builder => u8,
        i16 => Int16Builder => i16,
        u16 => UInt16Builder => u16,
        i32 => Int32Builder => i32,
        u32 => UInt32Builder => u32,
        i64 => Int64Builder => i64,
        u64 => UInt64Builder => u64,
    }

    impl_parse_array_typed! {
        bool => BooleanBuilder => bool,
        f32 => Float32Builder => f32,
        f64 => Float64Builder => f64,
        i8 => Int8Builder => i8,
        u8 => UInt8Builder => u8,
        i16 => Int16Builder => i16,
        u16 => UInt16Builder => u16,
        i32 => Int32Builder => i32,
        u32 => UInt32Builder => u32,
        i64 => Int64Builder => i64,
        u64 => UInt64Builder => u64,
    }

    impl_parse_primitive_typed! {
        bool => BooleanBuilder => bool,
        f32 => Float32Builder => f32,
        f64 => Float64Builder => f64,
        i8 => Int8Builder => i8,
        u8 => UInt8Builder => u8,
        i16 => Int16Builder => i16,
        u16 => UInt16Builder => u16,
        i32 => Int32Builder => i32,
        u32 => UInt32Builder => u32,
        i64 => Int64Builder => i64,
        u64 => UInt64Builder => u64,
        string => StringBuilder => String,
    }

    pub fn parse(&mut self) -> Result<()> {
        self.parse_without_header()
    }

    fn parse_without_header(&mut self) -> Result<()> {
        let msg_definition = self
            .msg_definition_table
            .get(self.type_name)
            .ok_or_else(|| Rosbag2ParquetError::SchemaError {
                type_name: self.type_name.to_owned(),
                message: "Message definition not found".to_string(),
            })?;
        let mut array_builders = self
            .array_builders_table
            .remove(&self.topic_name)
            .ok_or_else(|| Rosbag2ParquetError::SchemaError {
                type_name: self.type_name.to_owned(),
                message: format!("Array builders not found for topic: {}", self.topic_name),
            })?;

        let timestamp_builder = array_builders[0]
            .as_any_mut()
            .downcast_mut::<TimestampNanosecondBuilder>()
            .ok_or_else(|| Rosbag2ParquetError::SchemaError {
                type_name: self.type_name.to_owned(),
                message: "Failed to downcast timestamp builder".to_string(),
            })?;
        timestamp_builder.append_value(self.timestamp_ns);

        for (array_builder, field) in array_builders
            .iter_mut()
            .skip(1)
            .zip(msg_definition.fields.iter())
        {
            self.parse_field(field, array_builder)?;
        }

        self.array_builders_table
            .insert(self.topic_name.clone(), array_builders);
        Ok(())
    }

    fn parse_field(
        &mut self,
        field: &FieldDefinition<'a>,
        array_builder: &mut dyn ArrayBuilder,
    ) -> Result<()> {
        match &field.data_type {
            FieldType::Array { data_type, length } => {
                self.parse_array(data_type, length, array_builder)
            }
            FieldType::Sequence(base_type) => self.parse_sequence(base_type, array_builder),
            FieldType::Base(base_type) => self.parse_base_type(base_type, array_builder),
        }
    }

    fn parse_array(
        &mut self,
        data_type: &BaseType,
        length: &u32,
        array_builder: &mut dyn ArrayBuilder,
    ) -> Result<()> {
        match data_type {
            BaseType::Primitive(primitive) => match primitive {
                Primitive::Bool => self.parse_array_bool(array_builder, length)?,
                Primitive::Byte => self.parse_array_u8(array_builder, length)?,
                Primitive::Char => self.parse_array_char(array_builder, length)?,
                Primitive::Float32 => self.parse_array_f32(array_builder, length)?,
                Primitive::Float64 => self.parse_array_f64(array_builder, length)?,
                Primitive::Int8 => self.parse_array_i8(array_builder, length)?,
                Primitive::UInt8 => self.parse_array_u8(array_builder, length)?,
                Primitive::Int16 => self.parse_array_i16(array_builder, length)?,
                Primitive::UInt16 => self.parse_array_u16(array_builder, length)?,
                Primitive::Int32 => self.parse_array_i32(array_builder, length)?,
                Primitive::UInt32 => self.parse_array_u32(array_builder, length)?,
                Primitive::Int64 => self.parse_array_i64(array_builder, length)?,
                Primitive::UInt64 => self.parse_array_u64(array_builder, length)?,
                Primitive::String => self.parse_array_string(array_builder, length)?,
            },
            BaseType::Complex(name) => self.parse_array_complex(array_builder, name, length)?,
        }
        Ok(())
    }

    fn parse_array_char(
        &mut self,
        array_builder: &mut dyn ArrayBuilder,
        length: &u32,
    ) -> Result<()> {
        let string_builder = downcast_fixed_size_list_builder::<StringBuilder>(array_builder)?;
        for _ in 0..*length as usize {
            string_builder
                .values()
                .append_value(self.cdr_deserializer.deserialize_char()?.to_string());
        }
        string_builder.append(true);
        Ok(())
    }

    fn parse_array_string(
        &mut self,
        array_builder: &mut dyn ArrayBuilder,
        length: &u32,
    ) -> Result<()> {
        let string_builder = downcast_fixed_size_list_builder::<StringBuilder>(array_builder)?;
        for _ in 0..*length as usize {
            string_builder
                .values()
                .append_value(self.cdr_deserializer.deserialize_string()?);
        }
        string_builder.append(true);
        Ok(())
    }

    fn parse_array_complex(
        &mut self,
        array_builder: &mut dyn ArrayBuilder,
        name: &str,
        length: &u32,
    ) -> Result<()> {
        let substruct_builder = downcast_fixed_size_list_builder::<StructBuilder>(array_builder)?;

        for _ in 0..*length as usize {
            self.parse_complex(name, substruct_builder)?;
        }
        substruct_builder.append(true);
        Ok(())
    }

    fn parse_sequence(
        &mut self,
        data_type: &BaseType,
        array_builder: &mut dyn ArrayBuilder,
    ) -> Result<()> {
        match data_type {
            BaseType::Primitive(primitive) => match primitive {
                Primitive::Bool => self.parse_sequence_bool(array_builder)?,
                Primitive::Byte => self.parse_sequence_u8(array_builder)?,
                Primitive::Char => self.parse_sequence_char(array_builder)?,
                Primitive::Float32 => self.parse_sequence_f32(array_builder)?,
                Primitive::Float64 => self.parse_sequence_f64(array_builder)?,
                Primitive::Int8 => self.parse_sequence_i8(array_builder)?,
                Primitive::UInt8 => self.parse_sequence_u8(array_builder)?,
                Primitive::Int16 => self.parse_sequence_i16(array_builder)?,
                Primitive::UInt16 => self.parse_sequence_u16(array_builder)?,
                Primitive::Int32 => self.parse_sequence_i32(array_builder)?,
                Primitive::UInt32 => self.parse_sequence_u32(array_builder)?,
                Primitive::Int64 => self.parse_sequence_i64(array_builder)?,
                Primitive::UInt64 => self.parse_sequence_u64(array_builder)?,
                Primitive::String => self.parse_sequence_string(array_builder)?,
            },
            BaseType::Complex(name) => self.parse_sequence_complex(array_builder, name)?,
        }
        Ok(())
    }

    fn parse_sequence_char(&mut self, array_builder: &mut dyn ArrayBuilder) -> Result<()> {
        let length = self.cdr_deserializer.read_sequence_length()?;

        let string_builder = downcast_list_builder::<StringBuilder>(array_builder)?;
        for _ in 0..length as usize {
            string_builder
                .values()
                .append_value(self.cdr_deserializer.deserialize_char()?.to_string());
        }
        string_builder.append(true);
        Ok(())
    }

    fn parse_sequence_string(&mut self, array_builder: &mut dyn ArrayBuilder) -> Result<()> {
        let length = self.cdr_deserializer.read_sequence_length()?;

        let string_builder = downcast_list_builder::<StringBuilder>(array_builder)?;
        for _ in 0..length as usize {
            string_builder
                .values()
                .append_value(self.cdr_deserializer.deserialize_string()?);
        }
        string_builder.append(true);
        Ok(())
    }

    fn parse_sequence_complex(
        &mut self,
        array_builder: &mut dyn ArrayBuilder,
        name: &str,
    ) -> Result<()> {
        let length = self.cdr_deserializer.read_sequence_length()?;

        let list_builder = downcast_list_builder::<StructBuilder>(array_builder)?;
        let substruct_builder = list_builder
            .values()
            .as_any_mut()
            .downcast_mut::<StructBuilder>()
            .ok_or_else(|| Rosbag2ParquetError::SchemaError {
                type_name: self.type_name.to_owned(),
                message: "Failed to downcast to StructBuilder".to_string(),
            })?;

        for _ in 0..length as usize {
            self.parse_complex(name, substruct_builder)?;
        }
        list_builder.append(true);
        Ok(())
    }

    fn parse_base_type(
        &mut self,
        base_type: &BaseType,
        array_builder: &mut dyn ArrayBuilder,
    ) -> Result<()> {
        match base_type {
            BaseType::Primitive(primitive) => self.parse_primitive(primitive, array_builder)?,
            BaseType::Complex(name) => self.parse_complex(name, array_builder)?,
        }
        Ok(())
    }

    fn parse_complex(&mut self, name: &str, array_builder: &mut dyn ArrayBuilder) -> Result<()> {
        let msg_definition = self.msg_definition_table.get(name).ok_or_else(|| {
            Rosbag2ParquetError::SchemaError {
                type_name: name.to_string(),
                message: "Message definition not found for complex type".to_string(),
            }
        })?;
        let struct_builder = array_builder
            .as_any_mut()
            .downcast_mut::<StructBuilder>()
            .ok_or_else(|| Rosbag2ParquetError::SchemaError {
                type_name: self.type_name.to_owned(),
                message: "Failed to downcast to StructBuilder".to_string(),
            })?;

        for (i, field_builder) in struct_builder.field_builders_mut().iter_mut().enumerate() {
            let field = &msg_definition.fields[i];
            match &field.data_type {
                FieldType::Array { data_type, length } => {
                    self.parse_array(data_type, length, field_builder)?
                }
                FieldType::Sequence(base_type) => self.parse_sequence(base_type, field_builder)?,
                FieldType::Base(base_type) => self.parse_base_type(base_type, field_builder)?,
            }
        }

        struct_builder.append(true);
        Ok(())
    }

    fn parse_primitive(
        &mut self,
        primitive: &Primitive,
        array_builder: &mut dyn ArrayBuilder,
    ) -> Result<()> {
        match primitive {
            Primitive::Bool => self.parse_bool(array_builder)?,
            Primitive::Byte => self.parse_u8(array_builder)?,
            Primitive::Char => self.parse_char(array_builder)?,
            Primitive::Float32 => self.parse_f32(array_builder)?,
            Primitive::Float64 => self.parse_f64(array_builder)?,
            Primitive::Int8 => self.parse_i8(array_builder)?,
            Primitive::UInt8 => self.parse_u8(array_builder)?,
            Primitive::Int16 => self.parse_i16(array_builder)?,
            Primitive::UInt16 => self.parse_u16(array_builder)?,
            Primitive::Int32 => self.parse_i32(array_builder)?,
            Primitive::UInt32 => self.parse_u32(array_builder)?,
            Primitive::Int64 => self.parse_i64(array_builder)?,
            Primitive::UInt64 => self.parse_u64(array_builder)?,
            Primitive::String => self.parse_string(array_builder)?,
        }
        Ok(())
    }

    fn parse_char(&mut self, array_builder: &mut dyn ArrayBuilder) -> Result<()> {
        let byte_builder = downcast_fixed_size_list_builder::<StringBuilder>(array_builder)?;
        byte_builder
            .values()
            .append_value(self.cdr_deserializer.deserialize_char()?.to_string());
        Ok(())
    }
}
