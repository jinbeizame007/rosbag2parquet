//! CDR (Common Data Representation) to ROS message parser

use std::collections::HashMap;

use anyhow::Result;

use super::data::{BaseValue, Field, FieldValue, Message, PrimitiveValue};
use super::types::{BaseType, FieldDefinition, FieldType, MessageDefinition, Primitive};
use crate::cdr::CdrDeserializer;

pub struct SingleMessageCdrRosParser<'a> {
    msg_definition_table: &'a HashMap<&'a str, MessageDefinition<'a>>,
    cdr_deserializer: CdrDeserializer<'a>,
    name: String,
}

impl<'a> SingleMessageCdrRosParser<'a> {
    pub fn new(
        msg_definition_table: &'a HashMap<&'a str, MessageDefinition<'a>>,
        name: String,
        data: &'a [u8],
    ) -> Self {
        Self {
            msg_definition_table,
            cdr_deserializer: CdrDeserializer::new(data),
            name,
        }
    }

    pub fn parse(&mut self) -> Result<Message> {
        self.parse_without_header()
    }

    fn parse_without_header(&mut self) -> Result<Message> {
        let mut value = Message {
            name: self.name.clone(),
            value: Vec::new(),
        };

        let msg_definition = self
            .msg_definition_table
            .get(self.name.as_str())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Message definition not found for type: '{}'. Available types: {:?}",
                    self.name,
                    self.msg_definition_table.keys().collect::<Vec<_>>()
                )
            })?;

        for field in msg_definition.fields.iter() {
            let field_value = self.parse_field(field)?;
            value.value.push(field_value);
        }

        Ok(value)
    }

    fn parse_field(&mut self, field: &'a FieldDefinition<'a>) -> Result<Field> {
        let value = match &field.data_type {
            FieldType::Sequence(non_array_data_type) => {
                FieldValue::Sequence(self.parse_dynamic_array(non_array_data_type)?)
            }
            FieldType::Array { data_type, length } => {
                FieldValue::Array(self.parse_static_array(data_type, length)?)
            }
            FieldType::Base(non_array_data_type) => match non_array_data_type {
                BaseType::Primitive(prim) => {
                    FieldValue::Base(BaseValue::Primitive(self.parse_primitive(prim)?))
                }
                BaseType::Complex(name) => {
                    FieldValue::Base(BaseValue::Complex(self.parse_complex(name)?))
                }
            },
        };

        Ok(Field {
            name: field.name.to_string(),
            value,
        })
    }

    fn parse_static_array(
        &mut self,
        non_array_data_type: &BaseType,
        length: &u32,
    ) -> Result<Vec<BaseValue>> {
        let mut values = Vec::new();
        for _ in 0..*length {
            let value = self.parse_non_array_data_value(non_array_data_type)?;
            values.push(value);
        }
        Ok(values)
    }

    fn parse_dynamic_array(&mut self, non_array_data_type: &BaseType) -> Result<Vec<BaseValue>> {
        let length = self.cdr_deserializer.read_sequence_length()?;
        let mut values = Vec::with_capacity(length as usize);
        for _ in 0..length {
            let value = self.parse_non_array_data_value(non_array_data_type)?;
            values.push(value);
        }
        Ok(values)
    }

    fn parse_non_array_data_value(&mut self, non_array_data_type: &BaseType) -> Result<BaseValue> {
        match non_array_data_type {
            BaseType::Primitive(prim) => Ok(BaseValue::Primitive(self.parse_primitive(prim)?)),
            BaseType::Complex(name) => Ok(BaseValue::Complex(self.parse_complex(name)?)),
        }
    }

    fn parse_complex(&mut self, name: &str) -> Result<Message> {
        let msg_definition = self.msg_definition_table.get(name).ok_or_else(|| {
            anyhow::anyhow!(
                "Message definition not found for complex type: '{}'. Available types: {:?}",
                name,
                self.msg_definition_table.keys().collect::<Vec<_>>()
            )
        })?;
        let mut ros_msg_value = Message {
            name: name.to_string(),
            value: Vec::new(),
        };

        for field in msg_definition.fields.iter() {
            let field_value = self.parse_field(field)?;
            ros_msg_value.value.push(field_value);
        }

        Ok(ros_msg_value)
    }

    fn parse_primitive(&mut self, prim: &Primitive) -> Result<PrimitiveValue> {
        Ok(match prim {
            Primitive::Bool => PrimitiveValue::Bool(self.cdr_deserializer.deserialize_bool()?),
            Primitive::Byte => PrimitiveValue::Byte(self.cdr_deserializer.deserialize_u8()?),
            Primitive::Char => PrimitiveValue::Char(self.cdr_deserializer.deserialize_char()?),
            Primitive::Float32 => PrimitiveValue::Float32(self.cdr_deserializer.deserialize_f32()?),
            Primitive::Float64 => PrimitiveValue::Float64(self.cdr_deserializer.deserialize_f64()?),
            Primitive::Int8 => PrimitiveValue::Int8(self.cdr_deserializer.deserialize_i8()?),
            Primitive::UInt8 => PrimitiveValue::UInt8(self.cdr_deserializer.deserialize_u8()?),
            Primitive::Int16 => PrimitiveValue::Int16(self.cdr_deserializer.deserialize_i16()?),
            Primitive::UInt16 => PrimitiveValue::UInt16(self.cdr_deserializer.deserialize_u16()?),
            Primitive::Int32 => PrimitiveValue::Int32(self.cdr_deserializer.deserialize_i32()?),
            Primitive::UInt32 => PrimitiveValue::UInt32(self.cdr_deserializer.deserialize_u32()?),
            Primitive::Int64 => PrimitiveValue::Int64(self.cdr_deserializer.deserialize_i64()?),
            Primitive::UInt64 => PrimitiveValue::UInt64(self.cdr_deserializer.deserialize_u64()?),
            Primitive::String => {
                PrimitiveValue::String(self.cdr_deserializer.deserialize_string()?)
            }
        })
    }
}

pub struct CdrRosParser<'a> {
    msg_definition_table: &'a HashMap<&'a str, MessageDefinition<'a>>,
}

impl<'a> CdrRosParser<'a> {
    pub fn new(msg_definition_table: &'a HashMap<&'a str, MessageDefinition<'a>>) -> Self {
        Self {
            msg_definition_table,
        }
    }

    pub fn parse(&mut self, name: &str, data: &[u8]) -> Result<Message> {
        let mut single_message_parser =
            SingleMessageCdrRosParser::new(self.msg_definition_table, name.to_string(), data);
        single_message_parser.parse()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ros::types::test_helpers::*;
    use byteorder::{BigEndian, ByteOrder};

    #[test]
    fn test_cdr_ros_parser_vector3d() {
        let vector3d_msg_definition = create_vector3_definition();
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

        let mut cdr_ros_parser = CdrRosParser::new(&vector3d_msg_definition_table);
        let value = cdr_ros_parser.parse("Vector3", &data).unwrap();
        let expected = create_vector3_message(1.0, 2.0, 3.0);
        assert_eq!(value, expected);
    }
}
