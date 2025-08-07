//! CDR (Common Data Representation) deserializer for ROS2 messages

use std::collections::HashMap;

use byteorder::{BigEndian, ByteOrder, LittleEndian};

use crate::ros::{
    BaseType, BaseValue, Field, FieldDefinition, FieldType, FieldValue, Message,
    MessageDefinition, Primitive, PrimitiveValue,
};

#[derive(Copy, Clone, PartialEq, Debug)]
pub enum Endianness {
    BigEndian,
    LittleEndian,
}

pub struct CdrDeserializer<'a> {
    position: usize,
    msg_definition_table: &'a HashMap<&'a str, MessageDefinition<'a>>,
    byte_order: Endianness,
}

impl<'a> CdrDeserializer<'a> {
    pub fn new(msg_definition_table: &'a HashMap<&'a str, MessageDefinition<'a>>) -> Self {
        Self {
            position: 0,
            msg_definition_table,
            byte_order: Endianness::BigEndian,
        }
    }

    #[inline]
    fn align_to(&mut self, count: usize) {
        let modulo = (self.position - 4) % count;
        if modulo != 0 {
            self.position += count - modulo;
        }
    }

    #[inline]
    fn next_bytes<'b>(&mut self, data: &'b [u8], count: usize) -> &'b [u8] {
        self.position += count;
        &data[self.position - count..self.position]
    }

    pub fn parse<'b>(&mut self, name: &str, data: &[u8]) -> Message {
        self.byte_order = if data[1] == 0x00 {
            Endianness::BigEndian
        } else {
            Endianness::LittleEndian
        };

        self.position = 4;

        self.parse_without_header(name, data)
    }

    fn parse_without_header(&mut self, name: &str, data: &[u8]) -> Message {
        let mut value = Message {
            name: name.to_string(),
            value: Vec::new(),
        };

        for field in self.msg_definition_table.get(name).unwrap().fields.iter() {
            let field_value = self.parse_field(field, data);
            value.value.push(field_value);
        }

        value
    }

    fn parse_field(&mut self, field: &'a FieldDefinition<'a>, data: &[u8]) -> Field {
        let value = match &field.data_type {
            FieldType::Sequence(non_array_data_type) => {
                FieldValue::Sequence(self.parse_dynamic_array(non_array_data_type, data))
            }
            FieldType::Array { data_type, length } => {
                FieldValue::Array(self.parse_static_array(data_type, length, data))
            }
            FieldType::Base(non_array_data_type) => match non_array_data_type {
                BaseType::Primitive(prim) => {
                    FieldValue::Base(BaseValue::Primitive(self.parse_primitive(prim, data)))
                }
                BaseType::Complex(name) => {
                    FieldValue::Base(BaseValue::Complex(self.parse_complex(name, data)))
                }
            },
        };

        Field {
            name: field.name.to_string(),
            value,
        }
    }

    fn parse_static_array(
        &mut self,
        non_array_data_type: &BaseType,
        length: &u32,
        data: &[u8],
    ) -> Vec<BaseValue> {
        let mut values = Vec::new();
        for _ in 0..*length {
            let value = self.parse_non_array_data_value(non_array_data_type, data);
            values.push(value);
        }
        values
    }

    fn parse_dynamic_array(
        &mut self,
        non_array_data_type: &BaseType,
        data: &[u8],
    ) -> Vec<BaseValue> {
        self.align_to(4);
        let header = self.next_bytes(data, 4);
        let length = match self.byte_order {
            Endianness::BigEndian => BigEndian::read_u32(header),
            Endianness::LittleEndian => LittleEndian::read_u32(header),
        };
        let mut values = Vec::new();
        for _ in 0..length {
            let value = self.parse_non_array_data_value(non_array_data_type, data);
            values.push(value);
        }
        values
    }

    fn parse_non_array_data_value(
        &mut self,
        non_array_data_type: &BaseType,
        data: &[u8],
    ) -> BaseValue {
        match non_array_data_type {
            BaseType::Primitive(prim) => BaseValue::Primitive(self.parse_primitive(prim, data)),
            BaseType::Complex(name) => BaseValue::Complex(self.parse_complex(name, data)),
        }
    }

    fn parse_primitive(&mut self, prim: &Primitive, data: &[u8]) -> PrimitiveValue {
        match prim {
            Primitive::Bool => PrimitiveValue::Bool(self.deserialize_bool(data)),
            Primitive::Byte => PrimitiveValue::Byte(self.deserialize_u8(data)),
            Primitive::Char => PrimitiveValue::Char(self.deserialize_char(data)),
            Primitive::Float32 => PrimitiveValue::Float32(self.deserialize_f32(data)),
            Primitive::Float64 => PrimitiveValue::Float64(self.deserialize_f64(data)),
            Primitive::Int8 => PrimitiveValue::Int8(self.deserialize_i8(data)),
            Primitive::UInt8 => PrimitiveValue::UInt8(self.deserialize_u8(data)),
            Primitive::Int16 => PrimitiveValue::Int16(self.deserialize_i16(data)),
            Primitive::UInt16 => PrimitiveValue::UInt16(self.deserialize_u16(data)),
            Primitive::Int32 => PrimitiveValue::Int32(self.deserialize_i32(data)),
            Primitive::UInt32 => PrimitiveValue::UInt32(self.deserialize_u32(data)),
            Primitive::Int64 => PrimitiveValue::Int64(self.deserialize_i64(data)),
            Primitive::UInt64 => PrimitiveValue::UInt64(self.deserialize_u64(data)),
            Primitive::String => PrimitiveValue::String(self.deserialize_string(data)),
        }
    }

    #[inline]
    fn deserialize_f64(&mut self, data: &[u8]) -> f64 {
        self.align_to(8);
        let bytes = self.next_bytes(data, 8);
        match self.byte_order {
            Endianness::BigEndian => BigEndian::read_f64(bytes),
            Endianness::LittleEndian => LittleEndian::read_f64(bytes),
        }
    }

    #[inline]
    fn deserialize_f32(&mut self, data: &[u8]) -> f32 {
        self.align_to(4);
        let bytes = self.next_bytes(data, 4);
        match self.byte_order {
            Endianness::BigEndian => BigEndian::read_f32(bytes),
            Endianness::LittleEndian => LittleEndian::read_f32(bytes),
        }
    }

    #[inline]
    fn deserialize_bool(&mut self, data: &[u8]) -> bool {
        let bytes = self.next_bytes(data, 1);
        bytes[0] == 0x01
    }

    #[inline]
    fn deserialize_i8(&mut self, data: &[u8]) -> i8 {
        let bytes = self.next_bytes(data, 1);
        bytes[0] as i8
    }

    #[inline]
    fn deserialize_u8(&mut self, data: &[u8]) -> u8 {
        let bytes = self.next_bytes(data, 1);
        bytes[0]
    }

    #[inline]
    fn deserialize_char(&mut self, data: &[u8]) -> char {
        let byte = self.next_bytes(data, 1)[0];
        byte as char
    }

    #[inline]
    fn deserialize_i16(&mut self, data: &[u8]) -> i16 {
        self.align_to(2);
        let bytes = self.next_bytes(data, 2);
        match self.byte_order {
            Endianness::BigEndian => BigEndian::read_i16(bytes),
            Endianness::LittleEndian => LittleEndian::read_i16(bytes),
        }
    }

    #[inline]
    fn deserialize_u16(&mut self, data: &[u8]) -> u16 {
        self.align_to(2);
        let bytes = self.next_bytes(data, 2);
        match self.byte_order {
            Endianness::BigEndian => BigEndian::read_u16(bytes),
            Endianness::LittleEndian => LittleEndian::read_u16(bytes),
        }
    }

    #[inline]
    fn deserialize_i32(&mut self, data: &[u8]) -> i32 {
        self.align_to(4);
        let bytes = self.next_bytes(data, 4);
        match self.byte_order {
            Endianness::BigEndian => BigEndian::read_i32(bytes),
            Endianness::LittleEndian => LittleEndian::read_i32(bytes),
        }
    }

    #[inline]
    fn deserialize_u32(&mut self, data: &[u8]) -> u32 {
        self.align_to(4);
        let bytes = self.next_bytes(data, 4);
        match self.byte_order {
            Endianness::BigEndian => BigEndian::read_u32(bytes),
            Endianness::LittleEndian => LittleEndian::read_u32(bytes),
        }
    }

    #[inline]
    fn deserialize_i64(&mut self, data: &[u8]) -> i64 {
        self.align_to(8);
        let bytes = self.next_bytes(data, 8);
        match self.byte_order {
            Endianness::BigEndian => BigEndian::read_i64(bytes),
            Endianness::LittleEndian => LittleEndian::read_i64(bytes),
        }
    }

    #[inline]
    fn deserialize_u64(&mut self, data: &[u8]) -> u64 {
        self.align_to(8);
        let bytes = self.next_bytes(data, 8);
        match self.byte_order {
            Endianness::BigEndian => BigEndian::read_u64(bytes),
            Endianness::LittleEndian => LittleEndian::read_u64(bytes),
        }
    }

    fn deserialize_string(&mut self, data: &[u8]) -> String {
        self.align_to(4);
        let header = self.next_bytes(data, 4);
        let byte_length = match self.byte_order {
            Endianness::BigEndian => BigEndian::read_u32(header),
            Endianness::LittleEndian => LittleEndian::read_u32(header),
        };
        let bytes = self.next_bytes(data, byte_length as usize);
        let bytes_without_null = match bytes.split_last() {
            None => bytes,
            Some((_null_char, contents)) => contents,
        };
        std::str::from_utf8(bytes_without_null).unwrap().to_string()
    }

    fn parse_complex(&mut self, name: &str, data: &[u8]) -> Message {
        let msg_definition = self.msg_definition_table.get(name).unwrap();
        let mut ros_msg_value = Message {
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

#[cfg(test)]
mod tests {
    use super::*;
    use byteorder::{BigEndian, ByteOrder};
    use crate::ros::{BaseValue, Field, FieldValue, PrimitiveValue};

    #[test]
    fn test_cdr_deserializer_vector3d() {
        let vector3d_msg_definition = MessageDefinition::new(
            "Vector3",
            vec![
                FieldDefinition::new(
                    FieldType::Base(BaseType::Primitive(Primitive::Float64)),
                    "x",
                ),
                FieldDefinition::new(
                    FieldType::Base(BaseType::Primitive(Primitive::Float64)),
                    "y",
                ),
                FieldDefinition::new(
                    FieldType::Base(BaseType::Primitive(Primitive::Float64)),
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
            Message {
                name: "Vector3".to_string(),
                value: vec![
                    Field::new(
                        "x".to_string(),
                        FieldValue::Base(BaseValue::Primitive(PrimitiveValue::Float64(1.0)))
                    ),
                    Field::new(
                        "y".to_string(),
                        FieldValue::Base(BaseValue::Primitive(PrimitiveValue::Float64(2.0)))
                    ),
                    Field::new(
                        "z".to_string(),
                        FieldValue::Base(BaseValue::Primitive(PrimitiveValue::Float64(3.0)))
                    ),
                ]
            }
        );
    }
}