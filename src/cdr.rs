//! CDR (Common Data Representation) deserializer for ROS2 messages

use std::collections::HashMap;

use byteorder::{BigEndian, ByteOrder, LittleEndian};

use crate::ros::{
    BaseType, BaseValue, Field, FieldDefinition, FieldType, FieldValue, Message, MessageDefinition,
    Primitive, PrimitiveValue,
};

const CDR_HEADER_SIZE: usize = 4;

#[derive(Copy, Clone, PartialEq, Debug, Default)]
pub enum Endianness {
    #[default]
    BigEndian,
    LittleEndian,
}

impl Endianness {
    fn from_cdr_header(data: &[u8]) -> Self {
        match data[1] {
            0x00 => Self::BigEndian,
            _ => Self::LittleEndian,
        }
    }
}

#[derive(Default, Debug)]
pub struct CdrDeserializer<'a> {
    data: &'a [u8],
    byte_order: Endianness,
    position: usize,
}

impl<'a> CdrDeserializer<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        let byte_order = Endianness::from_cdr_header(data);

        Self {
            data,
            byte_order,
            position: CDR_HEADER_SIZE,
        }
    }

    #[inline]
    pub fn align_to(&mut self, count: usize) {
        let modulo = (self.position - CDR_HEADER_SIZE) % count;
        if modulo != 0 {
            self.position += count - modulo;
        }
    }

    #[inline]
    pub fn next_bytes(&mut self, count: usize) -> &'a [u8] {
        self.position += count;
        &self.data[self.position - count..self.position]
    }

    #[inline]
    pub fn read_sequence_length(&mut self) -> u32 {
        self.align_to(4);
        let header = self.next_bytes(4);
        match self.byte_order {
            Endianness::BigEndian => BigEndian::read_u32(header),
            Endianness::LittleEndian => LittleEndian::read_u32(header),
        }
    }

    pub fn deserialize_f64(&mut self) -> f64 {
        self.align_to(8);
        let bytes = self.next_bytes(8);
        match self.byte_order {
            Endianness::BigEndian => BigEndian::read_f64(bytes),
            Endianness::LittleEndian => LittleEndian::read_f64(bytes),
        }
    }

    pub fn deserialize_f32(&mut self) -> f32 {
        self.align_to(4);
        let bytes = self.next_bytes(4);
        match self.byte_order {
            Endianness::BigEndian => BigEndian::read_f32(bytes),
            Endianness::LittleEndian => LittleEndian::read_f32(bytes),
        }
    }

    pub fn deserialize_bool(&mut self) -> bool {
        let bytes = self.next_bytes(1);
        bytes[0] == 0x01
    }

    pub fn deserialize_i8(&mut self) -> i8 {
        let bytes = self.next_bytes(1);
        bytes[0] as i8
    }

    pub fn deserialize_u8(&mut self) -> u8 {
        let bytes = self.next_bytes(1);
        bytes[0]
    }

    pub fn deserialize_char(&mut self) -> char {
        let byte = self.next_bytes(1)[0];
        byte as char
    }

    pub fn deserialize_i16(&mut self) -> i16 {
        self.align_to(2);
        let bytes = self.next_bytes(2);
        match self.byte_order {
            Endianness::BigEndian => BigEndian::read_i16(bytes),
            Endianness::LittleEndian => LittleEndian::read_i16(bytes),
        }
    }

    pub fn deserialize_u16(&mut self) -> u16 {
        self.align_to(2);
        let bytes = self.next_bytes(2);
        match self.byte_order {
            Endianness::BigEndian => BigEndian::read_u16(bytes),
            Endianness::LittleEndian => LittleEndian::read_u16(bytes),
        }
    }

    pub fn deserialize_i32(&mut self) -> i32 {
        self.align_to(4);
        let bytes = self.next_bytes(4);
        match self.byte_order {
            Endianness::BigEndian => BigEndian::read_i32(bytes),
            Endianness::LittleEndian => LittleEndian::read_i32(bytes),
        }
    }

    pub fn deserialize_u32(&mut self) -> u32 {
        self.align_to(4);
        let bytes = self.next_bytes(4);
        match self.byte_order {
            Endianness::BigEndian => BigEndian::read_u32(bytes),
            Endianness::LittleEndian => LittleEndian::read_u32(bytes),
        }
    }

    pub fn deserialize_i64(&mut self) -> i64 {
        self.align_to(8);
        let bytes = self.next_bytes(8);
        match self.byte_order {
            Endianness::BigEndian => BigEndian::read_i64(bytes),
            Endianness::LittleEndian => LittleEndian::read_i64(bytes),
        }
    }

    pub fn deserialize_u64(&mut self) -> u64 {
        self.align_to(8);
        let bytes = self.next_bytes(8);
        match self.byte_order {
            Endianness::BigEndian => BigEndian::read_u64(bytes),
            Endianness::LittleEndian => LittleEndian::read_u64(bytes),
        }
    }

    pub fn deserialize_string(&mut self) -> String {
        self.align_to(4);
        let header = self.next_bytes(4);
        let byte_length = match self.byte_order {
            Endianness::BigEndian => BigEndian::read_u32(header),
            Endianness::LittleEndian => LittleEndian::read_u32(header),
        };
        let bytes = self.next_bytes(byte_length as usize);
        let bytes_without_null = match bytes.split_last() {
            None => bytes,
            Some((_null_char, contents)) => contents,
        };
        std::str::from_utf8(bytes_without_null)
            .unwrap_or_else(|err| {
                panic!(
                    "Failed to decode UTF-8 string from CDR data at position {}. Error: {}",
                    self.position - byte_length as usize,
                    err
                )
            })
            .to_string()
    }
}

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

    pub fn parse(&mut self) -> Message {
        self.parse_without_header()
    }

    fn parse_without_header(&mut self) -> Message {
        let mut value = Message {
            name: self.name.clone(),
            value: Vec::new(),
        };

        let msg_definition = self
            .msg_definition_table
            .get(self.name.as_str())
            .unwrap_or_else(|| {
                panic!(
                    "Message definition not found for type: '{}'. Available types: {:?}",
                    self.name,
                    self.msg_definition_table.keys().collect::<Vec<_>>()
                )
            });

        for field in msg_definition.fields.iter() {
            let field_value = self.parse_field(field);
            value.value.push(field_value);
        }

        value
    }

    fn parse_field(&mut self, field: &'a FieldDefinition<'a>) -> Field {
        let value = match &field.data_type {
            FieldType::Sequence(non_array_data_type) => {
                FieldValue::Sequence(self.parse_dynamic_array(non_array_data_type))
            }
            FieldType::Array { data_type, length } => {
                FieldValue::Array(self.parse_static_array(data_type, length))
            }
            FieldType::Base(non_array_data_type) => match non_array_data_type {
                BaseType::Primitive(prim) => {
                    FieldValue::Base(BaseValue::Primitive(self.parse_primitive(prim)))
                }
                BaseType::Complex(name) => {
                    FieldValue::Base(BaseValue::Complex(self.parse_complex(name)))
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
    ) -> Vec<BaseValue> {
        let mut values = Vec::new();
        for _ in 0..*length {
            let value = self.parse_non_array_data_value(non_array_data_type);
            values.push(value);
        }
        values
    }

    fn parse_dynamic_array(&mut self, non_array_data_type: &BaseType) -> Vec<BaseValue> {
        let length = self.cdr_deserializer.read_sequence_length();
        let mut values = Vec::with_capacity(length as usize);
        for _ in 0..length {
            let value = self.parse_non_array_data_value(non_array_data_type);
            values.push(value);
        }
        values
    }

    fn parse_non_array_data_value(&mut self, non_array_data_type: &BaseType) -> BaseValue {
        match non_array_data_type {
            BaseType::Primitive(prim) => BaseValue::Primitive(self.parse_primitive(prim)),
            BaseType::Complex(name) => BaseValue::Complex(self.parse_complex(name)),
        }
    }

    fn parse_complex(&mut self, name: &str) -> Message {
        let msg_definition = self.msg_definition_table.get(name).unwrap_or_else(|| {
            panic!(
                "Message definition not found for complex type: '{}'. Available types: {:?}",
                name,
                self.msg_definition_table.keys().collect::<Vec<_>>()
            )
        });
        let mut ros_msg_value = Message {
            name: name.to_string(),
            value: Vec::new(),
        };

        for field in msg_definition.fields.iter() {
            let field_value = self.parse_field(field);
            ros_msg_value.value.push(field_value);
        }

        ros_msg_value
    }

    fn parse_primitive(&mut self, prim: &Primitive) -> PrimitiveValue {
        match prim {
            Primitive::Bool => PrimitiveValue::Bool(self.cdr_deserializer.deserialize_bool()),
            Primitive::Byte => PrimitiveValue::Byte(self.cdr_deserializer.deserialize_u8()),
            Primitive::Char => PrimitiveValue::Char(self.cdr_deserializer.deserialize_char()),
            Primitive::Float32 => PrimitiveValue::Float32(self.cdr_deserializer.deserialize_f32()),
            Primitive::Float64 => PrimitiveValue::Float64(self.cdr_deserializer.deserialize_f64()),
            Primitive::Int8 => PrimitiveValue::Int8(self.cdr_deserializer.deserialize_i8()),
            Primitive::UInt8 => PrimitiveValue::UInt8(self.cdr_deserializer.deserialize_u8()),
            Primitive::Int16 => PrimitiveValue::Int16(self.cdr_deserializer.deserialize_i16()),
            Primitive::UInt16 => PrimitiveValue::UInt16(self.cdr_deserializer.deserialize_u16()),
            Primitive::Int32 => PrimitiveValue::Int32(self.cdr_deserializer.deserialize_i32()),
            Primitive::UInt32 => PrimitiveValue::UInt32(self.cdr_deserializer.deserialize_u32()),
            Primitive::Int64 => PrimitiveValue::Int64(self.cdr_deserializer.deserialize_i64()),
            Primitive::UInt64 => PrimitiveValue::UInt64(self.cdr_deserializer.deserialize_u64()),
            Primitive::String => PrimitiveValue::String(self.cdr_deserializer.deserialize_string()),
        }
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

    pub fn parse(&mut self, name: &str, data: &[u8]) -> Message {
        let mut single_message_parser =
            SingleMessageCdrRosParser::new(self.msg_definition_table, name.to_string(), data);
        single_message_parser.parse()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ros::test_helpers::*;
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
        let value = cdr_ros_parser.parse("Vector3", &data);
        let expected = create_vector3_message(1.0, 2.0, 3.0);
        assert_eq!(value, expected);
    }
}
