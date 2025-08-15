use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    ArrayBuilder, BooleanBuilder, FixedSizeListBuilder, Float32Builder, Float64Builder,
    Int16Builder, Int32Builder, Int64Builder, Int8Builder, ListBuilder, StringBuilder,
    StructBuilder, UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder,
};
use arrow::datatypes::Schema;
use arrow_array::RecordBatch;
use byteorder::{BigEndian, ByteOrder, LittleEndian};

use crate::arrow::core::{
    append_empty_list_builder, create_array_builder, downcast_fixed_size_list_builder,
    downcast_list_builder, impl_parse_array_typed, impl_parse_primitive_typed,
    impl_parse_sequence_typed,
};
use crate::cdr::Endianness;
use crate::ros::{BaseType, FieldDefinition, FieldType, Message, MessageDefinition, Primitive};

pub struct CdrArrowParser<'a> {
    array_builders_table: HashMap<String, Vec<Box<dyn ArrayBuilder>>>,
    msg_definition_table: &'a HashMap<&'a str, MessageDefinition<'a>>,
    schemas: &'a mut HashMap<&'a str, Arc<Schema>>,
    position: usize,
    byte_order: Endianness,
}

impl<'a> CdrArrowParser<'a> {
    pub fn new(
        msg_definition_table: &'a HashMap<&'a str, MessageDefinition<'a>>,
        schemas: &'a mut HashMap<&'a str, Arc<Schema>>,
    ) -> Self {
        let array_builders_table = schemas
            .iter()
            .map(|(name, schema)| {
                (
                    name.to_string(),
                    schema
                        .fields()
                        .iter()
                        .map(|field| create_array_builder(field.data_type()))
                        .collect::<Vec<Box<dyn ArrayBuilder>>>(),
                )
            })
            .collect::<HashMap<String, Vec<Box<dyn ArrayBuilder>>>>();

        Self {
            array_builders_table,
            msg_definition_table,
            schemas,
            position: 0,
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

    #[inline]
    fn read_length_from_header(&mut self, data: &[u8]) -> u32 {
        self.align_to(4);
        let header = self.next_bytes(data, 4);
        match self.byte_order {
            Endianness::BigEndian => BigEndian::read_u32(header),
            Endianness::LittleEndian => LittleEndian::read_u32(header),
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
        bool => BooleanBuilder => bool => FixedSizeListBuilder,
        f32 => Float32Builder => f32 => FixedSizeListBuilder,
        f64 => Float64Builder => f64 => FixedSizeListBuilder,
        i8 => Int8Builder => i8 => FixedSizeListBuilder,
        u8 => UInt8Builder => u8 => FixedSizeListBuilder,
        i16 => Int16Builder => i16 => FixedSizeListBuilder,
        u16 => UInt16Builder => u16 => FixedSizeListBuilder,
        i32 => Int32Builder => i32 => FixedSizeListBuilder,
        u32 => UInt32Builder => u32 => FixedSizeListBuilder,
        i64 => Int64Builder => i64 => FixedSizeListBuilder,
        u64 => UInt64Builder => u64 => FixedSizeListBuilder,
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

    pub fn parse(&mut self, name: String, data: &[u8]) {
        self.byte_order = if data[1] == 0x00 {
            Endianness::BigEndian
        } else {
            Endianness::LittleEndian
        };

        self.position = 4;

        self.parse_without_header(name, data);
    }

    fn parse_without_header(&mut self, name: String, data: &[u8]) {
        let msg_definition = self.msg_definition_table.get(name.as_str()).unwrap();
        let mut array_builders = self.array_builders_table.remove(&name).unwrap();

        for i in 0..array_builders.len() {
            self.parse_field(&msg_definition.fields[i], data, &mut array_builders[i]);
        }

        self.array_builders_table.insert(name, array_builders);
    }

    fn parse_field(
        &mut self,
        field: &FieldDefinition<'a>,
        data: &[u8],
        array_builder: &mut dyn ArrayBuilder,
    ) {
        match &field.data_type {
            FieldType::Array { data_type, length } => {
                self.parse_array(data_type, length, data, array_builder)
            }
            FieldType::Sequence(base_type) => self.parse_sequence(base_type, data, array_builder),
            FieldType::Base(base_type) => self.parse_base_type(base_type, data, array_builder),
        }
    }

    fn parse_array(
        &mut self,
        data_type: &BaseType,
        length: &u32,
        data: &[u8],
        array_builder: &mut dyn ArrayBuilder,
    ) {
        match data_type {
            BaseType::Primitive(primitive) => match primitive {
                Primitive::Bool => self.parse_array_bool(array_builder, data, length),
                Primitive::Byte => todo!(),
                Primitive::Char => todo!(),
                Primitive::Float32 => self.parse_array_f32(array_builder, data, length),
                Primitive::Float64 => self.parse_array_f64(array_builder, data, length),
                Primitive::Int8 => self.parse_array_i8(array_builder, data, length),
                Primitive::UInt8 => self.parse_array_u8(array_builder, data, length),
                Primitive::Int16 => self.parse_array_i16(array_builder, data, length),
                Primitive::UInt16 => self.parse_array_u16(array_builder, data, length),
                Primitive::Int32 => self.parse_array_i32(array_builder, data, length),
                Primitive::UInt32 => self.parse_array_u32(array_builder, data, length),
                Primitive::Int64 => self.parse_array_i64(array_builder, data, length),
                Primitive::UInt64 => self.parse_array_u64(array_builder, data, length),
                Primitive::String => {
                    let string_builder =
                        downcast_fixed_size_list_builder::<StringBuilder>(array_builder);
                    for _ in 0..*length as usize {
                        string_builder
                            .values()
                            .append_value(self.deserialize_string(data));
                    }
                    string_builder.append(true);
                }
            },
            BaseType::Complex(name) => {
                let substruct_builder =
                    downcast_fixed_size_list_builder::<StructBuilder>(array_builder);

                for _ in 0..*length as usize {
                    self.parse_complex(name, data, substruct_builder);
                }
                substruct_builder.append(true);
            }
        }
    }

    fn parse_sequence(
        &mut self,
        data_type: &BaseType,
        data: &[u8],
        array_builder: &mut dyn ArrayBuilder,
    ) {
        if data.is_empty() {
            append_empty_list_builder(array_builder);
            return;
        }

        match data_type {
            BaseType::Primitive(primitive) => match primitive {
                Primitive::Bool => self.parse_sequence_bool(array_builder, data),
                Primitive::Byte => todo!(),
                Primitive::Char => todo!(),
                Primitive::Float32 => self.parse_sequence_f32(array_builder, data),
                Primitive::Float64 => self.parse_sequence_f64(array_builder, data),
                Primitive::Int8 => self.parse_sequence_i8(array_builder, data),
                Primitive::UInt8 => self.parse_sequence_u8(array_builder, data),
                Primitive::Int16 => self.parse_sequence_i16(array_builder, data),
                Primitive::UInt16 => self.parse_sequence_u16(array_builder, data),
                Primitive::Int32 => self.parse_sequence_i32(array_builder, data),
                Primitive::UInt32 => self.parse_sequence_u32(array_builder, data),
                Primitive::Int64 => self.parse_sequence_i64(array_builder, data),
                Primitive::UInt64 => self.parse_sequence_u64(array_builder, data),
                Primitive::String => {
                    let length = self.read_length_from_header(data);

                    let string_builder = downcast_list_builder::<StringBuilder>(array_builder);
                    for _ in 0..length as usize {
                        string_builder
                            .values()
                            .append_value(self.deserialize_string(data));
                    }
                    string_builder.append(true);
                }
            },
            BaseType::Complex(name) => {
                let length = self.read_length_from_header(data);

                let list_builder = downcast_list_builder::<StructBuilder>(array_builder);
                let substruct_builder = list_builder
                    .values()
                    .as_any_mut()
                    .downcast_mut::<StructBuilder>()
                    .unwrap();

                for _ in 0..length as usize {
                    self.parse_complex(name, data, substruct_builder);
                }
                list_builder.append(true);
            }
        }
    }

    fn parse_base_type(
        &mut self,
        base_type: &BaseType,
        data: &[u8],
        array_builder: &mut dyn ArrayBuilder,
    ) {
        match base_type {
            BaseType::Primitive(primitive) => self.parse_primitive(primitive, data, array_builder),
            BaseType::Complex(name) => self.parse_complex(name, data, array_builder),
        }
    }

    fn parse_complex(&mut self, name: &str, data: &[u8], array_builder: &mut dyn ArrayBuilder) {
        let msg_definition = self.msg_definition_table.get(name).unwrap();
        let struct_builder = array_builder
            .as_any_mut()
            .downcast_mut::<StructBuilder>()
            .unwrap();

        for (i, field_builder) in struct_builder.field_builders_mut().iter_mut().enumerate() {
            let field = &msg_definition.fields[i];
            match &field.data_type {
                FieldType::Array { data_type, length } => {
                    self.parse_array(data_type, length, data, field_builder)
                }
                FieldType::Sequence(base_type) => {
                    self.parse_sequence(base_type, data, field_builder)
                }
                FieldType::Base(base_type) => self.parse_base_type(base_type, data, field_builder),
            }
        }

        struct_builder.append(true);
    }

    fn parse_primitive(
        &mut self,
        primitive: &Primitive,
        data: &[u8],
        array_builder: &mut dyn ArrayBuilder,
    ) {
        match primitive {
            Primitive::Bool => self.parse_bool(data, array_builder),
            Primitive::Byte => todo!(),
            Primitive::Char => todo!(),
            Primitive::Float32 => self.parse_f32(data, array_builder),
            Primitive::Float64 => self.parse_f64(data, array_builder),
            Primitive::Int8 => self.parse_i8(data, array_builder),
            Primitive::UInt8 => self.parse_u8(data, array_builder),
            Primitive::Int16 => self.parse_i16(data, array_builder),
            Primitive::UInt16 => self.parse_u16(data, array_builder),
            Primitive::Int32 => self.parse_i32(data, array_builder),
            Primitive::UInt32 => self.parse_u32(data, array_builder),
            Primitive::Int64 => self.parse_i64(data, array_builder),
            Primitive::UInt64 => self.parse_u64(data, array_builder),
            Primitive::String => self.parse_string(data, array_builder),
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

    pub fn finish(&mut self) -> HashMap<String, RecordBatch> {
        let mut batches = HashMap::new();
        let keys = self
            .array_builders_table
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for name in keys {
            let schema = self.schemas.remove(name.as_str()).unwrap();
            let mut builders = self.array_builders_table.remove(&name).unwrap();
            let built_array = builders
                .iter_mut()
                .map(|builder| builder.finish())
                .collect::<Vec<_>>();
            let batch = RecordBatch::try_new(schema, built_array);
            batches.insert(name.to_string(), batch.unwrap());
        }

        batches
    }
}