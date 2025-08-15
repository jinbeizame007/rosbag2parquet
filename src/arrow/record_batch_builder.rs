use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{
    ArrayBuilder, BooleanBuilder, FixedSizeListBuilder, Float32Builder, Float64Builder,
    Int16Builder, Int32Builder, Int64Builder, Int8Builder, ListBuilder, StringBuilder,
    StructBuilder, UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder,
};
use arrow::datatypes::Schema;
use arrow_array::RecordBatch;

use crate::arrow::core::{
    append_empty_list_builder, create_array_builder, downcast_list_builder,
    impl_append_array_typed, impl_append_primitive_typed, impl_append_sequence_typed,
};
use crate::ros::{BaseValue, BaseValueSliceExt, FieldValue, Message, PrimitiveValue};

pub struct RecordBatchBuilder<'a> {
    schemas: &'a HashMap<&'a str, Arc<Schema>>,
    messages: &'a Vec<Message>,
}

impl<'a> RecordBatchBuilder<'a> {
    pub fn new(schemas: &'a HashMap<&'a str, Arc<Schema>>, messages: &'a Vec<Message>) -> Self {
        Self { schemas, messages }
    }

    impl_append_sequence_typed! {
        bool => BooleanBuilder => bool => iter_bool,
        f32 => Float32Builder => f32 => iter_f32,
        f64 => Float64Builder => f64 => iter_f64,
        i8 => Int8Builder => i8 => iter_i8,
        u8 => UInt8Builder => u8 => iter_u8,
        i16 => Int16Builder => i16 => iter_i16,
        u16 => UInt16Builder => u16 => iter_u16,
        i32 => Int32Builder => i32 => iter_i32,
        u32 => UInt32Builder => u32 => iter_u32,
        i64 => Int64Builder => i64 => iter_i64,
        u64 => UInt64Builder => u64 => iter_u64,
    }

    fn append_sequence_string(&self, builder: &mut dyn ArrayBuilder, values: &[BaseValue]) {
        let list_builder = downcast_list_builder::<StringBuilder>(builder);
        for v in values.iter_string() {
            list_builder.values().append_value(v);
        }
        list_builder.append(true);
    }

    impl_append_array_typed! {
        bool => BooleanBuilder => bool => iter_bool,
        f32 => Float32Builder => f32 => iter_f32,
        f64 => Float64Builder => f64 => iter_f64,
        i8 => Int8Builder => i8 => iter_i8,
        u8 => UInt8Builder => u8 => iter_u8,
        i16 => Int16Builder => i16 => iter_i16,
        u16 => UInt16Builder => u16 => iter_u16,
        i32 => Int32Builder => i32 => iter_i32,
        u32 => UInt32Builder => u32 => iter_u32,
        i64 => Int64Builder => i64 => iter_i64,
        u64 => UInt64Builder => u64 => iter_u64,
    }

    fn append_array_string(&self, builder: &mut dyn ArrayBuilder, values: &[BaseValue]) {
        let array_builder = builder
            .as_any_mut()
            .downcast_mut::<FixedSizeListBuilder<StringBuilder>>()
            .unwrap();
        for value in values.iter_string() {
            array_builder.values().append_value(value);
        }
        array_builder.append(true);
    }

    impl_append_primitive_typed! {
        bool => BooleanBuilder => bool,
        byte => UInt8Builder => u8,
        char => UInt8Builder => u8,
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
        string => StringBuilder => &str,
    }

    pub fn build(&self, name: &str) -> Result<RecordBatch> {
        let schema = self
            .schemas
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("Schema not found for type: {}", name))?;
        let mut builders = schema
            .fields()
            .iter()
            .map(|field| create_array_builder(field.data_type()))
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

        RecordBatch::try_new(schema.clone(), arrays).context("Failed to create RecordBatch")
    }

    fn append_value(&self, builder: &mut dyn ArrayBuilder, value: &FieldValue) {
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

    fn append_array(&self, builder: &mut dyn ArrayBuilder, value: &[BaseValue]) {
        match &value[0] {
            BaseValue::Primitive(primitive) => match primitive {
                PrimitiveValue::Bool(_) => self.append_array_bool(builder, value),
                PrimitiveValue::Byte(_) => self.append_array_u8(builder, value),
                PrimitiveValue::Char(_) => self.append_array_u8(builder, value),
                PrimitiveValue::Float32(_) => self.append_array_f32(builder, value),
                PrimitiveValue::Float64(_) => self.append_array_f64(builder, value),
                PrimitiveValue::Int8(_) => self.append_array_i8(builder, value),
                PrimitiveValue::UInt8(_) => self.append_array_u8(builder, value),
                PrimitiveValue::Int16(_) => self.append_array_i16(builder, value),
                PrimitiveValue::UInt16(_) => self.append_array_u16(builder, value),
                PrimitiveValue::Int32(_) => self.append_array_i32(builder, value),
                PrimitiveValue::UInt32(_) => self.append_array_u32(builder, value),
                PrimitiveValue::Int64(_) => self.append_array_i64(builder, value),
                PrimitiveValue::UInt64(_) => self.append_array_u64(builder, value),
                PrimitiveValue::String(_) => self.append_array_string(builder, value),
            },
            BaseValue::Complex(_) => {
                let list_builder = downcast_list_builder::<StructBuilder>(builder);
                for complex_value in value.iter_complex() {
                    let substruct_builder = list_builder
                        .values()
                        .as_any_mut()
                        .downcast_mut::<StructBuilder>()
                        .unwrap();
                    self.append_complex(substruct_builder, complex_value);
                }
                list_builder.append(true);
            }
        }
    }

    fn append_sequence(&self, builder: &mut dyn ArrayBuilder, value: &[BaseValue]) {
        if value.is_empty() {
            append_empty_list_builder(builder);
            return;
        }

        match &value[0] {
            BaseValue::Primitive(primitive) => match primitive {
                PrimitiveValue::Bool(_) => self.append_sequence_bool(builder, value),
                PrimitiveValue::Byte(_) => self.append_sequence_u8(builder, value),
                PrimitiveValue::Char(_) => self.append_sequence_u8(builder, value),
                PrimitiveValue::Float32(_) => self.append_sequence_f32(builder, value),
                PrimitiveValue::Float64(_) => self.append_sequence_f64(builder, value),
                PrimitiveValue::Int8(_) => self.append_sequence_i8(builder, value),
                PrimitiveValue::UInt8(_) => self.append_sequence_u8(builder, value),
                PrimitiveValue::Int16(_) => self.append_sequence_i16(builder, value),
                PrimitiveValue::UInt16(_) => self.append_sequence_u16(builder, value),
                PrimitiveValue::Int32(_) => self.append_sequence_i32(builder, value),
                PrimitiveValue::UInt32(_) => self.append_sequence_u32(builder, value),
                PrimitiveValue::Int64(_) => self.append_sequence_i64(builder, value),
                PrimitiveValue::UInt64(_) => self.append_sequence_u64(builder, value),
                PrimitiveValue::String(_) => self.append_sequence_string(builder, value),
            },
            BaseValue::Complex(_) => self.append_sequence_complex_typed(builder, value),
        }
    }

    fn append_sequence_complex_typed(&self, builder: &mut dyn ArrayBuilder, values: &[BaseValue]) {
        let list_builder = downcast_list_builder::<StructBuilder>(builder);
        for complex_value in values.iter_complex() {
            let substruct_builder = list_builder
                .values()
                .as_any_mut()
                .downcast_mut::<StructBuilder>()
                .unwrap();
            self.append_complex(substruct_builder, complex_value);
        }
        list_builder.append(true);
    }

    fn append_complex(&self, struct_builder: &mut StructBuilder, message: &Message) {
        for (i, field_builder) in struct_builder.field_builders_mut().iter_mut().enumerate() {
            let field = &message.value[i];
            match &field.value {
                FieldValue::Base(base_value) => match base_value {
                    BaseValue::Primitive(primitive) => {
                        self.append_primitive(field_builder, primitive);
                    }
                    BaseValue::Complex(complex) => {
                        let substruct_builder = field_builder
                            .as_any_mut()
                            .downcast_mut::<StructBuilder>()
                            .unwrap();
                        self.append_complex(substruct_builder, complex);
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

    fn append_primitive(&self, builder: &mut dyn ArrayBuilder, value: &PrimitiveValue) {
        match value {
            PrimitiveValue::Bool(value) => self.append_primitive_bool(builder, *value),
            PrimitiveValue::Byte(value) => self.append_primitive_byte(builder, *value),
            PrimitiveValue::Char(value) => self.append_primitive_char(builder, *value as u8),
            PrimitiveValue::Float32(value) => self.append_primitive_f32(builder, *value),
            PrimitiveValue::Float64(value) => self.append_primitive_f64(builder, *value),
            PrimitiveValue::Int8(value) => self.append_primitive_i8(builder, *value),
            PrimitiveValue::UInt8(value) => self.append_primitive_u8(builder, *value),
            PrimitiveValue::Int16(value) => self.append_primitive_i16(builder, *value),
            PrimitiveValue::UInt16(value) => self.append_primitive_u16(builder, *value),
            PrimitiveValue::Int32(value) => self.append_primitive_i32(builder, *value),
            PrimitiveValue::UInt32(value) => self.append_primitive_u32(builder, *value),
            PrimitiveValue::UInt64(value) => self.append_primitive_u64(builder, *value),
            PrimitiveValue::Int64(value) => self.append_primitive_i64(builder, *value),
            PrimitiveValue::String(value) => self.append_primitive_string(builder, value),
        }
    }
}