use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{
    ArrayBuilder, BooleanBuilder, FixedSizeListBuilder, Float32Builder, Float64Builder,
    Int16Builder, Int32Builder, Int64Builder, Int8Builder, ListBuilder, StringBuilder,
    StructBuilder, UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder,
};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow_array::RecordBatch;
use byteorder::{BigEndian, ByteOrder, LittleEndian};

use crate::cdr::Endianness;
use crate::ros::{
    BaseType, BaseValue, BaseValueSliceExt, FieldDefinition, FieldType, FieldValue, Message,
    MessageDefinition, Primitive, PrimitiveValue,
};

// Common helper functions for Arrow builders
fn create_fixed_size_list_builder(field: &Field, length: i32) -> Box<dyn ArrayBuilder> {
    match field.data_type() {
        DataType::Boolean => Box::new(FixedSizeListBuilder::new(BooleanBuilder::new(), length)),
        DataType::UInt8 => Box::new(FixedSizeListBuilder::new(UInt8Builder::new(), length)),
        DataType::UInt16 => Box::new(FixedSizeListBuilder::new(UInt16Builder::new(), length)),
        DataType::UInt32 => Box::new(FixedSizeListBuilder::new(UInt32Builder::new(), length)),
        DataType::UInt64 => Box::new(FixedSizeListBuilder::new(UInt64Builder::new(), length)),
        DataType::Int8 => Box::new(FixedSizeListBuilder::new(Int8Builder::new(), length)),
        DataType::Int16 => Box::new(FixedSizeListBuilder::new(Int16Builder::new(), length)),
        DataType::Int32 => Box::new(FixedSizeListBuilder::new(Int32Builder::new(), length)),
        DataType::Int64 => Box::new(FixedSizeListBuilder::new(Int64Builder::new(), length)),
        DataType::Float32 => Box::new(FixedSizeListBuilder::new(Float32Builder::new(), length)),
        DataType::Float64 => Box::new(FixedSizeListBuilder::new(Float64Builder::new(), length)),
        DataType::Utf8 => Box::new(FixedSizeListBuilder::new(StringBuilder::new(), length)),
        DataType::Struct(sub_fields) => {
            let struct_builder = create_struct_builder(sub_fields);
            Box::new(FixedSizeListBuilder::new(struct_builder, length))
        }
        _ => unreachable!(),
    }
}

fn create_list_builder(field: &Field) -> Box<dyn ArrayBuilder> {
    match field.data_type() {
        DataType::Boolean => Box::new(ListBuilder::new(BooleanBuilder::new())),
        DataType::UInt8 => Box::new(ListBuilder::new(UInt8Builder::new())),
        DataType::UInt16 => Box::new(ListBuilder::new(UInt16Builder::new())),
        DataType::UInt32 => Box::new(ListBuilder::new(UInt32Builder::new())),
        DataType::UInt64 => Box::new(ListBuilder::new(UInt64Builder::new())),
        DataType::Int8 => Box::new(ListBuilder::new(Int8Builder::new())),
        DataType::Int16 => Box::new(ListBuilder::new(Int16Builder::new())),
        DataType::Int32 => Box::new(ListBuilder::new(Int32Builder::new())),
        DataType::Int64 => Box::new(ListBuilder::new(Int64Builder::new())),
        DataType::Float32 => Box::new(ListBuilder::new(Float32Builder::new())),
        DataType::Float64 => Box::new(ListBuilder::new(Float64Builder::new())),
        DataType::Utf8 => Box::new(ListBuilder::new(StringBuilder::new())),
        DataType::Struct(sub_fields) => {
            let struct_builder = create_struct_builder(sub_fields);
            Box::new(ListBuilder::new(struct_builder))
        }
        _ => unreachable!(),
    }
}

fn create_struct_builder(fields: &Fields) -> StructBuilder {
    let field_builders = fields
        .iter()
        .map(|field| create_array_builder(field.data_type()))
        .collect();
    StructBuilder::new(fields.clone(), field_builders)
}

fn create_array_builder(data_type: &DataType) -> Box<dyn ArrayBuilder> {
    match data_type {
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
        DataType::Struct(fields) => Box::new(create_struct_builder(fields)),
        DataType::FixedSizeList(field, length) => create_fixed_size_list_builder(field, *length),
        DataType::List(field) => create_list_builder(field),
        _ => unreachable!(),
    }
}

/// Appends an empty sequence to any ListBuilder type.
///
/// This function handles the common pattern of appending empty sequences
/// to Arrow ListBuilder instances. It uses runtime type checking to determine
/// the correct builder type and calls append(true) on it.
///
/// # Arguments
/// * `builder` - A mutable reference to any ArrayBuilder that should be a ListBuilder
fn append_empty_list_builder(builder: &mut dyn ArrayBuilder) {
    let any = builder.as_any_mut();
    if any.is::<ListBuilder<BooleanBuilder>>() {
        any.downcast_mut::<ListBuilder<BooleanBuilder>>()
            .unwrap()
            .append(true);
    } else if any.is::<ListBuilder<UInt8Builder>>() {
        any.downcast_mut::<ListBuilder<UInt8Builder>>()
            .unwrap()
            .append(true);
    } else if any.is::<ListBuilder<UInt16Builder>>() {
        any.downcast_mut::<ListBuilder<UInt16Builder>>()
            .unwrap()
            .append(true);
    } else if any.is::<ListBuilder<UInt32Builder>>() {
        any.downcast_mut::<ListBuilder<UInt32Builder>>()
            .unwrap()
            .append(true);
    } else if any.is::<ListBuilder<UInt64Builder>>() {
        any.downcast_mut::<ListBuilder<UInt64Builder>>()
            .unwrap()
            .append(true);
    } else if any.is::<ListBuilder<Int8Builder>>() {
        any.downcast_mut::<ListBuilder<Int8Builder>>()
            .unwrap()
            .append(true);
    } else if any.is::<ListBuilder<Int16Builder>>() {
        any.downcast_mut::<ListBuilder<Int16Builder>>()
            .unwrap()
            .append(true);
    } else if any.is::<ListBuilder<Int32Builder>>() {
        any.downcast_mut::<ListBuilder<Int32Builder>>()
            .unwrap()
            .append(true);
    } else if any.is::<ListBuilder<Int64Builder>>() {
        any.downcast_mut::<ListBuilder<Int64Builder>>()
            .unwrap()
            .append(true);
    } else if any.is::<ListBuilder<Float32Builder>>() {
        any.downcast_mut::<ListBuilder<Float32Builder>>()
            .unwrap()
            .append(true);
    } else if any.is::<ListBuilder<Float64Builder>>() {
        any.downcast_mut::<ListBuilder<Float64Builder>>()
            .unwrap()
            .append(true);
    } else if any.is::<ListBuilder<StringBuilder>>() {
        any.downcast_mut::<ListBuilder<StringBuilder>>()
            .unwrap()
            .append(true);
    } else if any.is::<ListBuilder<StructBuilder>>() {
        any.downcast_mut::<ListBuilder<StructBuilder>>()
            .unwrap()
            .append(true);
    }
}

fn downcast_list_builder<B>(builder: &mut dyn ArrayBuilder) -> &mut ListBuilder<B>
where
    B: ArrayBuilder,
{
    builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<B>>()
        .unwrap()
}

fn downcast_fixed_size_list_builder<B>(
    builder: &mut dyn ArrayBuilder,
) -> &mut FixedSizeListBuilder<B>
where
    B: ArrayBuilder,
{
    builder
        .as_any_mut()
        .downcast_mut::<FixedSizeListBuilder<B>>()
        .unwrap()
}

pub struct ArrowSchemaBuilder<'a> {
    message_definition_table: &'a HashMap<&'a str, MessageDefinition<'a>>,
}

impl<'a> ArrowSchemaBuilder<'a> {
    pub fn new(message_definition_table: &'a HashMap<&'a str, MessageDefinition<'a>>) -> Self {
        Self {
            message_definition_table,
        }
    }

    pub fn build(&self, name: &str) -> Result<Arc<Schema>> {
        let message_definition = self
            .message_definition_table
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("Message definition not found for type: {}", name))?;
        let fields: Vec<Field> = message_definition
            .fields
            .iter()
            .map(|field| self.ros_field_to_arrow_field(field))
            .collect();
        Ok(Arc::new(Schema::new(fields)))
    }

    pub fn build_all(&self) -> Result<HashMap<&'a str, Arc<Schema>>> {
        let mut schemas = HashMap::new();
        for (name, _message_definition) in self.message_definition_table.iter() {
            schemas.insert(*name, self.build(name)?);
        }
        Ok(schemas)
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
                let message_definition = self.message_definition_table.get(name.as_str())
                    .unwrap_or_else(|| panic!(
                        "Message definition not found for complex type: '{}'. Available types: {:?}",
                        name,
                        self.message_definition_table.keys().collect::<Vec<_>>()
                    ));
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
}

/// Macro to generate type-specific append methods for sequence types.
///
/// This macro generates methods that append values from a BaseValue slice
/// to Arrow ListBuilder instances. Each generated method is specialized
/// for a specific primitive type.
///
/// # Parameters
/// - `$short_name`: Short name for the type (e.g., bool, f32)
/// - `$builder_type`: Arrow builder type (e.g., BooleanBuilder)
/// - `$value_type`: Rust value type (e.g., bool, f32)
/// - `$iter_method`: Method to iterate values (e.g., iter_bool)
macro_rules! impl_append_sequence_typed {
    ($($short_name:ident => $builder_type:ident => $value_type:ty => $iter_method:ident),* $(,)?) => {
        $(
            paste::paste! {
                fn [<append_sequence_ $short_name>](&self, builder: &mut dyn ArrayBuilder, values: &[BaseValue]) {
                    let list_builder = downcast_list_builder::<$builder_type>(builder);
                    let collected: Vec<$value_type> = values.$iter_method().map(|v| *v).collect();
                    list_builder.values().append_slice(&collected);
                    list_builder.append(true);
                }
            }
        )*
    };
}

/// Macro to generate type-specific append methods for fixed-size array types.
///
/// This macro generates methods that append values from a BaseValue slice
/// to Arrow FixedSizeListBuilder instances. Each generated method is specialized
/// for a specific primitive type.
///
/// # Parameters
/// - `$short_name`: Short name for the type (e.g., bool, f32)
/// - `$builder_type`: Arrow builder type (e.g., BooleanBuilder)
/// - `$value_type`: Rust value type (e.g., bool, f32)
/// - `$iter_method`: Method to iterate values (e.g., iter_bool)
macro_rules! impl_append_array_typed {
    ($($short_name:ident => $builder_type:ident => $value_type:ty => $iter_method:ident),* $(,)?) => {
        $(
            paste::paste! {
                fn [<append_array_ $short_name>](&self, builder: &mut dyn ArrayBuilder, values: &[BaseValue]) {
                    let array_builder = builder
                        .as_any_mut()
                        .downcast_mut::<FixedSizeListBuilder<$builder_type>>()
                        .unwrap();

                    for value in values.$iter_method() {
                        array_builder.values().append_value(*value);
                    }
                    array_builder.append(true);
                }
            }
        )*
    };
}

/// Macro to generate type-specific append methods for primitive types.
///
/// This macro generates methods that append a single primitive value
/// to the corresponding Arrow builder. Each generated method is specialized
/// for a specific primitive type.
///
/// # Parameters
/// - `$short_name`: Short name for the type (e.g., bool, f32)
/// - `$builder_type`: Arrow builder type (e.g., BooleanBuilder)
/// - `$value_type`: Rust value type (e.g., bool, f32)
macro_rules! impl_append_primitive_typed {
    ($($short_name:ident => $builder_type:ident => $value_type:ty),* $(,)?) => {
        $(
            paste::paste! {
                fn [<append_primitive_ $short_name>](&self, builder: &mut dyn ArrayBuilder, value: $value_type) {
                    let typed_builder = builder.as_any_mut()
                        .downcast_mut::<$builder_type>()
                        .unwrap();
                    typed_builder.append_value(value);
                }
            }
        )*
    };
}

pub struct RecordBatchBuilder<'a> {
    schemas: &'a HashMap<&'a str, Arc<Schema>>,
    messages: &'a Vec<Message>,
}

impl<'a> RecordBatchBuilder<'a> {
    pub fn new(schemas: &'a HashMap<&'a str, Arc<Schema>>, messages: &'a Vec<Message>) -> Self {
        Self { schemas, messages }
    }

    // Generate type-specific sequence append methods
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

    // Generate type-specific array append methods
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

    // Generate type-specific primitive append methods
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

/// Macro to generate type-specific parse methods for CDR sequence types.
///
/// This macro generates methods that parse CDR-encoded sequences and append
/// them to Arrow ListBuilder instances. Each method handles the CDR header,
/// deserializes values, and appends them to the builder.
///
/// # Parameters
/// - `$short_name`: Short name for the type (e.g., bool, f32)
/// - `$builder_type`: Arrow builder type (e.g., BooleanBuilder)
/// - `$value_type`: Rust value type (e.g., bool, f32)
macro_rules! impl_parse_sequence_typed {
    ($($short_name:ident => $builder_type:ident => $value_type:ty),* $(,)?) => {
        $(
            paste::paste! {
                fn [<parse_sequence_ $short_name>](&mut self, builder: &mut dyn ArrayBuilder, data: &[u8]) {
                    let length = self.read_length_from_header(data);

                    let mut values = Vec::<$value_type>::with_capacity(length as usize);
                    for _i in 0..length as usize {
                        values.push(self.[<deserialize_$short_name>](data));
                    }

                    let list_builder = downcast_list_builder::<$builder_type>(builder);
                    list_builder.values().append_slice(&values);
                    list_builder.append(true);
                }
            }
        )*
    };
}

/// Macro to generate type-specific parse methods for CDR fixed-size array types.
///
/// This macro generates methods that parse CDR-encoded fixed-size arrays and
/// append them to Arrow FixedSizeListBuilder instances. Each method deserializes
/// a fixed number of values and appends them to the builder.
///
/// # Parameters
/// - `$short_name`: Short name for the type (e.g., bool, f32)
/// - `$builder_type`: Arrow builder type (e.g., BooleanBuilder)
/// - `$value_type`: Rust value type (e.g., bool, f32)
/// - `$list_type`: List builder type (FixedSizeListBuilder)
macro_rules! impl_parse_array_typed {
    ($($short_name:ident => $builder_type:ident => $value_type:ty => $list_type:ident),* $(,)?) => {
        $(
            paste::paste! {
                fn [<parse_array_ $short_name>](&mut self, builder: &mut dyn ArrayBuilder, data: &[u8], length: &u32) {
                    let mut values = Vec::<$value_type>::with_capacity(*length as usize);
                    for _i in 0..*length as usize {
                        values.push(self.[<deserialize_ $short_name>](data));
                    }

                    let array_builder = builder
                        .as_any_mut()
                        .downcast_mut::<$list_type<$builder_type>>()
                        .unwrap();
                    array_builder.values().append_slice(&values);
                    array_builder.append(true);
                }
            }
        )*
    };
}

/// Macro to generate type-specific parse methods for CDR primitive types.
///
/// This macro generates methods that parse CDR-encoded primitive values and
/// append them to the corresponding Arrow builder. Each method deserializes
/// a single value and appends it to the builder.
///
/// # Parameters
/// - `$short_name`: Short name for the type (e.g., bool, f32)
/// - `$builder_type`: Arrow builder type (e.g., BooleanBuilder)
/// - `$value_type`: Rust value type (e.g., bool, f32)
macro_rules! impl_parse_primitive_typed {
    ($($short_name:ident => $builder_type:ident => $value_type:ty),* $(,)?) => {
        $(
            paste::paste! {
                fn [<parse_ $short_name>](&mut self, data: &[u8], builder: &mut dyn ArrayBuilder) {
                    let typed_builder = builder.as_any_mut()
                        .downcast_mut::<$builder_type>()
                        .unwrap();
                    typed_builder.append_value(self.[<deserialize_ $short_name>](data));
                }
            }
        )*
    };
}

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

    // Generate type-specific sequence append methods
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

    // Generate type-specific array append methods
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

    // Generate type-specific primitive append methods
    impl_parse_primitive_typed! {
        bool => BooleanBuilder => bool,
        // byte => UInt8Builder => u8,
        // char => UInt8Builder => u8,
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
                    for _i in 0..*length as usize {
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

                for i in 0..*length as usize {
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
                    for _i in 0..length as usize {
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

                for _i in 0..length as usize {
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
