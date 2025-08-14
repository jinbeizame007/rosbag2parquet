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

use crate::ros::{
    BaseType, BaseValue, BaseValueSliceExt, FieldDefinition, FieldType, FieldValue, Message,
    MessageDefinition, Primitive, PrimitiveValue,
};

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

// Macro to generate type-specific append_sequence methods
macro_rules! impl_append_sequence_typed {
    ($($short_name:ident => $builder_type:ident => $value_type:ty => $iter_method:ident),* $(,)?) => {
        $(
            paste::paste! {
                fn [<append_sequence_ $short_name>](&self, builder: &mut dyn ArrayBuilder, values: &[BaseValue]) {
                    let list_builder = self.downcast_list_builder::<$builder_type>(builder);

                    // Extract and append values in optimized batch
                    // for value in values.$iter_method() {
                    //     list_builder.values().append_value(*value);
                    // }
                    list_builder.values().append_slice(
                        values.$iter_method().map(|v| *v).collect::<Vec<$value_type>>().as_slice(),
                    );
                    list_builder.append(true);
                }
            }
        )*
    };
}

// Macro to generate type-specific append_array methods
macro_rules! impl_append_array_typed {
    ($($short_name:ident => $builder_type:ident => $value_type:ty => $iter_method:ident => $list_type:ident),* $(,)?) => {
        $(
            paste::paste! {
                fn [<append_array_ $short_name>](&self, builder: &mut dyn ArrayBuilder, values: &[BaseValue]) {
                    let array_builder = builder
                        .as_any_mut()
                        .downcast_mut::<$list_type<$builder_type>>()
                        .unwrap();

                    // Extract and append values in optimized batch
                    for value in values.$iter_method() {
                        array_builder.values().append_value(*value);
                    }
                    array_builder.append(true);
                }
            }
        )*
    };
}

// Macro to generate type-specific append_primitive methods
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

    // Generate type-specific array append methods
    impl_append_array_typed! {
        bool => BooleanBuilder => bool => iter_bool => FixedSizeListBuilder,
        f32 => Float32Builder => f32 => iter_f32 => FixedSizeListBuilder,
        f64 => Float64Builder => f64 => iter_f64 => FixedSizeListBuilder,
        i8 => Int8Builder => i8 => iter_i8 => FixedSizeListBuilder,
        u8 => UInt8Builder => u8 => iter_u8 => FixedSizeListBuilder,
        i16 => Int16Builder => i16 => iter_i16 => FixedSizeListBuilder,
        u16 => UInt16Builder => u16 => iter_u16 => FixedSizeListBuilder,
        i32 => Int32Builder => i32 => iter_i32 => FixedSizeListBuilder,
        u32 => UInt32Builder => u32 => iter_u32 => FixedSizeListBuilder,
        i64 => Int64Builder => i64 => iter_i64 => FixedSizeListBuilder,
        u64 => UInt64Builder => u64 => iter_u64 => FixedSizeListBuilder,
    }

    // Special methods for String (needs reference handling)
    fn append_array_string(&self, builder: &mut dyn ArrayBuilder, values: &[BaseValue]) {
        let array_builder = builder
            .as_any_mut()
            .downcast_mut::<ListBuilder<StringBuilder>>()
            .unwrap();

        for value in values.iter_string() {
            array_builder.values().append_value(value);
        }
        array_builder.append(true);
    }

    fn append_sequence_string(&self, builder: &mut dyn ArrayBuilder, values: &[BaseValue]) {
        let list_builder = self.downcast_list_builder::<StringBuilder>(builder);
        for v in values.iter_string() {
            list_builder.values().append_value(v);
        }
        list_builder.append(true);
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

    fn create_array_builder(&self, data_type: &DataType) -> Box<dyn ArrayBuilder> {
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
            DataType::Struct(fields) => Box::new(self.build_struct_builder(fields)),
            DataType::FixedSizeList(field, length) => {
                self.build_fixed_size_list_builder(field, *length)
            }
            DataType::List(field) => self.build_list_builder(field),
            _ => unreachable!(),
        }
    }

    pub fn build(&self, name: &str) -> Result<RecordBatch> {
        let schema = self
            .schemas
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("Schema not found for type: {}", name))?;
        let mut builders = schema
            .fields()
            .iter()
            .map(|field| self.create_array_builder(field.data_type()))
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

    pub fn build_fixed_size_list_builder(
        &self,
        field: &Field,
        length: i32,
    ) -> Box<dyn ArrayBuilder> {
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
                let struct_builder = self.build_struct_builder(sub_fields);
                Box::new(FixedSizeListBuilder::new(struct_builder, length))
            }
            _ => unreachable!(),
        }
    }

    pub fn build_list_builder(&self, field: &Field) -> Box<dyn ArrayBuilder> {
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
                let struct_builder = self.build_struct_builder(sub_fields);
                Box::new(ListBuilder::new(struct_builder))
            }
            _ => unreachable!(),
        }
    }

    pub fn build_struct_builder(&self, fields: &Fields) -> StructBuilder {
        let field_builders = fields
            .iter()
            .map(|field| self.create_array_builder(field.data_type()))
            .collect();
        StructBuilder::new(fields.clone(), field_builders)
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
                let list_builder = self.downcast_list_builder::<StructBuilder>(builder);
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

    pub fn append_sequence(&self, builder: &mut dyn ArrayBuilder, value: &[BaseValue]) {
        if value.is_empty() {
            let any = builder.as_any_mut();
            if any.is::<ListBuilder<BooleanBuilder>>() {
                let list_builder = any.downcast_mut::<ListBuilder<BooleanBuilder>>().unwrap();
                list_builder.append(true);
            } else if any.is::<ListBuilder<UInt8Builder>>() {
                let list_builder = any.downcast_mut::<ListBuilder<UInt8Builder>>().unwrap();
                list_builder.append(true);
            } else if any.is::<ListBuilder<UInt16Builder>>() {
                let list_builder = any.downcast_mut::<ListBuilder<UInt16Builder>>().unwrap();
                list_builder.append(true);
            } else if any.is::<ListBuilder<UInt32Builder>>() {
                let list_builder = any.downcast_mut::<ListBuilder<UInt32Builder>>().unwrap();
                list_builder.append(true);
            } else if any.is::<ListBuilder<UInt64Builder>>() {
                let list_builder = any.downcast_mut::<ListBuilder<UInt64Builder>>().unwrap();
                list_builder.append(true);
            } else if any.is::<ListBuilder<Int8Builder>>() {
                let list_builder = any.downcast_mut::<ListBuilder<Int8Builder>>().unwrap();
                list_builder.append(true);
            } else if any.is::<ListBuilder<Int16Builder>>() {
                let list_builder = any.downcast_mut::<ListBuilder<Int16Builder>>().unwrap();
                list_builder.append(true);
            } else if any.is::<ListBuilder<Int32Builder>>() {
                let list_builder = any.downcast_mut::<ListBuilder<Int32Builder>>().unwrap();
                list_builder.append(true);
            } else if any.is::<ListBuilder<Int64Builder>>() {
                let list_builder = any.downcast_mut::<ListBuilder<Int64Builder>>().unwrap();
                list_builder.append(true);
            } else if any.is::<ListBuilder<Float32Builder>>() {
                let list_builder = any.downcast_mut::<ListBuilder<Float32Builder>>().unwrap();
                list_builder.append(true);
            } else if any.is::<ListBuilder<Float64Builder>>() {
                let list_builder = any.downcast_mut::<ListBuilder<Float64Builder>>().unwrap();
                list_builder.append(true);
            } else if any.is::<ListBuilder<StringBuilder>>() {
                let list_builder = any.downcast_mut::<ListBuilder<StringBuilder>>().unwrap();
                list_builder.append(true);
            } else if any.is::<ListBuilder<StructBuilder>>() {
                let list_builder = any.downcast_mut::<ListBuilder<StructBuilder>>().unwrap();
                list_builder.append(true);
            }
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
        let list_builder = self.downcast_list_builder::<StructBuilder>(builder);
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

    pub fn append_complex(&self, struct_builder: &mut StructBuilder, message: &Message) {
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

    pub fn append_primitive(&self, builder: &mut dyn ArrayBuilder, value: &PrimitiveValue) {
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

    fn downcast_list_builder<'b, B>(
        &'b self,
        builder: &'b mut dyn ArrayBuilder,
    ) -> &'b mut ListBuilder<B>
    where
        B: ArrayBuilder + 'b,
    {
        builder
            .as_any_mut()
            .downcast_mut::<ListBuilder<B>>()
            .unwrap()
    }
}
