use arrow::array::{
    ArrayBuilder, BooleanBuilder, FixedSizeListBuilder, Float32Builder, Float64Builder,
    Int16Builder, Int32Builder, Int64Builder, Int8Builder, ListBuilder, StringBuilder,
    StructBuilder, UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder,
};
use arrow::datatypes::{DataType, Field, Fields};

pub fn create_fixed_size_list_builder(field: &Field, length: i32) -> Box<dyn ArrayBuilder> {
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

pub fn create_list_builder(field: &Field) -> Box<dyn ArrayBuilder> {
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

pub fn create_struct_builder(fields: &Fields) -> StructBuilder {
    let field_builders = fields
        .iter()
        .map(|field| create_array_builder(field.data_type()))
        .collect();
    StructBuilder::new(fields.clone(), field_builders)
}

pub fn create_array_builder(data_type: &DataType) -> Box<dyn ArrayBuilder> {
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

pub fn append_empty_list_builder(builder: &mut dyn ArrayBuilder) {
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

pub fn downcast_list_builder<B>(builder: &mut dyn ArrayBuilder) -> &mut ListBuilder<B>
where
    B: ArrayBuilder,
{
    builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<B>>()
        .unwrap()
}

pub fn downcast_fixed_size_list_builder<B>(
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

                    let list_builder = $crate::arrow::core::downcast_list_builder::<$builder_type>(builder);
                    list_builder.values().append_slice(&values);
                    list_builder.append(true);
                }
            }
        )*
    };
}

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

pub(crate) use impl_parse_array_typed;
pub(crate) use impl_parse_primitive_typed;
pub(crate) use impl_parse_sequence_typed;
