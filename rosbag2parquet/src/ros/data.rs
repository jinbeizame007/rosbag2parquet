//! Runtime data structures for ROS2 messages

#[derive(Clone, Debug, PartialEq)]
pub struct Message {
    pub name: String,
    pub value: Vec<Field>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Field {
    pub name: String,
    pub value: FieldValue,
}

impl Field {
    pub fn new(name: String, value: FieldValue) -> Field {
        Field { name, value }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum FieldValue {
    Base(BaseValue),
    Array(Vec<BaseValue>),
    Sequence(Vec<BaseValue>),
}

#[derive(Clone, Debug, PartialEq)]
pub enum BaseValue {
    Primitive(PrimitiveValue),
    Complex(Message),
}

#[derive(Debug, Clone, PartialEq)]
pub enum PrimitiveValue {
    Bool(bool),
    Byte(u8),
    Char(char),
    Float32(f32),
    Float64(f64),
    Int8(i8),
    UInt8(u8),
    Int16(i16),
    UInt16(u16),
    Int32(i32),
    UInt32(u32),
    Int64(i64),
    UInt64(u64),
    String(String),
}

macro_rules! impl_iter_primitive {
    ($($method_name:ident => $rust_type:ty => $variant:ident),* $(,)?) => {
        $(
            fn $method_name(&self) -> impl Iterator<Item = &$rust_type> {
                self.iter().filter_map(|base_value| match base_value {
                    BaseValue::Primitive(PrimitiveValue::$variant(value)) => Some(value),
                    _ => unreachable!(),
                })
            }
        )*
    };
}

pub trait BaseValueSliceExt {
    fn iter_bool(&self) -> impl Iterator<Item = &bool>;
    fn iter_f32(&self) -> impl Iterator<Item = &f32>;
    fn iter_f64(&self) -> impl Iterator<Item = &f64>;
    fn iter_i8(&self) -> impl Iterator<Item = &i8>;
    fn iter_i16(&self) -> impl Iterator<Item = &i16>;
    fn iter_i32(&self) -> impl Iterator<Item = &i32>;
    fn iter_i64(&self) -> impl Iterator<Item = &i64>;
    fn iter_u8(&self) -> impl Iterator<Item = &u8>;
    fn iter_u16(&self) -> impl Iterator<Item = &u16>;
    fn iter_u32(&self) -> impl Iterator<Item = &u32>;
    fn iter_u64(&self) -> impl Iterator<Item = &u64>;
    fn iter_string(&self) -> impl Iterator<Item = &String>;
    fn iter_complex(&self) -> impl Iterator<Item = &Message>;
}

impl BaseValueSliceExt for [BaseValue] {
    impl_iter_primitive! {
        iter_bool => bool => Bool,
        iter_f32 => f32 => Float32,
        iter_f64 => f64 => Float64,
        iter_i8 => i8 => Int8,
        iter_i16 => i16 => Int16,
        iter_i32 => i32 => Int32,
        iter_i64 => i64 => Int64,
        iter_u8 => u8 => UInt8,
        iter_u16 => u16 => UInt16,
        iter_u32 => u32 => UInt32,
        iter_u64 => u64 => UInt64,
        iter_string => String => String,
    }

    fn iter_complex(&self) -> impl Iterator<Item = &Message> {
        self.iter().filter_map(|base_value| match base_value {
            BaseValue::Complex(message) => Some(message),
            _ => unreachable!(),
        })
    }
}
