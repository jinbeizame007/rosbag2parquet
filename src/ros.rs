//! ROS2 message type definitions

#[derive(Clone, Debug, PartialEq)]
pub struct MessageDefinition<'a> {
    pub name: &'a str,
    pub fields: Vec<FieldDefinition<'a>>,
}

impl<'a> MessageDefinition<'a> {
    pub fn new(name: &'a str, fields: Vec<FieldDefinition<'a>>) -> MessageDefinition<'a> {
        MessageDefinition { name, fields }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct FieldDefinition<'a> {
    pub data_type: FieldType,
    pub name: &'a str,
}

impl<'a> FieldDefinition<'a> {
    pub fn new(data_type: FieldType, name: &'a str) -> FieldDefinition<'a> {
        FieldDefinition { data_type, name }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum FieldType {
    Base(BaseType),
    Array { data_type: BaseType, length: u32 },
    Sequence(BaseType),
}

#[derive(Clone, Debug, PartialEq)]
pub enum BaseType {
    Primitive(Primitive),
    Complex(String),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Primitive {
    Bool,
    Byte,
    Char,
    Float32,
    Float64,
    Int8,
    UInt8,
    Int16,
    UInt16,
    Int32,
    UInt32,
    Int64,
    UInt64,
    String,
}

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

#[derive(Debug, Clone)]
pub struct SchemaSection<'a> {
    pub type_name: &'a str,
    pub content: &'a str,
}