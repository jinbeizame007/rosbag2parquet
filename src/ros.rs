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
    fn iter_bool(&self) -> impl Iterator<Item = &bool> {
        self.iter().filter_map(|base_value| match base_value {
            BaseValue::Primitive(PrimitiveValue::Bool(value)) => Some(value),
            _ => unreachable!(),
        })
    }

    fn iter_f32(&self) -> impl Iterator<Item = &f32> {
        self.iter().filter_map(|base_value| match base_value {
            BaseValue::Primitive(PrimitiveValue::Float32(value)) => Some(value),
            _ => unreachable!(),
        })
    }

    fn iter_f64(&self) -> impl Iterator<Item = &f64> {
        self.iter().filter_map(|base_value| match base_value {
            BaseValue::Primitive(PrimitiveValue::Float64(value)) => Some(value),
            _ => unreachable!(),
        })
    }

    fn iter_i8(&self) -> impl Iterator<Item = &i8> {
        self.iter().filter_map(|base_value| match base_value {
            BaseValue::Primitive(PrimitiveValue::Int8(value)) => Some(value),
            _ => unreachable!(),
        })
    }

    fn iter_i16(&self) -> impl Iterator<Item = &i16> {
        self.iter().filter_map(|base_value| match base_value {
            BaseValue::Primitive(PrimitiveValue::Int16(value)) => Some(value),
            _ => unreachable!(),
        })
    }

    fn iter_i32(&self) -> impl Iterator<Item = &i32> {
        self.iter().filter_map(|base_value| match base_value {
            BaseValue::Primitive(PrimitiveValue::Int32(value)) => Some(value),
            _ => unreachable!(),
        })
    }

    fn iter_i64(&self) -> impl Iterator<Item = &i64> {
        self.iter().filter_map(|base_value| match base_value {
            BaseValue::Primitive(PrimitiveValue::Int64(value)) => Some(value),
            _ => unreachable!(),
        })
    }

    fn iter_u8(&self) -> impl Iterator<Item = &u8> {
        self.iter().filter_map(|base_value| match base_value {
            BaseValue::Primitive(PrimitiveValue::UInt8(value)) => Some(value),
            _ => unreachable!(),
        })
    }

    fn iter_u16(&self) -> impl Iterator<Item = &u16> {
        self.iter().filter_map(|base_value| match base_value {
            BaseValue::Primitive(PrimitiveValue::UInt16(value)) => Some(value),
            _ => unreachable!(),
        })
    }

    fn iter_u32(&self) -> impl Iterator<Item = &u32> {
        self.iter().filter_map(|base_value| match base_value {
            BaseValue::Primitive(PrimitiveValue::UInt32(value)) => Some(value),
            _ => unreachable!(),
        })
    }

    fn iter_u64(&self) -> impl Iterator<Item = &u64> {
        self.iter().filter_map(|base_value| match base_value {
            BaseValue::Primitive(PrimitiveValue::UInt64(value)) => Some(value),
            _ => unreachable!(),
        })
    }

    fn iter_string(&self) -> impl Iterator<Item = &String> {
        self.iter().filter_map(|base_value| match base_value {
            BaseValue::Primitive(PrimitiveValue::String(value)) => Some(value),
            _ => unreachable!(),
        })
    }

    fn iter_complex(&self) -> impl Iterator<Item = &Message> {
        self.iter().filter_map(|base_value| match base_value {
            BaseValue::Complex(message) => Some(message),
            _ => unreachable!(),
        })
    }
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

#[cfg(test)]
pub mod test_helpers {
    use super::*;

    // MessageDefinition helper functions
    pub fn create_vector3_definition() -> MessageDefinition<'static> {
        MessageDefinition::new(
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
        )
    }

    pub fn create_time_definition() -> MessageDefinition<'static> {
        MessageDefinition::new(
            "Time",
            vec![
                FieldDefinition::new(
                    FieldType::Base(BaseType::Primitive(Primitive::Int32)),
                    "sec",
                ),
                FieldDefinition::new(
                    FieldType::Base(BaseType::Primitive(Primitive::UInt32)),
                    "nanosec",
                ),
            ],
        )
    }

    pub fn create_header_definition() -> MessageDefinition<'static> {
        MessageDefinition::new(
            "Header",
            vec![
                FieldDefinition::new(
                    FieldType::Base(BaseType::Complex("Time".to_string())),
                    "stamp",
                ),
                FieldDefinition::new(
                    FieldType::Base(BaseType::Primitive(Primitive::String)),
                    "frame_id",
                ),
            ],
        )
    }

    pub fn create_twist_definition() -> MessageDefinition<'static> {
        MessageDefinition::new(
            "Twist",
            vec![
                FieldDefinition::new(
                    FieldType::Base(BaseType::Complex("Vector3".to_string())),
                    "linear",
                ),
                FieldDefinition::new(
                    FieldType::Base(BaseType::Complex("Vector3".to_string())),
                    "angular",
                ),
            ],
        )
    }

    pub fn create_twist_stamped_definition() -> MessageDefinition<'static> {
        MessageDefinition::new(
            "TwistStamped",
            vec![
                FieldDefinition::new(
                    FieldType::Base(BaseType::Complex("Header".to_string())),
                    "header",
                ),
                FieldDefinition::new(
                    FieldType::Base(BaseType::Complex("Twist".to_string())),
                    "twist",
                ),
            ],
        )
    }

    pub fn create_joint_state_definition() -> MessageDefinition<'static> {
        MessageDefinition::new(
            "JointState",
            vec![
                FieldDefinition::new(
                    FieldType::Base(BaseType::Complex("Header".to_string())),
                    "header",
                ),
                FieldDefinition::new(
                    FieldType::Sequence(BaseType::Primitive(Primitive::String)),
                    "name",
                ),
                FieldDefinition::new(
                    FieldType::Sequence(BaseType::Primitive(Primitive::Float64)),
                    "position",
                ),
                FieldDefinition::new(
                    FieldType::Sequence(BaseType::Primitive(Primitive::Float64)),
                    "velocity",
                ),
                FieldDefinition::new(
                    FieldType::Sequence(BaseType::Primitive(Primitive::Float64)),
                    "effort",
                ),
            ],
        )
    }

    // Message value creation helper functions
    pub fn create_vector3_message(x: f64, y: f64, z: f64) -> Message {
        Message {
            name: "Vector3".to_string(),
            value: vec![
                Field::new(
                    "x".to_string(),
                    FieldValue::Base(BaseValue::Primitive(PrimitiveValue::Float64(x))),
                ),
                Field::new(
                    "y".to_string(),
                    FieldValue::Base(BaseValue::Primitive(PrimitiveValue::Float64(y))),
                ),
                Field::new(
                    "z".to_string(),
                    FieldValue::Base(BaseValue::Primitive(PrimitiveValue::Float64(z))),
                ),
            ],
        }
    }

    pub fn create_time_message(sec: i32, nanosec: u32) -> Message {
        Message {
            name: "Time".to_string(),
            value: vec![
                Field::new(
                    "sec".to_string(),
                    FieldValue::Base(BaseValue::Primitive(PrimitiveValue::Int32(sec))),
                ),
                Field::new(
                    "nanosec".to_string(),
                    FieldValue::Base(BaseValue::Primitive(PrimitiveValue::UInt32(nanosec))),
                ),
            ],
        }
    }

    pub fn create_header_message(time_msg: Message, frame_id: &str) -> Message {
        Message {
            name: "Header".to_string(),
            value: vec![
                Field::new(
                    "stamp".to_string(),
                    FieldValue::Base(BaseValue::Complex(time_msg)),
                ),
                Field::new(
                    "frame_id".to_string(),
                    FieldValue::Base(BaseValue::Primitive(PrimitiveValue::String(
                        frame_id.to_string(),
                    ))),
                ),
            ],
        }
    }

    pub fn create_quaternion_message(x: f64, y: f64, z: f64, w: f64) -> Message {
        Message {
            name: "Quaternion".to_string(),
            value: vec![
                Field::new(
                    "x".to_string(),
                    FieldValue::Base(BaseValue::Primitive(PrimitiveValue::Float64(x))),
                ),
                Field::new(
                    "y".to_string(),
                    FieldValue::Base(BaseValue::Primitive(PrimitiveValue::Float64(y))),
                ),
                Field::new(
                    "z".to_string(),
                    FieldValue::Base(BaseValue::Primitive(PrimitiveValue::Float64(z))),
                ),
                Field::new(
                    "w".to_string(),
                    FieldValue::Base(BaseValue::Primitive(PrimitiveValue::Float64(w))),
                ),
            ],
        }
    }

    pub fn create_string_message(data: &str) -> Message {
        Message {
            name: "String".to_string(),
            value: vec![Field::new(
                "data".to_string(),
                FieldValue::Base(BaseValue::Primitive(PrimitiveValue::String(
                    data.to_string(),
                ))),
            )],
        }
    }

    // FieldValue creation helper functions
    pub fn create_float64_array(values: Vec<f64>) -> FieldValue {
        FieldValue::Array(
            values
                .into_iter()
                .map(|v| BaseValue::Primitive(PrimitiveValue::Float64(v)))
                .collect(),
        )
    }

    pub fn create_string_sequence(values: Vec<&str>) -> FieldValue {
        FieldValue::Sequence(
            values
                .into_iter()
                .map(|v| BaseValue::Primitive(PrimitiveValue::String(v.to_string())))
                .collect(),
        )
    }

    pub fn create_float64_sequence(values: Vec<f64>) -> FieldValue {
        FieldValue::Sequence(
            values
                .into_iter()
                .map(|v| BaseValue::Primitive(PrimitiveValue::Float64(v)))
                .collect(),
        )
    }
}
