use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use arrow::datatypes::{DataType, Field, Schema};

use crate::ros::{
    BaseType, FieldDefinition, FieldType, MessageDefinition, Primitive,
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