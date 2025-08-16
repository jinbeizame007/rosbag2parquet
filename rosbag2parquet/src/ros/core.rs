//! Core utilities for ROS2 message processing

use nom::{
    branch::alt,
    bytes::complete::tag,
    character::complete::{alpha1, alphanumeric1},
    combinator::recognize,
    multi::many0,
    sequence::pair,
    IResult, Parser,
};

/// Extract the message type from a full ROS2 type name
/// e.g., "geometry_msgs/msg/Vector3" -> "Vector3"
pub fn extract_message_type(full_type_name: &str) -> &str {
    full_type_name.rsplit('/').next().unwrap_or(full_type_name)
}

/// Check if a line in a message definition is a constant definition
pub fn is_constant_line(line: &str) -> bool {
    line.contains("=") && !line.contains("[")
}

/// Parse a ROS2 identifier (field names, package names, etc.)
/// Specification: starts with [a-zA-Z], followed by alphanumeric and underscores
pub fn identifier(input: &str) -> IResult<&str, &str> {
    let mut parser = recognize(pair(alpha1, many0(alt((alphanumeric1, tag("_"))))));
    parser.parse(input)
}
