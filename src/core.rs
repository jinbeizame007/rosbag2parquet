pub fn extract_message_type(full_type_name: &str) -> &str {
    full_type_name.rsplit('/').next().unwrap_or(full_type_name)
}
