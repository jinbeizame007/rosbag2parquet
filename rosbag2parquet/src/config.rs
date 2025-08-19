use std::collections::HashSet;

use camino::Utf8PathBuf;

#[derive(Debug, Clone)]
pub struct Config {
    topic_filter: TopicFilter,
    output_dir: Option<Utf8PathBuf>,
}

impl Config {
    pub fn new(topic_filter: TopicFilter, output_dir: Option<Utf8PathBuf>) -> Self {
        Self {
            topic_filter,
            output_dir,
        }
    }

    pub fn topic_filter(&self) -> &TopicFilter {
        &self.topic_filter
    }

    pub fn output_dir(&self) -> Option<&Utf8PathBuf> {
        self.output_dir.as_ref()
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::new(TopicFilter::all(), None)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum TopicFilter {
    All,
    Include(HashSet<String>),
    Exclude(HashSet<String>),
}

impl TopicFilter {
    pub fn matches(&self, topic: &str) -> bool {
        match self {
            Self::All => true,
            Self::Include(topics) => topics.contains(topic),
            Self::Exclude(topics) => !topics.contains(topic),
        }
    }

    pub fn include<I>(topics: I) -> Self
    where
        I: IntoIterator<Item = String>,
    {
        Self::Include(topics.into_iter().collect())
    }

    pub fn exclude<I>(topics: I) -> Self
    where
        I: IntoIterator<Item = String>,
    {
        Self::Exclude(topics.into_iter().collect())
    }

    pub fn all() -> Self {
        Self::All
    }
}

impl Default for TopicFilter {
    fn default() -> Self {
        Self::All
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_all_filter() {
        let filter = TopicFilter::all();
        assert!(filter.matches("/any/topic"));
        assert!(filter.matches("/another/topic"));
        assert!(filter.matches(""));
    }

    #[test]
    fn test_include_filter() {
        let filter =
            TopicFilter::include(["/camera/image".to_string(), "/lidar/points".to_string()]);

        assert!(filter.matches("/camera/image"));
        assert!(filter.matches("/lidar/points"));
        assert!(!filter.matches("/other/topic"));
        assert!(!filter.matches("/camera/other"));
    }

    #[test]
    fn test_exclude_filter() {
        let filter = TopicFilter::exclude(["/diagnostics".to_string(), "/rosout".to_string()]);

        assert!(!filter.matches("/diagnostics"));
        assert!(!filter.matches("/rosout"));
        assert!(filter.matches("/camera/image"));
        assert!(filter.matches("/lidar/points"));
    }

    #[test]
    fn test_empty_include_filter() {
        let filter = TopicFilter::include(std::iter::empty::<String>());
        assert!(!filter.matches("/any/topic"));
    }

    #[test]
    fn test_empty_exclude_filter() {
        let filter = TopicFilter::exclude(std::iter::empty::<String>());
        assert!(filter.matches("/any/topic"));
    }

    #[test]
    fn test_default_filter() {
        let filter = TopicFilter::default();
        assert!(filter.matches("/any/topic"));
        assert_eq!(filter, TopicFilter::All);
    }

    #[test]
    fn test_clone_and_equality() {
        let filter1 = TopicFilter::include(["/topic1".to_string()]);
        let filter2 = filter1.clone();
        assert_eq!(filter1, filter2);

        let filter3 = TopicFilter::exclude(["/topic2".to_string()]);
        assert_ne!(filter1, filter3);
    }
}
