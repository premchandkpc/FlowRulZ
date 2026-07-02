use std::collections::HashMap;

/// Every message in FlowRulZ is an Event.
/// Payload is opaque bytes — the VM never cares about serialization format.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Event {
    pub id: String,
    pub topic: String,
    pub payload: Vec<u8>,
    pub headers: HashMap<String, String>,
    pub metadata: EventMetadata,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EventMetadata {
    pub mode: Mode,
    pub reply_to: String,
    pub correlation_id: String,
    pub trace_id: String,
    pub content_type: String,
    pub schema_name: String,
    pub schema_version: u32,
    pub partition: u32,
    pub offset: i64,
}

impl Default for EventMetadata {
    fn default() -> Self {
        EventMetadata {
            mode: Mode::Publish,
            reply_to: String::new(),
            correlation_id: String::new(),
            trace_id: String::new(),
            content_type: String::new(),
            schema_name: String::new(),
            schema_version: 0,
            partition: 0,
            offset: 0,
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Mode {
    Publish = 0,
    Request = 1,
    Reply = 2,
    Stream = 3,
    Workflow = 4,
    Internal = 5,
}

impl Mode {
    pub fn from_u8(v: u8) -> Option<Mode> {
        match v {
            0 => Some(Mode::Publish),
            1 => Some(Mode::Request),
            2 => Some(Mode::Reply),
            3 => Some(Mode::Stream),
            4 => Some(Mode::Workflow),
            5 => Some(Mode::Internal),
            _ => None,
        }
    }
}

impl Event {
    pub fn new(topic: &str, payload: Vec<u8>) -> Self {
        Event {
            id: uuid::Uuid::new_v4().to_string(),
            topic: topic.to_string(),
            payload,
            headers: HashMap::new(),
            metadata: EventMetadata::default(),
        }
    }

    pub fn with_mode(mut self, mode: Mode) -> Self {
        self.metadata.mode = mode;
        self
    }

    pub fn with_reply_to(mut self, reply_to: &str) -> Self {
        self.metadata.reply_to = reply_to.to_string();
        self
    }

    pub fn with_header(mut self, key: &str, value: &str) -> Self {
        self.headers.insert(key.to_string(), value.to_string());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_new() {
        let body = b"hello".to_vec();
        let event = Event::new("orders", body.clone());
        assert_eq!(event.topic, "orders");
        assert_eq!(event.payload, body);
        assert_eq!(event.metadata.mode, Mode::Publish);
        assert!(event.headers.is_empty());
        assert!(!event.id.is_empty());
    }

    #[test]
    fn test_event_with_mode() {
        let event = Event::new("test", vec![]).with_mode(Mode::Request);
        assert_eq!(event.metadata.mode, Mode::Request);
    }

    #[test]
    fn test_event_with_reply_to() {
        let event = Event::new("test", vec![]).with_reply_to("reply-queue");
        assert_eq!(event.metadata.reply_to, "reply-queue");
    }

    #[test]
    fn test_event_with_header() {
        let event = Event::new("test", vec![])
            .with_header("content-type", "application/json")
            .with_header("x-custom", "value");
        assert_eq!(event.headers.get("content-type").unwrap(), "application/json");
        assert_eq!(event.headers.get("x-custom").unwrap(), "value");
    }

    #[test]
    fn test_event_metadata_default() {
        let meta = EventMetadata::default();
        assert_eq!(meta.mode, Mode::Publish);
        assert!(meta.reply_to.is_empty());
        assert!(meta.correlation_id.is_empty());
        assert!(meta.trace_id.is_empty());
        assert_eq!(meta.partition, 0);
        assert_eq!(meta.offset, 0);
    }

    #[test]
    fn test_mode_from_u8() {
        assert_eq!(Mode::from_u8(0), Some(Mode::Publish));
        assert_eq!(Mode::from_u8(1), Some(Mode::Request));
        assert_eq!(Mode::from_u8(2), Some(Mode::Reply));
        assert_eq!(Mode::from_u8(3), Some(Mode::Stream));
        assert_eq!(Mode::from_u8(4), Some(Mode::Workflow));
        assert_eq!(Mode::from_u8(5), Some(Mode::Internal));
        assert_eq!(Mode::from_u8(255), None);
    }

    #[test]
    fn test_serialization_roundtrip() {
        let event = Event::new("test", vec![1, 2, 3])
            .with_mode(Mode::Request)
            .with_header("h", "v");
        let bytes = bincode::serialize(&event).unwrap();
        let deserialized: Event = bincode::deserialize(&bytes).unwrap();
        assert_eq!(deserialized.topic, "test");
        assert_eq!(deserialized.payload, vec![1, 2, 3]);
        assert_eq!(deserialized.metadata.mode, Mode::Request);
        assert_eq!(deserialized.headers.get("h").unwrap(), "v");
    }
}
