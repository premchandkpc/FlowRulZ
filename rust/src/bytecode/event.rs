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
