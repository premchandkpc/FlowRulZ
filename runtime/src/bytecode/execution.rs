use std::collections::HashMap;
use super::event::Event;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ExecutionContext {
    pub event: Event,
    pub body: Vec<u8>,
    pub variables: HashMap<String, Vec<u8>>,
    pub outputs: HashMap<String, Vec<u8>>,
    pub headers: HashMap<String, String>,
    pub failed: bool,
    pub errors: Vec<String>,
    pub hop_count: u16,
    pub retry_count: u32,
    pub deadline_ms: u64,
    pub ip: usize,
}

impl ExecutionContext {
    pub fn new(event: Event) -> Self {
        let body = event.payload.clone();
        ExecutionContext {
            event,
            body,
            variables: HashMap::new(),
            outputs: HashMap::new(),
            headers: HashMap::new(),
            failed: false,
            errors: Vec::new(),
            hop_count: 0,
            retry_count: 0,
            deadline_ms: 0,
            ip: 0,
        }
    }

    pub fn from_body(body: Vec<u8>) -> Self {
        let event = Event::new("default", body);
        ExecutionContext::new(event)
    }

    pub fn set_service_output(&mut self, name: &str, result: Vec<u8>) {
        self.outputs.insert(name.to_string(), result.clone());
        self.body = result;
    }

    pub fn get_service_output(&self, name: &str) -> Option<&[u8]> {
        self.outputs.get(name).map(|v| v.as_slice())
    }

    pub fn set_variable(&mut self, name: &str, value: Vec<u8>) {
        self.variables.insert(name.to_string(), value);
    }

    pub fn get_variable(&self, name: &str) -> Option<&[u8]> {
        self.variables.get(name).map(|v| v.as_slice())
    }
}
