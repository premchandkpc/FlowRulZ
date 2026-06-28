use std::collections::HashMap;
use super::event::Event;

/// ExecutionContext holds the full state of an in-flight event execution.
///
/// Instead of mutating a single JSON blob, services enrich this context:
/// - payload: the original input event
/// - body: current working payload (may be modified by Map/Gate)
/// - variables: store intermediate values during execution
/// - outputs: results of service calls, keyed by service name
/// - headers: mutable event headers
/// - metadata: event-level metadata (mode, trace, etc.)
///
/// This is the core data structure that flows through the VM.
#[derive(Debug, Clone)]
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
        }
    }

    pub fn from_body(body: Vec<u8>) -> Self {
        let event = Event::new("default", body);
        ExecutionContext::new(event)
    }

    /// Store a service's output and update the working body.
    pub fn set_service_output(&mut self, name: &str, result: Vec<u8>) {
        self.outputs.insert(name.to_string(), result.clone());
        self.body = result;
    }

    /// Get a service's output by name.
    pub fn get_service_output(&self, name: &str) -> Option<&[u8]> {
        self.outputs.get(name).map(|v| v.as_slice())
    }

    /// Set a named variable.
    pub fn set_variable(&mut self, name: &str, value: Vec<u8>) {
        self.variables.insert(name.to_string(), value);
    }

    /// Get a named variable.
    pub fn get_variable(&self, name: &str) -> Option<&[u8]> {
        self.variables.get(name).map(|v| v.as_slice())
    }
}
