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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execution_context_new() {
        let event = Event::new("test", b"body".to_vec());
        let ctx = ExecutionContext::new(event);
        assert_eq!(ctx.body, b"body");
        assert_eq!(ctx.event.topic, "test");
        assert_eq!(ctx.ip, 0);
        assert!(!ctx.failed);
        assert!(ctx.variables.is_empty());
        assert!(ctx.outputs.is_empty());
    }

    #[test]
    fn test_execution_context_from_body() {
        let ctx = ExecutionContext::from_body(b"my_body".to_vec());
        assert_eq!(ctx.body, b"my_body");
        assert_eq!(ctx.event.topic, "default");
    }

    #[test]
    fn test_set_service_output() {
        let mut ctx = ExecutionContext::from_body(b"{}".to_vec());
        ctx.set_service_output("validate", b"result".to_vec());
        assert_eq!(ctx.get_service_output("validate"), Some(b"result" as &[u8]));
        assert_eq!(ctx.body, b"result");
    }

    #[test]
    fn test_get_service_output_missing() {
        let ctx = ExecutionContext::from_body(b"{}".to_vec());
        assert!(ctx.get_service_output("missing").is_none());
    }

    #[test]
    fn test_set_variable() {
        let mut ctx = ExecutionContext::from_body(b"{}".to_vec());
        ctx.set_variable("key", b"value".to_vec());
        assert_eq!(ctx.get_variable("key"), Some(b"value" as &[u8]));
    }

    #[test]
    fn test_get_variable_missing() {
        let ctx = ExecutionContext::from_body(b"{}".to_vec());
        assert!(ctx.get_variable("missing").is_none());
    }

    #[test]
    fn test_serialization_roundtrip() {
        let mut ctx = ExecutionContext::from_body(b"{\"x\":1}".to_vec());
        ctx.set_service_output("svc1", b"resp".to_vec());
        ctx.ip = 3;
        ctx.hop_count = 2;
        let bytes = bincode::serialize(&ctx).unwrap();
        let deserialized: ExecutionContext = bincode::deserialize(&bytes).unwrap();
        assert_eq!(deserialized.body, ctx.body);
        assert_eq!(deserialized.ip, 3);
        assert_eq!(deserialized.hop_count, 2);
        assert_eq!(deserialized.get_service_output("svc1"), Some(b"resp" as &[u8]));
    }

    #[test]
    fn test_multiple_outputs() {
        let mut ctx = ExecutionContext::from_body(b"{}".to_vec());
        ctx.set_service_output("a", b"1".to_vec());
        ctx.set_service_output("b", b"2".to_vec());
        assert_eq!(ctx.get_service_output("a"), Some(b"1" as &[u8]));
        assert_eq!(ctx.get_service_output("b"), Some(b"2" as &[u8]));
        assert_eq!(ctx.outputs.len(), 2);
    }
}
