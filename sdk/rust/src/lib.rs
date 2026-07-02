use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

// -- modes --

pub const MODE_PUBLISH: u8 = 0;
pub const MODE_REQUEST: u8 = 1;
pub const MODE_REPLY: u8 = 2;
pub const MODE_STREAM: u8 = 3;
pub const MODE_WORKFLOW: u8 = 4;
pub const MODE_INTERNAL: u8 = 5;

// -- types --

#[derive(Debug, Serialize, Deserialize)]
pub struct Event {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub topic: String,
    pub payload: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headers: Option<HashMap<String, String>>,
    pub mode: u8,
}

#[derive(Debug)]
pub struct Config {
    pub address: String,
    pub api_key: Option<String>,
    pub timeout: Option<Duration>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            address: "http://localhost:8080".into(),
            api_key: None,
            timeout: Some(Duration::from_secs(30)),
        }
    }
}

#[derive(Debug)]
pub struct ExecuteOpts {
    pub timeout: Duration,
    pub headers: HashMap<String, String>,
}

impl Default for ExecuteOpts {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            headers: HashMap::new(),
        }
    }
}

// -- client --

pub struct FlowRulZClient {
    http: Client,
    cfg: Config,
}

impl FlowRulZClient {
    pub fn new(cfg: Config) -> Self {
        Self {
            http: Client::new(),
            cfg,
        }
    }

    pub async fn publish(&self, topic: &str, payload: impl Into<serde_json::Value>) {
        let evt = Event {
            id: None,
            topic: topic.into(),
            payload: payload.into(),
            headers: None,
            mode: MODE_PUBLISH,
        };
        self.send_event(&evt).await;
    }

    pub async fn request(
        &self,
        service: &str,
        payload: impl Into<serde_json::Value>,
        timeout: Option<Duration>,
    ) -> Vec<u8> {
        let evt = Event {
            id: None,
            topic: service.into(),
            payload: payload.into(),
            headers: None,
            mode: MODE_REQUEST,
        };
        self.round_trip(&evt, timeout).await
    }

    pub async fn execute(
        &self,
        rule_id: &str,
        payload: impl Into<serde_json::Value>,
        opts: Option<ExecuteOpts>,
    ) -> Vec<u8> {
        let opts = opts.unwrap_or_default();
        let evt = Event {
            id: None,
            topic: rule_id.into(),
            payload: payload.into(),
            headers: Some(opts.headers),
            mode: MODE_WORKFLOW,
        };
        self.round_trip(&evt, Some(opts.timeout)).await
    }

    pub async fn stream(
        &self,
        topic: &str,
        mut handler: impl FnMut(Vec<u8>),
    ) -> Result<(), Box<dyn std::error::Error>> {
        let url = format!("{}/stream/{}", self.cfg.address, topic);
        let mut req = self.http.get(&url);
        if let Some(ref key) = self.cfg.api_key {
            req = req.header("Authorization", format!("Bearer {}", key));
        }
        let resp = req.send().await?;
        let mut stream = resp.bytes_stream();
        use futures_util::StreamExt;
        while let Some(chunk) = stream.next().await {
            handler(chunk?.to_vec());
        }
        Ok(())
    }

    // -- internal --

    async fn send_event(&self, evt: &Event) {
        let body = serde_json::to_vec(evt).unwrap();
        let mut req = self
            .http
            .post(format!("{}/event", self.cfg.address))
            .header("Content-Type", "application/json")
            .header("X-FlowRulZ-Mode", evt.mode.to_string())
            .body(body);
        if let Some(ref key) = self.cfg.api_key {
            req = req.header("Authorization", format!("Bearer {}", key));
        }
        req.send().await.ok();
    }

    async fn round_trip(&self, evt: &Event, timeout: Option<Duration>) -> Vec<u8> {
        let body = serde_json::to_vec(evt).unwrap();
        let mut req = self
            .http
            .post(format!("{}/event", self.cfg.address))
            .header("Content-Type", "application/json")
            .header("X-FlowRulZ-Mode", evt.mode.to_string())
            .body(body);
        if let Some(ref key) = self.cfg.api_key {
            req = req.header("Authorization", format!("Bearer {}", key));
        }
        if let Some(t) = timeout.or(self.cfg.timeout) {
            req = req.timeout(t);
        }
        match req.send().await {
            Ok(r) => r.bytes().await.unwrap_or_default().to_vec(),
            Err(_) => vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_publish_send_event() {
        let cfg = Config::default();
        let _client = FlowRulZClient::new(cfg);
    }

    #[tokio::test]
    async fn test_execute_default_opts() {
        let cfg = Config::default();
        let client = FlowRulZClient::new(cfg);
        let opts = ExecuteOpts::default();
        assert_eq!(opts.timeout, Duration::from_secs(30));
    }

    #[tokio::test]
    async fn test_mode_constants() {
        assert_eq!(MODE_PUBLISH, 0);
        assert_eq!(MODE_REQUEST, 1);
        assert_eq!(MODE_REPLY, 2);
        assert_eq!(MODE_STREAM, 3);
        assert_eq!(MODE_WORKFLOW, 4);
        assert_eq!(MODE_INTERNAL, 5);
    }
}
