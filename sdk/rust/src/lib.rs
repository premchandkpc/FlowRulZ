use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

pub const MODE_PUBLISH: u8 = 0;
pub const MODE_REQUEST: u8 = 1;
pub const MODE_REPLY: u8 = 2;
pub const MODE_STREAM: u8 = 3;
pub const MODE_WORKFLOW: u8 = 4;
pub const MODE_INTERNAL: u8 = 5;

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

    pub async fn publish(&self, topic: &str, payload: impl Into<serde_json::Value>) -> Result<(), reqwest::Error> {
        let evt = Event {
            id: None,
            topic: topic.into(),
            payload: payload.into(),
            headers: None,
            mode: MODE_PUBLISH,
        };
        self.send_event(&evt).await
    }

    pub async fn request(
        &self,
        service: &str,
        payload: impl Into<serde_json::Value>,
        timeout: Option<Duration>,
    ) -> Result<Vec<u8>, reqwest::Error> {
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
    ) -> Result<Vec<u8>, reqwest::Error> {
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

    async fn send_event(&self, evt: &Event) -> Result<(), reqwest::Error> {
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
        req.send().await?.error_for_status()?;
        Ok(())
    }

    async fn round_trip(&self, evt: &Event, timeout: Option<Duration>) -> Result<Vec<u8>, reqwest::Error> {
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
        let resp = req.send().await?.error_for_status()?;
        let bytes = resp.bytes().await?;
        Ok(bytes.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{body_string_contains, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn test_publish_send_event() {
        let mock = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/event"))
            .and(header("Content-Type", "application/json"))
            .and(header("X-FlowRulZ-Mode", "0"))
            .and(body_string_contains("test-topic"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&mock)
            .await;

        let client = FlowRulZClient::new(Config {
            address: mock.uri(),
            api_key: None,
            timeout: None,
        });
        let result = client.publish("test-topic", serde_json::json!({"key": "value"})).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_publish_with_auth() {
        let mock = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/event"))
            .and(header("Authorization", "Bearer test-key"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&mock)
            .await;

        let client = FlowRulZClient::new(Config {
            address: mock.uri(),
            api_key: Some("test-key".into()),
            timeout: None,
        });
        let result = client.publish("topic", serde_json::json!({})).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_publish_server_error() {
        let mock = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/event"))
            .respond_with(ResponseTemplate::new(500))
            .expect(1)
            .mount(&mock)
            .await;

        let client = FlowRulZClient::new(Config {
            address: mock.uri(),
            api_key: None,
            timeout: None,
        });
        let result = client.publish("topic", serde_json::json!({})).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_request_success() {
        let mock = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/event"))
            .and(header("X-FlowRulZ-Mode", "1"))
            .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"result":"ok"}"#))
            .expect(1)
            .mount(&mock)
            .await;

        let client = FlowRulZClient::new(Config {
            address: mock.uri(),
            api_key: None,
            timeout: None,
        });
        let result = client.request("my-service", serde_json::json!({"q": 1}), None).await;
        assert!(result.is_ok());
        let body = result.unwrap();
        assert_eq!(body, br#"{"result":"ok"}"#);
    }

    #[tokio::test]
    async fn test_request_timeout() {
        let mock = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/event"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(5)))
            .expect(1)
            .mount(&mock)
            .await;

        let client = FlowRulZClient::new(Config {
            address: mock.uri(),
            api_key: None,
            timeout: None,
        });
        let result = client
            .request("svc", serde_json::json!({}), Some(Duration::from_millis(100)))
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_success() {
        let mock = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/event"))
            .and(header("X-FlowRulZ-Mode", "4"))
            .and(body_string_contains("my-rule"))
            .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"executed":true}"#))
            .expect(1)
            .mount(&mock)
            .await;

        let client = FlowRulZClient::new(Config {
            address: mock.uri(),
            api_key: None,
            timeout: None,
        });
        let result = client
            .execute("my-rule", serde_json::json!({"data": 1}), None)
            .await;
        assert!(result.is_ok());
        let body = result.unwrap();
        assert_eq!(body, br#"{"executed":true}"#);
    }

    #[tokio::test]
    async fn test_execute_with_opts() {
        let mock = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/event"))
            .and(header("X-FlowRulZ-Mode", "4"))
            .and(body_string_contains("X-Custom"))
            .respond_with(ResponseTemplate::new(200).set_body_string("ok"))
            .expect(1)
            .mount(&mock)
            .await;

        let client = FlowRulZClient::new(Config {
            address: mock.uri(),
            api_key: None,
            timeout: None,
        });
        let mut headers = HashMap::new();
        headers.insert("X-Custom".into(), "val".into());
        let opts = ExecuteOpts {
            timeout: Duration::from_secs(5),
            headers,
        };
        let result = client
            .execute("rule", serde_json::json!({}), Some(opts))
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_execute_default_opts() {
        let opts = ExecuteOpts::default();
        assert_eq!(opts.timeout, Duration::from_secs(30));
        assert!(opts.headers.is_empty());
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

    #[tokio::test]
    async fn test_config_default() {
        let cfg = Config::default();
        assert_eq!(cfg.address, "http://localhost:8080");
        assert!(cfg.api_key.is_none());
        assert_eq!(cfg.timeout, Some(Duration::from_secs(30)));
    }

    #[tokio::test]
    async fn test_event_serialization() {
        let evt = Event {
            id: Some("test-id".into()),
            topic: "my-topic".into(),
            payload: serde_json::json!({"key": "val"}),
            headers: None,
            mode: MODE_PUBLISH,
        };
        let json = serde_json::to_string(&evt).unwrap();
        assert!(json.contains("my-topic"));
        assert!(json.contains("test-id"));
    }
}
