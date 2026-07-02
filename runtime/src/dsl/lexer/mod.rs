mod token;

pub use token::{LexError, Token};

fn parse_comma_targets(s: &str, token_name: &str) -> Result<Vec<String>, LexError> {
    if s.is_empty() {
        return Err(LexError::EmptyOperand(token_name.to_string()));
    }
    Ok(s.split(',')
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty())
        .collect())
}

fn parse_retry(word: &str) -> Result<Token, LexError> {
    let rest = &word[1..];
    if rest.is_empty() {
        return Err(LexError::InvalidRetry(word.to_string()));
    }
    if let Some(colon_pos) = rest.find(':') {
        let count_str = &rest[..colon_pos];
        let strategy_part = &rest[colon_pos + 1..];
        let count: u8 = count_str
            .parse()
            .map_err(|_| LexError::InvalidRetry(word.to_string()))?;
        if let Some(second_colon) = strategy_part.find(':') {
            let strategy = strategy_part[..second_colon].to_string();
            let fixed_ms: u32 = strategy_part[second_colon + 1..]
                .parse()
                .map_err(|_| LexError::InvalidRetry(word.to_string()))?;
            Ok(Token::Retry {
                count,
                strategy: Some(strategy),
                fixed_ms: Some(fixed_ms),
            })
        } else {
            let strategy = strategy_part.to_string();
            Ok(Token::Retry {
                count,
                strategy: Some(strategy),
                fixed_ms: None,
            })
        }
    } else {
        let count: u8 = rest
            .parse()
            .map_err(|_| LexError::InvalidRetry(word.to_string()))?;
        Ok(Token::Retry {
            count,
            strategy: None,
            fixed_ms: None,
        })
    }
}

fn parse_chunk(word: &str) -> Result<Token, LexError> {
    let body = &word[6..];
    let parts: Vec<&str> = body.split(':').collect();
    if parts.len() != 2 {
        return Err(LexError::InvalidChunk(word.to_string()));
    }
    let count: u8 = parts[0]
        .parse()
        .map_err(|_| LexError::InvalidChunk(word.to_string()))?;
    let mode = parts[1].to_string();
    if mode != "seq" && mode != "par" {
        return Err(LexError::InvalidChunk(word.to_string()));
    }
    Ok(Token::Chunk { count, mode })
}

fn parse_gate(word: &str) -> Result<Token, LexError> {
    let body = &word[2..];
    let operators = ["==", "!=", ">=", "<=", ">", "<", "contains"];
    for op in &operators {
        if let Some(pos) = body.find(op) {
            let field = body[..pos].trim_end_matches('.').to_string();
            let value = body[pos + op.len()..].to_string();
            if field.is_empty() || value.is_empty() {
                return Err(LexError::EmptyOperand(word.to_string()));
            }
            return Ok(Token::Gate {
                field,
                op: op.to_string(),
                value,
            });
        }
    }
    Err(LexError::InvalidGateOp(word.to_string()))
}

fn classify(word: &str) -> Result<Token, LexError> {
    if word.is_empty() {
        return Err(LexError::UnknownToken("empty token".to_string()));
    }
    if word.starts_with("chunk:") {
        return parse_chunk(word);
    }
    if let Some(body) = word.strip_prefix("dag:") {
        let body = body.to_string();
        if body.is_empty() {
            return Err(LexError::InvalidDag(word.to_string()));
        }
        return Ok(Token::Dag(body));
    }
    if let Some(rest) = word.strip_prefix("delay:") {
        let ms: u64 = rest.parse().map_err(|_| LexError::InvalidDelay(word.to_string()))?;
        return Ok(Token::Delay(ms));
    }
    if let Some(body) = word.strip_prefix("schema:") {
        let body = body.to_string();
        if body.is_empty() {
            return Err(LexError::InvalidSchema(word.to_string()));
        }
        return Ok(Token::Schema(body));
    }
    if word.ends_with(':') && word.len() > 2 {
        let label = word[..word.len() - 1].to_string();
        return Ok(Token::Label(label));
    }

    let first = word.as_bytes()[0];
    match first {
        b'n' => {
            if word.len() < 3 || &word[1..2] != ":" {
                return Err(LexError::UnknownToken(word.to_string()));
            }
            let target = word[2..].to_string();
            if target.is_empty() {
                return Err(LexError::EmptyOperand(word.to_string()));
            }
            Ok(Token::Next(target))
        }
        b'a' => {
            if word.len() < 3 || &word[1..2] != ":" {
                return Err(LexError::UnknownToken(word.to_string()));
            }
            let target = word[2..].to_string();
            if target.is_empty() {
                return Err(LexError::EmptyOperand(word.to_string()));
            }
            Ok(Token::Async(target))
        }
        b'p' => {
            if word.len() < 3 || &word[1..2] != ":" {
                return Err(LexError::UnknownToken(word.to_string()));
            }
            let targets = parse_comma_targets(&word[2..], "p")?;
            if targets.is_empty() {
                return Err(LexError::EmptyOperand(word.to_string()));
            }
            Ok(Token::Parallel(targets))
        }
        b'c' if word == "c" => Ok(Token::Collect),
        b'f' => {
            if word.len() < 3 || &word[1..2] != ":" {
                return Err(LexError::UnknownToken(word.to_string()));
            }
            let target = word[2..].to_string();
            if target.is_empty() {
                return Err(LexError::EmptyOperand(word.to_string()));
            }
            Ok(Token::Fallback(target))
        }
        b'g' => {
            if word.len() < 4 || &word[1..2] != ":" {
                return Err(LexError::UnknownToken(word.to_string()));
            }
            parse_gate(word)
        }
        b's' => {
            if word.len() < 3 || &word[1..2] != ":" {
                return Err(LexError::UnknownToken(word.to_string()));
            }
            let field = word[2..].to_string();
            if field.is_empty() {
                return Err(LexError::EmptyOperand(word.to_string()));
            }
            Ok(Token::Split(field))
        }
        b'm' => {
            if word.len() < 3 || &word[1..2] != ":" {
                return Err(LexError::UnknownToken(word.to_string()));
            }
            let expr = word[2..].to_string();
            if expr.is_empty() {
                return Err(LexError::EmptyOperand(word.to_string()));
            }
            Ok(Token::Map(expr))
        }
        b'w' => {
            if word.len() < 3 || &word[1..2] != ":" {
                return Err(LexError::UnknownToken(word.to_string()));
            }
            if word.is_empty() {
                return Err(LexError::EmptyOperand(word.to_string()));
            }
            Ok(Token::Map(word.to_string()))
        }
        b'e' => {
            if word.len() < 3 || &word[1..2] != ":" {
                return Err(LexError::UnknownToken(word.to_string()));
            }
            let targets = parse_comma_targets(&word[2..], "e")?;
            if targets.is_empty() {
                return Err(LexError::EmptyOperand(word.to_string()));
            }
            Ok(Token::Emit(targets))
        }
        b'd' => {
            if word.len() == 1 {
                Ok(Token::Drop)
            } else {
                Err(LexError::UnknownToken(word.to_string()))
            }
        }
        b'b' => {
            if word.len() < 2 {
                return Err(LexError::InvalidBuffer(word.to_string()));
            }
            let n: u64 = word[1..]
                .parse()
                .map_err(|_| LexError::InvalidBuffer(word.to_string()))?;
            Ok(Token::Buffer(n))
        }
        b'k' => {
            if word.len() < 3 || &word[1..2] != ":" {
                return Err(LexError::UnknownToken(word.to_string()));
            }
            let field = word[2..].to_string();
            if field.is_empty() {
                return Err(LexError::EmptyOperand(word.to_string()));
            }
            Ok(Token::Key(field))
        }
        b'r' => parse_retry(word),
        b't' => {
            if word == "t" {
                return Err(LexError::InvalidTimeout(word.to_string()));
            }
            let ms: u64 = word[1..]
                .parse()
                .map_err(|_| LexError::InvalidTimeout(word.to_string()))?;
            Ok(Token::Timeout(ms))
        }
        b'|' => {
            if word.len() == 1 {
                Ok(Token::Pipe)
            } else {
                Err(LexError::UnknownToken(word.to_string()))
            }
        }
        b'j' if word.len() > 2 && &word[1..2] == ":" => {
            let label = word[2..].to_string();
            if label.is_empty() {
                return Err(LexError::InvalidJmp(word.to_string()));
            }
            Ok(Token::Jmp(label))
        }
        _ => Err(LexError::UnknownToken(word.to_string())),
    }
}

pub fn lex(input: &str) -> Result<Vec<Token>, LexError> {
    let mut tokens = Vec::new();
    for word in input.split_whitespace() {
        tokens.push(classify(word)?);
    }
    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_next() {
        let tokens = lex("n:validate").unwrap();
        assert_eq!(tokens, vec![Token::Next("validate".to_string())]);
    }

    #[test]
    fn test_async() {
        let tokens = lex("a:job-queue").unwrap();
        assert_eq!(tokens, vec![Token::Async("job-queue".to_string())]);
    }

    #[test]
    fn test_parallel() {
        let tokens = lex("p:fraud,inventory").unwrap();
        assert_eq!(
            tokens,
            vec![Token::Parallel(vec!["fraud".to_string(), "inventory".to_string()])]
        );
    }

    #[test]
    fn test_collect() {
        let tokens = lex("c").unwrap();
        assert_eq!(tokens, vec![Token::Collect]);
    }

    #[test]
    fn test_fallback() {
        let tokens = lex("f:dlq").unwrap();
        assert_eq!(tokens, vec![Token::Fallback("dlq".to_string())]);
    }

    #[test]
    fn test_gate_eq() {
        let tokens = lex("g:amount>10000").unwrap();
        assert_eq!(
            tokens,
            vec![Token::Gate {
                field: "amount".to_string(),
                op: ">".to_string(),
                value: "10000".to_string(),
            }]
        );
    }

    #[test]
    fn test_gate_contains() {
        let tokens = lex("g:status.containsbanned").unwrap();
        assert_eq!(
            tokens,
            vec![Token::Gate {
                field: "status".to_string(),
                op: "contains".to_string(),
                value: "banned".to_string(),
            }]
        );
    }

    #[test]
    fn test_gate_dotted_field() {
        let tokens = lex("g:user.tier==premium").unwrap();
        assert_eq!(
            tokens,
            vec![Token::Gate {
                field: "user.tier".to_string(),
                op: "==".to_string(),
                value: "premium".to_string(),
            }]
        );
    }

    #[test]
    fn test_split() {
        let tokens = lex("s:user_id").unwrap();
        assert_eq!(tokens, vec![Token::Split("user_id".to_string())]);
    }

    #[test]
    fn test_map() {
        let tokens = lex("m:.results[]").unwrap();
        assert_eq!(tokens, vec![Token::Map(".results[]".to_string())]);
    }

    #[test]
    fn test_emit() {
        let tokens = lex("e:notify,analytics").unwrap();
        assert_eq!(
            tokens,
            vec![Token::Emit(vec!["notify".to_string(), "analytics".to_string()])]
        );
    }

    #[test]
    fn test_drop() {
        let tokens = lex("d").unwrap();
        assert_eq!(tokens, vec![Token::Drop]);
    }

    #[test]
    fn test_buffer() {
        let tokens = lex("b100").unwrap();
        assert_eq!(tokens, vec![Token::Buffer(100)]);
    }

    #[test]
    fn test_key() {
        let tokens = lex("k:order_id").unwrap();
        assert_eq!(tokens, vec![Token::Key("order_id".to_string())]);
    }

    #[test]
    fn test_retry_default() {
        let tokens = lex("r3").unwrap();
        assert_eq!(
            tokens,
            vec![Token::Retry {
                count: 3,
                strategy: None,
                fixed_ms: None
            }]
        );
    }

    #[test]
    fn test_retry_exp() {
        let tokens = lex("r3:exp").unwrap();
        assert_eq!(
            tokens,
            vec![Token::Retry {
                count: 3,
                strategy: Some("exp".to_string()),
                fixed_ms: None
            }]
        );
    }

    #[test]
    fn test_retry_fixed() {
        let tokens = lex("r3:fixed:200").unwrap();
        assert_eq!(
            tokens,
            vec![Token::Retry {
                count: 3,
                strategy: Some("fixed".to_string()),
                fixed_ms: Some(200)
            }]
        );
    }

    #[test]
    fn test_pipe() {
        let tokens = lex("|").unwrap();
        assert_eq!(tokens, vec![Token::Pipe]);
    }

    #[test]
    fn test_timeout() {
        let tokens = lex("t500").unwrap();
        assert_eq!(tokens, vec![Token::Timeout(500)]);
    }

    #[test]
    fn test_chunk_seq() {
        let tokens = lex("chunk:10:seq").unwrap();
        assert_eq!(
            tokens,
            vec![Token::Chunk {
                count: 10,
                mode: "seq".to_string()
            }]
        );
    }

    #[test]
    fn test_chunk_par() {
        let tokens = lex("chunk:4:par").unwrap();
        assert_eq!(
            tokens,
            vec![Token::Chunk {
                count: 4,
                mode: "par".to_string()
            }]
        );
    }

    #[test]
    fn test_dag() {
        let tokens = lex("dag:{A:[B,C],D:[A]}").unwrap();
        assert_eq!(tokens, vec![Token::Dag("{A:[B,C],D:[A]}".to_string())]);
    }

    #[test]
    fn test_label() {
        let tokens = lex("mylabel:").unwrap();
        assert_eq!(tokens, vec![Token::Label("mylabel".to_string())]);
    }

    #[test]
    fn test_jmp() {
        let tokens = lex("j:mylabel").unwrap();
        assert_eq!(tokens, vec![Token::Jmp("mylabel".to_string())]);
    }

    #[test]
    fn test_delay() {
        let tokens = lex("delay:5000").unwrap();
        assert_eq!(tokens, vec![Token::Delay(5000)]);
    }

    #[test]
    fn test_delay_invalid() {
        assert!(lex("delay:").is_err());
        assert!(lex("delay:abc").is_err());
    }

    #[test]
    fn test_full_pipeline() {
        let dsl = "t500 n:validate t1000 p:fraud,inventory c f:dlq n:fulfill e:notify,analytics";
        let tokens = lex(dsl).unwrap();
        assert_eq!(tokens.len(), 8);
        assert_eq!(tokens[0], Token::Timeout(500));
        assert_eq!(tokens[1], Token::Next("validate".to_string()));
        assert_eq!(tokens[2], Token::Timeout(1000));
        assert_eq!(
            tokens[3],
            Token::Parallel(vec!["fraud".to_string(), "inventory".to_string()])
        );
        assert_eq!(tokens[4], Token::Collect);
        assert_eq!(tokens[5], Token::Fallback("dlq".to_string()));
        assert_eq!(tokens[6], Token::Next("fulfill".to_string()));
        assert_eq!(
            tokens[7],
            Token::Emit(vec!["notify".to_string(), "analytics".to_string()])
        );
    }

    #[test]
    fn test_gate_pipe_pipeline() {
        let dsl = "g:amount>10000 n:manual-review | t300 n:auto-approve f:hold-queue";
        let tokens = lex(dsl).unwrap();
        assert_eq!(tokens.len(), 6);
        assert_eq!(
            tokens[0],
            Token::Gate {
                field: "amount".to_string(),
                op: ">".to_string(),
                value: "10000".to_string()
            }
        );
        assert_eq!(tokens[1], Token::Next("manual-review".to_string()));
        assert_eq!(tokens[2], Token::Pipe);
        assert_eq!(tokens[3], Token::Timeout(300));
        assert_eq!(tokens[4], Token::Next("auto-approve".to_string()));
        assert_eq!(tokens[5], Token::Fallback("hold-queue".to_string()));
    }

    #[test]
    fn test_empty_token_error() {
        assert!(lex("").unwrap().is_empty());
    }

    #[test]
    fn test_invalid_token_error() {
        assert!(lex("xyz").is_err());
    }

    #[test]
    fn test_empty_operand() {
        assert!(lex("n:").is_err());
        assert!(lex("p:").is_err());
        assert!(lex("f:").is_err());
        assert!(lex("k:").is_err());
        assert!(lex("s:").is_err());
    }

    #[test]
    fn test_invalid_gate_op() {
        assert!(lex("g:field??value").is_err());
    }
}
