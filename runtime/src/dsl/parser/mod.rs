mod nodes;

pub use nodes::{ASTNode, ParseError, Pipeline};

use super::lexer::Token;

fn token_to_ast(token: &Token) -> Result<ASTNode, ParseError> {
    match token {
        Token::Next(t) => Ok(ASTNode::Next(t.clone())),
        Token::Async(t) => Ok(ASTNode::Async(t.clone())),
        Token::Parallel(ts) => Ok(ASTNode::Parallel(ts.clone())),
        Token::Collect => Ok(ASTNode::Collect),
        Token::Fallback(t) => Ok(ASTNode::Fallback(t.clone())),
        Token::Gate { field, op, value } => Ok(ASTNode::Gate {
            field: field.clone(),
            op: op.clone(),
            value: value.clone(),
        }),
        Token::Split(f) => Ok(ASTNode::Split(f.clone())),
        Token::Map(e) => Ok(ASTNode::Map(e.clone())),
        Token::Emit(ts) => Ok(ASTNode::Emit(ts.clone())),
        Token::Drop => Ok(ASTNode::Drop),
        Token::Buffer(n) => Ok(ASTNode::Buffer(*n)),
        Token::Key(k) => Ok(ASTNode::Key(k.clone())),
        Token::Retry {
            count,
            strategy,
            fixed_ms,
        } => Ok(ASTNode::Retry {
            count: *count,
            strategy: strategy.clone(),
            fixed_ms: *fixed_ms,
        }),
        Token::Pipe => Ok(ASTNode::Pipe),
        Token::Timeout(ms) => Ok(ASTNode::Timeout(*ms)),
        Token::Chunk { count, mode } => Ok(ASTNode::Chunk {
            count: *count,
            mode: mode.clone(),
        }),
        Token::Dag(body) => Ok(ASTNode::Dag(body.clone())),
        Token::Schema(body) => Ok(ASTNode::Schema(body.clone())),
        Token::Label(l) => Ok(ASTNode::Label(l.clone())),
        Token::Jmp(l) => Ok(ASTNode::Jmp(l.clone())),
        Token::Delay(ms) => Ok(ASTNode::Delay(*ms)),
    }
}

pub fn parse(tokens: &[Token]) -> Result<Pipeline, ParseError> {
    if tokens.is_empty() {
        return Err(ParseError::EmptyPipeline);
    }

    let mut nodes = Vec::new();
    let mut last_was_service = false;
    let mut last_was_retry = false;
    let mut pending_collect = false;
    let mut has_timeout = false;

    for token in tokens {
        let node = token_to_ast(token)?;

        match &node {
            ASTNode::Retry { .. } => {
                if !last_was_service {
                    return Err(ParseError::RetryWithoutPrecedingService(node.to_string()));
                }
                if last_was_retry {
                    return Err(ParseError::RetryAfterRetry(node.to_string()));
                }
                if let Some(prev) = nodes.last() {
                    match prev {
                        ASTNode::Parallel(_) => {
                            return Err(ParseError::RetryAfterParallel(node.to_string()))
                        }
                        ASTNode::Collect => {
                            return Err(ParseError::RetryAfterCollect(node.to_string()))
                        }
                        ASTNode::Fallback(_) => {
                            return Err(ParseError::RetryAfterFallback(node.to_string()))
                        }
                        ASTNode::Emit(_) => {
                            return Err(ParseError::RetryAfterEmit(node.to_string()))
                        }
                        ASTNode::Drop => {
                            return Err(ParseError::RetryAfterDrop(node.to_string()))
                        }
                        ASTNode::Gate { .. } => {
                            return Err(ParseError::RetryAfterGate(node.to_string()))
                        }
                        ASTNode::Pipe => {
                            return Err(ParseError::RetryAfterPipe(node.to_string()))
                        }
                        ASTNode::Label(_) => {
                            return Err(ParseError::RetryAfterLabel(node.to_string()))
                        }
                        ASTNode::Jmp(_) => {
                            return Err(ParseError::RetryAfterJmp(node.to_string()))
                        }
                        ASTNode::Retry { .. } => {
                            return Err(ParseError::RetryAfterRetry(node.to_string()))
                        }
                        _ => {}
                    }
                }
                last_was_retry = true;
                has_timeout = false;
            }
            ASTNode::Timeout(_) => {
                if has_timeout && !last_was_service {
                    return Err(ParseError::MultipleTimeoutsBetweenServices(
                        node.to_string(),
                    ));
                }
                has_timeout = true;
                last_was_service = false;
            }
            ASTNode::Chunk { .. } => {
                last_was_service = false;
                last_was_retry = false;
                has_timeout = false;
            }
            ASTNode::Next(_) | ASTNode::Async(_) => {
                last_was_service = true;
                last_was_retry = false;
                has_timeout = false;
            }
            ASTNode::Parallel(_) => {
                last_was_service = true;
                pending_collect = true;
                last_was_retry = false;
                has_timeout = false;
            }
            ASTNode::Collect => {
                if !pending_collect {
                    return Err(ParseError::CollectWithoutParallel(node.to_string()));
                }
                pending_collect = false;
                last_was_service = true;
                last_was_retry = false;
                has_timeout = false;
            }
            ASTNode::Emit(_) => {
                last_was_service = true;
                last_was_retry = false;
                has_timeout = false;
            }
            ASTNode::Drop => {
                last_was_service = true;
                last_was_retry = false;
                has_timeout = false;
            }
            ASTNode::Fallback(_) => {
                last_was_service = true;
                last_was_retry = false;
                has_timeout = false;
            }
            ASTNode::Pipe | ASTNode::Gate { .. } | ASTNode::Label(_) | ASTNode::Jmp(_) => {
                last_was_service = false;
                last_was_retry = false;
            }
            _ => {
                last_was_service = false;
                last_was_retry = false;
            }
        }

        nodes.push(node);
    }

    Ok(Pipeline { nodes })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_str(dsl: &str) -> Result<Pipeline, ParseError> {
        let tokens = crate::dsl::lexer::lex(dsl).unwrap();
        parse(&tokens)
    }

    #[test]
    fn test_simple_next() {
        let p = parse_str("n:validate").unwrap();
        assert_eq!(p.nodes.len(), 1);
        assert_eq!(p.nodes[0], ASTNode::Next("validate".to_string()));
    }

    #[test]
    fn test_next_with_retry() {
        let p = parse_str("n:validate r3").unwrap();
        assert_eq!(p.nodes.len(), 2);
        assert_eq!(p.nodes[0], ASTNode::Next("validate".to_string()));
        assert_eq!(
            p.nodes[1],
            ASTNode::Retry {
                count: 3,
                strategy: None,
                fixed_ms: None
            }
        );
    }

    #[test]
    fn test_retry_after_parallel_error() {
        let result = parse_str("p:a,b r3");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ParseError::RetryAfterParallel(_)));
    }

    #[test]
    fn test_collect_without_parallel_error() {
        let result = parse_str("c");
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ParseError::CollectWithoutParallel(_)
        ));
    }

    #[test]
    fn test_collect_after_parallel_ok() {
        let p = parse_str("p:a,b c").unwrap();
        assert_eq!(p.nodes.len(), 2);
    }

    #[test]
    fn test_gate_pipe_pipeline() {
        let p = parse_str("g:amount>10000 n:manual-review | t300 n:auto-approve").unwrap();
        assert_eq!(p.nodes.len(), 5);
        assert_eq!(
            p.nodes[0],
            ASTNode::Gate {
                field: "amount".to_string(),
                op: ">".to_string(),
                value: "10000".to_string()
            }
        );
        assert_eq!(p.nodes[1], ASTNode::Next("manual-review".to_string()));
        assert_eq!(p.nodes[2], ASTNode::Pipe);
        assert_eq!(p.nodes[3], ASTNode::Timeout(300));
        assert_eq!(p.nodes[4], ASTNode::Next("auto-approve".to_string()));
    }

    #[test]
    fn test_full_pipeline() {
        let p =
            parse_str("t500 n:validate t1000 p:fraud,inventory c f:dlq n:fulfill e:notify,analytics")
                .unwrap();
        assert_eq!(p.nodes.len(), 8);
    }

    #[test]
    fn test_chunk_next() {
        let p = parse_str("chunk:10:seq n:storage").unwrap();
        assert_eq!(p.nodes.len(), 2);
        assert_eq!(
            p.nodes[0],
            ASTNode::Chunk {
                count: 10,
                mode: "seq".to_string()
            }
        );
        assert_eq!(p.nodes[1], ASTNode::Next("storage".to_string()));
    }

    #[test]
    fn test_async() {
        let p = parse_str("a:job-queue e:analytics").unwrap();
        assert_eq!(p.nodes.len(), 2);
        assert_eq!(p.nodes[0], ASTNode::Async("job-queue".to_string()));
    }

    #[test]
    fn test_dag() {
        let p = parse_str("dag:{A:[B,C],D:[A]} e:audit").unwrap();
        assert_eq!(p.nodes.len(), 2);
    }

    #[test]
    fn test_empty_pipeline_error() {
        let result = parse_str("");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ParseError::EmptyPipeline));
    }

    #[test]
    fn test_drop() {
        let p = parse_str("d").unwrap();
        assert_eq!(p.nodes.len(), 1);
        assert_eq!(p.nodes[0], ASTNode::Drop);
    }

    #[test]
    fn test_label_jmp() {
        let p = parse_str("start: n:svc j:end end:").unwrap();
        assert_eq!(p.nodes.len(), 4);
        assert_eq!(p.nodes[0], ASTNode::Label("start".to_string()));
        assert_eq!(p.nodes[1], ASTNode::Next("svc".to_string()));
        assert_eq!(p.nodes[2], ASTNode::Jmp("end".to_string()));
        assert_eq!(p.nodes[3], ASTNode::Label("end".to_string()));
    }

    #[test]
    fn test_delay() {
        let p = parse_str("delay:5000 n:svc").unwrap();
        assert_eq!(p.nodes.len(), 2);
        assert_eq!(p.nodes[0], ASTNode::Delay(5000));
        assert_eq!(p.nodes[1], ASTNode::Next("svc".to_string()));
    }

    #[test]
    fn test_key_split() {
        let p = parse_str("k:order_id s:region").unwrap();
        assert_eq!(p.nodes.len(), 2);
        assert_eq!(p.nodes[0], ASTNode::Key("order_id".to_string()));
        assert_eq!(p.nodes[1], ASTNode::Split("region".to_string()));
    }
}
