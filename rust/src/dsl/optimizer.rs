use std::collections::HashSet;

use super::parser::{ASTNode, Pipeline};

#[derive(Debug, Clone)]
pub struct OptimizedPipeline {
    pub nodes: Vec<ASTNode>,
}

pub struct Optimizer;

impl Optimizer {
    pub fn new() -> Self {
        Optimizer
    }

    pub fn optimize(&self, pipeline: &Pipeline) -> OptimizedPipeline {
        let nodes = self.simplify_gates(&pipeline.nodes);
        let nodes = self.hoist_timeouts(&nodes);
        let nodes = self.merge_emits(&nodes);
        let nodes = self.remove_dead_code(&nodes);
        let nodes = self.merge_retries(&nodes);
        let nodes = self.remove_unused_labels(&nodes);
        let nodes = self.eliminate_redundant_jmps(&nodes);
        let nodes = self.remove_nops(&nodes);
        OptimizedPipeline { nodes }
    }

    fn simplify_gates(&self, nodes: &[ASTNode]) -> Vec<ASTNode> {
        let mut result = Vec::with_capacity(nodes.len());
        let mut i = 0;
        while i < nodes.len() {
            if let ASTNode::Gate { field, op, value } = &nodes[i] {
                if Self::is_numeric(field) && Self::is_numeric(value)
                    && !nodes[i + 1..].iter().any(|n| matches!(n, ASTNode::Pipe))
                {
                    let fv: f64 = field.parse().unwrap_or(0.0);
                    let vv: f64 = value.parse().unwrap_or(0.0);
                    let is_true = match op.as_str() {
                        "==" => (fv - vv).abs() < f64::EPSILON,
                        "!=" => (fv - vv).abs() >= f64::EPSILON,
                        ">" => fv > vv,
                        "<" => fv < vv,
                        ">=" => fv >= vv,
                        "<=" => fv <= vv,
                        _ => {
                            result.push(nodes[i].clone());
                            i += 1;
                            continue;
                        }
                    };
                    if is_true {
                        i += 1;
                        continue;
                    } else {
                        result.push(ASTNode::Drop);
                        i += 1;
                        continue;
                    }
                }
            }
            result.push(nodes[i].clone());
            i += 1;
        }
        result
    }

    fn is_numeric(s: &str) -> bool {
        s.parse::<f64>().is_ok()
    }

    fn hoist_timeouts(&self, nodes: &[ASTNode]) -> Vec<ASTNode> {
        let mut result = Vec::with_capacity(nodes.len());
        let mut pending_timeout: Option<u64> = None;
        let mut pending_retry: Option<ASTNode> = None;

        for node in nodes {
            match node {
                ASTNode::Timeout(ms) => {
                    pending_timeout = Some(*ms);
                }
                ASTNode::Retry {
                    count,
                    strategy,
                    fixed_ms,
                } => {
                    pending_retry = Some(ASTNode::Retry {
                        count: *count,
                        strategy: strategy.clone(),
                        fixed_ms: *fixed_ms,
                    });
                }
                ASTNode::Next(_) | ASTNode::Async(_) => {
                    if let Some(ms) = pending_timeout.take() {
                        result.push(ASTNode::Timeout(ms));
                    }
                    result.push(node.clone());
                    if let Some(retry) = pending_retry.take() {
                        result.push(retry);
                    }
                }
                _ => {
                    pending_timeout = None;
                    pending_retry = None;
                    result.push(node.clone());
                }
            }
        }

        if let Some(ms) = pending_timeout {
            result.push(ASTNode::Timeout(ms));
        }
        if let Some(retry) = pending_retry {
            result.push(retry);
        }

        result
    }

    fn merge_emits(&self, nodes: &[ASTNode]) -> Vec<ASTNode> {
        let mut result = Vec::with_capacity(nodes.len());
        let mut i = 0;

        while i < nodes.len() {
            if let ASTNode::Emit(ref targets) = nodes[i] {
                let mut merged = targets.clone();
                let mut j = i + 1;
                while j < nodes.len() {
                    if let ASTNode::Emit(ref next_targets) = nodes[j] {
                        merged.extend(next_targets.clone());
                        j += 1;
                    } else {
                        break;
                    }
                }
                result.push(ASTNode::Emit(merged));
                i = j;
            } else {
                result.push(nodes[i].clone());
                i += 1;
            }
        }

        result
    }

    fn remove_dead_code(&self, nodes: &[ASTNode]) -> Vec<ASTNode> {
        let mut result = Vec::with_capacity(nodes.len());
        let mut dead = false;
        let mut pending_labels = Vec::new();

        for node in nodes {
            if dead {
                match node {
                    ASTNode::Label(_) | ASTNode::Jmp(_) => {
                        for lbl in pending_labels.drain(..) {
                            result.push(lbl);
                        }
                        result.push(node.clone());
                        dead = false;
                    }
                    _ => {}
                }
                continue;
            }

            match node {
                ASTNode::Drop | ASTNode::Jmp(_) => {
                    result.push(node.clone());
                    dead = true;
                }
                ASTNode::Label(_) => {
                    pending_labels.push(node.clone());
                }
                _ => {
                    result.extend(pending_labels.drain(..));
                    result.push(node.clone());
                }
            }
        }

        result
    }

    fn remove_unused_labels(&self, nodes: &[ASTNode]) -> Vec<ASTNode> {
        let targets: HashSet<&str> = nodes
            .iter()
            .filter_map(|n| {
                if let ASTNode::Jmp(t) = n {
                    Some(t.as_str())
                } else {
                    None
                }
            })
            .collect();

        let mut result = Vec::with_capacity(nodes.len());
        for node in nodes {
            match node {
                ASTNode::Label(name) if !targets.contains(name.as_str()) => {
                    // Drop unreferenced labels
                }
                _ => {
                    result.push(node.clone());
                }
            }
        }
        result
    }

    fn eliminate_redundant_jmps(&self, nodes: &[ASTNode]) -> Vec<ASTNode> {
        let mut result = Vec::with_capacity(nodes.len());
        let mut i = 0;
        while i < nodes.len() {
            if let ASTNode::Jmp(target) = &nodes[i] {
                if i + 1 < nodes.len() {
                    if let ASTNode::Label(lbl) = &nodes[i + 1] {
                        if lbl == target {
                            i += 1;
                            continue;
                        }
                    }
                }
            }
            result.push(nodes[i].clone());
            i += 1;
        }
        result
    }

    fn merge_retries(&self, nodes: &[ASTNode]) -> Vec<ASTNode> {
        let mut result = Vec::with_capacity(nodes.len());
        let mut i = 0;

        while i < nodes.len() {
            if let ASTNode::Retry { .. } = nodes[i] {
                if i > 0 {
                    if let Some(ASTNode::Retry { .. }) = result.last() {
                        i += 1;
                        continue;
                    }
                }
                result.push(nodes[i].clone());
            } else {
                result.push(nodes[i].clone());
            }
            i += 1;
        }

        result
    }

    fn remove_nops(&self, nodes: &[ASTNode]) -> Vec<ASTNode> {
        nodes
            .iter()
            .filter(|n| !matches!(n, ASTNode::Pipe))
            .cloned()
            .collect()
    }
}

impl Default for Optimizer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::lexer;
    use crate::dsl::parser;

    fn optimize_str(dsl: &str) -> OptimizedPipeline {
        let tokens = lexer::lex(dsl).unwrap();
        let pipeline = parser::parse(&tokens).unwrap();
        let opt = Optimizer::new();
        opt.optimize(&pipeline)
    }

    #[test]
    fn test_hoist_timeout() {
        let opt = optimize_str("t500 n:validate");
        assert_eq!(opt.nodes.len(), 2);
        assert_eq!(opt.nodes[0], ASTNode::Timeout(500));
        assert_eq!(opt.nodes[1], ASTNode::Next("validate".to_string()));
    }

    #[test]
    fn test_merge_adjacent_emits() {
        let opt = optimize_str("e:a e:b e:c");
        assert_eq!(opt.nodes.len(), 1);
        assert_eq!(
            opt.nodes[0],
            ASTNode::Emit(vec!["a".to_string(), "b".to_string(), "c".to_string()])
        );
    }

    #[test]
    fn test_dead_code_after_drop() {
        let opt = optimize_str("d n:svc e:a");
        assert_eq!(opt.nodes.len(), 1);
        assert_eq!(opt.nodes[0], ASTNode::Drop);
    }

    #[test]
    fn test_labels_preserved_after_drop() {
        let opt = optimize_str("d end: n:svc");
        // label removed because nothing jumps to it
        assert_eq!(opt.nodes.len(), 2);
        assert_eq!(opt.nodes[0], ASTNode::Drop);
        assert_eq!(opt.nodes[1], ASTNode::Next("svc".to_string()));
    }

    #[test]
    fn test_label_jumped_to_preserved_after_drop() {
        let opt = optimize_str("d target: j:target");
        // Jmp after Drop is kept (dead code pass only drops after jmp, not jmp itself)
        assert_eq!(opt.nodes.len(), 3);
        assert_eq!(opt.nodes[0], ASTNode::Drop);
        assert_eq!(opt.nodes[1], ASTNode::Label("target".to_string()));
        assert_eq!(opt.nodes[2], ASTNode::Jmp("target".to_string()));
    }

    #[test]
    fn test_remove_pipes() {
        let opt = optimize_str("g:a>1 n:svc1 | n:svc2");
        assert_eq!(opt.nodes.len(), 3);
        assert!(!opt.nodes.iter().any(|n| matches!(n, ASTNode::Pipe)));
    }

    #[test]
    fn test_constant_gate_true() {
        let opt = optimize_str("g:1==1 n:svc");
        assert_eq!(opt.nodes.len(), 1);
        assert_eq!(opt.nodes[0], ASTNode::Next("svc".to_string()));
    }

    #[test]
    fn test_constant_gate_false() {
        let opt = optimize_str("g:1==2 n:svc");
        assert_eq!(opt.nodes.len(), 1);
        assert_eq!(opt.nodes[0], ASTNode::Drop);
    }

    #[test]
    fn test_constant_gate_numeric_ops() {
        let opt = optimize_str("g:5>3 n:svc");
        assert_eq!(opt.nodes.len(), 1);
        assert_eq!(opt.nodes[0], ASTNode::Next("svc".to_string()));

        let opt = optimize_str("g:3>5 n:svc");
        assert_eq!(opt.nodes.len(), 1);
        assert_eq!(opt.nodes[0], ASTNode::Drop);
    }

    #[test]
    fn test_constant_gate_skipped_with_pipe() {
        let opt = optimize_str("g:1==1 n:svc1 | n:svc2");
        assert_eq!(opt.nodes.len(), 3);
        assert!(opt.nodes.iter().any(|n| matches!(n, ASTNode::Gate { .. })));
    }

    #[test]
    fn test_constant_gate_not_numeric_skipped() {
        let opt = optimize_str("g:name==alice n:svc");
        assert_eq!(opt.nodes.len(), 2);
        assert!(opt.nodes.iter().any(|n| matches!(n, ASTNode::Gate { .. })));
    }

    #[test]
    fn test_dead_label_elimination() {
        let opt = optimize_str("unused: n:svc n:svc2");
        assert_eq!(opt.nodes.len(), 2);
        assert!(!opt.nodes.iter().any(|n| matches!(n, ASTNode::Label(_))));
    }

    #[test]
    fn test_used_label_preserved() {
        let opt = optimize_str("target: n:svc j:target");
        assert!(opt.nodes.iter().any(|n| matches!(n, ASTNode::Label(_))));
    }

    #[test]
    fn test_jump_to_self_eliminated() {
        let opt = optimize_str("n:svc j:end end:");
        assert_eq!(opt.nodes.len(), 2);
        assert_eq!(opt.nodes[0], ASTNode::Next("svc".to_string()));
        assert_eq!(opt.nodes[1], ASTNode::Label("end".to_string()));
    }

    #[test]
    fn test_jmp_triggers_dead_code() {
        let opt = optimize_str("n:svc j:end n:dead end:");
        assert!(!opt.nodes.iter().any(|n| {
            if let ASTNode::Next(t) = n { t == "dead" } else { false }
        }));
    }

    #[test]
    fn test_multiple_constant_gates_in_chain() {
        let opt = optimize_str("g:2>1 n:a g:1>2 n:b");
        assert_eq!(opt.nodes.len(), 2);
        assert_eq!(opt.nodes[0], ASTNode::Next("a".to_string()));
        assert_eq!(opt.nodes[1], ASTNode::Drop);
    }
}
