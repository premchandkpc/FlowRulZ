use std::collections::HashSet;

use super::Optimizer;
use crate::dsl::parser::ASTNode;

impl Optimizer {
    pub fn hoist_timeouts(&self, nodes: &[ASTNode]) -> Vec<ASTNode> {
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

    pub fn merge_emits(&self, nodes: &[ASTNode]) -> Vec<ASTNode> {
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

    pub fn remove_dead_code(&self, nodes: &[ASTNode]) -> Vec<ASTNode> {
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

    pub fn remove_unused_labels(&self, nodes: &[ASTNode]) -> Vec<ASTNode> {
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
                ASTNode::Label(name) if !targets.contains(name.as_str()) => {}
                _ => {
                    result.push(node.clone());
                }
            }
        }
        result
    }

    pub fn eliminate_redundant_jmps(&self, nodes: &[ASTNode]) -> Vec<ASTNode> {
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

    pub fn merge_retries(&self, nodes: &[ASTNode]) -> Vec<ASTNode> {
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

    pub fn remove_nops(&self, nodes: &[ASTNode]) -> Vec<ASTNode> {
        nodes
            .iter()
            .filter(|n| !matches!(n, ASTNode::Pipe))
            .cloned()
            .collect()
    }
}
