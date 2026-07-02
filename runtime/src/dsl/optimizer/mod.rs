mod constant_fold;
mod passes;

use crate::dsl::parser::{ASTNode, Pipeline};

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
        assert_eq!(opt.nodes.len(), 2);
        assert_eq!(opt.nodes[0], ASTNode::Drop);
        assert_eq!(opt.nodes[1], ASTNode::Next("svc".to_string()));
    }

    #[test]
    fn test_label_jumped_to_preserved_after_drop() {
        let opt = optimize_str("d target: j:target");
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
