use super::Optimizer;
use crate::dsl::parser::ASTNode;

impl Optimizer {
    pub fn simplify_gates(&self, nodes: &[ASTNode]) -> Vec<ASTNode> {
        let mut result = Vec::with_capacity(nodes.len());
        let mut i = 0;
        while i < nodes.len() {
            if let ASTNode::Gate { field, op, value } = &nodes[i] {
                if Self::is_numeric(field)
                    && Self::is_numeric(value)
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
}
