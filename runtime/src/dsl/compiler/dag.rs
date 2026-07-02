use std::collections::{HashMap, HashSet};

use super::error::CompileError;
use super::Compiler;
use crate::bytecode::plan::ExecutionPlan;

impl Compiler {
    pub fn compile_dag(
        &self,
        plan: &mut ExecutionPlan,
        body: &str,
    ) -> Result<u16, CompileError> {
        if body.is_empty() || body == "{}" {
            return Err(CompileError::DagEmpty(body.to_string()));
        }

        let content = body
            .trim_start_matches('{')
            .trim_end_matches('}')
            .trim();
        if content.is_empty() {
            return Err(CompileError::DagEmpty(body.to_string()));
        }

        let mut deps: HashMap<String, Vec<String>> = HashMap::new();
        for segment in content.split(',') {
            let seg = segment.trim();
            if seg.is_empty() {
                continue;
            }
            if let Some(colon_pos) = seg.find(':') {
                let node = seg[..colon_pos].trim().to_string();
                let dep_list = seg[colon_pos + 1..].trim();
                let inner = dep_list
                    .trim_start_matches('[')
                    .trim_end_matches(']')
                    .trim();
                let node_deps: Vec<String> = if inner.is_empty() {
                    Vec::new()
                } else {
                    inner
                        .split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect()
                };
                for dep in &node_deps {
                    deps.entry(dep.clone()).or_insert_with(Vec::new);
                }
                deps.insert(node, node_deps);
            } else {
                let node = seg.to_string();
                deps.entry(node).or_insert_with(Vec::new);
            }
        }

        if deps.is_empty() {
            return Err(CompileError::DagEmpty(body.to_string()));
        }

        for node_deps in deps.values() {
            for dep in node_deps {
                if !deps.contains_key(dep) {
                    return Err(CompileError::DagUnknownService(dep.clone()));
                }
            }
        }

        if Self::detect_cycle(&deps) {
            return Err(CompileError::DagCycle(body.to_string()));
        }

        let layers = Self::topological_sort(&deps);
        let mut dag_table = crate::bytecode::dag_table::DAGTable::new();

        let mut node_svc_ids: HashMap<String, u16> = HashMap::new();
        for (layer_idx, layer) in layers.iter().enumerate() {
            for node_name in layer {
                let svc_id = self.resolve_service(plan, node_name);
                node_svc_ids.insert(node_name.clone(), svc_id);
                let parent_ids: Vec<u16> = deps
                    .get(node_name)
                    .map(|parents| {
                        parents
                            .iter()
                            .filter_map(|p| node_svc_ids.get(p).copied())
                            .collect()
                    })
                    .unwrap_or_default();
                dag_table.nodes.push(crate::bytecode::dag_table::DAGNode {
                    service_id: svc_id,
                    layer: layer_idx as u8,
                    parent_ids,
                });
            }
            let svc_layer: Vec<u16> = layer
                .iter()
                .map(|n| plan.services.get_by_name(n).map(|e| e.id).unwrap_or(0))
                .collect();
            dag_table.layers.push(svc_layer);
        }

        let all_nodes: Vec<String> = deps.keys().cloned().collect();
        let depended_upon: HashSet<&str> =
            deps.values()
                .flat_map(|v| v.iter().map(|s| s.as_str()))
                .collect();
        for node in &all_nodes {
            if !depended_upon.contains(node.as_str()) {
                let svc_id = plan
                    .services
                    .get_by_name(node)
                    .map(|e| e.id)
                    .unwrap_or(0);
                dag_table.terminal_nodes.push(svc_id);
            }
        }

        let id = plan.dag_tables.len() as u16;
        plan.dag_tables.push(dag_table);
        Ok(id)
    }

    fn detect_cycle(deps: &HashMap<String, Vec<String>>) -> bool {
        let mut visited: HashMap<&str, bool> = HashMap::new();
        let mut in_stack: HashMap<&str, bool> = HashMap::new();

        for node in deps.keys() {
            visited.insert(node.as_str(), false);
            in_stack.insert(node.as_str(), false);
        }

        for node in deps.keys() {
            if !visited.get(node.as_str()).copied().unwrap_or(false)
                && Self::dfs_cycle(node, deps, &mut visited, &mut in_stack)
            {
                return true;
            }
        }
        false
    }

    fn dfs_cycle<'a>(
        node: &'a str,
        deps: &'a HashMap<String, Vec<String>>,
        visited: &mut HashMap<&'a str, bool>,
        in_stack: &mut HashMap<&'a str, bool>,
    ) -> bool {
        visited.insert(node, true);
        in_stack.insert(node, true);

        if let Some(children) = deps.get(node) {
            for child in children {
                if let Some(&is_visited) = visited.get(child.as_str()) {
                    if !is_visited {
                        if Self::dfs_cycle(child, deps, visited, in_stack) {
                            return true;
                        }
                    } else if let Some(&in_st) = in_stack.get(child.as_str()) {
                        if in_st {
                            return true;
                        }
                    }
                }
            }
        }

        in_stack.insert(node, false);
        false
    }

    fn topological_sort(deps: &HashMap<String, Vec<String>>) -> Vec<Vec<String>> {
        let mut in_degree: HashMap<String, usize> = HashMap::new();
        let mut adj: HashMap<String, Vec<String>> = HashMap::new();

        for (node, node_deps) in deps {
            in_degree.entry(node.clone()).or_insert(0);
            adj.entry(node.clone()).or_insert_with(Vec::new);

            for dep in node_deps {
                in_degree.entry(dep.clone()).or_insert(0);
                adj.entry(dep.clone())
                    .or_insert_with(Vec::new)
                    .push(node.clone());
            }
        }

        let mut layers = Vec::new();
        let mut current_layer: Vec<String> = in_degree
            .iter()
            .filter(|(_, &deg)| deg == 0)
            .map(|(n, _)| n.clone())
            .collect();
        current_layer.sort();

        while !current_layer.is_empty() {
            layers.push(current_layer.clone());

            let mut next_layer = Vec::new();
            for node in &current_layer {
                if let Some(children) = adj.get(node) {
                    for child in children {
                        if let Some(deg) = in_degree.get_mut(child) {
                            if *deg == 0 {
                                continue;
                            }
                            *deg -= 1;
                            if *deg == 0 {
                                next_layer.push(child.clone());
                            }
                        }
                    }
                }
            }
            next_layer.sort();
            current_layer = next_layer;
        }

        layers
    }
}
