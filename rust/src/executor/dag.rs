use std::collections::{HashMap, HashSet};

use crate::bytecode::instruction::Instruction;
use crate::bytecode::plan::ExecutionPlan;
use crate::bytecode::dag_table::{DAGFailurePolicy, MergeStrategy};

pub fn exec_dag<'a>(
    body: &[u8],
    instr: &Instruction,
    plan: &ExecutionPlan,
    caller: &dyn Fn(u16, &[u8], u64) -> Result<Vec<u8>, String>,
    arena: &'a crate::memory::arena::Arena,
) -> Result<&'a mut [u8], String> {
    let dag_id = instr.a as usize;
    let dag = &plan.dag_tables[dag_id];
    let mut results: HashMap<u16, Vec<u8>> = HashMap::new();
    let mut failed: HashSet<u16> = HashSet::new();

    for layer in &dag.layers {
        for &svc_id in layer {
            let node_idx = dag.nodes.iter().position(|n| n.service_id == svc_id);
            let node = match node_idx.and_then(|i| dag.nodes.get(i)) {
                Some(n) => n,
                None => continue,
            };

            // SkipDependents: skip node if any parent failed
            if dag.failure_policy == DAGFailurePolicy::SkipDependents {
                if node.parent_ids.iter().any(|p| failed.contains(p)) {
                    failed.insert(svc_id);
                    continue;
                }
            }

            // Build input body: merge parent results into original body
            let call_body = if node.parent_ids.is_empty() {
                body.to_vec()
            } else {
                let mut merged = match serde_json::from_slice::<serde_json::Value>(body) {
                    Ok(v) => v,
                    Err(_) => return Err(format!("dag node {}: failed to parse body", svc_id)),
                };
                for &parent_id in &node.parent_ids {
                    if let Some(parent_result) = results.get(&parent_id) {
                        if let Ok(parent_val) =
                            serde_json::from_slice::<serde_json::Value>(parent_result)
                        {
                            deep_merge(&mut merged, parent_val);
                        }
                    }
                }
                serde_json::to_vec(&merged).unwrap_or_else(|_| body.to_vec())
            };

            // Apply node timeout (indexed parallel to nodes)
            let timeout = node_idx
                .and_then(|i| dag.node_timeouts.get(i))
                .copied()
                .unwrap_or(0) as u64;

            let resp = caller(svc_id, &call_body, timeout);
            match resp {
                Ok(data) => {
                    results.insert(svc_id, data);
                }
                Err(e) => match dag.failure_policy {
                    DAGFailurePolicy::AbortAll => {
                        return Err(format!("dag node {}: {}", svc_id, e));
                    }
                    DAGFailurePolicy::ContinueOthers | DAGFailurePolicy::SkipDependents => {
                        failed.insert(svc_id);
                    }
                },
            }
        }
    }

    let merged = merge_dag_results(
        &dag.terminal_nodes,
        &results,
        &failed,
        plan,
        arena,
        dag.merge_strategy,
    );
    Ok(merged)
}

fn merge_dag_results<'a>(
    terminal_nodes: &[u16],
    results: &HashMap<u16, Vec<u8>>,
    failed: &HashSet<u16>,
    plan: &ExecutionPlan,
    arena: &'a crate::memory::arena::Arena,
    strategy: MergeStrategy,
) -> &'a mut [u8] {
    match strategy {
        MergeStrategy::ArrayConcat => {
            let mut arr = Vec::new();
            for &svc_id in terminal_nodes {
                if failed.contains(&svc_id) {
                    arr.push(serde_json::Value::Null);
                } else if let Some(resp) = results.get(&svc_id) {
                    match serde_json::from_slice::<serde_json::Value>(resp) {
                        Ok(val) => arr.push(val),
                        Err(_) => arr.push(serde_json::Value::String(
                            String::from_utf8_lossy(resp).to_string(),
                        )),
                    }
                }
            }
            let out = serde_json::to_vec(&serde_json::Value::Array(arr))
                .unwrap_or_default();
            arena.alloc_copy(&out)
        }
        MergeStrategy::DeepMerge => {
            let mut merged = serde_json::Value::Object(serde_json::Map::new());
            for &svc_id in terminal_nodes {
                if failed.contains(&svc_id) {
                    continue;
                }
                if let Some(resp) = results.get(&svc_id) {
                    if let Ok(val) = serde_json::from_slice::<serde_json::Value>(resp) {
                        deep_merge(&mut merged, val);
                    }
                }
            }
            let out = serde_json::to_vec(&merged).unwrap_or_default();
            arena.alloc_copy(&out)
        }
        MergeStrategy::ExplicitMap => {
            // ExplicitMap requires an explicit mapping config that doesn't exist yet.
            // Fall back to LastWins.
            merge_last_wins(terminal_nodes, results, failed, plan, arena)
        }
        MergeStrategy::LastWins => {
            merge_last_wins(terminal_nodes, results, failed, plan, arena)
        }
    }
}

fn merge_last_wins<'a>(
    terminal_nodes: &[u16],
    results: &HashMap<u16, Vec<u8>>,
    failed: &HashSet<u16>,
    plan: &ExecutionPlan,
    arena: &'a crate::memory::arena::Arena,
) -> &'a mut [u8] {
    let mut entries = Vec::new();
    for &svc_id in terminal_nodes {
        if failed.contains(&svc_id) {
            let svc_name = plan.services.get(svc_id).name.clone();
            entries.push(format!("\"{}\":null", svc_name));
        } else if let Some(resp) = results.get(&svc_id) {
            let svc_name = plan.services.get(svc_id).name.clone();
            entries.push(format!(
                "\"{}\":{}",
                svc_name,
                String::from_utf8_lossy(resp)
            ));
        }
    }
    let joined = entries.join(",");
    let result = format!("{{{}}}", joined);
    arena.alloc_copy(result.as_bytes())
}

fn deep_merge(a: &mut serde_json::Value, b: serde_json::Value) {
    match (a, b) {
        (serde_json::Value::Object(ref mut a_map), serde_json::Value::Object(b_map)) => {
            for (k, v) in b_map {
                if v.is_object() {
                    deep_merge(a_map.entry(k).or_insert(serde_json::Value::Object(serde_json::Map::new())), v);
                } else {
                    a_map.insert(k, v);
                }
            }
        }
        (a, b) => *a = b,
    }
}
