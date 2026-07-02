#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DAGNode {
    pub service_id: u16,
    pub layer: u8,
    #[serde(default)]
    pub parent_ids: Vec<u16>,
}

#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum DAGFailurePolicy {
    AbortAll,
    ContinueOthers,
    SkipDependents,
}

impl Default for DAGFailurePolicy {
    fn default() -> Self {
        DAGFailurePolicy::AbortAll
    }
}

#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum MergeStrategy {
    LastWins,
    ArrayConcat,
    DeepMerge,
    ExplicitMap,
}

impl Default for MergeStrategy {
    fn default() -> Self {
        MergeStrategy::LastWins
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DAGTable {
    pub nodes: Vec<DAGNode>,
    pub layers: Vec<Vec<u16>>,
    pub terminal_nodes: Vec<u16>,
    pub failure_policy: DAGFailurePolicy,
    pub node_timeouts: Vec<u32>,
    pub merge_strategy: MergeStrategy,
    pub distributed: bool,
}

impl DAGTable {
    pub fn new() -> Self {
        DAGTable {
            nodes: Vec::new(),
            layers: Vec::new(),
            terminal_nodes: Vec::new(),
            failure_policy: DAGFailurePolicy::default(),
            node_timeouts: Vec::new(),
            merge_strategy: MergeStrategy::default(),
            distributed: false,
        }
    }
}

impl Default for DAGTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dag_table_new() {
        let table = DAGTable::new();
        assert!(table.nodes.is_empty());
        assert!(table.layers.is_empty());
        assert!(table.terminal_nodes.is_empty());
        assert_eq!(table.failure_policy, DAGFailurePolicy::AbortAll);
        assert_eq!(table.merge_strategy, MergeStrategy::LastWins);
        assert!(!table.distributed);
    }

    #[test]
    fn test_dag_node() {
        let node = DAGNode {
            service_id: 5,
            layer: 1,
            parent_ids: vec![2, 3],
        };
        assert_eq!(node.service_id, 5);
        assert_eq!(node.layer, 1);
        assert_eq!(node.parent_ids, vec![2, 3]);
    }

    #[test]
    fn test_dag_failure_policy_default() {
        assert_eq!(DAGFailurePolicy::default(), DAGFailurePolicy::AbortAll);
    }

    #[test]
    fn test_merge_strategy_default() {
        assert_eq!(MergeStrategy::default(), MergeStrategy::LastWins);
    }

    #[test]
    fn test_serialization_roundtrip() {
        let mut table = DAGTable::new();
        table.nodes.push(DAGNode { service_id: 1, layer: 0, parent_ids: vec![] });
        table.layers.push(vec![1]);
        table.terminal_nodes.push(1);
        let bytes = bincode::serialize(&table).unwrap();
        let deserialized: DAGTable = bincode::deserialize(&bytes).unwrap();
        assert_eq!(deserialized.nodes.len(), 1);
        assert_eq!(deserialized.nodes[0].service_id, 1);
    }

    #[test]
    fn test_dag_table_with_all_fields() {
        let mut table = DAGTable::new();
        table.nodes.push(DAGNode { service_id: 10, layer: 0, parent_ids: vec![] });
        table.nodes.push(DAGNode { service_id: 20, layer: 1, parent_ids: vec![10] });
        table.layers.push(vec![10]);
        table.layers.push(vec![20]);
        table.terminal_nodes.push(20);
        table.failure_policy = DAGFailurePolicy::SkipDependents;
        table.merge_strategy = MergeStrategy::ArrayConcat;
        table.node_timeouts.push(5000);
        table.node_timeouts.push(3000);
        table.distributed = true;

        assert_eq!(table.nodes.len(), 2);
        assert_eq!(table.layers.len(), 2);
        assert_eq!(table.terminal_nodes, vec![20]);
        assert_eq!(table.failure_policy, DAGFailurePolicy::SkipDependents);
        assert_eq!(table.merge_strategy, MergeStrategy::ArrayConcat);
        assert_eq!(table.node_timeouts[0], 5000);
        assert!(table.distributed);
    }
}
