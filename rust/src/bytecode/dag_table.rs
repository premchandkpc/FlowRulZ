#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DAGNode {
    pub service_id: u16,
    pub layer: u8,
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
