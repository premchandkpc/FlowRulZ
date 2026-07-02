package node

type ExecuteRequest struct {
	RuleID   string
	Body     []byte
	Timeout  int64
	Metadata map[string]string
}

type ExecuteResponse struct {
	Body     []byte
	Duration int64
	RuleID   string
	Error    string
}
