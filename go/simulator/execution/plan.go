package execution

type OpCode int

const (
	OpNop OpCode = iota
	OpCallService
	OpValidate
	OpBranch
	OpPublish
	OpReturn
)

func (o OpCode) String() string {
	switch o {
	case OpNop:
		return "NOP"
	case OpCallService:
		return "CALL_SERVICE"
	case OpValidate:
		return "VALIDATE"
	case OpBranch:
		return "BRANCH"
	case OpPublish:
		return "PUBLISH"
	case OpReturn:
		return "RETURN"
	default:
		return "UNKNOWN"
	}
}

type Instruction struct {
	Op      OpCode
	Service string
	Args    []string
}

type Plan struct {
	ID           string
	Instructions []Instruction
	PlanBytes    []byte          // compiled bytecode for real VM execution
	ServiceNames map[uint16]string // svc_id → name mapping from compiled plan
	Tags         map[string]string
}

func NewPlan(id string, instructions []Instruction) *Plan {
	return &Plan{
		ID:           id,
		Instructions: instructions,
		Tags:         make(map[string]string),
	}
}

var OrderFlow = NewPlan("order-flow-v1", []Instruction{
	{Op: OpValidate, Args: []string{"order"}},
	{Op: OpCallService, Service: "inventory"},
	{Op: OpCallService, Service: "fraud"},
	{Op: OpBranch, Args: []string{"fraud_risk", "gt", "0.8"}},
	{Op: OpCallService, Service: "payment"},
	{Op: OpCallService, Service: "email"},
	{Op: OpPublish, Args: []string{"order_confirmed"}},
	{Op: OpReturn},
})

var OrderFlowAltPayment = NewPlan("order-flow-alt", []Instruction{
	{Op: OpValidate, Args: []string{"order"}},
	{Op: OpCallService, Service: "inventory"},
	{Op: OpBranch, Args: []string{"in_stock", "eq", "false"}},
	{Op: OpPublish, Args: []string{"out_of_stock"}},
	{Op: OpReturn},
	{Op: OpCallService, Service: "fraud"},
	{Op: OpCallService, Service: "payment"},
	{Op: OpCallService, Service: "email"},
	{Op: OpPublish, Args: []string{"order_confirmed"}},
	{Op: OpReturn},
})

var PaymentFlow = NewPlan("payment-flow-v1", []Instruction{
	{Op: OpValidate, Args: []string{"payment"}},
	{Op: OpCallService, Service: "payment"},
	{Op: OpCallService, Service: "loyalty"},
	{Op: OpPublish, Args: []string{"payment_completed"}},
	{Op: OpReturn},
})

var RefundFlow = NewPlan("refund-flow-v1", []Instruction{
	{Op: OpValidate, Args: []string{"refund"}},
	{Op: OpCallService, Service: "payment"},
	{Op: OpCallService, Service: "invoice"},
	{Op: OpPublish, Args: []string{"refund_processed"}},
	{Op: OpReturn},
})
