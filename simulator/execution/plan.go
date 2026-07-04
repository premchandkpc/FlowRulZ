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
}

func NewPlan(id string, instructions []Instruction) *Plan {
	return &Plan{
		ID:           id,
		Instructions: instructions,
	}
}

var OrderFlow = NewPlan("order-flow-v1", []Instruction{
	{Op: OpValidate, Args: []string{"order"}},
	{Op: OpCallService, Service: "inventory"},
	{Op: OpCallService, Service: "fraud"},
	{Op: OpBranch, Args: []string{"fraud_risk", "gt", "0.8"}},
	{Op: OpCallService, Service: "payment"},
	{Op: OpCallService, Service: "email"},
	{Op: OpCallService, Service: "notification"},
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

var ShippingFlow = NewPlan("shipping-flow-v1", []Instruction{
	{Op: OpValidate, Args: []string{"shipping"}},
	{Op: OpCallService, Service: "inventory"},
	{Op: OpBranch, Args: []string{"stock_level", "lt", "10"}},
	{Op: OpCallService, Service: "payment"},
	{Op: OpCallService, Service: "notification"},
	{Op: OpPublish, Args: []string{"shipping_scheduled"}},
	{Op: OpReturn},
})

var ServiceDiscoveryFlow = NewPlan("service-discovery", []Instruction{
	{Op: OpValidate, Args: []string{"service_query"}},
	{Op: OpCallService, Service: "inventory"},
	{Op: OpCallService, Service: "payment"},
	{Op: OpPublish, Args: []string{"service_registry"}},
	{Op: OpReturn},
})

var DeadLetterQueueFlow = NewPlan("dead-letter-queue", []Instruction{
	{Op: OpValidate, Args: []string{"failed_order"}},
	{Op: OpCallService, Service: "payment"},
	{Op: OpBranch, Args: []string{"payment_status", "eq", "failed"}},
	{Op: OpCallService, Service: "email"},
	{Op: OpCallService, Service: "notification"},
	{Op: OpPublish, Args: []string{"order_failed_dlq"}},
	{Op: OpReturn},
})

// ═══════════════════════════════════════════════════════════════════
// CUSTOMER DOMAIN
// ═══════════════════════════════════════════════════════════════════

var CustomerRegistrationFlow = NewPlan("customer-registration", []Instruction{
	{Op: OpValidate, Args: []string{"customer"}},
	{Op: OpCallService, Service: "identity"},
	{Op: OpCallService, Service: "authentication"},
	{Op: OpCallService, Service: "profile"},
	{Op: OpCallService, Service: "address"},
	{Op: OpCallService, Service: "email"},
	{Op: OpPublish, Args: []string{"customer_registered"}},
	{Op: OpReturn},
})

var CustomerLoginFlow = NewPlan("customer-login", []Instruction{
	{Op: OpValidate, Args: []string{"credentials"}},
	{Op: OpCallService, Service: "authentication"},
	{Op: OpCallService, Service: "authorization"},
	{Op: OpPublish, Args: []string{"login_success"}},
	{Op: OpReturn},
})

var SupportTicketFlow = NewPlan("support-ticket", []Instruction{
	{Op: OpValidate, Args: []string{"ticket"}},
	{Op: OpCallService, Service: "ai"},
	{Op: OpCallService, Service: "support"},
	{Op: OpCallService, Service: "customer"},
	{Op: OpCallService, Service: "email"},
	{Op: OpPublish, Args: []string{"ticket_resolved"}},
	{Op: OpReturn},
})

// ═══════════════════════════════════════════════════════════════════
// CATALOG DOMAIN
// ═══════════════════════════════════════════════════════════════════

var ProductSearchFlow = NewPlan("product-search", []Instruction{
	{Op: OpValidate, Args: []string{"search_query"}},
	{Op: OpCallService, Service: "search"},
	{Op: OpCallService, Service: "catalog"},
	{Op: OpCallService, Service: "inventory"},
	{Op: OpPublish, Args: []string{"search_results"}},
	{Op: OpReturn},
})

var RecommendationFlow = NewPlan("recommendation", []Instruction{
	{Op: OpValidate, Args: []string{"user_profile"}},
	{Op: OpCallService, Service: "profile"},
	{Op: OpCallService, Service: "ai"},
	{Op: OpCallService, Service: "catalog"},
	{Op: OpCallService, Service: "recommendation"},
	{Op: OpPublish, Args: []string{"recommendations"}},
	{Op: OpReturn},
})

var PriceCalculationFlow = NewPlan("price-calculation", []Instruction{
	{Op: OpValidate, Args: []string{"order_items"}},
	{Op: OpCallService, Service: "pricing"},
	{Op: OpCallService, Service: "promotion"},
	{Op: OpCallService, Service: "coupon"},
	{Op: OpCallService, Service: "tax"},
	{Op: OpPublish, Args: []string{"price_calculated"}},
	{Op: OpReturn},
})

// ═══════════════════════════════════════════════════════════════════
// ORDER DOMAIN
// ═══════════════════════════════════════════════════════════════════

var OrderCancellationFlow = NewPlan("order-cancellation", []Instruction{
	{Op: OpValidate, Args: []string{"cancel_request"}},
	{Op: OpCallService, Service: "order"},
	{Op: OpBranch, Args: []string{"order_status", "eq", "shipped"}},
	{Op: OpCallService, Service: "shipping"},
	{Op: OpCallService, Service: "inventory"},
	{Op: OpCallService, Service: "payment"},
	{Op: OpCallService, Service: "notification"},
	{Op: OpPublish, Args: []string{"order_cancelled"}},
	{Op: OpReturn},
})

var RefundProcessingFlow = NewPlan("refund-processing", []Instruction{
	{Op: OpValidate, Args: []string{"refund_request"}},
	{Op: OpCallService, Service: "fraud"},
	{Op: OpCallService, Service: "refund"},
	{Op: OpCallService, Service: "payment"},
	{Op: OpCallService, Service: "inventory"},
	{Op: OpCallService, Service: "notification"},
	{Op: OpCallService, Service: "audit"},
	{Op: OpPublish, Args: []string{"refund_processed"}},
	{Op: OpReturn},
})

var SubscriptionRenewalFlow = NewPlan("subscription-renewal", []Instruction{
	{Op: OpValidate, Args: []string{"subscription"}},
	{Op: OpCallService, Service: "billing"},
	{Op: OpCallService, Service: "payment"},
	{Op: OpCallService, Service: "invoice"},
	{Op: OpCallService, Service: "notification"},
	{Op: OpCallService, Service: "analytics"},
	{Op: OpPublish, Args: []string{"renewal_complete"}},
	{Op: OpReturn},
})

// ═══════════════════════════════════════════════════════════════════
// SHIPPING DOMAIN
// ═══════════════════════════════════════════════════════════════════

var ShippingScheduleFlow = NewPlan("shipping-schedule", []Instruction{
	{Op: OpValidate, Args: []string{"shipping_request"}},
	{Op: OpCallService, Service: "warehouse"},
	{Op: OpCallService, Service: "courier"},
	{Op: OpCallService, Service: "shipping"},
	{Op: OpCallService, Service: "notification"},
	{Op: OpPublish, Args: []string{"shipping_scheduled"}},
	{Op: OpReturn},
})

var WarehouseFulfillmentFlow = NewPlan("warehouse-fulfillment", []Instruction{
	{Op: OpValidate, Args: []string{"fulfillment_request"}},
	{Op: OpCallService, Service: "inventory"},
	{Op: OpCallService, Service: "warehouse"},
	{Op: OpCallService, Service: "shipping"},
	{Op: OpCallService, Service: "notification"},
	{Op: OpPublish, Args: []string{"fulfillment_complete"}},
	{Op: OpReturn},
})

// ═══════════════════════════════════════════════════════════════════
// NOTIFICATION DOMAIN
// ═══════════════════════════════════════════════════════════════════

var NotificationDispatchFlow = NewPlan("notification-dispatch", []Instruction{
	{Op: OpValidate, Args: []string{"notification_request"}},
	{Op: OpBranch, Args: []string{"channel", "eq", "email"}},
	{Op: OpCallService, Service: "email"},
	{Op: OpBranch, Args: []string{"channel", "eq", "sms"}},
	{Op: OpCallService, Service: "sms"},
	{Op: OpBranch, Args: []string{"channel", "eq", "push"}},
	{Op: OpCallService, Service: "push"},
	{Op: OpBranch, Args: []string{"channel", "eq", "webhook"}},
	{Op: OpCallService, Service: "webhook"},
	{Op: OpPublish, Args: []string{"notification_sent"}},
	{Op: OpReturn},
})

// ═══════════════════════════════════════════════════════════════════
// ANALYTICS DOMAIN
// ═══════════════════════════════════════════════════════════════════

var AnalyticsAggregationFlow = NewPlan("analytics-aggregation", []Instruction{
	{Op: OpValidate, Args: []string{"analytics_request"}},
	{Op: OpCallService, Service: "analytics"},
	{Op: OpCallService, Service: "audit"},
	{Op: OpCallService, Service: "reporting"},
	{Op: OpPublish, Args: []string{"analytics_complete"}},
	{Op: OpReturn},
})

// ═══════════════════════════════════════════════════════════════════
// AI DOMAIN
// ═══════════════════════════════════════════════════════════════════

var FraudDetectionFlow = NewPlan("fraud-detection", []Instruction{
	{Op: OpValidate, Args: []string{"transaction"}},
	{Op: OpCallService, Service: "ai"},
	{Op: OpCallService, Service: "fraud"},
	{Op: OpBranch, Args: []string{"risk_score", "gt", "0.8"}},
	{Op: OpCallService, Service: "notification"},
	{Op: OpPublish, Args: []string{"fraud_checked"}},
	{Op: OpReturn},
})

var DocumentProcessingFlow = NewPlan("document-processing", []Instruction{
	{Op: OpValidate, Args: []string{"document"}},
	{Op: OpCallService, Service: "ocr"},
	{Op: OpCallService, Service: "ai"},
	{Op: OpCallService, Service: "document"},
	{Op: OpPublish, Args: []string{"document_processed"}},
	{Op: OpReturn},
})

var ImageProcessingFlow = NewPlan("image-processing", []Instruction{
	{Op: OpValidate, Args: []string{"image"}},
	{Op: OpCallService, Service: "image"},
	{Op: OpCallService, Service: "ai"},
	{Op: OpCallService, Service: "document"},
	{Op: OpPublish, Args: []string{"image_processed"}},
	{Op: OpReturn},
})

var TranslationFlow = NewPlan("translation", []Instruction{
	{Op: OpValidate, Args: []string{"translation_request"}},
	{Op: OpCallService, Service: "translation"},
	{Op: OpCallService, Service: "ai"},
	{Op: OpPublish, Args: []string{"translation_complete"}},
	{Op: OpReturn},
})

// ═══════════════════════════════════════════════════════════════════
// UTILITY DOMAIN
// ═══════════════════════════════════════════════════════════════════

var CurrencyConversionFlow = NewPlan("currency-conversion", []Instruction{
	{Op: OpValidate, Args: []string{"conversion_request"}},
	{Op: OpCallService, Service: "currency"},
	{Op: OpCallService, Service: "geo"},
	{Op: OpPublish, Args: []string{"conversion_complete"}},
	{Op: OpReturn},
})

var GeoLookupFlow = NewPlan("geo-lookup", []Instruction{
	{Op: OpValidate, Args: []string{"geo_request"}},
	{Op: OpCallService, Service: "geo"},
	{Op: OpCallService, Service: "weather"},
	{Op: OpPublish, Args: []string{"geo_resolved"}},
	{Op: OpReturn},
})

// ═══════════════════════════════════════════════════════════════════
// COMPLEX WORKFLOWS
// ═══════════════════════════════════════════════════════════════════

var CompleteOrderWorkflow = NewPlan("complete-order-workflow", []Instruction{
	{Op: OpValidate, Args: []string{"order"}},
	{Op: OpCallService, Service: "customer"},
	{Op: OpCallService, Service: "inventory"},
	{Op: OpCallService, Service: "pricing"},
	{Op: OpCallService, Service: "promotion"},
	{Op: OpCallService, Service: "coupon"},
	{Op: OpCallService, Service: "tax"},
	{Op: OpCallService, Service: "fraud"},
	{Op: OpBranch, Args: []string{"fraud_risk", "gt", "0.8"}},
	{Op: OpCallService, Service: "payment"},
	{Op: OpCallService, Service: "order"},
	{Op: OpCallService, Service: "warehouse"},
	{Op: OpCallService, Service: "shipping"},
	{Op: OpCallService, Service: "courier"},
	{Op: OpCallService, Service: "invoice"},
	{Op: OpCallService, Service: "email"},
	{Op: OpCallService, Service: "sms"},
	{Op: OpCallService, Service: "notification"},
	{Op: OpCallService, Service: "analytics"},
	{Op: OpPublish, Args: []string{"order_complete"}},
	{Op: OpReturn},
})

var EcommerceCheckoutFlow = NewPlan("ecommerce-checkout", []Instruction{
	{Op: OpValidate, Args: []string{"checkout"}},
	{Op: OpCallService, Service: "customer"},
	{Op: OpCallService, Service: "cart"},
	{Op: OpCallService, Service: "pricing"},
	{Op: OpCallService, Service: "promotion"},
	{Op: OpCallService, Service: "tax"},
	{Op: OpCallService, Service: "shipping"},
	{Op: OpCallService, Service: "payment"},
	{Op: OpCallService, Service: "order"},
	{Op: OpCallService, Service: "notification"},
	{Op: OpPublish, Args: []string{"checkout_complete"}},
	{Op: OpReturn},
})
