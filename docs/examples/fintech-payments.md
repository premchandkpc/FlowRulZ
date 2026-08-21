# Fintech & Payments

Banking, payments, fraud detection, compliance, trading.

## 1. Payment Processing

### Flow DSL

```
version 1

flow PaymentProcessing

service auth
    type grpc
    address auth:50051

service fraud
    type grpc
    address fraud:50051

service payment-gateway
    type http
    url https://gateway.internal/charge
    method POST

service risk
    type grpc
    address risk:50051

service ledger
    type grpc
    address ledger:50051

service notifications
    type kafka
    brokers kafka:9092
    topic payment-events

service compliance
    type grpc
    address compliance:50051

service dlq
    type kafka
    brokers kafka:9092
    topic dead-letter-queue

retry
    attempts 3
    backoff exponential
    delay 1s

breaker
    failureRate 20
    window 60s
    cooldown 60s

timeout 15s

variables
    transaction_id string = ""
    risk_score float = 0.0
    auth_code string = ""

event PaymentInitiated
event PaymentAuthorized
event PaymentCaptured
event PaymentDeclined
event PaymentRefunded

workflow

Start

-> auth.ValidateSession

-> compliance.CheckAML

-> if compliance.aml_flag == "suspicious"
    then
        -> compliance.FileSAR
        -> notifications.SendComplianceAlert
        -> Return {error: "aml_flagged"}

-> risk.CalculateScore

-> if risk.score > 0.9
    then
        -> compliance.FileSAR
        -> notifications.SendFraudAlert
        -> Return {error: "high_risk"}

-> fraud.CheckTransaction

-> if fraud.blocked == true
    then
        -> ledger.RecordDecline
        -> notifications.SendDeclined
        -> emit PaymentDeclined
        -> Return {error: "fraud_blocked"}

-> payment-gateway.Authorize

-> if payment-gateway.status == "authorized"
    then
        -> ledger.RecordAuthorization
        -> payment-gateway.Capture
        -> ledger.RecordCapture
        -> emit PaymentCaptured
        -> notifications.SendSuccess
        -> Return {transaction_id, status: "captured"}
    else
        -> ledger.RecordDecline
        -> notifications.SendDeclined
        -> emit PaymentDeclined
        -> Return {error: "declined", reason: payment-gateway.error}

onError
    TimeoutError
        -> payment-gateway.Void
        -> dlq.SendTimeout
    Default
        -> dlq.SendGeneric

compensate
    payment-gateway.Authorize -> payment-gateway.Void
    payment-gateway.Capture -> payment-gateway.Refund
```

## 2. Fraud Detection Pipeline

### Bytecode DSL

```
schema:{!amount:float,!user_id:string,!merchant_id:string,!country:string} | p:velocity-check,device-check,location-check,merchant-check | c | m:{scores: {velocity: @[0], device: @[1], location: @[2], merchant: @[3]}, combined: @[0]*0.3 + @[1]*0.25 + @[2]*0.25 + @[3]*0.2} | g:combined>0.8 f:block | g:combined>0.5 n:manual-review r2:exp | n:auto-approve | e:fraud-decision
```

### Flow DSL

```
version 1

flow FraudDetection

service velocity
    type grpc
    address velocity:50051

service device
    type grpc
    address device:50051

service location
    type grpc
    address location:50051

service merchant
    type grpc
    address merchant:50051

service ml-score
    type http
    url https://ml.internal/score

service rules
    type grpc
    address rules:50051

service actions
    type grpc
    address actions:50051

service analytics
    type kafka
    brokers kafka:9092
    topic fraud-events

timeout 5s

workflow

Start

parallel
    -> velocity.Check
    -> device.Check
    -> location.Check
    -> merchant.Check
join

-> ml-score.Predict

-> rules.Evaluate

-> if rules.action == "block"
    then
        -> actions.BlockTransaction
        -> analytics.TrackBlocked
        -> Return {decision: "blocked"}
    else
        -> if rules.action == "review"
            then
                -> actions.FlagForReview
                -> analytics.TrackReview
                -> Return {decision: "review"}
            else
                -> actions.Approve
                -> analytics.TrackApproved
                -> Return {decision: "approved"}
```

## 3. Wire Transfer

### Flow DSL

```
version 1

flow WireTransfer

service auth
    type grpc
    address auth:50051

service accounts
    type grpc
    address accounts:50051

service compliance
    type grpc
    address compliance:50051

service fx
    type http
    url https://fx.internal/rate

service wire
    type http
    url https://wire.internal/send
    method POST

service notifications
    type kafka
    brokers kafka:9092
    topic wire-events

constants
    domestic_limit float = 25000.0
    international_limit float = 10000.0

retry
    attempts 2
    backoff exponential
    delay 2s

timeout 60s

workflow

Start

-> auth.ValidateSession

-> accounts.CheckBalance

-> if accounts.balance < @.amount
    then
        -> Return {error: "insufficient_funds"}

-> if @.is_international == true
    then
        -> compliance.CheckOFAC
        -> compliance.CheckSanctions
        -> if compliance.blocked == true
            then
                -> Return {error: "compliance_blocked"}
        -> fx.GetRate
        -> wire.SendInternational
    else
        -> if @.amount > domestic_limit
            then
                -> compliance.CheckLargeTransfer
                -> wire.SendWithVerification
            else
                -> wire.SendDomestic

-> accounts.Debit

-> accounts.Credit

-> notifications.SendConfirmation

-> Return {transfer_id, status: "completed"}

onError
    TimeoutError
        -> compliance.FileSuspiciousActivity
        -> dlq.Send
    Default
        -> dlq.Send
```

## 4. Account Reconciliation

### Flow DSL

```
version 1

flow AccountReconciliation

service internal-ledger
    type grpc
    address ledger:50051

service bank-api
    type http
    url https://bank.internal/statements

service matching
    type grpc
    address matching:50051

service exceptions
    type grpc
    address exceptions:50051

service reporting
    type http
    url https://reports.internal/generate

service notifications
    type kafka
    brokers kafka:9092
    topic reconciliation-events

constants
    MATCH_THRESHOLD float = 0.01

trigger cron
    schedule "0 2 * * *"

workflow

Start

parallel
    -> internal-ledger.GetTransactions
    -> bank-api.GetStatements
join

-> matching.AutoMatch

-> if matching.unmatched_count > 0
    then
        -> exceptions.Investigate
        -> if exceptions.resolvable == true
            then
                -> exceptions.AutoResolve
            else
                -> exceptions.FlagForReview
                -> notifications.SendExceptionAlert

-> reporting.GenerateReport

-> notifications.SendReconciliationComplete

-> Return {matched, unresolved, report_url}
```

## 5. Loan Application

### Flow DSL

```
version 1

flow LoanApplication

service auth
    type grpc
    address auth:50051

service credit
    type http
    url https://credit.internal/check
    method POST

service income
    type http
    url https://income.internal/verify

service identity
    type grpc
    address identity:50051

service pricing
    type grpc
    address pricing:50051

service underwriting
    type grpc
    address underwriting:50051

service documents
    type grpc
    address documents:50051

service notifications
    type kafka
    brokers kafka:9092
    topic loan-events

constants
    MIN_CREDIT_SCORE int = 650
    MAX_DTI_RATIO float = 0.43

timeout 30s

workflow

Start

-> auth.ValidateSession

-> identity.Verify

-> parallel
    -> credit.CheckScore
    -> income.Verify
    -> documents.Validate
join

-> if credit.score < MIN_CREDIT_SCORE
    then
        -> notifications.SendDeclined
        -> Return {status: "declined", reason: "credit_score"}

-> if income.dti_ratio > MAX_DTI_RATIO
    then
        -> notifications.SendDeclined
        -> Return {status: "declined", reason: "dti_ratio"}

-> pricing.CalculateRate

-> underwriting.Evaluate

-> if underwriting.decision == "approved"
    then
        -> documents.GenerateContract
        -> notifications.SendApproval
        -> Return {status: "approved", rate: pricing.rate, amount: @.amount}
    else
        -> if underwriting.decision == "counter_offer"
            then
                -> notifications.SendCounterOffer
                -> Return {status: "counter_offer", offered_amount: underwriting.offered}
            else
                -> notifications.SendDeclined
                -> Return {status: "declined"}

onError
    Default
        -> notifications.SendError
```

## 6. Trading Order Execution

### Bytecode DSL

```
schema:{!symbol:string,!side:enum[buy|sell],!quantity:int,!order_type:enum[market|limit]} | n:check-market | g:order_type==market n:execute-market | g:order_type==limit n:execute-limit | p:position-check,risk-check | c | g:risk.approved==true n:submit-order r3:fixed:100 f:cancel-order | n:confirm-execution | e:trade-executed,portfolio-update
```

### Flow DSL

```
version 1

flow TradingOrder

service market-data
    type grpc
    address market:50051

service portfolio
    type grpc
    address portfolio:50051

service risk
    type grpc
    address risk:50051

service execution
    type http
    url https://execution.internal/order
    method POST

service compliance
    type grpc
    address compliance:50051

service notifications
    type kafka
    brokers kafka:9092
    topic trading-events

constants
    MAX_POSITION_PCT float = 0.10
    MAX_ORDER_VALUE float = 100000.0

retry
    attempts 3
    backoff fixed
    delay 50ms

timeout 5s

workflow

Start

-> market-data.GetQuote

-> compliance.CheckTradingRules

parallel
    -> portfolio.CheckPosition
    -> risk.EvaluateOrder
join

-> if risk.approved == false
    then
        -> notifications.SendRiskRejected
        -> Return {error: "risk_rejected"}

-> if @.order_type == "market"
    then
        -> execution.SubmitMarketOrder
    else
        -> execution.SubmitLimitOrder

-> if execution.status == "filled"
    then
        -> portfolio.UpdatePosition
        -> notifications.SendFillConfirmation
        -> Return {fill_price: execution.fill_price, quantity: execution.filled_qty}
    else
        -> notifications.SendOrderPending
        -> Return {status: "pending", order_id: execution.order_id}

onError
    TimeoutError
        -> execution.CancelOrder
    Default
        -> execution.CancelOrder
```

## 7. KYC/AML Pipeline

### Flow DSL

```
version 1

flow KYCVerification

service identity
    type grpc
    address identity:50051

service document-verification
    type http
    url https://doc-verify.internal/scan

service liveness
    type http
    url https://liveness.internal/check

service sanctions
    type http
    url https://sanctions.internal/check

service pep
    type http
    url https://pep.internal/check

service adverse-media
    type http
    url https://media.internal/search

service kyc-store
    type grpc
    address kyc:50051

service notifications
    type kafka
    brokers kafka:9092
    topic kyc-events

timeout 30s

workflow

Start

-> identity.ExtractInfo

parallel
    -> document-verification.Scan
    -> liveness.Check
join

-> if document-verification.valid == false || liveness.passed == false
    then
        -> notifications.SendVerificationFailed
        -> Return {status: "failed", reason: "document_or_liveness"}

parallel
    -> sanctions.Check
    -> pep.Check
    -> adverse-media.Search
join

-> if sanctions.hit == true || pep.hit == true
    then
        -> kyc-store.RecordHit
        -> notifications.SendComplianceAlert
        -> Return {status: "blocked", reason: "sanctions_pep"}

-> if adverse-media.severity == "high"
    then
        -> kyc-store.FlagForReview
        -> Return {status: "pending_review"}

-> kyc-store.Approve

-> notifications.SendApproved

-> Return {status: "approved", risk_level: "low"}

onError
    Default
        -> kyc-store.RecordError
```

## 8. Subscription Billing

### Flow DSL

```
version 1

flow SubscriptionBilling

service subscriptions
    type grpc
    address subscriptions:50051

service payment
    type http
    url https://payment.internal/charge

service invoicing
    type grpc
    address invoicing:50051

service notifications
    type kafka
    brokers kafka:9092
    topic billing-events

service dlq
    type kafka
    brokers kafka:9092
    topic dead-letter-queue

retry
    attempts 3
    backoff exponential
    delay 1h

trigger cron
    schedule "0 0 1 * *"

workflow

Start

-> subscriptions.GetDueSubscriptions

-> foreach subscription in subscriptions.due
    -> invoicing.GenerateInvoice
    -> payment.Charge
    -> if payment.status == "success"
        then
            -> subscriptions.MarkPaid
            -> notifications.SendReceipt
        else
            -> if payment.error == "card_expired"
                then
                    -> subscriptions.MarkCardExpired
                    -> notifications.SendCardExpired
                else
                    -> subscriptions.MarkPaymentFailed
                    -> notifications.SendPaymentFailed

-> subscriptions.CheckOverdue

-> if subscriptions.has_overdue == true
    then
        -> subscriptions.SuspendOverdue
        -> notifications.SendSuspension

-> Return {billed, succeeded, failed}
```
