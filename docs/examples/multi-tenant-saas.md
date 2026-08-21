# Multi-Tenant SaaS

Tenant isolation, billing, provisioning, feature flags.

## 1. Tenant Provisioning

### Flow DSL

```
version 1

flow TenantProvisioning

service tenant-store
    type grpc
    address tenants:50051

service database
    type postgres
    connection postgres://db:5432/master

service auth
    type grpc
    address auth:50051

service billing
    type http
    url https://billing.internal/create-account

service notifications
    type kafka
    brokers kafka:9092
    topic tenant-events

timeout 60s

workflow

Start

-> tenant-store.CreateTenant

-> database.CreateSchema

-> auth provisionAdminUser

-> billing.CreateBillingAccount

-> notifications.SendWelcomeEmail

-> Return {tenant_id: tenant-store.tenant_id, admin_email: auth.admin_email}
```

## 2. Tenant-Scoped Request

### Flow DSL

```
version 1

flow TenantRequest

service auth
    type grpc
    address auth:50051

service tenant-resolver
    type grpc
    address resolver:50051

service rate-limiter
    type grpc
    address ratelimit:50051

service feature-flags
    type grpc
    address flags:50051

service business-logic
    type grpc
    address logic:50051

service cache
    type redis
    connection redis:6379

constants
    RATE_LIMIT_PER_MINUTE int = 1000

timeout 5s

workflow

Start

-> auth.Authenticate

-> tenant-resolver.Resolve

-> rate-limiter.CheckLimit

-> if rate-limiter.allowed == false
    then
        -> Return {error: "rate_limited", retry_after: rate-limiter.retry_after}

-> feature-flags.Evaluate

-> if feature-flags.enabled == false
    then
        -> Return {error: "feature_disabled"}

-> cache.GetCached

-> if cache.hit == true
    then
        -> Return cache.result
    else
        -> business-logic.Execute
        -> cache.Store

-> Return result
```

## 3. Usage-Based Billing

### Flow DSL

```
version 1

flow UsageBilling

service usage-tracker
    type grpc
    address usage:50051

service billing
    type http
    url https://billing.internal/charge
    method POST

service notifications
    type kafka
    brokers kafka:9092
    topic billing-events

constants
    BILLING_CYCLE_DAYS int = 30
    USAGE_TIERS string = "1000:0.01,10000:0.008,100000:0.005"

trigger cron
    schedule "0 0 1 * *"

workflow

Start

-> usage-tracker.CalculateUsage

-> foreach tenant in usage-tracker.tenants
    -> billing.GenerateInvoice
    -> billing.ChargeInvoice
    -> if billing.status == "success"
        then
            -> notifications.SendReceipt
        else
            -> notifications.SendPaymentFailed

-> Return {billed: usage-tracker.tenant_count}
```

## 4. Tenant Migration

### Flow DSL

```
version 1

flow TenantMigration

service tenant-store
    type grpc
    address tenants:50051

service source-db
    type postgres
    connection postgres://source:5432/tenant

service target-db
    type postgres
    connection postgres://target:5432/tenant

service data-migrator
    type grpc
    address migrator:50051

service validator
    type grpc
    address validator:50051

service notifications
    type kafka
    brokers kafka:9092
    topic migration-events

timeout 24h

workflow

Start

-> tenant-store.GetMigrationPlan

-> source-db.Backup

-> data-migrator.MigrateSchema

-> data-migrator.MigrateData

-> validator.ValidateData

-> if validator.mismatches == 0
    then
        -> tenant-store.SwitchTraffic
        -> notifications.SendMigrationComplete
    else
        -> source-db.Restore
        -> notifications.SendMigrationFailed

-> Return {migrated: data-migrator.row_count, mismatches: validator.mismatches}
```

## 5. Feature Flag Evaluation

### Bytecode DSL

```
schema:{!tenant_id:string,!feature_key:string,!context:object} | n:get-tenant | n:get-flag | g:flag.enabled==true n:serve-variant | g:flag.enabled==false f:serve-default | n:track-exposure | e:flag-evaluated
```

### Flow DSL

```
version 1

flow FeatureFlagEvaluation

service tenant-store
    type grpc
    address tenants:50051

service flag-store
    type grpc
    address flags:50051

service variant-resolver
    type grpc
    address variants:50051

service tracking
    type kafka
    brokers kafka:9092
    topic flag-exposures

timeout 1s

workflow

Start

-> tenant-store.GetTenant

-> flag-store.GetFlag

-> if flag.enabled == true
    then
        -> variant-resolver.Resolve
        -> tracking.TrackExposure
        -> Return {variant: variant-resolver.variant, enabled: true}
    else
        -> tracking.TrackExposure
        -> Return {variant: "control", enabled: false}
```
