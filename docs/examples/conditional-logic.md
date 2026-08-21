# Conditional Logic

Gates, if/else, switch/case, labeled jumps.

## Bytecode DSL

### Gate Operator

```
g:field<op>value
```

Branch based on a field value. If the condition is true, execution continues to the next operation. If false, the step is skipped (effectively a no-op for that instruction).

### Gate Operators

| Op | Meaning | Example |
|----|---------|---------|
| `==` | Equal | `g:status==active` |
| `!=` | Not equal | `g:role!=admin` |
| `>` | Greater than | `g:amount>1000` |
| `<` | Less than | `g:score<50` |
| `>=` | Greater or equal | `g:age>=18` |
| `<=` | Less or equal | `g:temp<=0` |
| `contains` | Substring/membership | `g:tags.contains vip` |

### Simple Gate

```
g:amount>10000 n:manual-review
```

If `amount > 10000`, call `manual-review`. Otherwise skip.

### Chained Gates

```
g:status==active g:role==admin n:admin-action
```

Both conditions must be true to reach `admin-action`.

### Gate + Fallback

```
g:amount>10000 n:manual-review f:auto-approve
```

If amount > 10000, manual review. Otherwise, auto-approve.

### Labeled Jumps

```
start: n:auth g:role==admin n:admin-panel j:end n:user-panel end: e:done
```

**Execution flow:**
1. `start:` — label (no-op)
2. `n:auth` — authenticate
3. `g:role==admin` — check if admin
4. `n:admin-panel` — if admin, call admin panel
5. `j:end` — jump to `end:`
6. `n:user-panel` — if not admin (gate was false), call user panel
7. `end:` — label
8. `e:done` — emit done event

This is how if/else works in the bytecode DSL — gates skip the next instruction if false, and jumps skip over code blocks.

## Flow DSL

### If/Else

```
version 1

flow RoleBasedAccess

service auth
    type grpc
    address auth:50051

service admin-panel
    type grpc
    address admin:50051

service user-panel
    type grpc
    address user:50051

workflow

Start

-> auth.Validate

-> if auth.role == "admin"
    then
        -> admin-panel.Dashboard
    else
        -> user-panel.Home

-> End
```

### Nested If/Else

```
-> if order.amount > 10000
    then
        -> if order.type == "business"
            then
                -> review.BusinessLargeOrder
            else
                -> review.PersonalLargeOrder
    else
        -> auto.Approve
```

### Switch/Case

```
version 1

flow OrderRouting

service standard
    type grpc
    address standard:50051

service express
    type grpc
    address express:50051

service overnight
    type grpc
    address overnight:50051

service economy
    type grpc
    address economy:50051

workflow

Start

-> switch order.shipping_method
    case "standard"
        -> standard.Ship
    case "express"
        -> express.Ship
    case "overnight"
        -> overnight.Ship
    default
        -> economy.Ship

-> End
```

### Switch on Multiple Fields

```
-> switch user.tier
    case "enterprise"
        -> switch order.region
            case "us"
                -> us.EnterpriseProcess
            case "eu"
                -> eu.EnterpriseProcess
            default
                -> global.EnterpriseProcess
    case "pro"
        -> pro.Process
    default
        -> free.Process
```

### If with Complex Conditions

```
-> if order.amount > 1000 && order.items.length > 10
    then
        -> warehouse.BulkProcess
    else
        -> warehouse.StandardProcess
```

## Combining Patterns

### Gate Chain in Bytecode

```
g:amount>10000 n:large-review g:risk>0.7 n:fraud-check f:auto-approve
```

**Flow:**
1. If amount > 10000 -> large review
2. If risk > 0.7 -> fraud check
3. Otherwise -> auto approve

### If/Else with Parallel Inside

```
-> if order.type == "bulk"
    then
        parallel
            -> warehouse.CheckCapacity
            -> logistics.CheckFleet
        join
    else
        -> warehouse.SinglePick

-> packing.Prepare
```

## Condition Evaluation

Conditions are evaluated against the current execution context:

| Source | Access Pattern | Example |
|--------|---------------|---------|
| Payload field | `field` | `g:amount>1000` |
| Service result | `service.field` | `g:auth.role==admin` |
| Variable | `var_name` | `g:retry_count<3` |
| Constant | `CONST_NAME` | `g:MAX_RETRIES>0` |
| Nested field | `field.subfield` | `g:order.total>100` |
| Array field | `field[index]` | `g:result[0]==pass` |
