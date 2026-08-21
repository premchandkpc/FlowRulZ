# Schema Validation

Type guards, required fields, enums, compile-time validation.

## Bytecode DSL

### Basic Schema

```
schema:{!order_id:string,!amount:float,status:string}
```

Validate the incoming payload:
- `order_id` — required, must be string
- `amount` — required, must be float
- `status` — optional, must be string if present

### Required Fields

Prefix with `!`:

```
schema:{!id:string,!email:string,!name:string}
```

If any required field is missing, the step fails with a type error.

### Schema + Pipeline

```
schema:{!order_id:string,!amount:float} t500 n:validate | n:process
```

Validate first, then process with a 500ms timeout.

### Schema Types

| Type | Description | Supports |
|------|-------------|----------|
| `string` | Text | Ordering, contains |
| `int` | Integer | Ordering, numeric |
| `float` | Floating point | Ordering, numeric |
| `bool` | Boolean | Equality only |
| `object` | JSON object | Equality only |
| `array` | JSON array | Contains only |
| `null` | Null value | Equality only |
| `any` | Any type | All operators pass |
| `enum[v1\|v2]` | Restricted values | Equality against allowed values |

### Enum Schema

```
schema:{status:enum[active|inactive|pending],role:enum[admin|user|guest]}
```

Only allows specified values.

### Complex Schema

```
schema:{!order_id:string,!amount:float,!items:array,user:{!id:string,email:string},priority:enum[low|medium|high]}
```

Nested objects with mixed required/optional fields.

## Flow DSL

### Schema in Service Declarations

Schema validation is implicit in the Flow DSL through variable typing:

```
version 1

flow TypedFlow

variables
    order_id string
    amount float
    status string

constants
    VALID_STATUSES string = "active,pending,shipped"

service validator
    type grpc
    address validator:50051

workflow

Start

-> validator.Validate

-> End
```

### Schema as Step Input

```
-> validator.Validate
    input
        !order_id string
        !amount float
        status string
```

## Compile-Time Checking

The Rust DSL compiler performs type checking at compile time:

```
schema:{!amount:int} g:amount>1000 n:process
```

The compiler verifies:
1. `amount` is declared as `int` in the schema
2. `>` operator is valid for `int` types
3. The pipeline is type-safe

### Type Check Errors

If you write:

```
schema:{name:string} g:name>100
```

The compiler rejects it: `>` is not valid for `string` type.

### Type Check Matrix

| Type | `==` | `!=` | `>` | `<` | `>=` | `<=` | `contains` |
|------|------|------|-----|-----|------|------|------------|
| string | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| int | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | — |
| float | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | — |
| bool | ✓ | ✓ | — | — | — | — | — |
| object | ✓ | ✓ | — | — | — | — | — |
| array | — | — | — | — | — | — | ✓ |
| null | ✓ | ✓ | — | — | — | — | — |
| any | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| enum | ✓ | ✓ | — | — | — | — | — |

## Use Cases

| Pattern | Example |
|---------|---------|
| API input validation | Ensure required fields before processing |
| Type-safe routing | Gate on typed fields for safe comparisons |
| Contract enforcement | Validate payloads match service expectations |
| Data quality gates | Reject malformed data early |
| Enum constraints | Restrict status fields to valid values |
