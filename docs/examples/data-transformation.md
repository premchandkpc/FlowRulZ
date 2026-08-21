# Data Transformation

Map expressions, JMESPath, built-in functions, payload reshaping.

## Bytecode DSL

### Basic Map

```
m:{status: "processed", timestamp: now()}
```

Replace the entire payload with a new object.

### Map with JMESPath

```
m:{user_id: user.id, total: items[*].price | sum(@)}
```

Extract and compute fields from the current payload using JMESPath expressions.

### Map Chaining

```
n:fetch | m:{data: @, source: "api"} | n:store
```

Fetch data, wrap it in a new structure, then store.

## Built-in Functions

### String Functions

```
m:{name: lower(@.name)}
m:{name: upper(@.name)}
m:{name: trim(@.name)}
m:{len: length(@.items)}
m:{part: substring(@.text, 0, 10)}
m:{clean: replace(@.text, "foo", "bar")}
m:{parts: split(@.csv, ",")}
m:{combined: concat(@.first, @.last)}
```

### Numeric Functions

```
m:{abs_val: abs(@.delta)}
m:{rounded: round(@.price)}
m:{ceiled: ceil(@.percentage)}
m:{floored: floor(@.score)}
m:{smallest: min(@.a, @.b)}
m:{largest: max(@.a, @.b)}
```

### Type Conversion

```
m:{id: to_string(@.user_id)}
m:{count: parse_int(@.count_str)}
m:{rate: parse_float(@.rate_str)}
m:{flag: parse_bool(@.flag_str)}
m:{type: typeof(@.value)}
```

### Encoding

```
m:{encoded: base64(@.data)}
m:{decoded: base64_decode(@.b64)}
m:{hashed: hash(@.password)}
m:{json_str: json(@.object)}
```

### Object/Array

```
m:{keys: keys(@.config)}
m:{merged: merge(@.defaults, @.overrides)}
m:{val: coalesce(@.primary, @.fallback)}
m:{val: default(@.missing, "N/A")}
```

### Utility

```
m:{id: uuid()}
m:{ts: now()}
m:{epoch: epoch()}
```

## Flow DSL

### With Variables

```
version 1

flow DataEnrichment

variables
    enriched_data object = {}

service fetcher
    type http
    url https://api.internal/data

service processor
    type grpc
    address processor:50051

workflow

Start

-> fetcher.GetData

-> processor.Transform

-> Return enriched_data
```

### Transformation Patterns

**Reshape payload:**
```
m:{user: @.user, order_total: @.items[*].price | sum(@), processed_at: now()}
```

**Filter array:**
```
m:{active_items: @.items[?status == "active"]}
```

**Flatten nested:**
```
m:{user_id: @.user.id, user_name: @.user.name, order_id: @.order.id}
```

**Add metadata:**
```
m:{data: @, metadata: {source: "api", timestamp: now(), version: "1.0"}}
```

## Complete Examples

### Order Enrichment Pipeline

```
n:fetch-order | m:{order: @, enriched: true, timestamp: now()} | n:store
```

### User Profile Transform

```
n:fetch-user | m:{id: @.id, display_name: concat(@.first_name, " ", @.last_name), email: lower(@.email)} | n:save
```

### Analytics Aggregation

```
n:fetch-events | m:{count: length(@.events), total: @.events[*].amount | sum(@), avg: @.events[*].amount | avg(@)} | n:store-analytics
```

### Data Normalization

```
m:{id: to_string(@.legacy_id), status: lower(@.Status), created: @.create_date, tags: split(@.tag_string, ",")}
```

## JMESPath Reference

JMESPath is used inside `m:` expressions for complex data extraction:

| Expression | Description |
|------------|-------------|
| `@` | Current element |
| `@.field` | Object field access |
| `@.a.b.c` | Nested field |
| `@[*]` | All array elements |
| `@.items[*].id` | Pluck field from array |
| `@.items[?status=="active"]` | Filter array |
| `@.items \| length(@)` | Pipe to function |
| `@.items[*].price \| sum(@)` | Sum array |
| `@.items \| sort_by(@, &price)` | Sort array |
| `@.items \| reverse(@)` | Reverse array |
| `@.a \|\| @.b` | Default/fallback |
| `keys(@)` | Object keys |
| `values(@)` | Object values |
| `merge(@.a, @.b)` | Merge objects |
| `type(@)` | Get type |
| `length(@)` | Length |
| `abs(@)` | Absolute value |
| `avg(@)` | Average |
| `ceil(@)` | Ceiling |
| `floor(@)` | Floor |
| `min(@)` | Minimum |
| `max(@)` | Maximum |
| `sum(@)` | Sum |
| `to_string(@)` | Convert to string |
| `to_number(@)` | Convert to number |
