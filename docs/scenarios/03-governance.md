# How Do We Maintain Governance at Scale?

## The Question

"If every team has its own sidecar, how do we ensure data access is governed, audited, and compliant? How do we prevent teams from accessing data they should not see?"

## The Short Answer

The sidecar is not a way around governance — it **is** the governance layer. Every query passes through the sidecar's routing engine, which parses the SQL, injects security filters, logs a structured audit event, and enforces the routing boundary that separates local access from cross-team access.

---

## 1. Audit Log: Every Query Logged as Structured JSON

Every query executed through dlsidecar — whether it runs on DuckDB or is forwarded to Starburst — produces a structured JSON audit event. There are no silent queries.

### What Gets Logged

| Field | Description |
|---|---|
| `timestamp` | ISO 8601 timestamp of query execution |
| `tenant_id` | The team's identifier (`DLS_TENANT_ID`) |
| `sql` | The original SQL statement |
| `engine` | Which engine executed it: `duckdb`, `starburst`, or `hybrid` |
| `routing_reason` | Why the router chose that engine (e.g., `owned_namespace`, `cross_team_ref`, `heavy_compute`) |
| `tables_accessed` | List of table references extracted from the SQL AST |
| `tables_owned` | Subset of accessed tables that belong to the team |
| `tables_unowned` | Subset that required cross-team access via Starburst |
| `rows_scanned` | Number of rows scanned |
| `bytes_scanned` | Number of bytes scanned |
| `duration_ms` | Query execution time |
| `status` | Success or failure |
| `row_filter_applied` | Whether row-level security was injected |

### Where Audit Events Go

Audit events are emitted to stdout as structured JSON, following Kubernetes logging conventions. From there, they can be collected by any log aggregation system — Fluent Bit, Datadog, Splunk, CloudWatch Logs, or an ELK stack.

Example audit event:

```json
{
  "event": "query_executed",
  "timestamp": "2026-04-05T14:23:17.442Z",
  "tenant_id": "team-payments",
  "engine": "starburst",
  "routing_reason": "cross_team_ref",
  "sql": "SELECT p.id, f.balance FROM payments.transactions p JOIN finance.accounts f ON p.account_id = f.id",
  "tables_accessed": ["payments.transactions", "finance.accounts"],
  "tables_owned": ["payments.transactions"],
  "tables_unowned": ["finance.accounts"],
  "rows_scanned": 142857,
  "bytes_scanned": 58720256,
  "duration_ms": 2340,
  "status": "success",
  "row_filter_applied": true
}
```

---

## 2. Row-Level Security: Injected via AST Transform

When `DLS_ROW_FILTER` is configured (e.g., `tenant_id = 'team-alpha'`), every DuckDB query has the row-level predicate injected into the SQL AST before execution. This is not string concatenation — it is a proper AST transformation using sqlglot.

### How It Works

1. The SQL is parsed into an AST by sqlglot
2. Every `FROM` clause referencing a real table is wrapped in a filtered subquery
3. The rewritten SQL is executed

**Before injection:**
```sql
SELECT * FROM events WHERE event_type = 'purchase'
```

**After injection:**
```sql
SELECT * FROM (
  SELECT * FROM events WHERE tenant_id = 'team-alpha'
) AS _governed
WHERE event_type = 'purchase'
```

### What It Handles

The `RowFilterInjector` correctly processes:

- Simple `SELECT` statements
- CTEs (`WITH` clauses)
- Subqueries
- JOINs (filter applied to each table independently)
- UNION / INTERSECT / EXCEPT
- Nested subqueries

### For Starburst Queries

When a query is forwarded to Starburst, row-level security is enforced by passing the tenant context as a session property rather than rewriting the SQL. Starburst's own access control layer handles enforcement, and the audit log records that the filter was applied.

---

## 3. Cross-Team Access: Forced Through Starburst

This is the core governance mechanism: **the routing boundary is the security boundary.**

The query router classifies every table reference as either "owned" or "unowned" by comparing it against the team's `DLS_OWNED_NAMESPACES` and `DLS_S3_BUCKETS` configuration.

| Table Location | Engine | Access Method |
|---|---|---|
| Team's own namespace | DuckDB | Direct access via IRSA, maximum speed |
| Team's own S3 bucket | DuckDB | Direct access via IRSA |
| Another team's namespace | Starburst | JWT-authenticated, TLS-encrypted, audited |
| Another team's S3 bucket | Starburst | Governed access through Starburst connector |

A team **cannot** query another team's data through DuckDB. The router will not allow it. If the SQL references a table outside the team's owned namespaces, the query is either:

- Forwarded entirely to Starburst (if all tables are unowned)
- Split into a hybrid execution (Starburst resolves foreign data, DuckDB joins locally with owned data)

This means that cross-team access is always:

1. **Authenticated** — JWT token validated by Starburst
2. **Encrypted** — TLS between sidecar and Starburst cluster
3. **Audited** — audit event includes `tables_unowned` field
4. **Metered** — `dlsidecar_cross_team_queries_total` Prometheus counter incremented

---

## 4. Prometheus Metrics: 30+ Metrics for Observability

dlsidecar exposes a comprehensive set of Prometheus metrics on its `/metrics` endpoint. These are designed for both operational monitoring and governance dashboards.

### Query Metrics

| Metric | Type | Labels | Purpose |
|---|---|---|---|
| `dlsidecar_queries_total` | Counter | engine, status, tenant | Total query count by engine and outcome |
| `dlsidecar_query_duration_seconds` | Histogram | engine, tenant | Query latency distribution |
| `dlsidecar_rows_scanned_total` | Counter | engine, tenant | Data access volume |
| `dlsidecar_bytes_scanned_total` | Counter | engine, tenant | Data access volume in bytes |
| `dlsidecar_routing_decisions_total` | Counter | path, reason | Routing decision breakdown |

### Governance Metrics

| Metric | Type | Labels | Purpose |
|---|---|---|---|
| `dlsidecar_cross_team_queries_total` | Counter | tenant, target_catalog | Cross-team access tracking |
| `dlsidecar_governance_blocks_total` | Counter | reason | Blocked unauthorized access attempts |
| `dlsidecar_schema_evolutions_total` | Counter | table | Schema change tracking |

### Cache and Performance

| Metric | Type | Labels | Purpose |
|---|---|---|---|
| `dlsidecar_query_cache_hits_total` | Counter | tenant | Cache effectiveness |
| `dlsidecar_query_cache_misses_total` | Counter | tenant | Cache miss rate |
| `dlsidecar_duckdb_memory_bytes_gauge` | Gauge | - | DuckDB memory consumption |

### Iceberg Maintenance

| Metric | Type | Labels | Purpose |
|---|---|---|---|
| `dlsidecar_iceberg_compaction_runs_total` | Counter | table, status | Compaction activity |
| `dlsidecar_iceberg_files_compacted_total` | Counter | table | Files merged |
| `dlsidecar_iceberg_snapshots_expired_total` | Counter | table | Snapshot cleanup |
| `dlsidecar_iceberg_orphans_deleted_total` | Counter | table | Orphan file cleanup |
| `dlsidecar_iceberg_small_files_gauge` | Gauge | table | Small file accumulation |

### Connection Health

| Metric | Type | Labels | Purpose |
|---|---|---|---|
| `dlsidecar_connection_status` | Gauge | source | Health of external connections |
| `dlsidecar_connection_reconnects_total` | Counter | source | Reconnection frequency |
| `dlsidecar_jwt_expiry_seconds_gauge` | Gauge | - | Time until JWT token expires |

---

## 5. The Routing Boundary IS the Governance Boundary

This is the key architectural insight for executives evaluating governance risk:

```
+-----------------------------+       +-----------------------------+
|  Team's Pod                 |       |  Starburst Cluster          |
|                             |       |                             |
|  App  -->  dlsidecar        |       |  Coordinator --> Workers    |
|              |               |       |                             |
|         Query Router         |       |  JWT validation             |
|           /       \          |       |  TLS encryption             |
|     DuckDB     Starburst ---------> |  Row-level security         |
|     (owned)    (cross-team)  |       |  Audit logging              |
+-----------------------------+       +-----------------------------+
```

- If the data is owned by the team, DuckDB handles it locally. Row filters are injected via AST transform. Audit events are emitted.
- If the data belongs to another team, the query must go through Starburst. There is no alternative path. Starburst enforces its own authentication, authorization, and audit on top of what the sidecar already provides.

There is no configuration that allows a team to bypass this boundary. The owned namespaces are set at deployment time via `DLS_OWNED_NAMESPACES` and cannot be changed at query time.

---

## Governance Checklist for Compliance Teams

| Requirement | How dlsidecar Addresses It |
|---|---|
| All data access must be logged | Structured JSON audit events for every query |
| Row-level access control | `DLS_ROW_FILTER` injected via sqlglot AST transform |
| Cross-team access requires authorization | Forced through Starburst with JWT + TLS |
| Data access must be metered | 30+ Prometheus metrics with tenant labels |
| No unauthorized schema changes | Schema evolution events tracked via metrics |
| Encryption in transit | TLS between sidecar and Starburst; localhost for DuckDB (no network) |
| Separation of duties | Teams own their namespace; cannot access other namespaces without Starburst |
