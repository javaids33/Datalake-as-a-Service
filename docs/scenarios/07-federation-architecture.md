# How Does Cross-Team Federation Work?

## The Question

"We have 15 teams. Sometimes they need to query each other's data. How does dlsidecar handle that without creating a security nightmare?"

## The Short Answer

Each team's DuckDB owns their namespace and provides direct, fast access to their own data. Starburst owns the cross-team gateway and enforces governance on every federated query. **No team can accidentally query another team's data without going through Starburst.** This is enforced architecturally, not by policy.

---

## The Two-Engine Model

The federation architecture rests on a clear division of responsibility between two engines:

| Engine | Owns | Access Model | Speed | Governance |
|---|---|---|---|---|
| DuckDB (in-pod) | Team's own namespace | Direct (IRSA to S3, local files) | Maximum — zero network hop for local, one hop for S3 | Row filter injection, audit logging |
| Starburst (shared cluster) | Cross-team gateway | Federated (JWT + TLS) | Distributed — multi-worker parallelism | JWT auth, TLS, row-level security, full audit trail |

This is not a preference or a configuration option. It is enforced by the query router at the AST level.

---

## How the Router Enforces the Boundary

When a query arrives, the router:

1. **Parses** the SQL into an AST using sqlglot
2. **Extracts** every table reference (catalog.schema.table)
3. **Classifies** each table as owned or unowned by checking against `DLS_OWNED_NAMESPACES` and `DLS_S3_BUCKETS`
4. **Routes** based on the classification:

```
All tables owned?
  YES --> DuckDB direct (PATH A)
  NO  --> Any tables owned?
            YES --> Hybrid: Starburst resolves foreign, DuckDB joins local (PATH C)
            NO  --> Starburst only (PATH B)
```

There is no PATH that allows DuckDB to directly access unowned namespaces. The router does not have a "bypass" mode. Even in dev mode, cross-team references are rejected unless Starburst is enabled.

### Example: What Happens When a Team Tries to Access Another Team's Data

```sql
-- Run by team-payments sidecar
SELECT * FROM finance.accounts WHERE id = 42
```

The router's analysis:

```
Table reference: finance.accounts
Owned namespaces: {payments}
Is 'finance' in owned namespaces? NO
--> Routing decision: STARBURST (reason: cross_team_ref)
```

If Starburst is not enabled (`DLS_STARBURST_ENABLED=false`), this query **fails with an error**. It does not fall back to DuckDB. The team must enable Starburst and have valid JWT credentials to access cross-team data.

---

## Federation Patterns

### Pattern 1: Simple Cross-Team Read

Team payments queries finance team's data.

```sql
SELECT account_type, COUNT(*)
FROM finance.accounts
GROUP BY account_type
```

**Execution**: Entire query forwarded to Starburst. Result returned to DuckDB and relayed to the caller.

```
payments pod                    Starburst cluster
+-----------+                   +-----------------+
| dlsidecar | -- JWT + TLS ---> | finance.accounts|
|           | <-- results ----- | (governed)      |
+-----------+                   +-----------------+
```

### Pattern 2: Hybrid Join (Own Data + Foreign Data)

Team payments joins their transactions with finance team's accounts.

```sql
SELECT p.transaction_id, p.amount, f.account_type
FROM payments.transactions p
JOIN finance.accounts f ON p.account_id = f.id
WHERE p.created_at >= '2026-01-01'
```

**Execution**: The router detects mixed ownership (payments = owned, finance = unowned) and uses HYBRID_PULL strategy.

```
payments pod                           Starburst cluster
+-------------------+                  +------------------+
| dlsidecar         |                  |                  |
|                   |                  |                  |
| DuckDB reads      |                  | Resolves         |
| payments.         |                  | finance.accounts |
| transactions      |  subquery for    | via governed     |
| from S3 via IRSA  |  finance data    | connector        |
|                   | ---------------> |                  |
|                   | <-- results ---- |                  |
|                   |                  |                  |
| DuckDB joins      |                  |                  |
| locally           |                  |                  |
| (payments +       |                  |                  |
|  finance results) |                  |                  |
+-------------------+                  +------------------+
```

**Why hybrid?** DuckDB can scan `payments.transactions` faster than Starburst (direct IRSA, no coordinator overhead). Only the foreign data needs to go through Starburst. DuckDB performs the final join locally.

### Pattern 3: Multi-Team Federation

A data platform team queries across three teams' data.

```sql
SELECT r.region,
       SUM(p.amount) AS payment_volume,
       COUNT(DISTINCT o.order_id) AS order_count,
       AVG(f.risk_score) AS avg_risk
FROM payments.transactions p
JOIN orders.order_lines o ON p.order_id = o.order_id
JOIN finance.risk_scores f ON p.account_id = f.account_id
JOIN reference.regions r ON o.region_code = r.code
WHERE p.created_at >= '2026-01-01'
GROUP BY r.region
```

**Execution**: Multiple unowned namespaces detected. Entire query pushed to Starburst, which has connectors to all catalogs and enforces access policies for each.

```
platform pod                    Starburst cluster
+-----------+                   +-------------------+
| dlsidecar | -- full query --> | payments catalog  |
|           |                   | orders catalog    |
|           |                   | finance catalog   |
|           |                   | reference catalog |
|           | <-- results ----- | (distributed join)|
+-----------+                   +-------------------+
```

### Pattern 4: Explicit Engine Hint

A developer knows their query is complex and wants to force Starburst execution, even though all tables are owned.

```sql
/*+ ENGINE(starburst) */
SELECT date_trunc('month', created_at) AS month,
       product_category,
       SUM(amount) AS total,
       NTILE(100) OVER (PARTITION BY product_category ORDER BY amount) AS percentile
FROM payments.transactions
WHERE created_at >= '2025-01-01'
```

**Execution**: The `/*+ ENGINE(starburst) */` hint overrides the router's default decision. The query is forwarded to Starburst regardless of ownership.

---

## Security Properties of the Federation Boundary

### What Is Enforced

| Property | How It Is Enforced |
|---|---|
| Team cannot read another team's S3 bucket directly | IRSA limits S3 access to the pod's service account role; DuckDB cannot assume another team's role |
| Team cannot query another team's namespace via DuckDB | Query router rejects unowned table references for DuckDB execution |
| Cross-team access requires valid JWT | Starburst validates JWT on every connection; expired or invalid tokens are rejected |
| Cross-team access is encrypted | TLS between sidecar and Starburst (configurable via `DLS_STARBURST_TLS_ENABLED`) |
| Cross-team access is audited | Both the sidecar and Starburst emit audit events; `dlsidecar_cross_team_queries_total` metric incremented |
| Row-level filters apply to cross-team data | Starburst enforces row-level security via session properties |

### What Cannot Happen

- A team cannot configure `DLS_OWNED_NAMESPACES` to include another team's namespace. Namespace ownership is set at deployment time and validated by the platform team's Helm chart review.
- A team cannot bypass the router. All queries go through the sidecar's API layer, which always invokes the router before execution.
- A team cannot query Starburst without a valid JWT. Token paths are mounted from Kubernetes secrets managed by the platform team.
- DuckDB cannot connect to another team's S3 bucket. IRSA binds the pod's service account to specific S3 bucket permissions.

---

## Architectural Diagram

```
Team A Pod                    Team B Pod                    Team C Pod
+------------+               +------------+               +------------+
| App        |               | App        |               | App        |
| dlsidecar  |               | dlsidecar  |               | dlsidecar  |
|   DuckDB   |               |   DuckDB   |               |   DuckDB   |
|   (owns    |               |   (owns    |               |   (owns    |
|    ns: A)  |               |    ns: B)  |               |    ns: C)  |
+-----+------+               +-----+------+               +-----+------+
      |                             |                             |
      | cross-team                  | cross-team                  | cross-team
      | queries only                | queries only                | queries only
      |                             |                             |
      +----------+    +------------+-------------+    +-----------+
                 |    |                          |    |
                 v    v                          v    v
              +-----------------------------------+
              |        Starburst Cluster           |
              |                                    |
              |  JWT validation per connection      |
              |  TLS encryption                    |
              |  Row-level security                |
              |  Full audit trail                  |
              |                                    |
              |  Catalogs:                         |
              |    A (team A data)                  |
              |    B (team B data)                  |
              |    C (team C data)                  |
              |    shared (reference data)          |
              +-----------------------------------+
              |              |              |
              v              v              v
           S3 (A)        S3 (B)        S3 (C)
```

Each team's DuckDB has direct, fast access to its own S3 bucket. Cross-team access is funneled through the single governed gateway. The routing boundary enforced by the query router is identical to the security boundary enforced by IRSA and Starburst's access control.

---

## Decision Framework: When to Use Each Path

| Scenario | Engine | Why |
|---|---|---|
| Read my own data, small scan | DuckDB | Fastest path, zero overhead |
| Read my own data, large scan | Starburst (offload) | Distributed compute, spill to disk |
| Read another team's data | Starburst | Only valid path; governed access |
| Join my data with another team's | Hybrid | DuckDB scans owned data fast; Starburst resolves foreign data |
| Ad-hoc exploration of a local file | DuckDB | No catalog needed, just `read_parquet()` |
| Cross-team reporting dashboard | Starburst | Consistent governed access for recurring queries |
| Data sharing between teams | Starburst + Iceberg catalog | Write to Iceberg, register in HMS, visible to all via Starburst |

---

## Frequently Asked Questions

**Q: What if two teams need to share a dataset frequently?**
A: The producing team writes to an Iceberg table and registers it in the shared catalog (HMS/Glue). The consuming team queries it through Starburst. No data copying, no ETL pipeline. The Iceberg table is the interface.

**Q: Can a team opt out of the federation boundary?**
A: No. The router enforces the boundary at the AST level. There is no configuration flag to disable it. This is by design — the routing boundary IS the security boundary.

**Q: What about reference data that all teams need?**
A: Create a shared namespace (e.g., `reference`) and register it as a Starburst catalog. All teams access it through Starburst. Alternatively, if the reference data is small, each team can maintain a local copy in their own namespace.

**Q: How does this interact with Starburst's own RBAC?**
A: They are complementary. The sidecar enforces which queries go through Starburst (routing boundary). Starburst enforces which data a given JWT identity can access (authorization boundary). Both layers must pass for a query to succeed.
