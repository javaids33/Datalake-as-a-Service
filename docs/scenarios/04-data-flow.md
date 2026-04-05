# How Does Data Actually Flow?

## The Question

"Walk me through concrete examples. When a developer runs a query, what actually happens under the hood?"

## Architecture Overview

Every query enters through the sidecar's API (`localhost:8765`), hits the query router, and is dispatched to the appropriate engine. The router uses sqlglot to parse the SQL AST, extract table references, and compare them against the team's owned namespaces and S3 buckets.

For the full architecture diagram, see [docs/images/architecture-overview.svg](../images/architecture-overview.svg).
For routing decision logic, see [docs/images/routing-decision.svg](../images/routing-decision.svg).

---

## Example A: Developer Reads a Local CSV

**Scenario**: A developer has a CSV file mounted into their pod at `/data/user_events.csv` and wants to explore it.

```sql
SELECT event_type, COUNT(*) as cnt
FROM read_csv_auto('/data/user_events.csv')
GROUP BY event_type
ORDER BY cnt DESC
LIMIT 20
```

### What Happens

```
Developer's code
    |
    v
localhost:8765/query  (FastAPI endpoint)
    |
    v
Query Router
    |-- sqlglot parses SQL
    |-- Detects read_csv_auto() function call
    |-- Routing reason: LOCAL_FILE
    |-- Engine: DUCKDB
    |
    v
DuckDB (in-process, in-pod)
    |-- Reads CSV from local filesystem
    |-- Executes GROUP BY in memory
    |-- Returns result
    |
    v
JSON response to developer
```

**Network hops**: Zero. Everything happens inside the pod.
**Latency**: Milliseconds for a typical CSV file.
**Governance**: Audit event emitted with `engine: duckdb`, `routing_reason: local_file`. Row filter injected if configured.

---

## Example B: Analyst Queries Their Team's S3 Bucket

**Scenario**: An analyst on the payments team queries transaction data stored as Parquet in their team's S3 bucket.

```sql
SELECT date_trunc('month', created_at) AS month,
       SUM(amount) AS total_amount,
       COUNT(*) AS transaction_count
FROM payments.transactions
WHERE created_at >= '2026-01-01'
GROUP BY 1
ORDER BY 1
```

### What Happens

```
Analyst's notebook
    |
    v
localhost:8765/query
    |
    v
Query Router
    |-- sqlglot parses SQL
    |-- Extracts table ref: payments.transactions
    |-- Checks DLS_OWNED_NAMESPACES: {"payments"}
    |-- payments.transactions IS owned
    |-- Estimated scan < DLS_DIRECT_SCAN_THRESHOLD
    |-- Routing reason: OWNED_NAMESPACE
    |-- Engine: DUCKDB
    |
    v
DuckDB (in-process)
    |-- Connects to S3 via IRSA credentials
    |-- Reads Parquet files from s3://payments-data/transactions/
    |-- Applies partition pruning (created_at >= 2026-01-01)
    |-- Executes aggregation in memory
    |-- Returns result
    |
    v
JSON response to analyst
```

**Network hops**: One (DuckDB to S3 within the same AWS region).
**Latency**: Seconds, depending on data volume and partition pruning effectiveness.
**Governance**: Audit event with `tables_owned: ["payments.transactions"]`. Row filter injected if `DLS_ROW_FILTER` is set.

---

## Example C: Data Scientist Joins Own Data with Finance Team's Data

**Scenario**: A data scientist on the payments team needs to join their transaction data with the finance team's account data to build a feature for a model.

```sql
SELECT p.transaction_id,
       p.amount,
       f.account_type,
       f.risk_score
FROM payments.transactions p
JOIN finance.accounts f ON p.account_id = f.id
WHERE p.created_at >= '2026-03-01'
```

### What Happens

```
Data scientist's notebook
    |
    v
localhost:8765/query
    |
    v
Query Router
    |-- sqlglot parses SQL
    |-- Extracts table refs:
    |     payments.transactions  --> OWNED
    |     finance.accounts       --> NOT OWNED (different team)
    |-- Mixed ownership detected
    |-- Routing reason: CROSS_TEAM_REF
    |-- Engine: HYBRID
    |-- Join strategy: HYBRID_PULL
    |
    v
Starburst (distributed cluster)              DuckDB (in-pod)
    |-- Receives subquery for                    |
    |   finance.accounts                         |-- Reads payments.transactions
    |-- JWT validated                            |   from S3 via IRSA
    |-- TLS connection                           |
    |-- Returns finance data                     |
    |   to DuckDB                                |
    +--------------------------------------------+
                        |
                   DuckDB joins locally
                        |
                        v
               JSON response to data scientist
```

**Network hops**: Two (DuckDB to Starburst, DuckDB to S3).
**Latency**: Seconds to tens of seconds, depending on the size of the finance dataset.
**Governance**: Audit event with `tables_owned: ["payments.transactions"]` and `tables_unowned: ["finance.accounts"]`. The `dlsidecar_cross_team_queries_total` metric is incremented. The finance team's data is accessed only through Starburst's governed layer.

For the data flow diagram, see [docs/images/data-flow.svg](../images/data-flow.svg).

---

## Example D: Heavy GROUP BY on 500GB Table

**Scenario**: An analyst needs to run a complex aggregation over a large historical dataset.

```sql
SELECT region,
       product_category,
       date_trunc('week', order_date) AS week,
       SUM(revenue) AS total_revenue,
       COUNT(DISTINCT customer_id) AS unique_customers,
       AVG(order_value) AS avg_order_value
FROM sales.orders
GROUP BY GROUPING SETS (
    (region, product_category, date_trunc('week', order_date)),
    (region, product_category),
    (region),
    ()
)
```

### What Happens

```
Analyst's query tool
    |
    v
localhost:8765/query
    |
    v
Query Router
    |-- sqlglot parses SQL
    |-- Detects GROUPING SETS (heavy compute pattern)
    |-- Estimated bytes: 500GB (exceeds DLS_DIRECT_SCAN_THRESHOLD)
    |-- Routing reason: HEAVY_COMPUTE + SIZE_THRESHOLD
    |-- Engine: STARBURST
    |-- Join strategy: STARBURST_OFFLOAD
    |-- offload_to_starburst: true
    |
    v
DuckDB relays query to Starburst
    |
    v
Starburst (distributed cluster)
    |-- Coordinator plans the query
    |-- Workers scan sales.orders in parallel
    |-- Distributed aggregation across workers
    |-- Returns result to DuckDB
    |
    v
DuckDB receives result, caches it, returns to caller
    |
    v
JSON response to analyst
```

**Network hops**: Two (DuckDB to Starburst, Starburst to S3).
**Latency**: Tens of seconds to minutes, depending on cluster size and data volume.
**Why not DuckDB?**: A 500GB scan with GROUPING SETS would exhaust a 4Gi DuckDB memory allocation. Starburst distributes this across multiple workers with spill-to-disk capability.
**Governance**: Full audit event. DuckDB can cache the result for subsequent queries within the cache TTL.

---

## Example E: Streaming Ingest from Kafka

**Scenario**: A service produces events to Kafka. The team wants those events to land in their Iceberg table for analytics, without building a separate Spark Streaming job.

### What Happens

```
Kafka topic: payments.events
    |
    v
dlsidecar Kafka bridge (streaming/kafka_bridge.py)
    |-- Consumes messages from topic
    |-- Deserializes Avro/JSON records
    |-- Buffers in DuckDB in-memory table
    |
    v
DuckDB (in-process)
    |-- Accumulates records in memory
    |-- When buffer reaches threshold (row count or time):
    |     |
    |     v
    |   Iceberg writer (iceberg/manager.py)
    |     |-- Converts DuckDB result to Arrow
    |     |-- Appends to Iceberg table
    |     |-- Commits new snapshot
    |     |-- Registers with HMS/Glue catalog
    |
    v
S3 (Parquet data files)
    |
    v
Starburst can now query the same table
(HMS registration makes it visible automatically)
```

**Data path**: Kafka --> DuckDB buffer --> Iceberg --> S3.
**Latency**: Near-real-time (buffer flush interval, typically seconds to minutes).
**What this replaces**: A dedicated Spark Structured Streaming job, which requires its own cluster, checkpoint management, and monitoring.
**Governance**: Every flush produces an audit event. Iceberg maintenance (compaction, snapshot expiry) runs on a schedule to prevent small file accumulation.

---

## Summary: Which Engine Handles What

| Scenario | Engine | Routing Reason | Network Hops |
|---|---|---|---|
| Local file (CSV, Parquet, Arrow) | DuckDB | `local_file` | 0 |
| Team's own S3 bucket, small scan | DuckDB | `owned_namespace` | 1 (S3) |
| Cross-team join | Hybrid | `cross_team_ref` | 2 (S3 + Starburst) |
| Heavy aggregation, large scan | Starburst | `heavy_compute` / `size_threshold` | 2 (Starburst + S3) |
| Streaming ingest | DuckDB + Iceberg | N/A (write path) | 1 (S3) |
| Explicit hint `/*+ ENGINE(starburst) */` | Starburst | `explicit_hint` | 2 (Starburst + S3) |

The routing decision is transparent: the `dlsidecar_routing_decisions_total` Prometheus metric tracks every decision by path and reason, and the audit log includes the full routing rationale for every query.
