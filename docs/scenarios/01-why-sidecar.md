# Why a Sidecar Instead of a Shared Service?

## The Question

"We already have shared data infrastructure. Why deploy a sidecar into every pod instead of routing everything through a central service?"

## The Short Answer

A sidecar gives each team a **local data engine** (DuckDB) that handles lightweight work at zero latency, while still federating heavy or cross-team queries through a **shared distributed engine** (Starburst). You get the speed of local compute and the governance of a centralized platform — without forcing every query through a network hop.

---

## Cost: No Dedicated Cluster Per Team

With a traditional approach, each team that needs analytics capability ends up with one of two expensive options:

| Approach | What You Pay For |
|---|---|
| Dedicated Spark/Trino cluster per team | Idle compute 90% of the time, ops burden per cluster |
| Shared query service with a queue | Central bottleneck, cross-team noisy-neighbor problems |
| **dlsidecar** | DuckDB uses pod memory you already allocated; Starburst cluster is shared and amortized |

The sidecar adds **zero new infrastructure** for lightweight work. DuckDB runs inside the pod's existing memory allocation (256Mi to 4Gi depending on workload). The shared Starburst cluster only handles queries that genuinely need distributed compute — cross-team joins, heavy aggregations over large datasets, governed federation. That cluster's cost is amortized across every team in the organization.

There is no per-team cluster to provision, no per-team cluster to keep patched, and no per-team cluster sitting idle overnight.

---

## Latency: Zero Network Hop for Local Work

When a developer reads a local CSV, queries their own team's S3 bucket, or transforms data for a notebook, the query never leaves the pod:

```
Developer's code  -->  localhost:8765  -->  DuckDB (in-pod)  -->  result
```

There is no load balancer, no service mesh routing, no queue. DuckDB processes the query in-memory and returns the result over a local socket. For typical data prep tasks — reading Parquet files, running transforms, caching intermediate results — this means **sub-millisecond overhead** compared to calling DuckDB directly.

Only queries that the router identifies as needing distributed compute (cross-team data, heavy analytics, large scans) are forwarded to Starburst. The routing decision is made by parsing the SQL AST with sqlglot and checking table references against the team's owned namespaces and S3 buckets.

---

## Governance: Every Query Audited, Row-Level Filters Injected

A sidecar is not a bypass of governance — it **is** the governance boundary.

Every query that passes through dlsidecar is:

1. **Parsed** — sqlglot extracts table references and query structure
2. **Routed** — the query router classifies the query (local, distributed, or hybrid)
3. **Filtered** — if `DLS_ROW_FILTER` is configured, row-level predicates are injected into the SQL AST before execution
4. **Audited** — a structured JSON audit event is emitted for every query, including the routing decision, tables accessed, engine used, and execution time
5. **Metered** — Prometheus metrics track queries by engine, tenant, status, bytes scanned, and routing path

Because the sidecar sits in the data path for every query, there is no way to access data without going through it. Cross-team queries are forced through Starburst, which adds JWT authentication and TLS encryption on top of the sidecar's own audit trail.

The routing boundary IS the governance boundary. If a query references a table outside the team's owned namespaces, it cannot be executed locally — it must go through Starburst's governed access layer.

---

## Developer Experience: localhost:8765, No VPN, No Tickets

From a developer's perspective, dlsidecar looks like a local database:

```bash
# Connect from any tool that speaks SQL
curl -X POST http://localhost:8765/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM read_parquet(\"/data/events.parquet\") LIMIT 10"}'
```

There is no VPN to connect through, no service account to request, no ticket to file with a platform team. The sidecar is deployed as part of the team's own Kubernetes pod, configured with environment variables, and accessible on localhost.

For teams that need access to Starburst for cross-team queries, the sidecar handles authentication transparently — JWT tokens are managed internally, TLS certificates are mounted from Kubernetes secrets, and connection management is handled by DuckDB's Trino extension.

### What This Replaces

| Before dlsidecar | With dlsidecar |
|---|---|
| File a ticket to get Spark cluster access | `helm install dlsidecar` in your namespace |
| Wait for data engineering to build a pipeline | Query S3 directly via IRSA from your pod |
| VPN into a Trino dashboard to run ad-hoc queries | `curl localhost:8765/query` |
| Request cross-team data access through email chains | Query through Starburst with automatic JWT auth |
| Manually manage Parquet file compaction | Sidecar runs Iceberg maintenance on a schedule |

---

## When a Sidecar Is NOT the Right Answer

The sidecar model works best when:

- Teams need to query their own data (S3, local files, Iceberg tables) frequently
- Cross-team federation is needed but should be governed
- Developer velocity matters — teams should not wait for central platform changes

The sidecar model is not ideal when:

- A single central team processes all queries on behalf of the organization (use Starburst directly)
- Workloads are exclusively batch ETL with no interactive component (use Spark/Airflow)
- The organization has fewer than 3-4 teams that need independent data access
