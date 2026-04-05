# What Does This Cost?

## The Question

"Give me a breakdown. What are the line items, and how does it compare to what we are paying today?"

## Cost Components

dlsidecar has four cost dimensions. Three of them are near-zero marginal cost for most teams.

---

### 1. DuckDB: Pod Memory Only

DuckDB is an embedded, open-source, in-process database. There is no license fee, no per-query charge, and no separate infrastructure.

| Resource | Typical Allocation | Notes |
|---|---|---|
| Memory | 256Mi (dev) to 4Gi (heavy analytics) | Comes from the pod's existing resource request |
| CPU | Shares pod CPU | DuckDB uses available threads; configurable via `DLS_DUCKDB_THREADS` |
| Disk | Optional local SSD for spill | Only needed if queries exceed memory; uses pod ephemeral storage |

**Cost impact**: If your pod already requests 1Gi of memory, allocating 512Mi to DuckDB costs nothing incremental. You are paying for that memory whether DuckDB uses it or not.

For most development and lightweight analytics workloads, 512Mi is sufficient. Data science workloads that cache large intermediate results may need 2-4Gi.

---

### 2. Starburst: Shared Cluster, Amortized

Starburst (or Trino) runs as a shared cluster in the EKS environment. Its cost is fixed infrastructure that gets amortized across every team that uses it.

| Component | Typical Sizing | Cost Model |
|---|---|---|
| Coordinator | 1 node, 8 vCPU / 32Gi | Fixed cost, shared |
| Workers | 3-10 nodes, auto-scaling | Scales with aggregate query load |
| License (if Starburst Enterprise) | Per-node or per-query | Negotiated enterprise agreement |

**Key insight**: Without dlsidecar, each team that needs analytics either gets its own Starburst/Spark cluster (expensive, idle) or shares one cluster for all queries (bottleneck). With dlsidecar, the shared Starburst cluster only handles queries that genuinely require distributed compute — cross-team joins, heavy aggregations, governed access. Lightweight work stays in DuckDB and never touches Starburst.

In practice, organizations see **60-80% of queries handled entirely by DuckDB**, meaning the Starburst cluster can be sized significantly smaller than a "Starburst for everything" deployment.

---

### 3. S3: Same Cost Regardless

S3 costs (storage + API calls) are the same whether data is accessed by DuckDB via IRSA or by Starburst via its own credentials. The bytes stored and the GET requests made are identical.

| Access Path | S3 Cost |
|---|---|
| DuckDB reads team's S3 bucket via IRSA | Standard S3 pricing |
| Starburst reads S3 via its connector | Standard S3 pricing |
| dlsidecar writes Iceberg to S3 | Standard S3 pricing |

The only S3 cost difference is indirect: DuckDB's query cache (`dlsidecar_query_cache_hits_total` metric) can reduce repeated S3 reads. If the same query runs multiple times within the cache TTL, S3 GET requests are avoided entirely.

---

### 4. Sidecar Overhead

The dlsidecar container itself is lightweight:

| Resource | Value |
|---|---|
| Container image | ~120MB |
| Base memory footprint | ~50Mi (Python process + FastAPI + DuckDB) |
| CPU at idle | Negligible |
| Network | localhost only for DuckDB; egress to S3 and Starburst as needed |

---

## Comparison: dlsidecar vs Alternatives

The following table compares annual cost for a mid-size organization (10 teams, ~50 analysts/engineers, moderate query volume).

| Cost Dimension | dlsidecar | Dedicated Spark Cluster per Team | Snowflake |
|---|---|---|---|
| **Compute (lightweight queries)** | $0 (pod memory) | $150K-300K/yr (10 clusters, mostly idle) | Per-credit billing, often $200K+/yr |
| **Compute (heavy analytics)** | $40K-80K/yr (shared Starburst, 3-6 workers) | Included in cluster cost above | Included in credit billing |
| **Storage** | S3 standard pricing (~$23/TB/mo) | S3 + HDFS in some configs | Snowflake storage pricing (~$40/TB/mo) |
| **Licensing** | $0 (DuckDB is MIT-licensed) | Spark: $0; EMR: markup | Snowflake enterprise license |
| **Operations** | 1 Helm chart per team, shared Starburst cluster | 10 clusters to manage, patch, monitor | Managed, but vendor lock-in |
| **Onboarding time** | Minutes (Helm install + env vars) | Weeks (cluster provisioning, IAM, networking) | Days (account setup, role grants) |
| **Data gravity** | Data stays in your S3 buckets | Data stays in your S3/HDFS | Data must be loaded into Snowflake |

### Estimated Annual Total (10 teams)

| Solution | Estimated Annual Cost | Notes |
|---|---|---|
| **dlsidecar** | **$50K-100K** | Shared Starburst + S3; DuckDB is free |
| Dedicated Spark per team | $200K-400K | Most clusters idle 80%+ of the time |
| Snowflake | $250K-500K | Credit consumption scales with query volume |
| Shared Trino (no sidecar) | $80K-150K | All queries hit Trino; larger cluster needed |

---

## Where the Savings Come From

1. **No idle clusters**. DuckDB uses memory that is already allocated to the pod. There is no separate infrastructure to keep running.

2. **Smaller shared cluster**. Because DuckDB handles 60-80% of queries locally, the Starburst cluster serves fewer queries and can be sized smaller.

3. **No data movement**. Data stays in S3. There is no ETL pipeline to load data into a separate analytics warehouse. No Snowflake ingestion jobs, no EMR copy steps.

4. **No license fees for lightweight work**. DuckDB is MIT-licensed. You pay for Starburst only for the distributed compute you actually need.

5. **Self-service onboarding**. Teams onboard in minutes with a Helm install. There is no platform engineering ticket, no cluster provisioning, no IAM role chain to set up manually.

---

## How to Forecast Your Cost

To estimate dlsidecar cost for your organization:

1. **Count teams** that need data access. Each gets a sidecar (zero incremental compute cost).
2. **Estimate cross-team query volume**. This determines Starburst cluster sizing.
3. **Measure current S3 usage**. This cost does not change.
4. **Check existing Starburst/Trino licensing**. If you already have an enterprise agreement, the sidecar deployment adds no license cost.

The `dlsidecar_routing_decisions_total` Prometheus metric, labeled by `path` (duckdb, starburst, hybrid), gives you a live breakdown of where queries are executing — use this to right-size your Starburst cluster over time.
