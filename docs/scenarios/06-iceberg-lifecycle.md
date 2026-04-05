# Who Manages the Iceberg Tables?

## The Question

"Iceberg is great, but someone has to run compaction, expire snapshots, clean up orphan files. Are we going to need a separate Spark job for that? Who owns it?"

## The Short Answer

**The sidecar owns the lifecycle.** dlsidecar includes a built-in Iceberg maintenance scheduler that runs compaction, snapshot expiry, orphan cleanup, and manifest rewriting on cron schedules inside the pod. No separate Spark job, no Airflow DAG, no additional infrastructure.

---

## The Problem: Small File Explosion

Without maintenance, Iceberg tables degrade over time. Every write operation (append, overwrite, streaming flush) creates new data files and a new snapshot. In a streaming or frequent-write scenario, this leads to:

- **Hundreds of small Parquet files** instead of a few large ones (hurts scan performance)
- **Thousands of snapshots** that will never be read again (metadata bloat)
- **Orphan files** left behind by failed or retried writes (wasted storage)

Traditional solutions require running a separate Spark job on a schedule to perform maintenance. That means a separate cluster, a separate scheduler (Airflow, Step Functions), separate monitoring, and separate IAM permissions.

dlsidecar eliminates all of that.

---

## Maintenance Operations

### 1. Compaction

Compaction merges small data files into larger ones, targeting a configurable file size (default: 128MB via `DLS_ICEBERG_TARGET_FILE_SIZE_MB`).

**When it runs**: On a cron schedule configured via `DLS_ICEBERG_COMPACTION_CRON` (default: hourly).

**What it does**:

1. Scans the Iceberg table's metadata for data files below the target size
2. Reads the small files into DuckDB
3. Writes them back as fewer, larger Parquet files
4. Commits a new snapshot that replaces the old files with the compacted ones
5. Guards against compacting tables that are mid-write (`mark_writing` / `mark_write_complete` lock)

**Metrics emitted**:

| Metric | Description |
|---|---|
| `dlsidecar_iceberg_compaction_runs_total` | Compaction runs by table and status (success/failure) |
| `dlsidecar_iceberg_files_compacted_total` | Number of files merged |
| `dlsidecar_iceberg_small_files_gauge` | Current count of small files per table |
| `dlsidecar_iceberg_avg_file_size_mb_gauge` | Average data file size per table |

**Before compaction**: 500 files, average 2MB each (1GB total)
**After compaction**: 8 files, average 128MB each (1GB total, same data, much faster to scan)

### 2. Snapshot Expiry

Every write to an Iceberg table creates a new snapshot. Old snapshots allow time travel but consume metadata space and reference data files that cannot be deleted.

**When it runs**: On a cron schedule configured via `DLS_ICEBERG_SNAPSHOT_EXPIRY_CRON`.

**What it does**:

1. Lists all snapshots for the table
2. Keeps at least `DLS_ICEBERG_SNAPSHOT_MIN_COUNT` snapshots (default: 5)
3. Expires snapshots older than `DLS_ICEBERG_SNAPSHOT_MIN_AGE_HOURS` (default: 72 hours)
4. Removes expired snapshot metadata and marks their exclusive data files for deletion

**Configuration**:

| Setting | Default | Description |
|---|---|---|
| `DLS_ICEBERG_SNAPSHOT_EXPIRY_ENABLED` | `false` | Enable snapshot expiry |
| `DLS_ICEBERG_SNAPSHOT_EXPIRY_CRON` | `0 2 * * *` | Cron schedule (default: 2 AM daily) |
| `DLS_ICEBERG_SNAPSHOT_MIN_AGE_HOURS` | `72` | Minimum snapshot age before expiry |
| `DLS_ICEBERG_SNAPSHOT_MIN_COUNT` | `5` | Always keep at least this many snapshots |

**Metric**: `dlsidecar_iceberg_snapshots_expired_total`

### 3. Orphan Cleanup

Orphan files are data files on S3 that are not referenced by any snapshot. They are typically created by failed writes, retried operations, or race conditions during concurrent writes.

**When it runs**: On a cron schedule configured via `DLS_ICEBERG_ORPHAN_CLEANUP_CRON`.

**What it does**:

1. Lists all data files on S3 under the table's data directory
2. Lists all data files referenced by any current snapshot
3. Deletes files that exist on S3 but are not referenced by any snapshot
4. Only deletes files older than a safety threshold to avoid deleting files from in-progress writes

**Metric**: `dlsidecar_iceberg_orphans_deleted_total`

### 4. HMS Registration

When the sidecar creates or updates an Iceberg table, it registers it with the configured catalog (Hive Metastore, AWS Glue, or REST catalog). This makes the table **automatically visible to Starburst** and any other tool that reads from the same catalog.

| Catalog Type | Registration Method | Starburst Visibility |
|---|---|---|
| Hive Metastore (HMS) | Thrift registration via `DLS_HMS_URI` | Immediate (Starburst reads same HMS) |
| AWS Glue | Glue Data Catalog API | Immediate (Starburst Glue connector) |
| REST Catalog | REST API call to `DLS_ICEBERG_CATALOG_URI` | Immediate (Starburst REST connector) |

This means a team can write data from their pod, and within seconds that data is queryable by any other team through Starburst — with full governance applied.

---

## What This Replaces

| Maintenance Task | Without dlsidecar | With dlsidecar |
|---|---|---|
| Compaction | Spark job on EMR/EKS, scheduled via Airflow | Built-in cron inside the sidecar |
| Snapshot expiry | Spark job or manual API calls | Built-in cron inside the sidecar |
| Orphan cleanup | Custom script, often forgotten | Built-in cron inside the sidecar |
| Catalog registration | Manual `ALTER TABLE` or separate registration job | Automatic on every write |
| Monitoring | Custom Spark metrics + CloudWatch | Prometheus metrics out of the box |
| Scheduling | Airflow DAG or Step Functions | APScheduler inside the sidecar (AsyncIOScheduler) |

### Infrastructure Eliminated

- No separate Spark/EMR cluster for maintenance
- No Airflow DAGs to maintain
- No additional IAM roles for maintenance jobs
- No separate monitoring for maintenance job failures
- No coordination between maintenance jobs and write workloads (the sidecar uses write guards)

---

## Lifecycle Ownership Model

```
Team's Pod
+--------------------------------------------------+
|                                                  |
|  App  -->  dlsidecar                             |
|              |                                   |
|         DuckDB Engine                            |
|              |                                   |
|         Iceberg Writer                           |
|              |-- append / overwrite              |
|              |-- mark_writing() lock             |
|              |                                   |
|         Iceberg Maintenance Scheduler            |
|              |-- Compaction    (hourly)           |
|              |-- Snapshot expiry (daily)          |
|              |-- Orphan cleanup  (daily)          |
|              |-- Write guard: skips table if      |
|              |   mark_writing() is active         |
|                                                  |
+--------------------------------------------------+
              |
              v
         S3 (Parquet data files)
              |
              v
         Catalog (HMS / Glue / REST)
              |
              v
         Starburst (can query immediately)
```

The team does not need to think about maintenance. The sidecar handles it. If they want to tune the schedule or thresholds, they adjust environment variables:

```yaml
env:
  - name: DLS_ICEBERG_COMPACTION_ENABLED
    value: "true"
  - name: DLS_ICEBERG_COMPACTION_CRON
    value: "0 * * * *"  # Every hour
  - name: DLS_ICEBERG_TARGET_FILE_SIZE_MB
    value: "128"
  - name: DLS_ICEBERG_SNAPSHOT_EXPIRY_ENABLED
    value: "true"
  - name: DLS_ICEBERG_SNAPSHOT_MIN_AGE_HOURS
    value: "72"
  - name: DLS_ICEBERG_SNAPSHOT_MIN_COUNT
    value: "5"
  - name: DLS_ICEBERG_ORPHAN_CLEANUP_ENABLED
    value: "true"
```

---

## Frequently Asked Questions

**Q: What happens if the pod restarts mid-compaction?**
A: Iceberg commits are atomic. If the pod dies before the new snapshot is committed, the old files remain intact. The next compaction run will pick up where it left off. No data is lost or corrupted.

**Q: Can compaction run while the app is writing?**
A: The maintenance scheduler respects the `mark_writing()` / `mark_write_complete()` guard. If a table is actively being written to, compaction skips it and retries on the next schedule.

**Q: What if I need to do time travel after snapshots are expired?**
A: Configure `DLS_ICEBERG_SNAPSHOT_MIN_COUNT` and `DLS_ICEBERG_SNAPSHOT_MIN_AGE_HOURS` to retain enough history for your use case. The defaults (5 snapshots, 72 hours) are conservative. Increase them if your compliance requirements mandate longer retention.

**Q: Does this work with Glue Data Catalog?**
A: Yes. Set `DLS_ICEBERG_CATALOG_TYPE=glue` and `DLS_ICEBERG_GLUE_CATALOG_ID` to your AWS account ID. The sidecar uses pyiceberg's Glue catalog integration.
