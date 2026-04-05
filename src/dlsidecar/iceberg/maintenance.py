"""
Iceberg maintenance scheduler — compaction, snapshot expiry, orphan cleanup,
and manifest rewriting. Runs on APScheduler cron jobs inside the sidecar.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from dlsidecar.config import settings
from dlsidecar.governance.audit import emit_audit_event
from dlsidecar.governance.metrics import (
    ICEBERG_AVG_FILE_SIZE_MB,
    ICEBERG_COMPACTION_RUNS,
    ICEBERG_FILES_COMPACTED,
    ICEBERG_ORPHANS_DELETED,
    ICEBERG_SMALL_FILES,
    ICEBERG_SNAPSHOTS_EXPIRED,
)

logger = logging.getLogger("dlsidecar.iceberg.maintenance")


class IcebergMaintenance:
    """Runs Iceberg maintenance operations on a schedule."""

    def __init__(self, catalog=None):
        self._catalog = catalog
        self._scheduler = AsyncIOScheduler()
        self._running_tables: set[str] = set()  # Guard against mid-write compaction

    def set_catalog(self, catalog) -> None:
        self._catalog = catalog

    def _get_catalog(self):
        if self._catalog is not None:
            return self._catalog
        from pyiceberg.catalog import load_catalog

        config: dict[str, Any] = {"type": settings.iceberg_catalog_type}
        if settings.iceberg_catalog_type == "glue":
            config["warehouse"] = settings.iceberg_warehouse
        elif settings.iceberg_catalog_type == "rest":
            config["uri"] = settings.iceberg_catalog_uri
            config["warehouse"] = settings.iceberg_warehouse
        elif settings.iceberg_catalog_type == "hive":
            config["uri"] = settings.hms_uri
            config["warehouse"] = settings.iceberg_warehouse
        self._catalog = load_catalog("default", **config)
        return self._catalog

    def mark_writing(self, table_id: str) -> None:
        self._running_tables.add(table_id)

    def mark_write_complete(self, table_id: str) -> None:
        self._running_tables.discard(table_id)

    def start(self) -> None:
        """Register all enabled maintenance jobs and start the scheduler."""
        if not settings.iceberg_enabled:
            logger.info("Iceberg not enabled, skipping maintenance scheduler")
            return

        if settings.iceberg_compaction_enabled:
            self._add_cron_job("compaction", settings.iceberg_compaction_cron, self.run_compaction)

        if settings.iceberg_snapshot_expiry_enabled:
            self._add_cron_job("snapshot_expiry", settings.iceberg_snapshot_expiry_cron, self.run_snapshot_expiry)

        if settings.iceberg_orphan_cleanup_enabled:
            self._add_cron_job("orphan_cleanup", settings.iceberg_orphan_cleanup_cron, self.run_orphan_cleanup)

        if settings.iceberg_manifest_rewrite_enabled:
            self._add_cron_job("manifest_rewrite", settings.iceberg_manifest_rewrite_cron, self.run_manifest_rewrite)

        self._scheduler.start()
        logger.info("Iceberg maintenance scheduler started")

    def stop(self) -> None:
        self._scheduler.shutdown(wait=False)

    def _add_cron_job(self, name: str, cron_expr: str, func) -> None:
        parts = cron_expr.split()
        if len(parts) != 5:
            logger.error("Invalid cron expression for %s: %s", name, cron_expr)
            return
        minute, hour, day, month, dow = parts
        self._scheduler.add_job(
            func,
            "cron",
            minute=minute,
            hour=hour,
            day=day,
            month=month,
            day_of_week=dow,
            id=f"iceberg_{name}",
            replace_existing=True,
        )
        logger.info("Scheduled iceberg_%s: %s", name, cron_expr)

    async def run_compaction(self) -> list[dict[str, Any]]:
        """Run compaction on all managed tables that need it."""
        catalog = self._get_catalog()
        results = []

        for table_id in self._list_managed_tables():
            if table_id in self._running_tables:
                logger.info("Skipping compaction for %s (write in progress)", table_id)
                continue

            start = time.monotonic()
            try:
                table = catalog.load_table(table_id)
                stats = self._get_file_stats(table)

                if stats["avg_file_size_mb"] >= 32 and stats["file_count"] <= 200:
                    logger.debug("Skipping compaction for %s (healthy file stats)", table_id)
                    continue

                files_before = stats["file_count"]
                bytes_before = stats["total_bytes"]

                # Rewrite data files
                table.rewrite_data_files(
                    target_size_in_bytes=settings.iceberg_target_file_size_mb * 1024 * 1024
                )

                table.refresh()
                stats_after = self._get_file_stats(table)
                duration = time.monotonic() - start

                result = {
                    "table": table_id,
                    "status": "success",
                    "files_before": files_before,
                    "files_after": stats_after["file_count"],
                    "bytes_before": bytes_before,
                    "bytes_after": stats_after["total_bytes"],
                    "duration_seconds": round(duration, 2),
                }
                results.append(result)

                ICEBERG_COMPACTION_RUNS.labels(table=table_id, status="success").inc()
                ICEBERG_FILES_COMPACTED.labels(table=table_id).inc(files_before - stats_after["file_count"])
                ICEBERG_SMALL_FILES.labels(table=table_id).set(stats_after["small_file_count"])
                ICEBERG_AVG_FILE_SIZE_MB.labels(table=table_id).set(stats_after["avg_file_size_mb"])

                emit_audit_event("maintenance", extra={"operation": "compaction", **result})
                logger.info("Compacted %s: %d → %d files", table_id, files_before, stats_after["file_count"])

            except Exception as exc:
                ICEBERG_COMPACTION_RUNS.labels(table=table_id, status="error").inc()
                logger.error("Compaction failed for %s: %s", table_id, exc)
                results.append({"table": table_id, "status": "error", "error": str(exc)})

        return results

    async def run_snapshot_expiry(self) -> list[dict[str, Any]]:
        """Expire old snapshots on all managed tables."""
        catalog = self._get_catalog()
        results = []

        for table_id in self._list_managed_tables():
            try:
                table = catalog.load_table(table_id)
                snapshots = list(table.metadata.snapshots)

                if len(snapshots) <= settings.iceberg_snapshot_min_count:
                    continue

                min_age_ms = settings.iceberg_snapshot_min_age_hours * 3600 * 1000
                now_ms = int(time.time() * 1000)
                expired_count = 0

                for snap in snapshots:
                    age_ms = now_ms - snap.timestamp_ms
                    remaining = len(snapshots) - expired_count
                    if age_ms > min_age_ms and remaining > settings.iceberg_snapshot_min_count:
                        expired_count += 1

                if expired_count > 0:
                    table.expire_snapshots(
                        older_than_ms=now_ms - min_age_ms,
                        retain_last=settings.iceberg_snapshot_min_count,
                    )

                    ICEBERG_SNAPSHOTS_EXPIRED.labels(table=table_id).inc(expired_count)
                    result = {"table": table_id, "expired": expired_count}
                    results.append(result)
                    emit_audit_event("maintenance", extra={"operation": "snapshot_expiry", **result})
                    logger.info("Expired %d snapshots for %s", expired_count, table_id)

            except Exception as exc:
                logger.error("Snapshot expiry failed for %s: %s", table_id, exc)
                results.append({"table": table_id, "status": "error", "error": str(exc)})

        return results

    async def run_orphan_cleanup(self, dry_run: bool = True) -> list[dict[str, Any]]:
        """Clean up orphan files older than retention threshold.

        Dry-run first: log files to delete, confirm count < 10% of total before deleting.
        """
        catalog = self._get_catalog()
        results = []

        for table_id in self._list_managed_tables():
            try:
                table = catalog.load_table(table_id)

                # Get orphan file candidates
                retention_ms = settings.iceberg_orphan_retention_hours * 3600 * 1000
                now_ms = int(time.time() * 1000)
                older_than_ms = now_ms - retention_ms

                orphan_result = table.delete_orphan_files(older_than_ms=older_than_ms, dry_run=True)
                orphan_files = list(orphan_result) if orphan_result else []

                stats = self._get_file_stats(table)
                total_files = stats["file_count"]

                result = {
                    "table": table_id,
                    "orphan_count": len(orphan_files),
                    "total_files": total_files,
                    "dry_run": dry_run,
                }

                if orphan_files:
                    pct = (len(orphan_files) / max(total_files, 1)) * 100
                    if pct > 10:
                        logger.warning(
                            "Orphan cleanup for %s: %d orphans (%.1f%% of total) — skipping, too many",
                            table_id, len(orphan_files), pct,
                        )
                        result["status"] = "skipped_high_percentage"
                    elif not dry_run:
                        table.delete_orphan_files(older_than_ms=older_than_ms, dry_run=False)
                        ICEBERG_ORPHANS_DELETED.labels(table=table_id).inc(len(orphan_files))
                        result["status"] = "deleted"
                        emit_audit_event("maintenance", extra={"operation": "orphan_cleanup", **result})
                        logger.info("Deleted %d orphan files for %s", len(orphan_files), table_id)
                    else:
                        result["status"] = "dry_run"
                        logger.info("Dry run: found %d orphan files for %s", len(orphan_files), table_id)
                else:
                    result["status"] = "clean"

                results.append(result)

            except Exception as exc:
                logger.error("Orphan cleanup failed for %s: %s", table_id, exc)
                results.append({"table": table_id, "status": "error", "error": str(exc)})

        return results

    async def run_manifest_rewrite(self) -> list[dict[str, Any]]:
        """Rewrite manifests when count exceeds threshold."""
        catalog = self._get_catalog()
        results = []

        for table_id in self._list_managed_tables():
            try:
                table = catalog.load_table(table_id)
                current_snapshot = table.current_snapshot()
                if not current_snapshot:
                    continue

                manifest_count = len(current_snapshot.manifests(table.io))
                if manifest_count <= settings.iceberg_max_manifests:
                    continue

                table.rewrite_manifests()

                table.refresh()
                new_snapshot = table.current_snapshot()
                new_count = len(new_snapshot.manifests(table.io)) if new_snapshot else 0

                result = {
                    "table": table_id,
                    "manifests_before": manifest_count,
                    "manifests_after": new_count,
                }
                results.append(result)
                emit_audit_event("maintenance", extra={"operation": "manifest_rewrite", **result})
                logger.info("Rewrote manifests for %s: %d → %d", table_id, manifest_count, new_count)

            except Exception as exc:
                logger.error("Manifest rewrite failed for %s: %s", table_id, exc)
                results.append({"table": table_id, "status": "error", "error": str(exc)})

        return results

    def _list_managed_tables(self) -> list[str]:
        """List all tables in owned namespaces."""
        catalog = self._get_catalog()
        tables = []
        for ns in settings.owned_namespace_set:
            try:
                for table_id in catalog.list_tables(ns):
                    tables.append(f"{table_id[0]}.{table_id[1]}" if isinstance(table_id, tuple) else str(table_id))
            except Exception:
                pass
        return tables

    def _get_file_stats(self, table) -> dict[str, Any]:
        """Get file statistics for a table."""
        try:
            current = table.current_snapshot()
            if not current:
                return {"file_count": 0, "total_bytes": 0, "avg_file_size_mb": 0, "small_file_count": 0}

            manifests = current.manifests(table.io)
            file_count = 0
            total_bytes = 0
            small_file_count = 0
            target_size = settings.iceberg_target_file_size_mb * 1024 * 1024

            for manifest in manifests:
                for entry in manifest.fetch_manifest_entry(table.io):
                    file_count += 1
                    size = entry.data_file.file_size_in_bytes
                    total_bytes += size
                    if size < 32 * 1024 * 1024:  # < 32MB
                        small_file_count += 1

            avg_mb = (total_bytes / file_count / 1024 / 1024) if file_count > 0 else 0
            return {
                "file_count": file_count,
                "total_bytes": total_bytes,
                "avg_file_size_mb": round(avg_mb, 1),
                "small_file_count": small_file_count,
            }
        except Exception:
            return {"file_count": 0, "total_bytes": 0, "avg_file_size_mb": 0, "small_file_count": 0}
