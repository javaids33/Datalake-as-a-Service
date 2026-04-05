"""
Egress NetworkPolicy generator — the highest-leverage onboarding tool.

Given the current sidecar config, generates the exact Kubernetes NetworkPolicy
egress YAML stanzas that the dev needs to paste into their infra PR.
Every datasource toggle immediately emits the correct egress rules.
"""

from __future__ import annotations

import logging
from typing import Any

import yaml

from dlsidecar.config import Settings, settings

logger = logging.getLogger("dlsidecar.onboarding.egress_generator")


def generate_egress_rules(cfg: Settings | None = None) -> list[dict[str, Any]]:
    """Generate Kubernetes NetworkPolicy egress rules from the current config."""
    cfg = cfg or settings
    rules: list[dict[str, Any]] = []

    # S3 access (always needed for DuckDB direct)
    if cfg.s3_bucket_list:
        rule: dict[str, Any] = {
            "_comment": "S3 access (DuckDB direct via IRSA)",
            "ports": [{"port": 443, "protocol": "TCP"}],
        }
        if cfg.s3_endpoint_url:
            # On-prem / MinIO — point to specific endpoint
            rule["to"] = [{"ipBlock": {"cidr": "0.0.0.0/0"}}]
            rule["_comment"] += f" — endpoint: {cfg.s3_endpoint_url}"
        else:
            # AWS S3 — use VPC endpoint or wide CIDR
            rule["to"] = [{"ipBlock": {"cidr": "0.0.0.0/0"}}]
            rule["_comment"] += " — consider using a VPC endpoint for S3"
        rules.append(rule)

    # Starburst cluster
    if cfg.starburst_enabled and cfg.starburst_host:
        rules.append({
            "_comment": "Starburst cluster",
            "ports": [
                {"port": cfg.starburst_port, "protocol": "TCP"},
            ],
            "to": [{
                "namespaceSelector": {
                    "matchLabels": {"kubernetes.io/metadata.name": "starburst"},
                },
            }],
        })

    # HMS (Hive Metastore Thrift)
    if cfg.hms_enabled:
        hms_port = 9083
        if cfg.hms_uri:
            parts = cfg.hms_uri.replace("thrift://", "").split(":")
            if len(parts) > 1:
                try:
                    hms_port = int(parts[1])
                except ValueError:
                    pass
        rules.append({
            "_comment": "Hive Metastore (Thrift)",
            "ports": [{"port": hms_port, "protocol": "TCP"}],
            "to": [{
                "podSelector": {"matchLabels": {"app": "hive-metastore"}},
            }],
        })

    # Iceberg catalog (REST or Glue)
    if cfg.iceberg_enabled:
        if cfg.iceberg_catalog_type == "rest" and cfg.iceberg_catalog_uri:
            rules.append({
                "_comment": f"Iceberg REST catalog: {cfg.iceberg_catalog_uri}",
                "ports": [{"port": 443, "protocol": "TCP"}],
                "to": [{"ipBlock": {"cidr": "0.0.0.0/0"}}],
            })
        elif cfg.iceberg_catalog_type == "glue":
            rules.append({
                "_comment": "AWS Glue catalog (HTTPS)",
                "ports": [{"port": 443, "protocol": "TCP"}],
                "to": [{"ipBlock": {"cidr": "0.0.0.0/0"}}],
            })

    # Kafka
    if cfg.kafka_enabled and cfg.kafka_brokers:
        kafka_ports = [9092, 9093]
        rules.append({
            "_comment": "Kafka brokers",
            "ports": [{"port": p, "protocol": "TCP"} for p in kafka_ports],
            "to": [{
                "namespaceSelector": {
                    "matchLabels": {"kubernetes.io/metadata.name": "kafka"},
                },
            }],
        })

        if cfg.kafka_schema_registry_url:
            rules.append({
                "_comment": "Kafka Schema Registry",
                "ports": [{"port": 8081, "protocol": "TCP"}],
                "to": [{"ipBlock": {"cidr": "0.0.0.0/0"}}],
            })

    # Snowflake
    if cfg.snowflake_enabled:
        rules.append({
            "_comment": f"Snowflake ({cfg.snowflake_account})",
            "ports": [{"port": 443, "protocol": "TCP"}],
            "to": [{"ipBlock": {"cidr": "0.0.0.0/0"}}],
        })

    # Databricks
    if cfg.databricks_enabled and cfg.databricks_host:
        rules.append({
            "_comment": f"Databricks ({cfg.databricks_host})",
            "ports": [{"port": 443, "protocol": "TCP"}],
            "to": [{"ipBlock": {"cidr": "0.0.0.0/0"}}],
        })

    # PostgreSQL
    if cfg.postgres_enabled:
        rules.append({
            "_comment": "PostgreSQL",
            "ports": [{"port": 5432, "protocol": "TCP"}],
            "to": [{"ipBlock": {"cidr": "0.0.0.0/0"}}],
        })

    # MySQL
    if cfg.mysql_enabled:
        rules.append({
            "_comment": "MySQL",
            "ports": [{"port": 3306, "protocol": "TCP"}],
            "to": [{"ipBlock": {"cidr": "0.0.0.0/0"}}],
        })

    # MongoDB
    if cfg.mongo_enabled:
        rules.append({
            "_comment": "MongoDB",
            "ports": [{"port": 27017, "protocol": "TCP"}],
            "to": [{"ipBlock": {"cidr": "0.0.0.0/0"}}],
        })

    # Azure Blob
    if cfg.azure_enabled:
        rules.append({
            "_comment": f"Azure Blob Storage ({cfg.azure_storage_account})",
            "ports": [{"port": 443, "protocol": "TCP"}],
            "to": [{"ipBlock": {"cidr": "0.0.0.0/0"}}],
        })

    # GCS
    if cfg.gcs_enabled:
        rules.append({
            "_comment": f"Google Cloud Storage ({cfg.gcs_bucket})",
            "ports": [{"port": 443, "protocol": "TCP"}],
            "to": [{"ipBlock": {"cidr": "0.0.0.0/0"}}],
        })

    # External lake
    if cfg.external_lake_enabled:
        if cfg.external_lake_hms_uri:
            rules.append({
                "_comment": "External lake HMS",
                "ports": [{"port": 9083, "protocol": "TCP"}],
                "to": [{"ipBlock": {"cidr": "0.0.0.0/0"}}],
            })
        if cfg.external_lake_catalog_uri:
            rules.append({
                "_comment": "External lake REST catalog",
                "ports": [{"port": 443, "protocol": "TCP"}],
                "to": [{"ipBlock": {"cidr": "0.0.0.0/0"}}],
            })

    # OTLP endpoint
    if cfg.otlp_endpoint:
        rules.append({
            "_comment": "OTLP telemetry endpoint",
            "ports": [{"port": 4317, "protocol": "TCP"}, {"port": 4318, "protocol": "TCP"}],
            "to": [{"ipBlock": {"cidr": "0.0.0.0/0"}}],
        })

    # DNS (always needed)
    rules.append({
        "_comment": "DNS resolution (kube-dns)",
        "ports": [{"port": 53, "protocol": "UDP"}, {"port": 53, "protocol": "TCP"}],
        "to": [{"namespaceSelector": {}}],
    })

    return rules


def generate_network_policy_yaml(
    cfg: Settings | None = None,
    name: str = "dlsidecar-egress",
    namespace: str = "default",
    pod_labels: dict[str, str] | None = None,
) -> str:
    """Generate a complete Kubernetes NetworkPolicy YAML document."""
    cfg = cfg or settings
    rules = generate_egress_rules(cfg)

    # Strip _comment from rules for the actual YAML (add as inline comments)
    clean_rules = []
    for rule in rules:
        clean = {k: v for k, v in rule.items() if k != "_comment"}
        clean_rules.append(clean)

    policy = {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {
            "name": name,
            "namespace": namespace,
        },
        "spec": {
            "podSelector": {
                "matchLabels": pod_labels or {"app": "dlsidecar"},
            },
            "policyTypes": ["Egress"],
            "egress": clean_rules,
        },
    }

    # Generate YAML with comments for each rule
    lines = ["# Auto-generated by dlsidecar egress generator"]
    lines.append(f"# Datasources: {', '.join(r.get('_comment', '') for r in rules)}")
    lines.append("---")
    lines.append(yaml.dump(policy, default_flow_style=False, sort_keys=False))
    return "\n".join(lines)


def generate_egress_summary(cfg: Settings | None = None) -> list[dict[str, str]]:
    """Return a summary of egress rules for the UI display."""
    rules = generate_egress_rules(cfg)
    return [
        {
            "description": rule.get("_comment", "Unknown"),
            "ports": ", ".join(f"{p['port']}/{p.get('protocol', 'TCP')}" for p in rule.get("ports", [])),
            "destination": _summarize_destination(rule.get("to", [])),
        }
        for rule in rules
    ]


def _summarize_destination(to_list: list[dict]) -> str:
    if not to_list:
        return "any"
    parts = []
    for dest in to_list:
        if "ipBlock" in dest:
            parts.append(dest["ipBlock"]["cidr"])
        elif "namespaceSelector" in dest:
            labels = dest["namespaceSelector"].get("matchLabels", {})
            if labels:
                parts.append(f"namespace:{list(labels.values())[0]}")
            else:
                parts.append("all-namespaces")
        elif "podSelector" in dest:
            labels = dest["podSelector"].get("matchLabels", {})
            parts.append(f"pods:{list(labels.values())[0]}" if labels else "all-pods")
    return ", ".join(parts) or "any"
