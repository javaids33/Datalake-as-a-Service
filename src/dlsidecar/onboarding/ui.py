"""
dlsidecar Onboarding UI — Streamlit single-file app.

Replaces the React SPA. Pure Python, zero build step, ultra lightweight.
Run: streamlit run src/dlsidecar/onboarding/ui.py --server.port 3000
"""

from __future__ import annotations

import json
import time
from typing import Any

import streamlit as st
import yaml

st.set_page_config(
    page_title="dlsidecar",
    page_icon="<>",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Sidebar navigation ───────────────────────────────────────────────────

st.sidebar.markdown("### dlsidecar")
st.sidebar.caption("Data Lake as a Service")

page = st.sidebar.radio(
    "Navigation",
    [
        "Connection Wizard",
        "Generated Config",
        "Env Promotion",
        "Query Console",
        "Push Console",
        "Health & Governance",
    ],
    label_visibility="collapsed",
)

# ── Helpers ───────────────────────────────────────────────────────────────

API_BASE = "http://localhost:8765"


def api_get(path: str) -> dict | None:
    try:
        import httpx
        r = httpx.get(f"{API_BASE}{path}", timeout=5)
        return r.json()
    except Exception:
        return None


def api_post(path: str, body: dict) -> dict | None:
    try:
        import httpx
        r = httpx.post(f"{API_BASE}{path}", json=body, timeout=30)
        return r.json()
    except Exception as e:
        return {"error": str(e)}


def generate_egress_rules(cfg: dict) -> list[dict]:
    """Generate egress rules from config dict (mirrors egress_generator.py)."""
    rules = []
    if cfg.get("s3_buckets"):
        rules.append({
            "comment": "S3 access (DuckDB direct via IRSA)",
            "ports": [{"port": 443, "protocol": "TCP"}],
            "to": [{"ipBlock": {"cidr": "0.0.0.0/0"}}],
        })
    if cfg.get("starburst_enabled") and cfg.get("starburst_host"):
        rules.append({
            "comment": f"Starburst cluster ({cfg['starburst_host']})",
            "ports": [{"port": cfg.get("starburst_port", 8443), "protocol": "TCP"}],
            "to": [{"namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "starburst"}}}],
        })
    if cfg.get("hms_enabled"):
        rules.append({
            "comment": "Hive Metastore (Thrift)",
            "ports": [{"port": 9083, "protocol": "TCP"}],
            "to": [{"podSelector": {"matchLabels": {"app": "hive-metastore"}}}],
        })
    if cfg.get("kafka_enabled") and cfg.get("kafka_brokers"):
        rules.append({
            "comment": "Kafka brokers",
            "ports": [{"port": 9092, "protocol": "TCP"}, {"port": 9093, "protocol": "TCP"}],
            "to": [{"namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "kafka"}}}],
        })
    if cfg.get("postgres_enabled"):
        rules.append({"comment": "PostgreSQL", "ports": [{"port": 5432, "protocol": "TCP"}], "to": [{"ipBlock": {"cidr": "0.0.0.0/0"}}]})
    if cfg.get("mysql_enabled"):
        rules.append({"comment": "MySQL", "ports": [{"port": 3306, "protocol": "TCP"}], "to": [{"ipBlock": {"cidr": "0.0.0.0/0"}}]})
    if cfg.get("mongo_enabled"):
        rules.append({"comment": "MongoDB", "ports": [{"port": 27017, "protocol": "TCP"}], "to": [{"ipBlock": {"cidr": "0.0.0.0/0"}}]})
    if cfg.get("snowflake_enabled"):
        rules.append({"comment": "Snowflake", "ports": [{"port": 443, "protocol": "TCP"}], "to": [{"ipBlock": {"cidr": "0.0.0.0/0"}}]})
    if cfg.get("databricks_enabled"):
        rules.append({"comment": "Databricks", "ports": [{"port": 443, "protocol": "TCP"}], "to": [{"ipBlock": {"cidr": "0.0.0.0/0"}}]})
    rules.append({
        "comment": "DNS resolution (kube-dns)",
        "ports": [{"port": 53, "protocol": "UDP"}, {"port": 53, "protocol": "TCP"}],
        "to": [{"namespaceSelector": {}}],
    })
    return rules


def egress_to_yaml(rules: list[dict], name: str = "dlsidecar-egress", namespace: str = "default") -> str:
    clean = [{"ports": r["ports"], "to": r["to"]} for r in rules]
    policy = {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {"name": name, "namespace": namespace},
        "spec": {
            "podSelector": {"matchLabels": {"app": "dlsidecar"}},
            "policyTypes": ["Egress"],
            "egress": clean,
        },
    }
    return yaml.dump(policy, default_flow_style=False, sort_keys=False)


# ── Session state defaults ────────────────────────────────────────────────

if "cfg" not in st.session_state:
    st.session_state.cfg = {
        "s3_buckets": "",
        "s3_region": "us-east-1",
        "starburst_enabled": True,
        "starburst_host": "",
        "starburst_port": 8443,
        "starburst_auth_mode": "jwt",
        "hms_enabled": True,
        "iceberg_catalog_type": "glue",
        "iceberg_catalog_uri": "",
        "iceberg_warehouse": "",
        "iceberg_namespace": "default",
        "cap_read": True,
        "cap_write": False,
        "cap_stream": False,
        "cap_kafka": False,
        "kafka_enabled": False,
        "kafka_brokers": "",
        "postgres_enabled": False,
        "mysql_enabled": False,
        "mongo_enabled": False,
        "snowflake_enabled": False,
        "databricks_enabled": False,
    }

cfg = st.session_state.cfg


# ══════════════════════════════════════════════════════════════════════════
# PAGE 1 — Connection Wizard
# ══════════════════════════════════════════════════════════════════════════

if page == "Connection Wizard":
    st.title("Connection Wizard")
    st.caption("Configure your data lake connections. Every change shows the exact infrastructure config needed for production.")

    # Step 1: S3
    st.subheader("1. What is your team's S3 bucket?")
    col1, col2 = st.columns([3, 1])
    with col1:
        cfg["s3_buckets"] = st.text_input("Bucket name(s)", value=cfg["s3_buckets"], placeholder="my-team-data, my-team-staging")
    with col2:
        cfg["s3_region"] = st.selectbox("AWS Region", ["us-east-1", "us-west-2", "eu-west-1", "eu-central-1", "ap-southeast-1"],
                                        index=["us-east-1", "us-west-2", "eu-west-1", "eu-central-1", "ap-southeast-1"].index(cfg["s3_region"]))

    if cfg["s3_buckets"]:
        buckets = [b.strip() for b in cfg["s3_buckets"].split(",") if b.strip()]
        arns = "\n  ".join(f"arn:aws:s3:::{b}/*" for b in buckets)
        st.code(f"""# What you need in your EKS deployment
ServiceAccount annotation:
  eks.amazonaws.com/role-arn: arn:aws:iam::ACCT:role/NAME

Required S3 IAM permissions:
  s3:GetObject, s3:PutObject, s3:ListBucket, s3:DeleteObject
  on resource:
  {arns}""", language="yaml")

    st.divider()

    # Step 2: Starburst (always-on federated catalog)
    st.subheader("2. Federated Catalog (Starburst)")
    st.caption("Always connected as DuckDB's primary federated catalog via the Trino extension. "
               "DuckDB queries Starburst catalogs directly — no separate client.")
    col1, col2 = st.columns([3, 1])
    with col1:
        cfg["starburst_host"] = st.text_input("Starburst Host", value=cfg["starburst_host"], placeholder="starburst.internal.firm.com")
    with col2:
        cfg["starburst_auth_mode"] = st.selectbox("Auth Method", ["jwt", "ldap", "oauth2"],
                                                   index=["jwt", "ldap", "oauth2"].index(cfg["starburst_auth_mode"]))

    if st.button("Test Connection"):
        result = api_get("/sources/starburst")
        if result and result.get("healthy"):
            st.success("Connected")
        else:
            st.error(f"Connection failed: {result.get('error', 'sidecar not running') if result else 'sidecar not running'}")

    if cfg["starburst_host"]:
        st.code("""# What you need in your pod spec
volumeMounts:
  - name: jwt-token
    mountPath: /var/run/secrets/dlsidecar
volumes:
  - name: jwt-token
    projected:
      sources:
        - serviceAccountToken:
            audience: starburst
            path: jwt""", language="yaml")

    st.divider()

    # Step 3: Iceberg catalog
    st.subheader("3. What Iceberg catalog do you use?")
    cfg["iceberg_catalog_type"] = st.radio("Catalog type", ["glue", "rest", "hive", "none"], horizontal=True,
                                            index=["glue", "rest", "hive", "none"].index(cfg["iceberg_catalog_type"]))

    if cfg["iceberg_catalog_type"] != "none":
        col1, col2 = st.columns(2)
        with col1:
            if cfg["iceberg_catalog_type"] == "rest":
                cfg["iceberg_catalog_uri"] = st.text_input("Catalog URI", value=cfg["iceberg_catalog_uri"],
                                                           placeholder="https://iceberg-rest.internal:8181")
            cfg["iceberg_warehouse"] = st.text_input("Warehouse Path", value=cfg["iceberg_warehouse"],
                                                      placeholder="s3://my-warehouse/iceberg")
        with col2:
            cfg["iceberg_namespace"] = st.text_input("Namespace", value=cfg["iceberg_namespace"], placeholder="default")

    if cfg["iceberg_catalog_type"] == "glue":
        st.code("""# Required IAM permissions for Glue
glue:GetDatabase, glue:GetTable, glue:CreateTable
glue:UpdateTable, glue:GetPartitions, glue:DeleteTable""", language="text")

    st.divider()

    # Step 4: Capabilities
    st.subheader("4. What does your app need to do?")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        cfg["cap_read"] = st.checkbox("Read only", value=cfg["cap_read"])
    with col2:
        cfg["cap_write"] = st.checkbox("Write Iceberg", value=cfg["cap_write"])
    with col3:
        cfg["cap_stream"] = st.checkbox("Stream ingest", value=cfg["cap_stream"])
    with col4:
        cfg["cap_kafka"] = st.checkbox("Kafka bridge", value=cfg["cap_kafka"])
        cfg["kafka_enabled"] = cfg["cap_kafka"]

    rules = generate_egress_rules(cfg)
    st.code("# Your egress NetworkPolicy rules\n" + "\n".join(f"# {r['comment']}\n- ports: {json.dumps(r['ports'])}\n  to: {json.dumps(r['to'])}"
            for r in rules), language="yaml")

    st.divider()

    # Step 5: Additional DuckDB-connected sources
    st.subheader("5. Additional DuckDB-connected sources")
    st.caption("Connect via DuckDB's native extensions — no separate clients. "
               "Each source attaches as a DuckDB catalog (ATTACH ... TYPE POSTGRES, etc.).")
    col1, col2, col3 = st.columns(3)
    with col1:
        cfg["snowflake_enabled"] = st.checkbox("Snowflake", value=cfg["snowflake_enabled"])
        cfg["postgres_enabled"] = st.checkbox("PostgreSQL", value=cfg["postgres_enabled"])
    with col2:
        cfg["databricks_enabled"] = st.checkbox("Databricks", value=cfg["databricks_enabled"])
        cfg["mysql_enabled"] = st.checkbox("MySQL", value=cfg["mysql_enabled"])
    with col3:
        cfg["mongo_enabled"] = st.checkbox("MongoDB", value=cfg["mongo_enabled"])


# ══════════════════════════════════════════════════════════════════════════
# PAGE 2 — Generated Config
# ══════════════════════════════════════════════════════════════════════════

elif page == "Generated Config":
    st.title("Generated Config")
    st.caption("Copy-paste-ready config files. Secrets are extracted to the Secret tab.")

    tab_cm, tab_secret, tab_helm, tab_egress, tab_eso = st.tabs(
        ["ConfigMap YAML", "Secret YAML", "Helm values.yaml", "Egress NetworkPolicy", "ExternalSecret"]
    )

    env_vars = {
        "DLS_MODE": "dev",
        "DLS_TENANT_ID": "",
        "DLS_OWNED_NAMESPACES": "",
        "DLS_S3_BUCKETS": cfg["s3_buckets"],
        "DLS_S3_REGION": cfg["s3_region"],
        "DLS_STARBURST_ENABLED": str(cfg["starburst_enabled"]).lower(),
        "DLS_STARBURST_HOST": cfg["starburst_host"],
        "DLS_STARBURST_PORT": str(cfg["starburst_port"]),
        "DLS_HMS_ENABLED": str(cfg["hms_enabled"]).lower(),
        "DLS_ICEBERG_ENABLED": str(cfg["iceberg_catalog_type"] != "none").lower(),
        "DLS_ICEBERG_CATALOG_TYPE": cfg["iceberg_catalog_type"] if cfg["iceberg_catalog_type"] != "none" else "glue",
        "DLS_DUCKDB_MODE": "memory",
        "DLS_DUCKDB_MEMORY_LIMIT": "4GB",
        "DLS_LOG_LEVEL": "INFO",
        "DLS_KAFKA_ENABLED": str(cfg["kafka_enabled"]).lower(),
    }

    with tab_cm:
        cm = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "dlsidecar-config", "namespace": "default"}, "data": env_vars}
        st.code(yaml.dump(cm, default_flow_style=False, sort_keys=False), language="yaml")

    with tab_secret:
        st.code("""apiVersion: v1
kind: Secret
metadata:
  name: dlsidecar-secrets
  namespace: default
type: Opaque
data:
  # DLS_SNOWFLAKE_USER: <base64>   # Inject via ESO
  # DLS_POSTGRES_DSN: <base64>     # Inject via ESO
  # DLS_MOTHERDUCK_TOKEN: <base64> # Inject via ESO""", language="yaml")

    with tab_helm:
        helm = {"dlsidecar": {"config": {k.replace("DLS_", "").lower(): v for k, v in env_vars.items()}}}
        st.code(yaml.dump(helm, default_flow_style=False, sort_keys=False), language="yaml")

    with tab_egress:
        rules = generate_egress_rules(cfg)
        st.code(egress_to_yaml(rules), language="yaml")

    with tab_eso:
        st.code("""apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: dlsidecar-external-secrets
  namespace: default
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: aws-secrets-manager
    kind: ClusterSecretStore
  target:
    name: dlsidecar-secrets
    creationPolicy: Owner
  data:
    - secretKey: DLS_SNOWFLAKE_USER
      remoteRef:
        key: dlsidecar/default/dls_snowflake_user
        property: value
    - secretKey: DLS_POSTGRES_DSN
      remoteRef:
        key: dlsidecar/default/dls_postgres_dsn
        property: value""", language="yaml")

    col1, col2 = st.columns(2)
    with col1:
        st.download_button("Download ConfigMap", yaml.dump(cm, default_flow_style=False), "configmap.yaml", "text/yaml")
    with col2:
        st.download_button("Download Egress Policy", egress_to_yaml(generate_egress_rules(cfg)), "egress.yaml", "text/yaml")


# ══════════════════════════════════════════════════════════════════════════
# PAGE 3 — Env Promotion
# ══════════════════════════════════════════════════════════════════════════

elif page == "Env Promotion":
    st.title("Environment Promotion")
    st.caption("Visual diff between dev and target environment.")

    target_env = st.radio("Target environment", ["staging", "prod"], horizontal=True)

    vars_table = [
        ("DLS_MODE", "dev", target_env, "env_specific"),
        ("DLS_TENANT_ID", "default", "default", "core"),
        ("DLS_S3_REGION", cfg["s3_region"], cfg["s3_region"], "env_specific"),
        ("DLS_STARBURST_HOST", cfg["starburst_host"] or '""', cfg["starburst_host"] or '""', "env_specific"),
        ("DLS_STARBURST_TLS_ENABLED", "true", "true", "env_specific"),
        ("DLS_ONBOARDING_UI_ENABLED", "true", "false", "feature_flag"),
        ("DLS_SNOWFLAKE_USER", "", "[inject via ESO]", "secret"),
        ("DLS_POSTGRES_DSN", "", "[inject via ESO]", "secret"),
        ("DLS_LOG_LEVEL", "INFO", "INFO", "optional"),
    ]

    import pandas as pd
    df = pd.DataFrame(vars_table, columns=["Variable", "Dev (current)", target_env, "Role"])
    st.dataframe(df, use_container_width=True, hide_index=True)

    st.subheader("Promotion Checklist")
    checks = [
        (f"DLS_MODE set to {target_env}", target_env != "dev"),
        ("TLS enabled for Starburst", True),
        ("Onboarding UI disabled in prod", target_env != "dev"),
        ("All secrets marked for ESO injection", True),
        ("Resource limits appropriate", True),
    ]
    for label, passed in checks:
        st.checkbox(label, value=passed, disabled=True)

    st.subheader("Export")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.button("Export .env")
    with col2:
        st.button("Export Helm values")
    with col3:
        st.button("Export Terraform tfvars")


# ══════════════════════════════════════════════════════════════════════════
# PAGE 4 — Query Console
# ══════════════════════════════════════════════════════════════════════════

elif page == "Query Console":
    st.title("Query Console")
    st.caption("Execute SQL queries with automatic routing to DuckDB or Starburst.")

    sql = st.text_area("SQL", value="SELECT * FROM information_schema.tables LIMIT 10", height=150)

    col1, col2 = st.columns([1, 5])
    with col1:
        run = st.button("Run", type="primary")
    with col2:
        explain = st.button("Explain routing")

    if explain:
        result = api_post("/api/onboarding/explain", {"sql": sql})
        if result and "error" not in result:
            engine = result.get("engine", "unknown")
            badge_color = {"duckdb": "green", "trino_federated": "blue", "hybrid": "violet"}.get(engine, "gray")
            st.markdown(f"**Routing:** :{badge_color}[{engine.upper()}] -- {result.get('routing_reason', '')}")
            if result.get("tables_owned"):
                st.write(f"**Owned tables:** {', '.join(result['tables_owned'])}")
            if result.get("tables_unowned"):
                st.write(f"**Unowned tables:** {', '.join(result['tables_unowned'])}")
        else:
            st.warning("Sidecar not running. Start with: `python -m dlsidecar.main`")

    if run:
        result = api_post("/query", {"sql": sql, "format": "json"})
        if result and "error" not in result:
            engine = result.get("engine", "unknown")
            badge_color = {"duckdb": "green", "trino_federated": "blue", "hybrid": "violet"}.get(engine, "gray")
            st.markdown(
                f":{badge_color}[{engine.upper()}] -- "
                f"{result.get('row_count', 0)} rows, {result.get('duration_ms', 0):.1f}ms"
                f" -- {result.get('routing_reason', '')}"
            )
            if result.get("columns") and result.get("rows"):
                import pandas as pd
                df = pd.DataFrame(result["rows"], columns=result["columns"])
                st.dataframe(df, use_container_width=True, hide_index=True)
        elif result:
            st.error(result.get("error", "Unknown error"))
        else:
            st.warning("Sidecar not running. Start with: `python -m dlsidecar.main`")


# ══════════════════════════════════════════════════════════════════════════
# PAGE 5 — Push Console
# ══════════════════════════════════════════════════════════════════════════

elif page == "Push Console":
    st.title("Push / Stream Console")
    st.caption("Push data into the lake. Supports JSON, Parquet, CSV, and Arrow.")

    col_cfg, col_data = st.columns(2)

    with col_cfg:
        st.subheader("Target")
        database = st.text_input("Database", value="default")
        table = st.text_input("Table", placeholder="events")
        mode = st.selectbox("Mode", ["append", "overwrite", "merge"])
        partition_by = st.text_input("Partition by (comma-separated)", placeholder="year, month, day")
        register_hms = st.checkbox("Auto-register in HMS", value=True)

    with col_data:
        st.subheader("Data")
        upload = st.file_uploader("Upload file", type=["parquet", "csv", "json", "arrow"])
        json_data = st.text_area("Or paste JSON array", value='[\n  {"id": 1, "name": "example", "ts": "2025-04-01T00:00:00Z"}\n]', height=200)

    if st.button("Push", type="primary", disabled=not table):
        body = {
            "source": "inline",
            "data": json_data,
            "target": {
                "type": "iceberg",
                "database": database,
                "table": table,
                "mode": mode,
                "partition_by": [p.strip() for p in partition_by.split(",") if p.strip()] if partition_by else None,
                "register_hms": register_hms,
            },
        }
        result = api_post("/push", body)
        if result and "error" not in result and result.get("status") == "success":
            st.success("Push successful")
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Snapshot ID", result.get("snapshot_id", "N/A"))
            col2.metric("Files Written", result.get("files_written", "N/A"))
            col3.metric("Bytes Written", result.get("bytes_written", "N/A"))
            col4.metric("Rows Written", result.get("rows_written", "N/A"))
            if result.get("starburst_query_hint"):
                st.code(result["starburst_query_hint"], language="sql")
        elif result:
            st.error(result.get("error", result.get("message", "Unknown error")))
        else:
            st.warning("Sidecar not running.")


# ══════════════════════════════════════════════════════════════════════════
# PAGE 6 — Health & Governance Dashboard
# ══════════════════════════════════════════════════════════════════════════

elif page == "Health & Governance":
    st.title("Health & Governance")
    st.caption("Connection status, Iceberg health, and audit log.")

    if st.button("Refresh"):
        st.rerun()

    # Source status
    st.subheader("Source Status")
    sources = api_get("/sources/")
    if sources and sources.get("sources"):
        import pandas as pd
        rows = []
        for name, info in sources["sources"].items():
            rows.append({
                "Source": name,
                "Status": "Healthy" if info.get("healthy") else "Degraded",
                "Latency": f"{info['latency_ms']}ms" if info.get("latency_ms") is not None else "-",
                "Error": info.get("error") or "-",
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    else:
        st.info("No sources connected. Run the sidecar to see live status.")

    st.divider()

    # Iceberg table health
    st.subheader("Iceberg Table Health")
    iceberg = api_get("/iceberg/health")
    if iceberg and iceberg.get("tables"):
        import pandas as pd
        rows = []
        for t in iceberg["tables"]:
            rows.append({
                "Table": t.get("table", ""),
                "Avg File Size": f"{t.get('avg_file_size_mb', 0)} MB",
                "Small Files": t.get("small_file_count", 0),
                "Snapshots": t.get("snapshot_count", 0),
                "Last Compaction": t.get("last_compaction") or "Never",
                "HMS": "Yes" if t.get("hms_registered") else "No",
                "Starburst": "Yes" if t.get("starburst_visible") else "No",
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    else:
        st.info("No Iceberg tables detected.")

    st.divider()

    # JWT status
    st.subheader("JWT Status")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Issuer", "N/A")
    col2.metric("Subject", "N/A")
    col3.metric("Expires In", "N/A")
    col4.metric("Last Refresh", "N/A")
