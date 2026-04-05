# How Long Does It Take a New Team to Onboard?

## The Question

"We have 15 teams that need data access. How long does it take each one to get started, and what does the process look like?"

## The Short Answer

A team can go from zero to querying their own S3 data in **under 10 minutes**. The sidecar provides four onboarding tools that eliminate the manual steps that traditionally take weeks.

---

## Step 1: Helm Install (3 Lines)

For a development environment, onboarding starts with a Helm install:

```bash
helm repo add dlsidecar https://charts.example.com/dlsidecar
helm repo update
helm install dlsidecar dlsidecar/dlsidecar \
  --namespace team-payments \
  --set tenant.id=team-payments \
  --set tenant.ownedNamespaces=payments \
  --set s3.buckets=payments-data-dev \
  --set mode=dev
```

That is it for dev mode. The sidecar starts, DuckDB initializes, and the team can query local files and their S3 bucket immediately at `localhost:8765`.

### What `mode=dev` Does

| Behavior | Dev Mode | Production Mode |
|---|---|---|
| Starburst connection | Disabled (not needed for local work) | Enabled with JWT + TLS |
| Iceberg maintenance | Disabled | Compaction, snapshot expiry, orphan cleanup on schedule |
| Row-level security | Optional | Enforced if `DLS_ROW_FILTER` is set |
| Audit logging | Enabled (stdout) | Enabled (stdout, structured JSON) |
| DuckDB memory | 256Mi default | Configured per workload |

---

## Step 2: Connection Wizard

For teams that need more than basic S3 access — Starburst federation, HMS catalog, Iceberg writes, Kafka streaming — the connection wizard generates the complete configuration.

The wizard is aware of every environment variable the sidecar supports and classifies them by role:

| Role | Description | Examples |
|---|---|---|
| `CORE` | Always required | `DLS_TENANT_ID`, `DLS_OWNED_NAMESPACES`, `DLS_S3_BUCKETS` |
| `SECRET` | Must come from Vault / External Secrets Operator | JWT tokens, TLS certificates |
| `ENV_SPECIFIC` | Changes per environment | `DLS_STARBURST_HOST`, `DLS_S3_REGION`, `DLS_ICEBERG_CATALOG_URI` |
| `FEATURE_FLAG` | Toggles a capability | `DLS_STARBURST_ENABLED`, `DLS_ICEBERG_ENABLED`, `DLS_HMS_ENABLED` |
| `OPTIONAL` | Sensible defaults provided | `DLS_DUCKDB_MEMORY_LIMIT`, `DLS_STARBURST_CATALOG` |

The wizard walks the team through which features they need and outputs:

1. A complete `values.yaml` for Helm
2. A `ConfigMap` with non-secret environment variables
3. An `ExternalSecret` manifest for secrets (compatible with AWS Secrets Manager, HashiCorp Vault)
4. A summary of what IRSA roles are needed

---

## Step 3: Egress Generator

Kubernetes NetworkPolicy is the most common source of onboarding friction. The egress generator eliminates it entirely.

Given the sidecar's current configuration, it produces the **exact** Kubernetes NetworkPolicy YAML the team needs. Every data source toggle immediately emits the correct egress rules.

### Example Output

If the team enables S3, Starburst, and HMS, the generator produces:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: dlsidecar-egress
  namespace: team-payments
spec:
  podSelector:
    matchLabels:
      app: dlsidecar
  policyTypes:
    - Egress
  egress:
    # S3 access (DuckDB direct via IRSA)
    - ports:
        - port: 443
          protocol: TCP
      to:
        - ipBlock:
            cidr: 0.0.0.0/0
    # Starburst cluster
    - ports:
        - port: 8443
          protocol: TCP
      to:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: starburst
    # HMS (Hive Metastore Thrift)
    - ports:
        - port: 9083
          protocol: TCP
      to:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: hive-metastore
    # Kafka brokers
    - ports:
        - port: 9092
          protocol: TCP
      to:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: kafka
```

The team pastes this into their infrastructure PR. No guessing about ports, no debugging connectivity issues after deployment.

### How It Works

The egress generator reads the sidecar's `Settings` object and emits rules for every enabled data source:

- S3 access (always, if buckets are configured)
- Starburst cluster (if `DLS_STARBURST_ENABLED`)
- HMS / Hive Metastore (if `DLS_HMS_ENABLED`)
- Kafka brokers (if streaming is configured)
- Iceberg catalog REST endpoint (if `DLS_ICEBERG_CATALOG_TYPE=rest`)
- AWS Glue endpoint (if `DLS_ICEBERG_CATALOG_TYPE=glue`)

---

## Step 4: Environment Promoter

Once the team has a working dev configuration, the environment promoter handles promotion to staging and production.

### What Changes Between Environments

| Setting | Dev | Staging | Prod |
|---|---|---|---|
| `DLS_MODE` | `dev` | `staging` | `production` |
| `DLS_S3_BUCKETS` | `payments-data-dev` | `payments-data-staging` | `payments-data-prod` |
| `DLS_STARBURST_HOST` | (disabled) | `starburst.staging.internal` | `starburst.prod.internal` |
| `DLS_ICEBERG_WAREHOUSE` | `s3://iceberg-dev/` | `s3://iceberg-staging/` | `s3://iceberg-prod/` |
| `DLS_ROW_FILTER` | (optional) | `tenant_id = 'team-payments'` | `tenant_id = 'team-payments'` |
| Iceberg maintenance | Disabled | Enabled | Enabled |
| JWT token path | N/A | `/var/run/secrets/jwt/token` | `/var/run/secrets/jwt/token` |

The promoter takes the dev `values.yaml`, applies the environment-specific overrides, and produces a complete manifest for the target environment. It validates that:

- All `CORE` variables are present
- All `SECRET` variables reference ExternalSecret manifests (not hardcoded values)
- All `ENV_SPECIFIC` variables have been updated for the target environment
- Feature flags are explicitly set (not left at dev defaults)

---

## Onboarding Timeline: dlsidecar vs Traditional Approaches

| Step | dlsidecar | Traditional Data Pipeline |
|---|---|---|
| Request data access | Not needed (self-service) | 1-5 days (ticket, approval chain) |
| Provision compute infrastructure | Not needed (uses pod memory) | 1-2 weeks (Spark/EMR cluster, IAM roles) |
| Configure networking | Egress generator: 2 minutes | 1-3 days (NetworkPolicy debugging, firewall rules) |
| Set up authentication | Helm values + ExternalSecret: 5 minutes | 1-5 days (service accounts, role bindings, key rotation) |
| First query | Immediate after deploy | After pipeline is built and tested |
| Cross-team federation | Enable Starburst flag: 1 minute | Weeks (data sharing agreements, ETL to shared warehouse) |
| Production promotion | Environment promoter: 10 minutes | Days (separate infrastructure review, capacity planning) |
| **Total** | **10-30 minutes** | **2-6 weeks** |

---

## What Teams Do NOT Need to Do

The following tasks are handled by the sidecar and do not require team action:

- **Iceberg table maintenance** — compaction, snapshot expiry, and orphan cleanup run on cron schedules inside the sidecar
- **JWT token refresh** — the auth module manages token lifecycle automatically
- **Connection health monitoring** — Prometheus metrics track connection status; reconnection is automatic
- **Query routing** — the router handles engine selection transparently; teams just write SQL
- **Audit compliance** — every query is logged automatically; no opt-in required
