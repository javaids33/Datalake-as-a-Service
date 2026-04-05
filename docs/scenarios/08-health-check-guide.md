# Health Check Guide

## Overview

The dlsidecar preflight checker reads Kubernetes resources from inside the pod and validates every connection in the chain -- from ServiceAccount to S3, from JWT to Starburst, from DNS to NetworkPolicy. If something is misconfigured, it logs an actionable warning with the exact YAML you need to fix it.

```
GET /healthcheck/preflight     Full report (all 17 checks)
GET /healthcheck/preflight/summary   Quick pass/warn/fail counts
```

## What Gets Checked

The preflight runs 17 checks in dependency order. Each check returns one of four statuses:

| Status | Meaning |
|---|---|
| **pass** | Working correctly |
| **warn** | Works but may cause issues — review the fix suggestion |
| **fail** | Broken — the sidecar cannot function correctly until this is fixed |
| **skip** | Not applicable (feature disabled or not in K8s) |

---

## Check Reference

### 1. Kubernetes Environment

**What:** Verifies the pod is running inside Kubernetes by checking for the ServiceAccount token at `/var/run/secrets/kubernetes.io/serviceaccount/token`.

**When it warns:**
- `KUBERNETES_SERVICE_HOST` is set but the SA token is not mounted
- Fix: Ensure `automountServiceAccountToken: true` in the pod spec

**When it skips:** Running locally or in dev mode outside K8s.

---

### 2. ServiceAccount Token

**What:** Reads and decodes the K8s ServiceAccount JWT to verify it is present, non-empty, and structurally valid.

**When it fails:**
- Token file exists but is empty
- Token is not valid JWT format

**What it reports on pass:** The SA subject (e.g., `system:serviceaccount:default:dlsidecar`).

---

### 3. IRSA (IAM Roles for Service Accounts)

**What:** Checks for `AWS_ROLE_ARN` and `AWS_WEB_IDENTITY_TOKEN_FILE` environment variables that indicate IRSA is configured. Verifies the token file exists.

**When it warns:**
- S3 buckets are configured but `AWS_ROLE_ARN` is not set
- Fix includes the exact ServiceAccount annotation needed:
```yaml
annotations:
  eks.amazonaws.com/role-arn: arn:aws:iam::ACCT:role/YOUR-ROLE
```

**When it fails:**
- `AWS_ROLE_ARN` is set but the token file is missing — the IRSA projected volume is not mounted

---

### 4. JWT Mount (Starburst)

**What:** Verifies the JWT token for Starburst authentication is mounted at `DLS_JWT_TOKEN_PATH`.

**When it fails:**
- Token file does not exist
- Token file is empty
- Fix includes the exact projected volume mount YAML:
```yaml
volumes:
  - name: jwt-token
    projected:
      sources:
        - serviceAccountToken:
            audience: starburst
            path: jwt
```

---

### 5. JWT Validity

**What:** Decodes the Starburst JWT (without signature verification) and checks:
- Is it expired?
- Is it expiring within 5 minutes?
- Does the `aud` claim include the expected audiences?

**When it warns:**
- Token expires in less than 5 minutes
- Missing expected audience (e.g., `starburst` or `dlsidecar`)

**When it fails:**
- Token is already expired
- Token cannot be decoded

**What it reports:** Subject, issuer, audience, and time until expiry.

---

### 6. Environment Variables

**What:** Validates that critical `DLS_*` environment variables are set and consistent.

**When it warns:**
- `DLS_TENANT_ID` is still "default" — audit logs will not identify the team
- `DLS_OWNED_NAMESPACES` is empty — all queries will route to Starburst
- `DLS_S3_BUCKETS` is empty in non-dev mode

**When it fails:**
- `DLS_STARBURST_HOST` is not set when Starburst is enabled in staging/prod

---

### 7. DNS Resolution

**What:** Resolves hostnames for all configured external services (Starburst, HMS, Kafka).

**When it fails:**
- Any hostname cannot be resolved
- Fix includes the DNS egress rule:
```yaml
egress:
  - ports: [{port: 53, protocol: UDP}, {port: 53, protocol: TCP}]
    to: [{namespaceSelector: {}}]
```

**What it reports on pass:** Each hostname and its resolved IP address.

---

### 8. S3 Connectivity

**What:** Calls `s3.head_bucket()` for each configured bucket to verify read access.

**When it fails with 403:**
- IAM role lacks `s3:ListBucket` permission
- Fix lists the required IAM permissions and IRSA setup

**When it fails with 404:**
- Bucket does not exist — check `DLS_S3_BUCKETS` spelling

**When it fails with credential error:**
- IRSA is not working — check the ServiceAccount annotation and OIDC provider trust

---

### 9. Starburst Connectivity

**What:** Opens a TCP connection to the Starburst host and port. If TLS is enabled, performs a TLS handshake and validates the certificate.

**When it fails (timeout):**
- NetworkPolicy egress rule is missing or wrong
- Fix includes the exact egress rule:
```yaml
egress:
  - ports: [{port: 8443}]
    to: [{namespaceSelector:
           {matchLabels:
             {kubernetes.io/metadata.name: starburst}}}]
```

**When it fails (connection refused):**
- Starburst is not running or the port is wrong

**When it fails (DNS):**
- Hostname cannot be resolved — check the service FQDN

**When it warns (TLS):**
- TCP is OK but TLS handshake fails — set `DLS_STARBURST_TLS_CERT_PATH` for private CAs

---

### 10. HMS Connectivity

**What:** Opens a TCP connection to the Hive Metastore Thrift port.

**When it fails:** Same patterns as Starburst — timeout means missing egress rule for port 9083.

---

### 11. Kafka Connectivity

**What:** Tests TCP connectivity to each Kafka broker in `DLS_KAFKA_BROKERS`.

**When it warns:** Some brokers unreachable but at least one is reachable.
**When it fails:** All brokers unreachable.

---

### 12. Iceberg Catalog

**What:** Validates the Iceberg catalog configuration. For REST catalogs, tests TCP connectivity to the endpoint. For Glue, checks that the catalog ID is set.

---

### 13. DuckDB Extensions

**What:** Attempts to load each extension in `DLS_DUCKDB_EXTENSIONS`. If loading fails, attempts to install from the extension repository.

**When it warns:**
- Some extensions failed to load (e.g., no internet access for `INSTALL`)
- Fix: pre-install extensions in the Dockerfile at build time

---

### 14. NetworkPolicy Alignment

**What:** If the ServiceAccount has RBAC to read NetworkPolicies, fetches them from the K8s API and compares the egress rules against what the egress generator says the config needs.

**When it warns:**
- No RBAC to read NetworkPolicies (403) — this check is optional
- NetworkPolicy exists but is missing ports that the config requires
- No NetworkPolicies found in the namespace

**When it passes:** NetworkPolicy egress rules align with the current config.

This is the most powerful check for preventing "it works in dev but breaks in prod" issues.

---

### 15. Resource Limits

**What:** Reads the container memory limit from cgroups and compares it against `DLS_DUCKDB_MEMORY_LIMIT`.

**When it warns:**
- DuckDB memory is >90% of container memory — risk of OOMKill under load

**When it fails:**
- DuckDB memory exceeds container memory — guaranteed OOMKill

---

### 16. Shared Volume

**What:** Checks that the shared file path (`DLS_DUCKDB_SHARED_FILE_PATH`) is writable, for Arrow IPC file exchange between the app container and the sidecar.

**When it warns:** Directory does not exist — needs a shared `emptyDir` volume.
**When it fails:** Directory exists but is not writable.

---

### 17. TLS Certificates

**What:** If `DLS_STARBURST_TLS_CERT_PATH` is set, verifies the file exists and contains a valid PEM certificate.

---

## Example Output

```json
{
  "timestamp": "2026-04-05T20:00:00Z",
  "pod_name": "my-app-7f8d9c-x4k2p",
  "namespace": "team-alpha",
  "summary": {
    "healthy": false,
    "passed": 11,
    "warned": 3,
    "failed": 2,
    "skipped": 1,
    "total": 17
  },
  "checks": [
    {
      "name": "k8s_environment",
      "status": "pass",
      "message": "Running inside K8s pod in namespace 'team-alpha'"
    },
    {
      "name": "irsa",
      "status": "pass",
      "message": "IRSA configured: arn:aws:iam::123456789:role/team-alpha-dlsidecar"
    },
    {
      "name": "jwt_mount",
      "status": "fail",
      "message": "JWT token not found at /var/run/secrets/dlsidecar/jwt",
      "fix": "Mount the projected token volume:\n  volumeMounts:\n    - name: jwt-token\n      mountPath: /var/run/secrets/dlsidecar\n  volumes:\n    - name: jwt-token\n      projected:\n        sources:\n          - serviceAccountToken:\n              audience: starburst\n              path: jwt"
    },
    {
      "name": "starburst",
      "status": "fail",
      "message": "TCP connection to starburst.internal:8443 timed out (5s)",
      "fix": "Check NetworkPolicy egress rules:\n  egress:\n    - ports: [{port: 8443}]\n      to: [{namespaceSelector: {matchLabels: {kubernetes.io/metadata.name: starburst}}}]"
    },
    {
      "name": "network_policy",
      "status": "warn",
      "message": "NetworkPolicy exists but may be missing egress ports: {8443}",
      "fix": "Regenerate the NetworkPolicy with the egress generator:\n  GET /api/onboarding/egress/yaml"
    }
  ]
}
```

## Running Preflight at Startup

The preflight checker runs automatically during sidecar startup. Warnings are logged but do not block startup. Failures are logged as errors.

To run manually:
```bash
curl -s localhost:8765/healthcheck/preflight | jq .
```

To get just the summary:
```bash
curl -s localhost:8765/healthcheck/preflight/summary | jq .
```

## Integrating with Monitoring

The preflight report can be scraped by your monitoring system. A simple approach:

```yaml
# PrometheusRule for alerting on preflight failures
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: dlsidecar-preflight
spec:
  groups:
    - name: dlsidecar
      rules:
        - alert: DLSidecarPreflightFailed
          expr: probe_success{job="dlsidecar-preflight"} == 0
          for: 5m
          labels:
            severity: critical
          annotations:
            summary: "dlsidecar preflight check failing"
            runbook: "curl the /healthcheck/preflight endpoint and review the fix suggestions"
```

## Troubleshooting Flowchart

```
Pod won't start?
  |
  +-> GET /healthcheck/preflight
  |
  +-> Check "fail" items first
  |     |
  |     +-> jwt_mount failed?
  |     |     -> Add projected serviceAccountToken volume
  |     |
  |     +-> irsa failed?
  |     |     -> Add eks.amazonaws.com/role-arn annotation to SA
  |     |
  |     +-> starburst failed (timeout)?
  |     |     -> Add egress rule for port 8443 to starburst namespace
  |     |
  |     +-> s3 failed (403)?
  |     |     -> Add s3:ListBucket to IAM policy
  |     |
  |     +-> dns failed?
  |           -> Add egress rule for port 53 UDP to kube-dns
  |
  +-> Check "warn" items next
        |
        +-> network_policy warned?
        |     -> Run GET /api/onboarding/egress/yaml and apply
        |
        +-> resource_limits warned?
              -> Reduce DLS_DUCKDB_MEMORY_LIMIT or increase pod memory
```
