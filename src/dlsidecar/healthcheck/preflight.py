"""
Preflight checker — validates the full connection chain from inside the pod.

Reads Kubernetes resources (ServiceAccount, ConfigMap, NetworkPolicy, Secrets),
validates mounts, DNS, connectivity, IAM, and logs actionable warnings for
every misconfiguration it finds.

Run at startup or on-demand via GET /healthcheck/preflight.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import socket
import ssl
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

from dlsidecar.config import settings

logger = logging.getLogger("dlsidecar.healthcheck.preflight")

# ── K8s paths available inside every pod ──────────────────────────────────
K8S_SA_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"
K8S_SA_NAMESPACE_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
K8S_SA_CA_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
K8S_API_HOST = "https://kubernetes.default.svc"


class CheckStatus(str, Enum):
    PASS = "pass"
    WARN = "warn"
    FAIL = "fail"
    SKIP = "skip"


@dataclass
class CheckResult:
    name: str
    status: CheckStatus
    message: str
    detail: str | None = None
    fix: str | None = None  # Actionable fix suggestion


@dataclass
class PreflightReport:
    timestamp: str = ""
    pod_name: str = ""
    namespace: str = ""
    checks: list[CheckResult] = field(default_factory=list)
    passed: int = 0
    warned: int = 0
    failed: int = 0
    skipped: int = 0

    def add(self, result: CheckResult) -> None:
        self.checks.append(result)
        if result.status == CheckStatus.PASS:
            self.passed += 1
        elif result.status == CheckStatus.WARN:
            self.warned += 1
        elif result.status == CheckStatus.FAIL:
            self.failed += 1
        else:
            self.skipped += 1

    @property
    def healthy(self) -> bool:
        return self.failed == 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "pod_name": self.pod_name,
            "namespace": self.namespace,
            "summary": {
                "healthy": self.healthy,
                "passed": self.passed,
                "warned": self.warned,
                "failed": self.failed,
                "skipped": self.skipped,
                "total": len(self.checks),
            },
            "checks": [
                {
                    "name": c.name,
                    "status": c.status.value,
                    "message": c.message,
                    "detail": c.detail,
                    "fix": c.fix,
                }
                for c in self.checks
            ],
        }

    def log_summary(self) -> None:
        """Log the full report with warnings for anything that isn't PASS."""
        logger.info(
            "Preflight: %d passed, %d warned, %d failed, %d skipped",
            self.passed, self.warned, self.failed, self.skipped,
        )
        for c in self.checks:
            if c.status == CheckStatus.FAIL:
                logger.error("PREFLIGHT FAIL: [%s] %s | fix: %s", c.name, c.message, c.fix or "see docs")
            elif c.status == CheckStatus.WARN:
                logger.warning("PREFLIGHT WARN: [%s] %s | fix: %s", c.name, c.message, c.fix or "see docs")


class PreflightChecker:
    """Runs all preflight checks and produces a report."""

    def __init__(self):
        self._k8s_client = None

    async def run(self) -> PreflightReport:
        """Run all checks and return a PreflightReport."""
        from datetime import datetime, timezone

        report = PreflightReport(
            timestamp=datetime.now(timezone.utc).isoformat(),
            pod_name=os.environ.get("POD_NAME", os.environ.get("HOSTNAME", "unknown")),
            namespace=self._read_namespace(),
        )

        # Run checks in order: K8s env → mounts → DNS → connectivity → IAM → config
        checks = [
            self._check_k8s_environment,
            self._check_serviceaccount,
            self._check_irsa_annotation,
            self._check_jwt_mount,
            self._check_jwt_validity,
            self._check_env_vars_loaded,
            self._check_dns_resolution,
            self._check_s3_connectivity,
            self._check_starburst_connectivity,
            self._check_hms_connectivity,
            self._check_iceberg_catalog,
            self._check_kafka_connectivity,
            self._check_duckdb_extensions,
            self._check_network_policy_alignment,
            self._check_resource_limits,
            self._check_shared_volume,
            self._check_tls_certificates,
        ]

        for check_fn in checks:
            try:
                result = await check_fn()
                report.add(result)
            except Exception as exc:
                report.add(CheckResult(
                    name=check_fn.__name__.replace("_check_", ""),
                    status=CheckStatus.FAIL,
                    message=f"Check crashed: {exc}",
                ))

        report.log_summary()
        return report

    # ══════════════════════════════════════════════════════════════════════
    # KUBERNETES ENVIRONMENT CHECKS
    # ══════════════════════════════════════════════════════════════════════

    async def _check_k8s_environment(self) -> CheckResult:
        """Verify we're running inside a Kubernetes pod."""
        sa_token = Path(K8S_SA_TOKEN_PATH)
        ns_file = Path(K8S_SA_NAMESPACE_PATH)

        if sa_token.exists() and ns_file.exists():
            ns = ns_file.read_text().strip()
            return CheckResult(
                name="k8s_environment",
                status=CheckStatus.PASS,
                message=f"Running inside K8s pod in namespace '{ns}'",
            )

        if os.environ.get("KUBERNETES_SERVICE_HOST"):
            return CheckResult(
                name="k8s_environment",
                status=CheckStatus.WARN,
                message="KUBERNETES_SERVICE_HOST set but SA token not mounted",
                fix="Ensure automountServiceAccountToken is not disabled in the pod spec",
            )

        return CheckResult(
            name="k8s_environment",
            status=CheckStatus.SKIP,
            message="Not running inside Kubernetes (local/dev mode)",
        )

    async def _check_serviceaccount(self) -> CheckResult:
        """Verify the service account token is mounted and readable."""
        sa_token = Path(K8S_SA_TOKEN_PATH)
        if not sa_token.exists():
            return CheckResult(
                name="serviceaccount",
                status=CheckStatus.SKIP,
                message="No K8s service account token found (not in K8s)",
            )

        token = sa_token.read_text().strip()
        if not token:
            return CheckResult(
                name="serviceaccount",
                status=CheckStatus.FAIL,
                message="Service account token file exists but is empty",
                fix="Check that the ServiceAccount exists and has a token",
            )

        # Decode JWT header to check it's valid format
        try:
            import jwt as pyjwt
            claims = pyjwt.decode(token, options={"verify_signature": False})
            sa_name = claims.get("sub", "unknown")
            return CheckResult(
                name="serviceaccount",
                status=CheckStatus.PASS,
                message=f"Service account token valid: {sa_name}",
                detail=json.dumps({k: v for k, v in claims.items() if k in ("sub", "iss", "aud", "exp")}, default=str),
            )
        except Exception as exc:
            return CheckResult(
                name="serviceaccount",
                status=CheckStatus.WARN,
                message=f"Service account token exists but could not decode: {exc}",
            )

    async def _check_irsa_annotation(self) -> CheckResult:
        """Check if IRSA (IAM Roles for Service Accounts) is configured.

        In K8s, the projected token for IRSA is at the standard SA path.
        The AWS_ROLE_ARN and AWS_WEB_IDENTITY_TOKEN_FILE env vars indicate IRSA.
        """
        role_arn = os.environ.get("AWS_ROLE_ARN", "")
        token_file = os.environ.get("AWS_WEB_IDENTITY_TOKEN_FILE", "")

        if role_arn and token_file:
            if Path(token_file).exists():
                return CheckResult(
                    name="irsa",
                    status=CheckStatus.PASS,
                    message=f"IRSA configured: {role_arn}",
                    detail=f"Token file: {token_file}",
                )
            return CheckResult(
                name="irsa",
                status=CheckStatus.FAIL,
                message=f"IRSA role set ({role_arn}) but token file missing: {token_file}",
                fix="Verify the ServiceAccount annotation eks.amazonaws.com/role-arn is correct "
                    "and the projected token volume is mounted",
            )

        if settings.s3_bucket_list:
            return CheckResult(
                name="irsa",
                status=CheckStatus.WARN,
                message="S3 buckets configured but IRSA not detected (no AWS_ROLE_ARN env var)",
                fix="Add to your ServiceAccount:\n"
                    "  annotations:\n"
                    "    eks.amazonaws.com/role-arn: arn:aws:iam::ACCT:role/YOUR-ROLE\n"
                    "Required IAM permissions: s3:GetObject, s3:PutObject, s3:ListBucket on your bucket(s)",
            )

        return CheckResult(
            name="irsa",
            status=CheckStatus.SKIP,
            message="No S3 buckets configured, IRSA check skipped",
        )

    # ══════════════════════════════════════════════════════════════════════
    # JWT / AUTH MOUNT CHECKS
    # ══════════════════════════════════════════════════════════════════════

    async def _check_jwt_mount(self) -> CheckResult:
        """Verify the JWT token for Starburst is mounted."""
        if not settings.starburst_enabled:
            return CheckResult(name="jwt_mount", status=CheckStatus.SKIP, message="Starburst disabled")

        jwt_path = Path(settings.jwt_token_path)
        if jwt_path.exists():
            size = jwt_path.stat().st_size
            if size > 0:
                return CheckResult(
                    name="jwt_mount",
                    status=CheckStatus.PASS,
                    message=f"JWT token mounted at {settings.jwt_token_path} ({size} bytes)",
                )
            return CheckResult(
                name="jwt_mount",
                status=CheckStatus.FAIL,
                message=f"JWT token file exists but is empty: {settings.jwt_token_path}",
                fix="Check the projected serviceAccountToken volume:\n"
                    "  volumes:\n"
                    "    - name: jwt-token\n"
                    "      projected:\n"
                    "        sources:\n"
                    "          - serviceAccountToken:\n"
                    "              audience: starburst\n"
                    "              path: jwt",
            )

        return CheckResult(
            name="jwt_mount",
            status=CheckStatus.FAIL,
            message=f"JWT token not found at {settings.jwt_token_path}",
            fix="Mount the projected token volume:\n"
                "  volumeMounts:\n"
                f"    - name: jwt-token\n"
                f"      mountPath: {str(Path(settings.jwt_token_path).parent)}\n"
                "  volumes:\n"
                "    - name: jwt-token\n"
                "      projected:\n"
                "        sources:\n"
                "          - serviceAccountToken:\n"
                "              audience: starburst\n"
                "              path: jwt",
        )

    async def _check_jwt_validity(self) -> CheckResult:
        """Decode the JWT and check expiry, audience, issuer."""
        if not settings.starburst_enabled:
            return CheckResult(name="jwt_validity", status=CheckStatus.SKIP, message="Starburst disabled")

        jwt_path = Path(settings.jwt_token_path)
        if not jwt_path.exists():
            return CheckResult(name="jwt_validity", status=CheckStatus.SKIP, message="No JWT to validate")

        try:
            import jwt as pyjwt
            token = jwt_path.read_text().strip()
            claims = pyjwt.decode(token, options={"verify_signature": False})

            exp = claims.get("exp", 0)
            now = time.time()
            remaining = exp - now

            issues = []
            if remaining <= 0:
                issues.append(f"TOKEN EXPIRED {abs(remaining):.0f}s ago")
            elif remaining < 300:
                issues.append(f"Token expires in {remaining:.0f}s (< 5 min)")

            aud = claims.get("aud", [])
            if isinstance(aud, str):
                aud = [aud]
            expected_auds = settings.jwt_audience_list
            for ea in expected_auds:
                if ea not in aud:
                    issues.append(f"Missing expected audience '{ea}' (has: {aud})")

            if issues:
                return CheckResult(
                    name="jwt_validity",
                    status=CheckStatus.WARN if remaining > 0 else CheckStatus.FAIL,
                    message="; ".join(issues),
                    detail=json.dumps({k: v for k, v in claims.items() if k in ("sub", "iss", "aud", "exp")}, default=str),
                    fix="If expired, the projected SA token should auto-rotate. "
                        "Check DLS_JWT_REFRESH_BEFORE_EXPIRY_SECONDS and the token expirationSeconds in the volume spec.",
                )

            return CheckResult(
                name="jwt_validity",
                status=CheckStatus.PASS,
                message=f"JWT valid: sub={claims.get('sub')}, expires in {remaining/60:.0f}m",
                detail=json.dumps({k: v for k, v in claims.items() if k in ("sub", "iss", "aud", "exp")}, default=str),
            )
        except Exception as exc:
            return CheckResult(
                name="jwt_validity",
                status=CheckStatus.FAIL,
                message=f"Failed to decode JWT: {exc}",
                fix="Verify the token at DLS_JWT_TOKEN_PATH is a valid JWT",
            )

    # ══════════════════════════════════════════════════════════════════════
    # CONFIG / ENV VAR CHECKS
    # ══════════════════════════════════════════════════════════════════════

    async def _check_env_vars_loaded(self) -> CheckResult:
        """Verify critical environment variables are set."""
        missing = []
        warnings = []

        if not settings.tenant_id or settings.tenant_id == "default":
            warnings.append("DLS_TENANT_ID is 'default' — set a meaningful tenant ID for audit logs")

        if not settings.owned_namespaces:
            warnings.append("DLS_OWNED_NAMESPACES is empty — all tables will route to Starburst")

        if settings.starburst_enabled and not settings.starburst_host and settings.mode != "dev":
            missing.append("DLS_STARBURST_HOST required when Starburst is enabled in non-dev mode")

        if not settings.s3_bucket_list and settings.mode != "dev":
            warnings.append("DLS_S3_BUCKETS is empty — no team bucket configured for DuckDB direct access")

        if missing:
            return CheckResult(
                name="env_vars",
                status=CheckStatus.FAIL,
                message=f"Missing required env vars: {'; '.join(missing)}",
                fix="Set these in your ConfigMap or Helm values",
            )

        if warnings:
            return CheckResult(
                name="env_vars",
                status=CheckStatus.WARN,
                message="; ".join(warnings),
                fix="Review your DLS_* environment variables in the ConfigMap",
            )

        return CheckResult(
            name="env_vars",
            status=CheckStatus.PASS,
            message=f"Config loaded: mode={settings.mode}, tenant={settings.tenant_id}, "
                    f"namespaces={settings.owned_namespaces}",
        )

    # ══════════════════════════════════════════════════════════════════════
    # DNS RESOLUTION CHECKS
    # ══════════════════════════════════════════════════════════════════════

    async def _check_dns_resolution(self) -> CheckResult:
        """Verify DNS resolution for critical services."""
        targets: list[tuple[str, str]] = []

        if settings.starburst_enabled and settings.starburst_host:
            targets.append(("starburst", settings.starburst_host))

        if settings.hms_enabled and settings.hms_uri:
            host = settings.hms_uri.replace("thrift://", "").split(":")[0]
            targets.append(("hms", host))

        if settings.kafka_enabled and settings.kafka_brokers:
            first_broker = settings.kafka_brokers.split(",")[0].strip().split(":")[0]
            targets.append(("kafka", first_broker))

        if not targets:
            return CheckResult(name="dns", status=CheckStatus.SKIP, message="No external services to resolve")

        failures = []
        resolved = []
        for label, host in targets:
            try:
                addrs = socket.getaddrinfo(host, None, socket.AF_UNSPEC, socket.SOCK_STREAM)
                ip = addrs[0][4][0] if addrs else "?"
                resolved.append(f"{label}={host} -> {ip}")
            except socket.gaierror as exc:
                failures.append(f"{label}={host}: {exc}")

        if failures:
            return CheckResult(
                name="dns",
                status=CheckStatus.FAIL,
                message=f"DNS resolution failed: {'; '.join(failures)}",
                detail=f"Resolved: {'; '.join(resolved)}" if resolved else None,
                fix="Check your NetworkPolicy egress rules include DNS (port 53 UDP/TCP to kube-dns). "
                    "Also verify the service hostname exists in the target namespace:\n"
                    "  egress:\n"
                    "    - ports: [{port: 53, protocol: UDP}]\n"
                    "      to: [{namespaceSelector: {}}]",
            )

        return CheckResult(
            name="dns",
            status=CheckStatus.PASS,
            message=f"DNS resolution OK: {'; '.join(resolved)}",
        )

    # ══════════════════════════════════════════════════════════════════════
    # CONNECTIVITY CHECKS
    # ══════════════════════════════════════════════════════════════════════

    async def _check_s3_connectivity(self) -> CheckResult:
        """Test S3 bucket access via a lightweight HEAD request."""
        if not settings.s3_bucket_list:
            return CheckResult(name="s3", status=CheckStatus.SKIP, message="No S3 buckets configured")

        try:
            import boto3
            from botocore.exceptions import ClientError, NoCredentialsError

            kwargs: dict[str, Any] = {"region_name": settings.s3_region}
            if settings.s3_endpoint_url:
                kwargs["endpoint_url"] = settings.s3_endpoint_url

            s3 = boto3.client("s3", **kwargs)
            results = []
            failures = []

            for bucket in settings.s3_bucket_list:
                try:
                    start = time.monotonic()
                    s3.head_bucket(Bucket=bucket)
                    latency = (time.monotonic() - start) * 1000
                    results.append(f"{bucket} ({latency:.0f}ms)")
                except NoCredentialsError:
                    failures.append(f"{bucket}: no credentials (IRSA not working?)")
                except ClientError as exc:
                    code = exc.response.get("Error", {}).get("Code", "?")
                    if code == "403":
                        failures.append(f"{bucket}: 403 Forbidden — IAM role lacks s3:ListBucket permission")
                    elif code == "404":
                        failures.append(f"{bucket}: bucket does not exist")
                    else:
                        failures.append(f"{bucket}: {code} {exc}")

            if failures:
                return CheckResult(
                    name="s3",
                    status=CheckStatus.FAIL,
                    message=f"S3 access failed: {'; '.join(failures)}",
                    detail=f"Accessible: {'; '.join(results)}" if results else None,
                    fix="Verify IRSA is configured:\n"
                        "  1. ServiceAccount has annotation eks.amazonaws.com/role-arn\n"
                        "  2. IAM role trusts the OIDC provider\n"
                        "  3. IAM policy grants s3:GetObject, s3:PutObject, s3:ListBucket\n"
                        "  4. Egress NetworkPolicy allows port 443 to S3 (or VPC endpoint)",
                )

            return CheckResult(
                name="s3",
                status=CheckStatus.PASS,
                message=f"S3 accessible: {'; '.join(results)}",
            )
        except ImportError:
            return CheckResult(name="s3", status=CheckStatus.SKIP, message="boto3 not installed")

    async def _check_starburst_connectivity(self) -> CheckResult:
        """Test TCP connectivity to Starburst and optionally run SELECT 1."""
        if not settings.starburst_enabled:
            return CheckResult(name="starburst", status=CheckStatus.SKIP, message="Starburst disabled")

        if not settings.starburst_host:
            return CheckResult(
                name="starburst",
                status=CheckStatus.WARN if settings.mode == "dev" else CheckStatus.FAIL,
                message="DLS_STARBURST_HOST not set",
                fix="Set DLS_STARBURST_HOST in your ConfigMap",
            )

        host = settings.starburst_host
        port = settings.starburst_port

        # TCP connectivity check
        try:
            start = time.monotonic()
            sock = socket.create_connection((host, port), timeout=5)
            latency = (time.monotonic() - start) * 1000
            sock.close()
        except socket.timeout:
            return CheckResult(
                name="starburst",
                status=CheckStatus.FAIL,
                message=f"TCP connection to {host}:{port} timed out (5s)",
                fix="Check NetworkPolicy egress rules:\n"
                    f"  egress:\n"
                    f"    - ports: [{{port: {port}}}]\n"
                    f"      to: [{{namespaceSelector: {{matchLabels: "
                    f"{{kubernetes.io/metadata.name: starburst}}}}}}]",
            )
        except ConnectionRefusedError:
            return CheckResult(
                name="starburst",
                status=CheckStatus.FAIL,
                message=f"Connection refused: {host}:{port}",
                fix=f"Verify Starburst is running at {host}:{port} and the port is correct (default: 8443)",
            )
        except socket.gaierror:
            return CheckResult(
                name="starburst",
                status=CheckStatus.FAIL,
                message=f"Cannot resolve hostname: {host}",
                fix="Check DNS resolution. The Starburst service must be resolvable from this namespace. "
                    "If it's in another namespace, use the FQDN: starburst-coordinator.starburst.svc.cluster.local",
            )
        except Exception as exc:
            return CheckResult(
                name="starburst",
                status=CheckStatus.FAIL,
                message=f"TCP connection failed: {exc}",
            )

        # TLS check if enabled
        tls_detail = ""
        if settings.starburst_tls_enabled:
            try:
                ctx = ssl.create_default_context()
                if settings.starburst_tls_cert_path:
                    ctx.load_verify_locations(settings.starburst_tls_cert_path)
                conn = ctx.wrap_socket(socket.create_connection((host, port), timeout=5), server_hostname=host)
                cert = conn.getpeercert()
                conn.close()
                tls_detail = f", TLS verified (CN={cert.get('subject', [[('?','?')]])[0][0][1]})"
            except ssl.SSLError as exc:
                return CheckResult(
                    name="starburst",
                    status=CheckStatus.WARN,
                    message=f"TCP OK ({latency:.0f}ms) but TLS handshake failed: {exc}",
                    fix="If using a private CA, set DLS_STARBURST_TLS_CERT_PATH to the CA bundle. "
                        "Or verify the cert CN matches the hostname.",
                )

        return CheckResult(
            name="starburst",
            status=CheckStatus.PASS,
            message=f"Starburst reachable: {host}:{port} ({latency:.0f}ms{tls_detail})",
        )

    async def _check_hms_connectivity(self) -> CheckResult:
        """Test TCP connectivity to Hive Metastore."""
        if not settings.hms_enabled:
            return CheckResult(name="hms", status=CheckStatus.SKIP, message="HMS disabled")

        uri = settings.hms_uri.replace("thrift://", "")
        parts = uri.split(":")
        host = parts[0]
        port = int(parts[1]) if len(parts) > 1 else 9083

        try:
            start = time.monotonic()
            sock = socket.create_connection((host, port), timeout=5)
            latency = (time.monotonic() - start) * 1000
            sock.close()
            return CheckResult(
                name="hms",
                status=CheckStatus.PASS,
                message=f"HMS reachable: {host}:{port} ({latency:.0f}ms)",
            )
        except socket.timeout:
            return CheckResult(
                name="hms",
                status=CheckStatus.FAIL,
                message=f"TCP connection to HMS {host}:{port} timed out",
                fix="Check NetworkPolicy egress rules:\n"
                    "  egress:\n"
                    f"    - ports: [{{port: {port}}}]\n"
                    "      to: [{podSelector: {matchLabels: {app: hive-metastore}}}]",
            )
        except Exception as exc:
            return CheckResult(
                name="hms",
                status=CheckStatus.FAIL,
                message=f"HMS connection failed: {exc}",
                fix=f"Verify HMS is running at {host}:{port}. Check DLS_HMS_URI.",
            )

    async def _check_kafka_connectivity(self) -> CheckResult:
        """Test TCP connectivity to Kafka brokers."""
        if not settings.kafka_enabled or not settings.kafka_brokers:
            return CheckResult(name="kafka", status=CheckStatus.SKIP, message="Kafka disabled")

        brokers = [b.strip() for b in settings.kafka_brokers.split(",") if b.strip()]
        reachable = []
        unreachable = []

        for broker in brokers:
            parts = broker.split(":")
            host = parts[0]
            port = int(parts[1]) if len(parts) > 1 else 9092
            try:
                sock = socket.create_connection((host, port), timeout=3)
                sock.close()
                reachable.append(broker)
            except Exception:
                unreachable.append(broker)

        if unreachable:
            return CheckResult(
                name="kafka",
                status=CheckStatus.FAIL if not reachable else CheckStatus.WARN,
                message=f"Kafka brokers unreachable: {', '.join(unreachable)}",
                detail=f"Reachable: {', '.join(reachable)}" if reachable else None,
                fix="Check NetworkPolicy egress for Kafka:\n"
                    "  egress:\n"
                    "    - ports: [{port: 9092}, {port: 9093}]\n"
                    "      to: [{namespaceSelector: {matchLabels: {kubernetes.io/metadata.name: kafka}}}]",
            )

        return CheckResult(
            name="kafka",
            status=CheckStatus.PASS,
            message=f"Kafka brokers reachable: {', '.join(reachable)}",
        )

    async def _check_iceberg_catalog(self) -> CheckResult:
        """Verify Iceberg catalog connectivity."""
        if not settings.iceberg_enabled:
            return CheckResult(name="iceberg_catalog", status=CheckStatus.SKIP, message="Iceberg disabled")

        if settings.iceberg_catalog_type == "glue":
            if not settings.iceberg_glue_catalog_id and settings.mode != "dev":
                return CheckResult(
                    name="iceberg_catalog",
                    status=CheckStatus.WARN,
                    message="Glue catalog ID not set (DLS_ICEBERG_GLUE_CATALOG_ID)",
                    fix="Set DLS_ICEBERG_GLUE_CATALOG_ID to your AWS account ID. "
                        "IRSA role needs: glue:GetDatabase, glue:GetTable, glue:CreateTable, glue:UpdateTable",
                )
            return CheckResult(
                name="iceberg_catalog",
                status=CheckStatus.PASS,
                message=f"Iceberg catalog: Glue (catalog_id={settings.iceberg_glue_catalog_id or 'default'})",
            )

        elif settings.iceberg_catalog_type == "rest":
            if not settings.iceberg_catalog_uri:
                return CheckResult(
                    name="iceberg_catalog",
                    status=CheckStatus.FAIL,
                    message="REST catalog selected but DLS_ICEBERG_CATALOG_URI not set",
                    fix="Set DLS_ICEBERG_CATALOG_URI to your Iceberg REST catalog endpoint",
                )
            # TCP check
            try:
                from urllib.parse import urlparse
                parsed = urlparse(settings.iceberg_catalog_uri)
                host = parsed.hostname
                port = parsed.port or 443
                sock = socket.create_connection((host, port), timeout=5)
                sock.close()
                return CheckResult(
                    name="iceberg_catalog",
                    status=CheckStatus.PASS,
                    message=f"Iceberg REST catalog reachable: {settings.iceberg_catalog_uri}",
                )
            except Exception as exc:
                return CheckResult(
                    name="iceberg_catalog",
                    status=CheckStatus.FAIL,
                    message=f"Cannot reach Iceberg REST catalog: {exc}",
                    fix="Check egress NetworkPolicy allows HTTPS to the catalog endpoint",
                )

        return CheckResult(
            name="iceberg_catalog",
            status=CheckStatus.PASS,
            message=f"Iceberg catalog: {settings.iceberg_catalog_type}",
        )

    # ══════════════════════════════════════════════════════════════════════
    # DUCKDB / LOCAL CHECKS
    # ══════════════════════════════════════════════════════════════════════

    async def _check_duckdb_extensions(self) -> CheckResult:
        """Verify required DuckDB extensions are available."""
        try:
            import duckdb
            conn = duckdb.connect(":memory:")
            loaded = []
            failed = []
            for ext in settings.extension_list:
                try:
                    conn.execute(f"LOAD '{ext}'")
                    loaded.append(ext)
                except Exception:
                    try:
                        conn.execute(f"INSTALL '{ext}'")
                        conn.execute(f"LOAD '{ext}'")
                        loaded.append(ext)
                    except Exception:
                        failed.append(ext)
            conn.close()

            if failed:
                return CheckResult(
                    name="duckdb_extensions",
                    status=CheckStatus.WARN,
                    message=f"Some DuckDB extensions failed to load: {', '.join(failed)}",
                    detail=f"Loaded: {', '.join(loaded)}",
                    fix="Pre-install extensions in the Dockerfile:\n"
                        "  RUN python -c \"import duckdb; conn = duckdb.connect(':memory:'); "
                        f"conn.execute(\\\"INSTALL '{failed[0]}'\\\")\"\n"
                        "Or check egress allows HTTPS to extensions.duckdb.org",
                )

            return CheckResult(
                name="duckdb_extensions",
                status=CheckStatus.PASS,
                message=f"DuckDB extensions loaded: {', '.join(loaded)}",
            )
        except Exception as exc:
            return CheckResult(
                name="duckdb_extensions",
                status=CheckStatus.FAIL,
                message=f"DuckDB extension check failed: {exc}",
            )

    # ══════════════════════════════════════════════════════════════════════
    # NETWORK POLICY ALIGNMENT
    # ══════════════════════════════════════════════════════════════════════

    async def _check_network_policy_alignment(self) -> CheckResult:
        """Check if the K8s API is accessible to read NetworkPolicy.

        If we can read the NetworkPolicy, compare it against what egress_generator
        says we need. If we can't (no RBAC), warn about it.
        """
        k8s_host = os.environ.get("KUBERNETES_SERVICE_HOST")
        if not k8s_host:
            return CheckResult(
                name="network_policy",
                status=CheckStatus.SKIP,
                message="Not in K8s — cannot validate NetworkPolicy",
            )

        sa_token_path = Path(K8S_SA_TOKEN_PATH)
        if not sa_token_path.exists():
            return CheckResult(
                name="network_policy",
                status=CheckStatus.SKIP,
                message="No SA token to call K8s API",
            )

        namespace = self._read_namespace()
        token = sa_token_path.read_text().strip()

        try:
            import httpx
            ca_path = K8S_SA_CA_PATH if Path(K8S_SA_CA_PATH).exists() else False
            k8s_port = os.environ.get("KUBERNETES_SERVICE_PORT", "443")
            url = f"https://{k8s_host}:{k8s_port}/apis/networking.k8s.io/v1/namespaces/{namespace}/networkpolicies"

            r = httpx.get(
                url,
                headers={"Authorization": f"Bearer {token}"},
                verify=ca_path if ca_path else False,
                timeout=5,
            )

            if r.status_code == 403:
                return CheckResult(
                    name="network_policy",
                    status=CheckStatus.WARN,
                    message="Cannot read NetworkPolicies (403 Forbidden) — SA lacks RBAC",
                    fix="To enable preflight NetworkPolicy validation, grant the SA:\n"
                        "  apiGroups: [networking.k8s.io]\n"
                        "  resources: [networkpolicies]\n"
                        "  verbs: [get, list]\n"
                        "This is optional — the sidecar works without it.",
                )

            if r.status_code != 200:
                return CheckResult(
                    name="network_policy",
                    status=CheckStatus.WARN,
                    message=f"K8s API returned {r.status_code} when reading NetworkPolicies",
                )

            policies = r.json().get("items", [])
            if not policies:
                return CheckResult(
                    name="network_policy",
                    status=CheckStatus.WARN,
                    message="No NetworkPolicies found in this namespace — egress may be unrestricted or blocked by a cluster-wide default",
                    fix="Run the egress generator to create the correct policy:\n"
                        "  GET /api/onboarding/egress/yaml",
                )

            # Check if any policy matches our pod
            matching = [p for p in policies if "dlsidecar" in json.dumps(p.get("spec", {}).get("podSelector", {}))]
            if matching:
                # Validate egress rules exist
                egress_rules = matching[0].get("spec", {}).get("egress", [])
                from dlsidecar.onboarding.egress_generator import generate_egress_rules
                needed = generate_egress_rules()
                needed_ports = set()
                for rule in needed:
                    for p in rule.get("ports", []):
                        needed_ports.add(p.get("port"))
                actual_ports = set()
                for rule in egress_rules:
                    for p in rule.get("ports", []):
                        actual_ports.add(p.get("port"))

                missing_ports = needed_ports - actual_ports
                if missing_ports:
                    return CheckResult(
                        name="network_policy",
                        status=CheckStatus.WARN,
                        message=f"NetworkPolicy exists but may be missing egress ports: {missing_ports}",
                        fix="Regenerate the NetworkPolicy with the egress generator:\n"
                            "  GET /api/onboarding/egress/yaml\n"
                            f"  Missing ports: {missing_ports}",
                    )

                return CheckResult(
                    name="network_policy",
                    status=CheckStatus.PASS,
                    message=f"NetworkPolicy found with {len(egress_rules)} egress rules — aligned with config",
                )

            return CheckResult(
                name="network_policy",
                status=CheckStatus.WARN,
                message=f"Found {len(policies)} NetworkPolicies but none match 'dlsidecar' pod selector",
                fix="Ensure your NetworkPolicy has podSelector.matchLabels.app: dlsidecar",
            )

        except ImportError:
            return CheckResult(name="network_policy", status=CheckStatus.SKIP, message="httpx not available")
        except Exception as exc:
            return CheckResult(
                name="network_policy",
                status=CheckStatus.WARN,
                message=f"Could not query K8s API: {exc}",
            )

    # ══════════════════════════════════════════════════════════════════════
    # RESOURCE / VOLUME CHECKS
    # ══════════════════════════════════════════════════════════════════════

    async def _check_resource_limits(self) -> CheckResult:
        """Check if memory limits are appropriate for DuckDB."""
        # Read cgroup memory limit (works in K8s containers)
        cgroup_paths = [
            "/sys/fs/cgroup/memory/memory.limit_in_bytes",  # cgroup v1
            "/sys/fs/cgroup/memory.max",                    # cgroup v2
        ]
        mem_limit = None
        for p in cgroup_paths:
            try:
                val = Path(p).read_text().strip()
                if val != "max" and int(val) < 2**62:
                    mem_limit = int(val)
                    break
            except Exception:
                continue

        if mem_limit is None:
            return CheckResult(
                name="resource_limits",
                status=CheckStatus.SKIP,
                message="Cannot determine container memory limit (not in cgroup)",
            )

        mem_limit_gb = mem_limit / (1024**3)
        duckdb_limit = settings.duckdb_memory_limit
        # Parse DuckDB limit
        duckdb_bytes = _parse_memory(duckdb_limit)
        duckdb_gb = duckdb_bytes / (1024**3) if duckdb_bytes else 0

        if duckdb_gb > mem_limit_gb * 0.9:
            return CheckResult(
                name="resource_limits",
                status=CheckStatus.WARN,
                message=f"DuckDB memory limit ({duckdb_limit}) is >90% of container limit ({mem_limit_gb:.1f}GB)",
                fix="Set DLS_DUCKDB_MEMORY_LIMIT to ~70% of the container memory limit to leave room for "
                    "Python, FastAPI, and other components. Example: container=4Gi -> DuckDB=3GB",
            )

        if duckdb_gb > mem_limit_gb:
            return CheckResult(
                name="resource_limits",
                status=CheckStatus.FAIL,
                message=f"DuckDB memory limit ({duckdb_limit}) EXCEEDS container limit ({mem_limit_gb:.1f}GB) — OOMKill risk",
                fix=f"Reduce DLS_DUCKDB_MEMORY_LIMIT or increase the pod memory limit to at least {duckdb_gb * 1.3:.0f}GB",
            )

        return CheckResult(
            name="resource_limits",
            status=CheckStatus.PASS,
            message=f"Memory: container={mem_limit_gb:.1f}GB, DuckDB={duckdb_limit} ({duckdb_gb/mem_limit_gb*100:.0f}% of limit)",
        )

    async def _check_shared_volume(self) -> CheckResult:
        """Check if the shared file path is writable (for Arrow IPC exchange)."""
        shared_path = Path(settings.duckdb_shared_file_path).parent
        if not shared_path.exists():
            return CheckResult(
                name="shared_volume",
                status=CheckStatus.WARN,
                message=f"Shared volume directory does not exist: {shared_path}",
                fix="Mount a shared emptyDir volume between the app container and the sidecar:\n"
                    "  volumes:\n"
                    "    - name: shared\n"
                    "      emptyDir: {}\n"
                    "  containers:\n"
                    "    - name: app\n"
                    "      volumeMounts:\n"
                    "        - name: shared\n"
                    f"          mountPath: {shared_path}",
            )

        # Test write
        test_file = shared_path / ".dlsidecar_health_check"
        try:
            test_file.write_text("ok")
            test_file.unlink()
            return CheckResult(
                name="shared_volume",
                status=CheckStatus.PASS,
                message=f"Shared volume writable: {shared_path}",
            )
        except PermissionError:
            return CheckResult(
                name="shared_volume",
                status=CheckStatus.FAIL,
                message=f"Shared volume not writable: {shared_path}",
                fix="Check the volume mount permissions and securityContext.runAsUser",
            )
        except Exception as exc:
            return CheckResult(
                name="shared_volume",
                status=CheckStatus.WARN,
                message=f"Shared volume check failed: {exc}",
            )

    async def _check_tls_certificates(self) -> CheckResult:
        """Check TLS certificate files if configured."""
        if not settings.starburst_tls_enabled:
            return CheckResult(name="tls_certs", status=CheckStatus.SKIP, message="TLS not enabled")

        if not settings.starburst_tls_cert_path:
            return CheckResult(
                name="tls_certs",
                status=CheckStatus.PASS,
                message="TLS enabled with system CA bundle (no custom cert)",
            )

        cert_path = Path(settings.starburst_tls_cert_path)
        if not cert_path.exists():
            return CheckResult(
                name="tls_certs",
                status=CheckStatus.FAIL,
                message=f"TLS cert file not found: {settings.starburst_tls_cert_path}",
                fix="Mount the CA certificate bundle:\n"
                    "  volumeMounts:\n"
                    f"    - name: tls-certs\n"
                    f"      mountPath: {cert_path.parent}\n"
                    "  volumes:\n"
                    "    - name: tls-certs\n"
                    "      secret:\n"
                    "        secretName: starburst-ca-cert",
            )

        # Basic validation
        content = cert_path.read_text()
        if "BEGIN CERTIFICATE" not in content:
            return CheckResult(
                name="tls_certs",
                status=CheckStatus.FAIL,
                message=f"File at {settings.starburst_tls_cert_path} does not appear to be a PEM certificate",
            )

        return CheckResult(
            name="tls_certs",
            status=CheckStatus.PASS,
            message=f"TLS cert loaded: {settings.starburst_tls_cert_path} ({len(content)} bytes)",
        )

    # ── Helpers ───────────────────────────────────────────────────────────

    def _read_namespace(self) -> str:
        ns_path = Path(K8S_SA_NAMESPACE_PATH)
        if ns_path.exists():
            return ns_path.read_text().strip()
        return os.environ.get("POD_NAMESPACE", "unknown")


def _parse_memory(mem_str: str) -> int | None:
    """Parse memory strings like '4GB', '512MB', '2Gi'."""
    m = re.match(r"(\d+(?:\.\d+)?)\s*(GB|GiB|Gi|MB|MiB|Mi|KB|KiB|Ki|B)?", mem_str, re.IGNORECASE)
    if not m:
        return None
    val = float(m.group(1))
    unit = (m.group(2) or "B").upper()
    multipliers = {"B": 1, "KB": 1024, "KI": 1024, "KIB": 1024,
                   "MB": 1024**2, "MI": 1024**2, "MIB": 1024**2,
                   "GB": 1024**3, "GI": 1024**3, "GIB": 1024**3}
    return int(val * multipliers.get(unit, 1))
