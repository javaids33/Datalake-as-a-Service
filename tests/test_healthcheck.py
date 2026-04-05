"""Test preflight health checker — validates all check functions."""

import os

os.environ.setdefault("DLS_MODE", "dev")

import pytest

from dlsidecar.healthcheck.preflight import (
    CheckResult,
    CheckStatus,
    PreflightChecker,
    PreflightReport,
    _parse_memory,
)


class TestPreflightReport:
    def test_empty_report_is_healthy(self):
        r = PreflightReport()
        assert r.healthy is True

    def test_failed_check_makes_unhealthy(self):
        r = PreflightReport()
        r.add(CheckResult(name="test", status=CheckStatus.FAIL, message="broken"))
        assert r.healthy is False
        assert r.failed == 1

    def test_warn_is_still_healthy(self):
        r = PreflightReport()
        r.add(CheckResult(name="test", status=CheckStatus.WARN, message="careful"))
        assert r.healthy is True
        assert r.warned == 1

    def test_to_dict_structure(self):
        r = PreflightReport(timestamp="2025-01-01", pod_name="test-pod", namespace="default")
        r.add(CheckResult(name="k8s", status=CheckStatus.PASS, message="ok"))
        r.add(CheckResult(name="s3", status=CheckStatus.FAIL, message="no access", fix="add IAM"))
        d = r.to_dict()
        assert d["summary"]["passed"] == 1
        assert d["summary"]["failed"] == 1
        assert d["summary"]["healthy"] is False
        assert len(d["checks"]) == 2
        assert d["checks"][1]["fix"] == "add IAM"

    def test_counters(self):
        r = PreflightReport()
        r.add(CheckResult(name="a", status=CheckStatus.PASS, message=""))
        r.add(CheckResult(name="b", status=CheckStatus.WARN, message=""))
        r.add(CheckResult(name="c", status=CheckStatus.FAIL, message=""))
        r.add(CheckResult(name="d", status=CheckStatus.SKIP, message=""))
        assert r.passed == 1
        assert r.warned == 1
        assert r.failed == 1
        assert r.skipped == 1


class TestParseMemory:
    def test_gb(self):
        assert _parse_memory("4GB") == 4 * 1024**3

    def test_gi(self):
        assert _parse_memory("4Gi") == 4 * 1024**3

    def test_mb(self):
        assert _parse_memory("512MB") == 512 * 1024**2

    def test_bare_number(self):
        assert _parse_memory("1024") == 1024

    def test_invalid(self):
        assert _parse_memory("lots") is None


class TestPreflightChecks:
    """Test individual check functions in local (non-K8s) mode."""

    @pytest.mark.asyncio
    async def test_k8s_environment_local(self):
        checker = PreflightChecker()
        result = await checker._check_k8s_environment()
        # Should be SKIP when not in K8s
        assert result.status in (CheckStatus.SKIP, CheckStatus.PASS, CheckStatus.WARN)

    @pytest.mark.asyncio
    async def test_serviceaccount_local(self):
        checker = PreflightChecker()
        result = await checker._check_serviceaccount()
        assert result.status in (CheckStatus.SKIP, CheckStatus.PASS)

    @pytest.mark.asyncio
    async def test_irsa_no_buckets(self):
        checker = PreflightChecker()
        result = await checker._check_irsa_annotation()
        # With default empty buckets in dev mode, should skip or warn
        assert result.status in (CheckStatus.SKIP, CheckStatus.WARN)

    @pytest.mark.asyncio
    async def test_env_vars_dev_mode(self):
        checker = PreflightChecker()
        result = await checker._check_env_vars_loaded()
        # Dev mode with defaults should at least warn
        assert result.status in (CheckStatus.PASS, CheckStatus.WARN)

    @pytest.mark.asyncio
    async def test_dns_resolution_runs(self):
        checker = PreflightChecker()
        result = await checker._check_dns_resolution()
        # In local dev, DNS may fail for K8s-only hostnames — that's expected
        assert result.status in (CheckStatus.SKIP, CheckStatus.PASS, CheckStatus.FAIL)

    @pytest.mark.asyncio
    async def test_s3_no_buckets(self):
        checker = PreflightChecker()
        result = await checker._check_s3_connectivity()
        assert result.status == CheckStatus.SKIP

    @pytest.mark.asyncio
    async def test_starburst_no_host(self):
        checker = PreflightChecker()
        result = await checker._check_starburst_connectivity()
        # Dev mode with no host should warn, not fail
        assert result.status in (CheckStatus.WARN, CheckStatus.SKIP)

    @pytest.mark.asyncio
    async def test_kafka_disabled(self):
        checker = PreflightChecker()
        result = await checker._check_kafka_connectivity()
        assert result.status == CheckStatus.SKIP

    @pytest.mark.asyncio
    async def test_duckdb_extensions(self):
        checker = PreflightChecker()
        result = await checker._check_duckdb_extensions()
        # Should load at least some extensions
        assert result.status in (CheckStatus.PASS, CheckStatus.WARN)

    @pytest.mark.asyncio
    async def test_network_policy_local(self):
        checker = PreflightChecker()
        result = await checker._check_network_policy_alignment()
        # Not in K8s, should skip
        assert result.status == CheckStatus.SKIP

    @pytest.mark.asyncio
    async def test_tls_not_configured(self):
        checker = PreflightChecker()
        result = await checker._check_tls_certificates()
        # TLS enabled but no custom cert path → uses system CA
        assert result.status in (CheckStatus.PASS, CheckStatus.SKIP)

    @pytest.mark.asyncio
    async def test_full_preflight_runs(self):
        """Smoke test: the full preflight suite completes without crashing."""
        checker = PreflightChecker()
        report = await checker.run()
        assert len(report.checks) >= 10
        assert report.passed + report.warned + report.failed + report.skipped == len(report.checks)
