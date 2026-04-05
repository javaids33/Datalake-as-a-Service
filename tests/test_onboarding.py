"""Test onboarding: egress generator, promoter, validator."""

import os

os.environ.setdefault("DLS_MODE", "dev")

import pytest

from dlsidecar.config import Settings
from dlsidecar.onboarding.egress_generator import (
    generate_egress_rules,
    generate_egress_summary,
    generate_network_policy_yaml,
)
from dlsidecar.onboarding.promoter import (
    classify_vars,
    export_configmap_yaml,
    export_dotenv,
    export_external_secret,
    export_helm_values,
    export_secret_yaml,
    get_config_dict,
    validate_promotion,
)


class TestEgressGenerator:
    def test_s3_rule_generated(self):
        cfg = Settings(s3_buckets="my-bucket", mode="dev")
        rules = generate_egress_rules(cfg)
        s3_rules = [r for r in rules if "S3" in r.get("_comment", "")]
        assert len(s3_rules) > 0

    def test_starburst_rule_generated(self):
        cfg = Settings(starburst_enabled=True, starburst_host="starburst.test", mode="dev")
        rules = generate_egress_rules(cfg)
        sb_rules = [r for r in rules if "Starburst" in r.get("_comment", "")]
        assert len(sb_rules) > 0

    def test_hms_rule_generated(self):
        cfg = Settings(hms_enabled=True, mode="dev")
        rules = generate_egress_rules(cfg)
        hms_rules = [r for r in rules if "Metastore" in r.get("_comment", "")]
        assert len(hms_rules) > 0

    def test_kafka_rule_generated(self):
        cfg = Settings(kafka_enabled=True, kafka_brokers="kafka:9092", mode="dev")
        rules = generate_egress_rules(cfg)
        kafka_rules = [r for r in rules if "Kafka" in r.get("_comment", "")]
        assert len(kafka_rules) > 0

    def test_dns_always_present(self):
        cfg = Settings(mode="dev")
        rules = generate_egress_rules(cfg)
        dns_rules = [r for r in rules if "DNS" in r.get("_comment", "")]
        assert len(dns_rules) > 0

    def test_no_starburst_when_disabled(self):
        cfg = Settings(starburst_enabled=False, mode="dev")
        rules = generate_egress_rules(cfg)
        sb_rules = [r for r in rules if "Starburst" in r.get("_comment", "")]
        assert len(sb_rules) == 0

    def test_network_policy_yaml(self):
        cfg = Settings(s3_buckets="my-bucket", mode="dev")
        yaml_str = generate_network_policy_yaml(cfg)
        assert "NetworkPolicy" in yaml_str
        assert "dlsidecar-egress" in yaml_str

    def test_egress_summary(self):
        cfg = Settings(s3_buckets="my-bucket", mode="dev")
        summary = generate_egress_summary(cfg)
        assert len(summary) > 0
        assert all("description" in s for s in summary)

    def test_postgres_egress(self):
        cfg = Settings(postgres_enabled=True, mode="dev")
        rules = generate_egress_rules(cfg)
        pg_rules = [r for r in rules if "PostgreSQL" in r.get("_comment", "")]
        assert len(pg_rules) > 0
        assert pg_rules[0]["ports"][0]["port"] == 5432

    def test_snowflake_egress(self):
        cfg = Settings(snowflake_enabled=True, snowflake_account="test", mode="dev")
        rules = generate_egress_rules(cfg)
        sf_rules = [r for r in rules if "Snowflake" in r.get("_comment", "")]
        assert len(sf_rules) > 0


class TestPromoter:
    def test_get_config_dict(self):
        d = get_config_dict()
        assert "DLS_MODE" in d
        assert "DLS_TENANT_ID" in d

    def test_classify_vars(self):
        classified = classify_vars()
        assert "core" in classified
        assert "secret" in classified
        assert "env_specific" in classified

    def test_validate_promotion_dev_mode_fails(self):
        issues = validate_promotion({"DLS_MODE": "dev"})
        mode_issues = [i for i in issues if i["check"] == "DLS_MODE"]
        assert any(i["status"] == "fail" for i in mode_issues)

    def test_validate_promotion_prod_passes(self):
        issues = validate_promotion({"DLS_MODE": "prod", "DLS_STARBURST_TLS_ENABLED": True, "DLS_ONBOARDING_UI_ENABLED": False})
        mode_issues = [i for i in issues if i["check"] == "DLS_MODE"]
        assert any(i["status"] == "pass" for i in mode_issues)

    def test_export_dotenv(self):
        result = export_dotenv({"DLS_MODE": "prod", "DLS_TENANT_ID": "test"})
        assert "DLS_MODE=prod" in result

    def test_export_configmap_yaml(self):
        result = export_configmap_yaml({"DLS_MODE": "prod"})
        assert "ConfigMap" in result

    def test_export_secret_yaml(self):
        result = export_secret_yaml({"DLS_POSTGRES_DSN": "postgres://..."})
        assert "Secret" in result

    def test_export_helm_values(self):
        result = export_helm_values({"DLS_MODE": "prod"})
        assert "dlsidecar" in result

    def test_export_external_secret(self):
        result = export_external_secret({"DLS_POSTGRES_DSN": "x"})
        assert "ExternalSecret" in result
