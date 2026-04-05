"""
Environment promoter — generates config for different environments,
diffs dev vs staging/prod, and exports in multiple formats.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import yaml

from dlsidecar.config import ENV_VAR_ROLES, EnvVarRole, Settings, settings

logger = logging.getLogger("dlsidecar.onboarding.promoter")


def get_config_dict(cfg: Settings | None = None) -> dict[str, Any]:
    """Export current config as a flat dict with DLS_ prefix."""
    cfg = cfg or settings
    result = {}
    for field_name, field_info in cfg.model_fields.items():
        env_name = f"DLS_{field_name.upper()}"
        value = getattr(cfg, field_name)
        result[env_name] = value
    return result


def classify_vars(cfg: Settings | None = None) -> dict[str, list[dict[str, Any]]]:
    """Classify all env vars by their role for the promotion UI."""
    config = get_config_dict(cfg)
    classified = {"core": [], "secret": [], "env_specific": [], "optional": [], "feature_flag": []}
    for env_name, value in config.items():
        role = ENV_VAR_ROLES.get(env_name, EnvVarRole.OPTIONAL)
        classified[role.value].append({"name": env_name, "value": value, "role": role.value})
    return classified


def diff_environments(
    source_cfg: Settings,
    target_values: dict[str, Any],
) -> list[dict[str, Any]]:
    """Diff two environments and highlight changes."""
    source = get_config_dict(source_cfg)
    diffs = []
    for env_name, source_val in source.items():
        target_val = target_values.get(env_name, source_val)
        role = ENV_VAR_ROLES.get(env_name, EnvVarRole.OPTIONAL)
        changed = str(source_val) != str(target_val)
        diffs.append({
            "name": env_name,
            "source_value": source_val,
            "target_value": target_val,
            "role": role.value,
            "changed": changed,
            "needs_attention": role in (EnvVarRole.ENV_SPECIFIC, EnvVarRole.SECRET) and not changed,
        })
    return diffs


def validate_promotion(target_values: dict[str, Any]) -> list[dict[str, str]]:
    """Run promotion checklist validation."""
    issues = []

    mode = target_values.get("DLS_MODE", "dev")
    if mode == "dev":
        issues.append({"check": "DLS_MODE", "status": "fail", "message": "DLS_MODE is still 'dev'"})
    else:
        issues.append({"check": "DLS_MODE", "status": "pass", "message": f"DLS_MODE set to '{mode}'"})

    # Check TLS
    tls = target_values.get("DLS_STARBURST_TLS_ENABLED", True)
    if not tls:
        issues.append({"check": "TLS", "status": "fail", "message": "Starburst TLS is disabled"})
    else:
        issues.append({"check": "TLS", "status": "pass", "message": "Starburst TLS enabled"})

    # Check UI disabled in prod
    ui = target_values.get("DLS_ONBOARDING_UI_ENABLED", False)
    if ui and mode == "prod":
        issues.append({"check": "UI", "status": "fail", "message": "Onboarding UI should be disabled in prod"})
    else:
        issues.append({"check": "UI", "status": "pass", "message": "Onboarding UI correctly configured"})

    # Check secrets not hardcoded
    for env_name, role in ENV_VAR_ROLES.items():
        if role == EnvVarRole.SECRET:
            val = target_values.get(env_name, "")
            if val and val not in ("", "${" + env_name + "}"):
                issues.append({
                    "check": env_name,
                    "status": "warn",
                    "message": f"{env_name} appears hardcoded — should be injected via ESO/Vault",
                })

    # Check env-specific vars changed from dev defaults
    source = get_config_dict()
    for env_name, role in ENV_VAR_ROLES.items():
        if role == EnvVarRole.ENV_SPECIFIC:
            if target_values.get(env_name) == source.get(env_name):
                issues.append({
                    "check": env_name,
                    "status": "warn",
                    "message": f"{env_name} has same value as dev — verify this is intentional",
                })

    return issues


def export_dotenv(values: dict[str, Any]) -> str:
    lines = []
    for k, v in sorted(values.items()):
        role = ENV_VAR_ROLES.get(k, EnvVarRole.OPTIONAL)
        if role == EnvVarRole.SECRET:
            lines.append(f"# {k}=  # INJECT VIA ESO/VAULT")
        else:
            comment = ""
            if role == EnvVarRole.ENV_SPECIFIC:
                comment = "  # CHANGE FOR STAGING/PROD"
            lines.append(f"{k}={v}{comment}")
    return "\n".join(lines)


def export_configmap_yaml(values: dict[str, Any], name: str = "dlsidecar-config", namespace: str = "default") -> str:
    data = {}
    for k, v in sorted(values.items()):
        role = ENV_VAR_ROLES.get(k, EnvVarRole.OPTIONAL)
        if role != EnvVarRole.SECRET:
            data[k] = str(v)
    cm = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": name, "namespace": namespace},
        "data": data,
    }
    return yaml.dump(cm, default_flow_style=False, sort_keys=False)


def export_secret_yaml(values: dict[str, Any], name: str = "dlsidecar-secrets", namespace: str = "default") -> str:
    import base64

    data = {}
    for k, v in sorted(values.items()):
        role = ENV_VAR_ROLES.get(k, EnvVarRole.OPTIONAL)
        if role == EnvVarRole.SECRET and v:
            data[k] = base64.b64encode(str(v).encode()).decode()
    secret = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": name, "namespace": namespace},
        "type": "Opaque",
        "data": data,
    }
    return yaml.dump(secret, default_flow_style=False, sort_keys=False)


def export_helm_values(values: dict[str, Any]) -> str:
    helm = {"dlsidecar": {"config": {}, "secrets": {}}}
    for k, v in sorted(values.items()):
        role = ENV_VAR_ROLES.get(k, EnvVarRole.OPTIONAL)
        key = k.replace("DLS_", "").lower()
        if role == EnvVarRole.SECRET:
            helm["dlsidecar"]["secrets"][key] = str(v)
        else:
            helm["dlsidecar"]["config"][key] = v
    return yaml.dump(helm, default_flow_style=False, sort_keys=False)


def export_terraform_tfvars(values: dict[str, Any]) -> str:
    lines = []
    for k, v in sorted(values.items()):
        role = ENV_VAR_ROLES.get(k, EnvVarRole.OPTIONAL)
        tf_name = k.lower()
        if isinstance(v, bool):
            lines.append(f'{tf_name} = {"true" if v else "false"}')
        elif isinstance(v, (int, float)):
            lines.append(f"{tf_name} = {v}")
        else:
            lines.append(f'{tf_name} = "{v}"')
        if role == EnvVarRole.SECRET:
            lines[-1] += "  # SENSITIVE — use Vault provider"
    return "\n".join(lines)


def export_external_secret(
    values: dict[str, Any],
    name: str = "dlsidecar-external-secrets",
    namespace: str = "default",
    secret_store: str = "aws-secrets-manager",
) -> str:
    """Generate ExternalSecret CR for external-secrets-operator."""
    data_refs = []
    for k in sorted(values.keys()):
        role = ENV_VAR_ROLES.get(k, EnvVarRole.OPTIONAL)
        if role == EnvVarRole.SECRET:
            data_refs.append({
                "secretKey": k,
                "remoteRef": {
                    "key": f"dlsidecar/{namespace}/{k.lower()}",
                    "property": "value",
                },
            })

    es = {
        "apiVersion": "external-secrets.io/v1beta1",
        "kind": "ExternalSecret",
        "metadata": {"name": name, "namespace": namespace},
        "spec": {
            "refreshInterval": "1h",
            "secretStoreRef": {"name": secret_store, "kind": "ClusterSecretStore"},
            "target": {"name": f"{name}-target", "creationPolicy": "Owner"},
            "data": data_refs,
        },
    }
    return yaml.dump(es, default_flow_style=False, sort_keys=False)
