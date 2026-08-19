"""Shared framework code for the SML CI/CD pipeline DAG factory.

All environment-touching work runs in short-lived Kubernetes pods (node:20
image) in the `airflow` namespace. Each deploy pod mints its own AtScale
public-API token at runtime from OAuth credentials, so there are no
long-lived tokens to expire or get invalidated (AtScale public tokens are
single-active per user). Kubernetes secrets (namespace `airflow`):

  atscale-dev-oauth / atscale-qa-oauth / atscale-live-oauth
      keys: clientSecret, adminUser, adminPassword
  github-token                                          key: token

Per-repo configuration lives in pipelines.yaml (same folder) — see
generate_dags.py. Instance-level overrides via Airflow Variables:
  atscale_domain   target AtScale trio (default from pipelines.yaml defaults)
  github_token     PAT for commit statuses / machine PRs (Variable, not secret)
"""
import os

import yaml
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret

_HERE = os.path.dirname(os.path.abspath(__file__))

# Subdomain per pipeline env; hostname = https://<subdomain>.<atscale_domain>
ATSCALE_SUBDOMAINS = {"dev": "dev", "qa": "qa", "live": "prod"}


def load_registry() -> dict:
    """pipelines.yaml with defaults resolved onto each enabled entry."""
    with open(os.path.join(_HERE, "pipelines.yaml")) as f:
        reg = yaml.safe_load(f)
    defaults = reg.get("defaults", {})
    pipelines = []
    for p in reg.get("pipelines", []):
        if not p.get("enabled", False):
            continue
        cfg = {**defaults, **p}
        cfg.setdefault("default_branch", "main")
        cfg.setdefault("dev_catalog_prefix", cfg["slug"])
        pipelines.append(cfg)
    return {"defaults": defaults, "pipelines": pipelines}


def repo_var(key: str, default):
    """Read an Airflow Variable at TASK runtime (never at DAG-parse time)."""
    try:
        try:
            from airflow.sdk import Variable          # Airflow 3
        except ImportError:                            # pragma: no cover
            from airflow.models import Variable        # Airflow 2 fallback
        try:
            return Variable.get(key, default=default)
        except TypeError:                              # pragma: no cover
            return Variable.get(key, default_var=default)
    except Exception:                                  # noqa: BLE001
        return default


def domain_tmpl(cfg: dict) -> str:
    """Jinja form of the domain for templated fields — resolved at task
    runtime, so DAG parsing never hits the metadata DB."""
    return "{{ var.value.get('atscale_domain', '" + cfg["atscale_domain"] + "') }}"


def atscale_host(cfg: dict, env: str) -> str:
    """Base URL for env, for use INSIDE task code (runtime Variable lookup)."""
    domain = repo_var("atscale_domain", cfg["atscale_domain"])
    return f"https://{ATSCALE_SUBDOMAINS[env]}.{domain}"


def oauth_secrets(env: str) -> list[Secret]:
    """OAuth creds the pod uses to mint a fresh public-API token at runtime."""
    name = f"atscale-{env}-oauth"
    return [
        Secret("env", "ATSCALE_CLIENT_SECRET", name, "clientSecret"),
        Secret("env", "ATSCALE_ADMIN_USER", name, "adminUser"),
        Secret("env", "ATSCALE_ADMIN_PASSWORD", name, "adminPassword"),
    ]


github_token_secret = Secret("env", "GITHUB_TOKEN", "github-token", "token")


def deploy_pod(cfg: dict, task_id: str, env: str, git_ref: str,
               catalog_name: str | None = None,
               catalog_label: str | None = None) -> KubernetesPodOperator:
    """Clone cfg['repo'] at git_ref and `sml-cli atscale-deploy` it to env.

    Mints a short-lived public-API token in-pod: OAuth password grant ->
    /api/auth/token/public -> ATSCALE_API_TOKEN. No stored deploy token.
    """
    host = f"https://{ATSCALE_SUBDOMAINS[env]}.{domain_tmpl(cfg)}"
    sml_cli = cfg["sml_cli"]
    # private repos: clone with the github-token secret (PAT with repo read)
    private = cfg.get("private", False)
    clone_url = (f"https://x-access-token:${{GITHUB_TOKEN}}@github.com/{cfg['repo']}.git"
                 if private else f"https://github.com/{cfg['repo']}.git")
    flags = ""
    if catalog_name:
        flags += f' --catalog-name="{catalog_name}"'
    if catalog_label:
        flags += f' --catalog-label="{catalog_label}"'
    script = f"""set -euo pipefail
HOST="{host}"
OT=$(curl -sk -X POST "$HOST/auth/realms/atscale/protocol/openid-connect/token" \
  -d grant_type=password -d client_id=atscale-public-api \
  --data-urlencode client_secret="$ATSCALE_CLIENT_SECRET" \
  --data-urlencode username="$ATSCALE_ADMIN_USER" \
  --data-urlencode password="$ATSCALE_ADMIN_PASSWORD" \
  | sed -n 's/.*"access_token":"\\([^"]*\\)".*/\\1/p')
export ATSCALE_API_TOKEN=$(curl -sk -X POST -H "Authorization: Bearer $OT" \
  "$HOST/api/auth/token/public" | sed -n 's/.*"token":"\\([^"]*\\)".*/\\1/p')
test -n "$ATSCALE_API_TOKEN" || {{ echo "failed to mint public token"; exit 1; }}
export ATSCALE_API_URL="$HOST/api"
git clone "{clone_url}" /work && cd /work
git checkout {git_ref}
npx -y {sml_cli} install .
npx -y {sml_cli} validate . | tee /tmp/validate.log
grep -q "Validation SUCCESSFUL" /tmp/validate.log
npx -y {sml_cli} atscale-deploy .{flags}
"""
    return KubernetesPodOperator(
        task_id=task_id,
        name=f"{task_id.replace('_', '-')}-{cfg['slug']}"[:63],
        namespace="airflow",
        in_cluster=True,
        image=cfg["node_image"],
        cmds=["bash", "-c"],
        arguments=[script],
        env_vars={"NODE_TLS_REJECT_UNAUTHORIZED": "0"},  # self-signed certs
        secrets=oauth_secrets(env) + ([github_token_secret] if private else []),
        get_logs=True,
        is_delete_operator_pod=True,
        startup_timeout_seconds=300,
    )


def github_status(repo: str, sha: str, state: str, context: str,
                  description: str):
    """POST a commit status back to GitHub. Never fatal — a status-post hiccup
    must not fail an otherwise-successful deploy."""
    import requests
    try:
        token = repo_var("github_token", None)
        if not token:
            print("Airflow variable 'github_token' not set — skipping status post.")
            return
        r = requests.post(
            f"https://api.github.com/repos/{repo}/statuses/{sha}",
            headers={"Authorization": f"Bearer {token}",
                     "Accept": "application/vnd.github+json"},
            json={"state": state, "context": context,
                  "description": description[:140]},
            timeout=30,
        )
        print(f"GitHub status {context}={state} for {sha}: HTTP {r.status_code}")
    except Exception as exc:  # noqa: BLE001
        print(f"GitHub status post failed (non-fatal): {exc}")
