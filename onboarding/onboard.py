#!/usr/bin/env python3
"""Onboard a model repo onto the SML CI/CD pipeline.

    python3 onboard.py --slug my-model --repo owner/my-model \\
        [--model-clone /path/to/local/clone] [--domain atscale-se-demo.com]

Does, in order:
  1. Registers the repo as a catalog repository on dev/qa/prod AtScale
     (POST /api/repo, OAuth password grant — same pattern as
     config-export/repos/setup_repos.py). Needs per-instance env creds or
     kubectl access (printed below when missing).
  2. Prints the pipelines.yaml snippet to add (via PR to THIS repo).
  3. If --model-clone is given: writes the 3 stub workflows into it
     (commit + PR them yourself, or let your agent do it).
  4. Prints the gh commands for Actions vars/secrets + branch protection.

Idempotent — safe to re-run.
"""
import argparse
import json
import os
import ssl
import subprocess
import sys
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
CTX = ssl.create_default_context()
CTX.check_hostname = False
CTX.verify_mode = ssl.CERT_NONE  # demo self-signed certs

SUBDOMAINS = {"dev": "atscale-dev", "qa": "atscale-qa", "prod": "atscale"}


def token(host, client_secret, user, password):
    data = urllib.parse.urlencode({
        "grant_type": "password", "client_id": "atscale-public-api",
        "client_secret": client_secret, "username": user, "password": password,
    }).encode()
    req = urllib.request.Request(
        f"{host}/auth/realms/atscale/protocol/openid-connect/token", data=data)
    with urllib.request.urlopen(req, context=CTX, timeout=30) as r:
        return json.load(r)["access_token"]


def kubectl_secret(ns, secret, key):
    out = subprocess.run(
        ["kubectl", "get", "secret", secret, "-n", ns, "-o",
         f"jsonpath={{.data.{key}}}"], capture_output=True, text=True)
    if out.returncode:
        return None
    import base64
    return base64.b64decode(out.stdout).decode()


def register(host, ns, name, url):
    cs = kubectl_secret(ns, "atscale-kc-clients", "publicApi")
    au = kubectl_secret(ns, "atscale-kc-users", "atscaleAdmin")
    ap = kubectl_secret(ns, "atscale-kc-users", "atscaleAdminPassword")
    if not all([cs, au, ap]):
        print(f"  [{host}] SKIPPED — no kubectl access to ns {ns} "
              f"(run setup_repos.py per instance instead)")
        return
    tok = token(host, cs, au, ap)
    body = json.dumps({"name": name, "url": url, "type": "catalog",
                       "defaultBranch": "main"}).encode()
    req = urllib.request.Request(f"{host}/api/repo", data=body, method="POST",
                                 headers={"Authorization": f"Bearer {tok}",
                                          "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, context=CTX, timeout=30) as r:
            print(f"  [{host}] {r.status} {json.load(r).get('id')}")
    except urllib.error.HTTPError as e:
        print(f"  [{host}] HTTP {e.code}: {e.read()[:120]}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--slug", required=True)
    ap.add_argument("--repo", required=True, help="owner/name on GitHub")
    ap.add_argument("--name", help="AtScale repo-connection name (default: slug)")
    ap.add_argument("--model-clone", help="local clone to write stub workflows into")
    ap.add_argument("--domain", default="atscale-se-demo.com")
    args = ap.parse_args()
    url = f"https://github.com/{args.repo}.git"

    print(f"== 1. Registering '{args.name or args.slug}' on the AtScale instances")
    for sub, ns in SUBDOMAINS.items():
        register(f"https://{sub}.{args.domain}", ns, args.name or args.slug, url)

    print(f"\n== 2. Add to airflow/dags/pipelines.yaml (PR to atscale-cicd-pipeline):")
    print(f"""  - slug: {args.slug}
    enabled: true
    repo: {args.repo}
    dev_catalog_prefix: {args.slug}""")

    if args.model_clone:
        dst = os.path.join(args.model_clone, ".github", "workflows")
        os.makedirs(dst, exist_ok=True)
        for f in ("validate.yml", "deploy-qa.yml", "release-live.yml"):
            content = open(os.path.join(HERE, "templates", f)).read()
            open(os.path.join(dst, f), "w").write(
                content.replace("__SLUG__", args.slug))
        print(f"\n== 3. Stub workflows written to {dst} — commit + PR them.")
    else:
        print("\n== 3. Copy onboarding/templates/*.yml into the model repo's "
              ".github/workflows/, replacing __SLUG__ with your slug.")

    print(f"""\n== 4. GitHub repo config (once):
  gh variable set AIRFLOW_URL     -R {args.repo} -b "http://airflow.{args.domain}:8080"
  gh secret   set AIRFLOW_USER    -R {args.repo} -b "admin"
  gh secret   set AIRFLOW_PASSWORD -R {args.repo} -b "<the Airflow password>"
  # branch protection: require the 'validate' check + PR on main
  # brand-new repo? open the Actions tab once in the browser to enable workflows
  # private repo? configure git credentials on each AtScale instance""")


if __name__ == "__main__":
    main()
