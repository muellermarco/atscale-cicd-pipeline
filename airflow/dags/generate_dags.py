"""DAG factory — one set of pipeline DAGs per entry in pipelines.yaml.

Deploy DAGs (deploy_dev/deploy_qa/release_live) start UNPAUSED; machine DAGs
(baseline_refresh/usage_trace/nightly) start paused — they need the
`github-token` secret / SQL Variables before their pods can run.
"""
from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret

from sml_pipeline_common import (atscale_host, deploy_pod, github_status,
                                 github_token_secret, load_registry)

START = datetime(2026, 8, 1)


def build_deploy_dev(cfg):
    prefix = cfg["dev_catalog_prefix"]

    @dag(dag_id=f"deploy_dev__{cfg['slug']}", schedule=None, start_date=START,
         catchup=False, max_active_runs=1, is_paused_upon_creation=False,
         tags=["sml-pipeline", cfg["slug"]],
         doc_md=f"Deploy a feature branch of {cfg['repo']} to the dev instance "
                f"as its own catalog ({prefix}-<branch>).",
         params={"branch": cfg["default_branch"]})
    def _dag():
        deploy_pod(cfg, task_id="deploy_branch_to_dev", env="dev",
                   git_ref="{{ params.branch }}",
                   catalog_name=prefix + "-{{ params.branch | replace('/', '-') }}",
                   catalog_label=prefix + " ({{ params.branch }})")
    return _dag()


def build_deploy_qa(cfg):
    repo, slug = cfg["repo"], cfg["slug"]

    @dag(dag_id=f"deploy_qa__{slug}", schedule=None, start_date=START,
         catchup=False, max_active_runs=1, is_paused_upon_creation=False,
         tags=["sml-pipeline", slug],
         doc_md=f"Deploy {repo}@main to the qa instance on merge (triggered "
                f"by the deploy-qa GitHub Action with conf {{'sha': …}}).")
    def _dag():
        deploy = deploy_pod(cfg, task_id="deploy_main_to_qa", env="qa",
                            git_ref="{{ dag_run.conf.get('sha', '"
                                    + cfg["default_branch"] + "') }}",
                            branch_suffix=cfg["default_branch"])

        @task
        def smoke_check():
            import requests
            r = requests.get(atscale_host(cfg, "qa"), verify=False, timeout=30)
            assert r.status_code < 500, f"QA unhealthy: HTTP {r.status_code}"
            print(f"QA responded with HTTP {r.status_code}")

        @task(trigger_rule="all_success")
        def report_success(**ctx):
            sha = (ctx["dag_run"].conf or {}).get("sha")
            if sha:
                github_status(repo, sha, "success", "atscale/deploy-qa",
                              "Deploy to QA succeeded")

        @task(trigger_rule="one_failed")
        def report_failure(**ctx):
            sha = (ctx["dag_run"].conf or {}).get("sha")
            if sha:
                github_status(repo, sha, "failure", "atscale/deploy-qa",
                              "Deploy to QA failed")

        smoke = smoke_check()
        deploy >> smoke
        smoke >> report_success()
        [deploy, smoke] >> report_failure()
    return _dag()


def build_release_live(cfg):
    @dag(dag_id=f"release_live__{cfg['slug']}", schedule=None, start_date=START,
         catchup=False, max_active_runs=1, is_paused_upon_creation=False,
         tags=["sml-pipeline", cfg["slug"]],
         doc_md=f"Deploy a signed-off tag of {cfg['repo']} to the live "
                f"instance. Rollback = re-run with the previous tag.",
         params={"tag": ""})
    def _dag():
        deploy = deploy_pod(cfg, task_id="deploy_tag_to_live", env="live",
                            git_ref="{{ dag_run.conf.get('tag') or params.tag }}",
                            branch_suffix=cfg["default_branch"])
        if cfg.get("openmetadata"):
            # optional registry block: openmetadata: {warehouse_service: <OM service>, packages: [owner/repo]}
            deploy >> build_om_sync(cfg)
    return _dag()


OM_SCRIPT = """set -euo pipefail
pip install -q pyyaml requests
git clone -q "https://github.com/${GITHUB_REPO}.git" /work/main && git -C /work/main checkout "${GIT_REF}"
ARGS="--repo /work/main"
for p in ${PACKAGE_REPOS:-}; do git clone -q "https://github.com/$p.git" "/work/$(basename $p)"; ARGS="$ARGS --repo /work/$(basename $p)"; done
curl -sf "https://raw.githubusercontent.com/muellermarco/atscale-cicd-pipeline/main/tools/om_sync.py" -o /work/om_sync.py
python /work/om_sync.py $ARGS --env live --warehouse-service "${WAREHOUSE_SERVICE}"
"""


def build_om_sync(cfg):
    """After a live release: register the released model + lineage in OpenMetadata.
    Needs secret openmetadata-bot (key token) in ns airflow."""
    om = cfg["openmetadata"]
    return KubernetesPodOperator(
        task_id="sync_openmetadata", name=f"om-sync-{cfg['slug']}"[:63],
        namespace="airflow", in_cluster=True, image="python:3.12-slim",
        cmds=["bash", "-c"], arguments=[OM_SCRIPT],
        env_vars={"GITHUB_REPO": cfg["repo"], "WAREHOUSE_SERVICE": om["warehouse_service"],
                  "PACKAGE_REPOS": " ".join(om.get("packages", [])),
                  "GIT_REF": "{{ dag_run.conf.get('tag') or params.tag }}"},
        secrets=[Secret("env", "OM_TOKEN", "openmetadata-bot", "token")],
        get_logs=True, is_delete_operator_pod=True, startup_timeout_seconds=300,
    )


BASELINE_SCRIPT = """set -euo pipefail
git config --global user.name  "baseline-refresh bot"
git config --global user.email "bot@atscale-se-demo.com"
git clone https://x-access-token:${GITHUB_TOKEN}@github.com/${GITHUB_REPO}.git /work
cd /work
git checkout baseline
git clone --depth 1 "https://github.com/${UPSTREAM_REPO}.git" /upstream
for d in calculations connections datasets dimensions metrics models catalog.yml package.yml; do
  rm -rf "/work/$d"
  [ -e "/upstream/$d" ] && cp -R "/upstream/$d" "/work/$d" || true
done
if git diff --quiet; then echo "No drift — baseline is current."; exit 0; fi
git add -A
git commit -m "baseline refresh $(date +%F): sync from upstream model source"
git push origin baseline
curl -sf -X POST "https://api.github.com/repos/${GITHUB_REPO}/pulls" \\
  -H "Authorization: Bearer ${GITHUB_TOKEN}" -H "Accept: application/vnd.github+json" \\
  -d '{"title":"Drift: baseline refresh","head":"baseline","base":"main","body":"Weekly machine PR — upstream model source changed. Review like any other change."}' \\
  || echo "PR already open or creation failed — see logs."
"""

USAGE_SCRIPT = """set -euo pipefail
git config --global user.name  "usage-trace bot"
git config --global user.email "bot@atscale-se-demo.com"
git clone https://x-access-token:${GITHUB_TOKEN}@github.com/${GITHUB_REPO}.git /work
cd /work
BRANCH="usage-backlog-$(date +%Y%m%d)"
git checkout -b "$BRANCH"
mkdir -p docs
cat > docs/USAGE_BACKLOG.md <<EOF
# Usage-priority backlog

Machine-generated on $(date +%F) by the usage_trace DAG.
Ranks subject areas by observed query traffic on Live — translation and
go-live work is picked from the top. (Demo placeholder ranking.)

| Rank | Subject area | Relative traffic |
|---|---|---|
| 1 | Store Sales | high |
| 2 | Customer | medium |
| 3 | Item / Inventory | low |
EOF
if git diff --quiet -- docs/USAGE_BACKLOG.md 2>/dev/null && git ls-files --error-unmatch docs/USAGE_BACKLOG.md >/dev/null 2>&1; then
  echo "Backlog unchanged — nothing to do."; exit 0
fi
git add docs/USAGE_BACKLOG.md
git commit -m "usage backlog refresh $(date +%F)"
git push origin "$BRANCH"
BODY=$(printf '{"title":"Usage backlog refresh","head":"%s","base":"main","body":"Weekly machine PR — refreshed usage-priority backlog."}' "$BRANCH")
curl -sf -X POST "https://api.github.com/repos/${GITHUB_REPO}/pulls" \\
  -H "Authorization: Bearer ${GITHUB_TOKEN}" -H "Accept: application/vnd.github+json" \\
  -d "$BODY" || echo "PR creation failed — see logs."
"""


def _machine_pod(cfg, task_id, script, extra_env=None):
    return KubernetesPodOperator(
        task_id=task_id,
        name=f"{task_id.replace('_', '-')}-{cfg['slug']}"[:63],
        namespace="airflow", in_cluster=True, image=cfg["node_image"],
        cmds=["bash", "-c"], arguments=[script],
        env_vars={"GITHUB_REPO": cfg["repo"], **(extra_env or {})},
        secrets=[github_token_secret],
        get_logs=True, is_delete_operator_pod=True,
        startup_timeout_seconds=300,
    )


def build_baseline_refresh(cfg):
    @dag(dag_id=f"baseline_refresh__{cfg['slug']}", schedule="0 5 * * 1",
         start_date=START, catchup=False, is_paused_upon_creation=True,
         tags=["sml-pipeline", cfg["slug"], "machine"],
         doc_md=f"Weekly machine PR: re-sync {cfg['repo']}@baseline from "
                f"{cfg['baseline_upstream']} and open a drift PR. Needs the "
                f"github-token secret.")
    def _dag():
        _machine_pod(cfg, "sync_baseline_and_open_pr", BASELINE_SCRIPT,
                     {"UPSTREAM_REPO": cfg["baseline_upstream"]})
    return _dag()


def build_usage_trace(cfg):
    @dag(dag_id=f"usage_trace__{cfg['slug']}", schedule="0 6 * * 1",
         start_date=START, catchup=False, is_paused_upon_creation=True,
         tags=["sml-pipeline", cfg["slug"], "machine"],
         doc_md=f"Weekly machine PR: refresh the usage-priority backlog in "
                f"{cfg['repo']}. Needs the github-token secret.")
    def _dag():
        _machine_pod(cfg, "refresh_backlog_and_open_pr", USAGE_SCRIPT)
    return _dag()


def build_nightly(cfg):
    catalog = cfg["nightly"]["catalog"]

    @dag(dag_id=f"nightly__{cfg['slug']}", schedule="0 2 * * *",
         start_date=START, catchup=False, is_paused_upon_creation=True,
         tags=["sml-pipeline", cfg["slug"], "machine"],
         doc_md=f"Nightly parity check qa vs live on catalog {catalog}. Needs "
                f"Airflow Variables atscale_sql_user/password; skips without.")
    def _dag():
        @task
        def run_parity():
            from airflow.exceptions import AirflowSkipException
            from sml_pipeline_common import repo_var
            user = repo_var("atscale_sql_user", None)
            password = repo_var("atscale_sql_password", None)
            if not user or not password:
                raise AirflowSkipException(
                    "atscale_sql_user/atscale_sql_password not set — skipping.")
            import psycopg2
            engines = {"qa": "atscale-engine-sql.atscale-qa.svc.cluster.local",
                       "live": "atscale-engine-sql.atscale.svc.cluster.local"}
            queries = ["SELECT COUNT(*) FROM information_schema.tables"]
            results = {}
            for env, host in engines.items():
                conn = psycopg2.connect(host=host, port=15432, dbname=catalog,
                                        user=user, password=password,
                                        connect_timeout=30)
                with conn, conn.cursor() as cur:
                    results[env] = []
                    for q in queries:
                        cur.execute(q)
                        results[env].append(cur.fetchall())
                conn.close()
            mism = [i for i, (a, b) in enumerate(zip(results["qa"], results["live"])) if a != b]
            for i, q in enumerate(queries):
                flag = "MISMATCH" if i in mism else "ok"
                print(f"[{flag}] {q}\n  qa={results['qa'][i]}  live={results['live'][i]}")
            assert not mism, f"{len(mism)} parity mismatch(es) qa vs live"
        run_parity()
    return _dag()


for _cfg in load_registry()["pipelines"]:
    _slug = _cfg["slug"].replace("-", "_")
    globals()[f"dag_deploy_dev_{_slug}"] = build_deploy_dev(_cfg)
    globals()[f"dag_deploy_qa_{_slug}"] = build_deploy_qa(_cfg)
    globals()[f"dag_release_live_{_slug}"] = build_release_live(_cfg)
    if _cfg.get("baseline_upstream"):
        globals()[f"dag_baseline_{_slug}"] = build_baseline_refresh(_cfg)
    if _cfg.get("usage_trace"):
        globals()[f"dag_usage_{_slug}"] = build_usage_trace(_cfg)
    if _cfg.get("nightly", {}).get("catalog"):
        globals()[f"dag_nightly_{_slug}"] = build_nightly(_cfg)
