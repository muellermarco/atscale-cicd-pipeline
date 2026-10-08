# atscale-cicd-pipeline

The **CI/CD framework** for AtScale SML model repositories: one pipeline,
many models. Model repos stay pure SML plus three 3-line workflow stubs;
everything else — validation logic, deploy orchestration, machine PRs —
lives here, versioned and upgraded in one place.

```
 model repo (per model)                 THIS repo (once)
 ──────────────────────                 ────────────────
 SML files                              airflow/dags/pipelines.yaml   ← registry
 .github/workflows/  ── uses: ──▶       .github/workflows/            ← reusable logic
   validate.yml   (3-line stub)         airflow/dags/generate_dags.py ← DAG factory
   deploy-qa.yml  (3-line stub)         tools/validate_sml.py         ← cross-ref gate
   release-live.yml (3-line stub)       onboarding/onboard.py
```

Airflow **git-syncs `airflow/dags/` from this repo** and generates, per
registry entry: `deploy_dev__<slug>`, `deploy_qa__<slug>`,
`release_live__<slug>` (unpaused), plus optional `baseline_refresh__<slug>`,
`usage_trace__<slug>`, `nightly__<slug>` (paused; need the `github-token`
secret / SQL Variables).

## Onboarding a new model repo

```sh
python3 onboarding/onboard.py --slug my-model --repo owner/my-model \
  --model-clone /path/to/local/clone
```

Then: merge the printed `pipelines.yaml` entry (PR to this repo), commit the
stub workflows in the model repo, run the printed `gh` config commands.
Details each step prints itself. Total: two small PRs.

## Runtime contract (the AKS demo environment provides all of this)

- Airflow ≥ 3 with git-sync on this repo, KubernetesPodOperator RBAC, and
  namespace secrets `atscale-{dev,qa,live}-oauth` (+ optional `github-token`).
- Three AtScale instances at `dev/qa/prod.<atscale_domain>` (default in
  `pipelines.yaml`; Airflow Variable `atscale_domain` overrides).
- Model repos registered as catalog repositories on each instance
  (onboard.py does this).

Environment build + operations: see the `cicd-demo` folder docs
(SETUP.md / AKS_BUILD_NOTES.md / OPERATIONS.md).
