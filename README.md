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
`release_live__<slug>` (unpaused), plus optional `usage_trace__<slug>`,
`nightly__<slug>` (paused; need the `github-token`
secret / SQL Variables).

## Catalog naming

QA and live deploy each catalog as `<unique_name>_<branch>` (label likewise),
e.g. `Sales Insights - Snowflake_main`. Base names are read from the repo's
catalog file (`catalog.yml` or `atscale.yml`) at deploy time; the branch is the
registry entry's `default_branch`. Dev keeps `<dev_catalog_prefix>-<branch>`.
sml-cli has no force-deploy flag — a distinct name is how a name/project-ID
conflict with an older catalog is avoided. A `nightly.catalog` entry must use
the suffixed name.

## Onboarding a new model repo

Two repositories are involved. Keep them straight:

| | **This repo** (`atscale-cicd-pipeline`) | **The model repo** (your SML files) |
|---|---|---|
| Needs a local clone? | **Yes** — `onboard.py` and the stub templates live here, and you will open a PR here. | **Optional** — pass `--model-clone` and the script writes the stubs for you; otherwise copy them in by hand. |
| Must already exist on GitHub? | Yes | **Yes** — the script registers its GitHub URL on the AtScale instances. |
| What changes | one new entry in `airflow/dags/pipelines.yaml` | three 3-line workflow stubs in `.github/workflows/` |

Prerequisites on the machine you run it from:

- `python3` (stdlib only, no pip installs).
- `kubectl` access to the AKS namespaces `atscale-dev`, `atscale-qa`,
  `atscale` (used to read the AtScale admin creds). Without it, step 1 is
  skipped and you register the repo per instance with
  `config-export/repos/setup_repos.py` instead.
- `gh` CLI, logged in with rights on the model repo (for step 4).

Run, from a clone of this repo:

```sh
python3 onboarding/onboard.py --slug <slug> --repo <owner>/<repo> \
  --model-clone /path/to/local/clone/of/the/model/repo   # optional
```

- `<slug>`: short lowercase identifier you choose for the model (e.g.
  `salesinsights-snowflake`). It becomes the DAG suffix (`deploy_qa__<slug>`),
  the AtScale repo-connection name, the default `dev_catalog_prefix`, and the
  `__SLUG__` value in the stub workflows.
- `<owner>/<repo>`: the model's GitHub repository (e.g.
  `muellermarco/sml-salesinsights-snowflake`). Used to build the clone URL
  registered on each AtScale instance. It need not match the slug.

The script is idempotent and does four things, in order:

1. Registers the model repo as a catalog repository on dev / qa / prod AtScale.
2. Prints the `pipelines.yaml` entry — add it to this repo and open a PR.
3. Writes the three stub workflows into the model clone (or, without
   `--model-clone`, tells you to copy `onboarding/templates/*.yml` in and
   replace `__SLUG__`) — commit them to the model repo and open a PR.
4. Prints the `gh` commands that set the Airflow URL / credentials on the
   model repo and the branch-protection hint — run them once.

Total: two small PRs, one per repo.

## Runtime contract (the AKS demo environment provides all of this)

- Airflow ≥ 3 with git-sync on this repo, KubernetesPodOperator RBAC, and
  namespace secrets `atscale-{dev,qa,live}-oauth` (+ optional `github-token`).
- Three AtScale instances at `dev/qa/prod.<atscale_domain>` (default in
  `pipelines.yaml`; Airflow Variable `atscale_domain` overrides).
- Model repos registered as catalog repositories on each instance
  (onboard.py does this).

Environment build + operations: see the `cicd-demo` folder docs
(SETUP.md / AKS_BUILD_NOTES.md / OPERATIONS.md).
