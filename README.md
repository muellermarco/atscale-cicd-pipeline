# atscale-cicd-pipeline

A shared CI/CD framework for AtScale SML model repositories: one pipeline, many semantic models.

SML (Semantic Modeling Language) is the YAML format in which AtScale stores a semantic model as code. Each model repository holds only its SML files and three short workflow stubs. This repository holds everything else: the validation logic, the deploy orchestration in Airflow, the onboarding script, and the sync to OpenMetadata. You change and upgrade the pipeline in one place, and every model repository picks up the change.

The main readers are platform engineers who run the pipeline and modelers who onboard a model repository.

- [What this repository contains](#what-this-repository-contains)
- [Technologies](#technologies)
- [Pipeline registry](#pipeline-registry)
- [Catalog naming](#catalog-naming)
- [Onboard a new model repository](#onboard-a-new-model-repository)
- [Deploy a change](#deploy-a-change)
- [Runtime requirements](#runtime-requirements)
- [Project status and contact](#project-status-and-contact)

## What this repository contains

```
 model repo (one per model)            THIS repo (once)
 --------------------------            ----------------
 SML files                             airflow/dags/pipelines.yaml         <- registry
 .github/workflows/  -- uses: -->      .github/workflows/                  <- reusable workflows
   validate.yml      (stub)            airflow/dags/generate_dags.py       <- DAG factory
   deploy-qa.yml     (stub)            airflow/dags/sml_pipeline_common.py <- shared deploy code
   release-live.yml  (stub)            tools/validate_sml.py               <- cross-reference check
                                       tools/om_sync.py                    <- OpenMetadata sync
                                       om_connectors/                      <- OpenMetadata connector
                                       onboarding/onboard.py + templates/
```

Airflow git-syncs `airflow/dags/` from this repository. For each entry in the registry, the DAG factory generates these DAGs:

| DAG | Trigger | What it does | Starts |
|---|---|---|---|
| `deploy_dev__<slug>` | Manual, parameter `branch` | Deploys a feature branch to the dev instance | Unpaused |
| `deploy_qa__<slug>` | `deploy-qa` workflow on merge to `main` | Deploys `main` to the QA instance, runs a smoke check, posts a commit status | Unpaused |
| `release_live__<slug>` | `release-live` workflow on a `v*` tag | Deploys the tag to the live instance. If the entry has an `openmetadata` block, it then runs `tools/om_sync.py` | Unpaused |
| `usage_trace__<slug>` | Weekly, only if `usage_trace: true` | Opens a machine pull request with a usage backlog | Paused |
| `nightly__<slug>` | Nightly, only if `nightly.catalog` is set | Compares QA and live over the SQL interface | Paused |

Each deploy runs `sml-cli install`, `sml-cli validate`, and `sml-cli atscale-deploy` in a short-lived Kubernetes pod. The pod creates its own short-lived AtScale API token, so the pipeline stores no deploy token.

The `om_connectors/` folder holds a custom OpenMetadata connector that reads published catalogs from the AtScale container API. See [om_connectors/README.md](om_connectors/README.md).

## Technologies

- Python 3, standard library only, for `onboarding/onboard.py`
- Python 3.12 with PyYAML for `tools/validate_sml.py`, and with PyYAML and `requests` for `tools/om_sync.py`
- Apache Airflow 3.2.2 with the `cncf.kubernetes` provider (KubernetesPodOperator)
- `sml-cli` 2025.12.0 (set in `airflow/dags/pipelines.yaml`, key `defaults.sml_cli`, and in `.github/workflows/validate.yml`)
- Container image `node:20-bookworm` for deploy pods, `python:3.12-slim` for the OpenMetadata sync pod
- OpenMetadata 2.0.4.0 (server and ingestion packages must have the same version)
- GitHub Actions (reusable workflows), the `gh` CLI, and `kubectl` for onboarding
- AtScale: three instances (dev, QA, live) on Kubernetes

## Pipeline registry

`airflow/dags/pipelines.yaml` is the single source of truth for onboarded model repositories. It has one `defaults` block and one entry for each model repository under `pipelines`.

| Key | Purpose | Default | Required |
|---|---|---|---|
| `defaults.atscale_domain` | Domain of the three AtScale instances (`dev.<domain>`, `qa.<domain>`, `prod.<domain>`). The Airflow Variable `atscale_domain` overrides it. | Set in the file | Yes |
| `defaults.sml_cli` | `sml-cli` package and version for deploy pods | `sml-cli@2025.12.0` | Yes |
| `defaults.node_image` | Image for deploy and machine pods | `node:20-bookworm` | Yes |
| `slug` | Short lowercase ID. Becomes the DAG suffix and the stub input. | None | Yes |
| `enabled` | Only entries with `enabled: true` get DAGs | `false` | Yes |
| `repo` | GitHub repository, `<owner>/<repo>` | None | Yes |
| `private` | `true` for a private repository. Pods then clone with the `github-token` secret. | `false` | No |
| `path` | Subfolder that holds the catalog, for repositories with several catalogs | `.` | No |
| `dev_catalog_prefix` | Catalog name prefix on the dev instance | `slug` | No |
| `default_branch` | Branch for QA and live, and the catalog name suffix | `main` | No |
| `openmetadata.warehouse_service` | OpenMetadata database service that holds the warehouse tables. Turns on the sync after a live release. | None | No |
| `usage_trace` | `true` generates the weekly `usage_trace__<slug>` DAG | None | No |
| `nightly.catalog` | Catalog for the nightly parity check. Use the suffixed name (see below). | None | No |

Shared package repositories (SML repositories with no catalog, such as `sml-tpcds-common`) are not on the pipeline. They get no registry entry and no stubs. The validate run of each consuming repository checks them.

## Catalog naming

QA and live deploy each catalog as `<unique_name>_<branch>`, and the label as `<label>_<branch>`. Example: `Sales Insights - Snowflake_main`.

- The pod reads the base names from the repository's catalog file (`catalog.yml` or `atscale.yml`) at deploy time.
- The branch is the entry's `default_branch`.
- Dev uses `<dev_catalog_prefix>-<branch>`.

`sml-cli` has no force-deploy flag. A distinct catalog name prevents a name or project ID conflict with an older catalog. A `nightly.catalog` entry must use the suffixed name.

## Onboard a new model repository

Two repositories take part:

| | This repository | The model repository |
|---|---|---|
| Local clone needed? | Yes. `onboard.py` and the stub templates are here, and you open a pull request here. | Optional. With `--model-clone`, the script writes the stubs for you. |
| Must exist on GitHub? | Yes | Yes. The script registers its GitHub URL on the AtScale instances. |
| What changes | One new entry in `airflow/dags/pipelines.yaml` | Three short workflow stubs in `.github/workflows/` |

### Prerequisites

- `python3`. The script uses only the standard library.
- `kubectl` access to the AtScale namespaces `atscale-dev`, `atscale-qa`, and `atscale`. The script reads the AtScale admin credentials there.
- `gh` CLI, logged in with admin rights on the model repository.

If you do not have `kubectl` access, the script skips the registration. Then use `infrastructure/config-export/repos/setup_repos.py` in the private repository `atscale-cicd-infrastructure` for each instance.

### Arguments of `onboard.py`

| Argument | Purpose | Default | Required |
|---|---|---|---|
| `--slug` | Short lowercase ID you choose, for example `salesinsights-snowflake` | None | Yes |
| `--repo` | Model repository on GitHub, `<owner>/<repo>`. It need not match the slug. | None | Yes |
| `--name` | Name of the repository connection on AtScale | Value of `--slug` | No |
| `--model-clone` | Local clone of the model repository. The script writes the stubs into it. | None | No |
| `--domain` | Domain of the AtScale instances and Airflow | Set in the script | No |

The slug becomes the DAG suffix (`deploy_qa__<slug>`), the default `dev_catalog_prefix`, and the `__SLUG__` value in the stubs.

### Steps

1. Clone this repository and open its folder.
2. Run the onboarding script:

   ```sh
   python3 onboarding/onboard.py --slug <slug> --repo <owner>/<repo> \
     --domain <domain> --model-clone /path/to/model/clone
   ```

   Result: the script registers the model repository on dev, QA, and prod, and prints a registry entry.
3. Add the printed entry to `airflow/dags/pipelines.yaml`.
4. If the model repository is private, add `private: true` to the entry.
5. Open a pull request in this repository and merge it.
   Result: after the next git-sync, Airflow shows the DAGs for the new slug.
6. If you did not use `--model-clone`, copy `onboarding/templates/*.yml` to `.github/workflows/` in the model repository.
7. If you copied the stubs by hand, replace `__SLUG__` with your slug.
8. Commit the stubs to the model repository and open a pull request.
9. Run the `gh` commands that the script printed in step 4 of its output.
   Result: the model repository has the variable `AIRFLOW_URL` and the secrets `AIRFLOW_USER` and `AIRFLOW_PASSWORD`.
10. Set branch protection on `main` to require a pull request and the `validate` check.

The script is idempotent. You can run it again with no side effects.

If the catalog is in a subfolder, set `path` in the registry entry and pass the same `path` to the validate stub (`with: path: <folder>`).

## Deploy a change

Do these steps in the model repository. The reusable workflows are in `.github/workflows/` of this repository.

1. Create a feature branch and change the SML files.
2. If you want to test on dev, trigger `deploy_dev__<slug>` in Airflow with the parameter `branch`.
   Result: the dev instance has the catalog `<dev_catalog_prefix>-<branch>`.
3. Open a pull request to `main`.
   Result: the `validate` check runs `sml-cli install`, `sml-cli validate`, and `tools/validate_sml.py`.
4. Merge the pull request when the check is green.
   Result: the `deploy-qa` workflow triggers `deploy_qa__<slug>`. The QA instance has `<unique_name>_main`. The commit shows the status `atscale/deploy-qa`.
5. Tag the commit that you want to release, for example `git tag v1.4`.
6. Push the tag: `git push origin v1.4`.
   Result: the `release-live` workflow triggers `release_live__<slug>`, and the live instance has the new version.
7. If the entry has an `openmetadata` block, examine the task `sync_openmetadata` in the same DAG run.
   Result: OpenMetadata shows the released model and its lineage to the warehouse tables.

To roll back, trigger `release_live__<slug>` with the parameter `tag` set to the previous tag.

## Runtime requirements

The Kubernetes environment must supply these items. The build and operation of that environment are documented in the private repository `atscale-cicd-infrastructure`, folder `docs/` (`SETUP.md`, `OPERATIONS.md`).

- Airflow 3 with git-sync on this repository and RBAC for KubernetesPodOperator in the namespace `airflow`.
- Kubernetes secrets in the namespace `airflow`:
  - `atscale-dev-oauth`, `atscale-qa-oauth`, `atscale-live-oauth` (keys `clientSecret`, `adminUser`, `adminPassword`).
  - `github-token` (key `token`). Required for private repositories, for `usage_trace__<slug>`, and for the OpenMetadata sync of private repositories.
  - `openmetadata-bot` (key `token`). Required for the OpenMetadata sync.
- Airflow Pools `atscale-dev`, `atscale-qa`, and `atscale-live`, with one slot each. AtScale API tokens are single-active per user, so deploys to one instance must not run in parallel.
- Airflow Variables, all optional:
  - `atscale_domain` overrides `defaults.atscale_domain`.
  - `github_token` lets the QA DAG post commit statuses.
  - `atscale_sql_user` and `atscale_sql_password` are for `nightly__<slug>`. Without them, the DAG skips.
- Three AtScale instances at `dev.<domain>`, `qa.<domain>`, and `prod.<domain>`.
- Each model repository registered as a catalog repository on each instance. `onboard.py` does this.

## Project status and contact

- Status: in active use for AtScale demo environments. The pipeline changes as new model repositories join.
- Owner: AtScale Solutions Engineering, EMEA.
- Questions and problems: open an issue at https://github.com/muellermarco/atscale-cicd-pipeline/issues.
