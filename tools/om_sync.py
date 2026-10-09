#!/usr/bin/env python3
"""Push an AtScale SML model repo into OpenMetadata and link it to the
warehouse tables it reads (lineage).

OpenMetadata has no native AtScale connector, so each SML `model` becomes a
DashboardDataModel under a CustomDashboard service ("atscale-<env>"), with
its metrics + dimension attributes as columns, and an upstream lineage edge
from every warehouse table the model's datasets read. The warehouse tables
themselves must already be ingested by OM's native connector (Snowflake,
Databricks, BigQuery); --warehouse-service names that OM database service.

  om_sync.py --repo ./sml-salesinsights-snowflake --repo ./sml-salesinsights-common \
      --env live --warehouse-service snowflake_partner --dry-run

Env: OM_URL (default in-cluster), OM_TOKEN (bot JWT). Idempotent (PUT upserts).
"""
import argparse, glob, os, re, sys
import yaml

DATA_MODEL_TYPE = "SupersetDataModel"   # OM has no generic type; verify against your OM version's dashboardDataModel enum
OM_URL = os.environ.get("OM_URL", "http://openmetadata.openmetadata.svc.cluster.local:8585/api")


def load_sml(repos):
    objs = {}
    for r in repos:
        for f in glob.glob(os.path.join(r, "**", "*.yml"), recursive=True):
            try:
                d = yaml.safe_load(open(f))
            except yaml.YAMLError:
                continue
            if isinstance(d, dict) and d.get("object_type") and d.get("unique_name"):
                objs.setdefault(d["object_type"], {})[d["unique_name"]] = d
    return objs


def model_columns(model, objs):
    """Columns = model metrics (+ referenced dimension attributes)."""
    cols = []
    for m in model.get("metrics", []):
        o = objs.get("metric", {}).get(m["unique_name"], {})
        cols.append({"name": m["unique_name"], "displayName": o.get("label", m["unique_name"]),
                     "description": o.get("description", f"Metric ({o.get('calculation_method', 'calculated')})"),
                     "dataType": "DOUBLE", "dataTypeDisplay": "metric",
                     "tags": [], "children": []})
    for d in model.get("dimensions", []):
        name = d if isinstance(d, str) else d.get("unique_name")
        dim = objs.get("dimension", {}).get(name, {})
        for lvl in dim.get("level_attributes", []):
            cols.append({"name": f"{name}.{lvl['unique_name']}", "displayName": lvl.get("label", lvl["unique_name"]),
                         "description": lvl.get("description", ""), "dataType": "STRING",
                         "dataTypeDisplay": "dimension attribute"})
    return cols


def model_datasets(model, objs):
    """Datasets feeding a model: via its metrics and the dimensions' attributes."""
    names = set()
    for m in model.get("metrics", []):
        ds = objs.get("metric", {}).get(m["unique_name"], {}).get("dataset")
        if ds: names.add(ds)
    for d in model.get("dimensions", []):
        dim = objs.get("dimension", {}).get(d if isinstance(d, str) else d.get("unique_name"), {})
        for lvl in dim.get("level_attributes", []):
            for a in [lvl] + lvl.get("secondary_attributes", []):
                if a.get("dataset"): names.add(a["dataset"])
    return names


def table_fqns(ds, objs, svc):
    """Warehouse tables a dataset reads: its `table`, or db.schema.table refs in its `sql`."""
    conn = objs.get("connection", {}).get(ds.get("connection_id"), {})
    out = []
    if ds.get("table") and conn.get("database") and conn.get("schema"):
        out.append(f"{svc}.{conn['database']}.{conn['schema']}.{ds['table']}")
    for m in re.finditer(r'(?i)\b(?:from|join)\s+([\w$]+)\.([\w$]+)\.([\w$]+)', ds.get("sql") or ""):
        out.append(f"{svc}." + ".".join(m.groups()))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", action="append", required=True, help="SML repo dir (repeat for packages, e.g. -common)")
    ap.add_argument("--env", default="live")
    ap.add_argument("--warehouse-service", required=True)
    ap.add_argument("--catalog", default=None,
                    help="deployed catalog name; data models are then named '<catalog>.<model>' "
                         "(matches the atscale_xmla connector, which owns the columns)")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    objs = load_sml(a.repo)
    svc = f"atscale-{a.env}"
    plan = []
    for name, model in objs.get("model", {}).items():
        tables = [t for d in model_datasets(model, objs) if d in objs.get("dataset", {})
                  for t in table_fqns(objs["dataset"][d], objs, a.warehouse_service)]
        uniq = {t.lower(): t for t in tables}            # SQL refs differ in case from connection values
        plan.append((name, model, model_columns(model, objs), sorted(uniq.values())))
    for name, _, cols, tables in plan:
        print(f"model {name!r}: {len(cols)} columns, upstream tables: {tables or 'none resolved'}")
    if a.dry_run:
        return 0

    import requests
    s = requests.Session()
    s.headers.update({"Authorization": f"Bearer {os.environ['OM_TOKEN']}"})
    def call(method, path, **kw):
        r = s.request(method, f"{OM_URL}{path}", timeout=60, **kw)
        if r.status_code >= 300:
            print(f"{method} {path} -> {r.status_code} {r.text[:300]}"); r.raise_for_status()
        return r.json() if r.text.strip() else {}      # lineage PUT answers with an empty body
    call("PUT", "/v1/services/dashboardServices", json={
        "name": svc, "serviceType": "CustomDashboard",
        "description": f"AtScale semantic layer ({a.env})",
        "connection": {"config": {"type": "CustomDashboard", "sourcePythonClass": "atscale.none"}}})
    for name, model, cols, tables in plan:
        dm_name = f"{a.catalog}.{name}" if a.catalog else name
        # the XMLA connector (om_connectors.atscale_xmla) owns the data models once it has
        # run; only create one here if it is missing, never overwrite its columns
        r0 = s.get(f"{OM_URL}/v1/dashboard/datamodels/name/{svc}.model.{dm_name}", timeout=60)
        if r0.status_code == 200:
            dm = r0.json(); print(f"model {dm_name!r} exists — lineage only")
        else:
            dm = call("PUT", "/v1/dashboard/datamodels", json={
                "name": dm_name, "displayName": model.get("label", name), "service": svc,
                "project": a.catalog, "description": model.get("description", f"AtScale model {name}"),
                "dataModelType": DATA_MODEL_TYPE, "columns": cols})
        for fqn in tables:
            svc_name, rest = fqn.split(".", 1)           # OM keeps the warehouse's own casing;
            for cand in (fqn, f"{svc_name}.{rest.upper()}", f"{svc_name}.{rest.lower()}"):  # never re-case the service
                r = s.get(f"{OM_URL}/v1/tables/name/{cand}", timeout=60)
                if r.status_code != 404:
                    break
            if r.status_code == 404:
                print(f"  table {fqn} not in OM yet (ingest the warehouse first) — skipped"); continue
            call("PUT", "/v1/lineage", json={"edge": {
                "fromEntity": {"id": r.json()["id"], "type": "table"},
                "toEntity": {"id": dm["id"], "type": "dashboardDataModel"}}})
            print(f"  lineage {fqn} -> {svc}.{dm_name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
