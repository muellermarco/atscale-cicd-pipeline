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
from urllib.parse import quote
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


def metric_obj(objs, name):
    """A model's metric list mixes plain metrics and MDX calculations (metric_calc)."""
    return objs.get("metric", {}).get(name) or objs.get("metric_calc", {}).get(name) or {}


def measure_refs(calc):
    """unique_names of the measures an MDX expression references ([Measures].[x])."""
    return list(dict.fromkeys(re.findall(r"\[Measures\]\.\[([^\]]+)\]", str(calc.get("expression", "")), re.I)))


def field_sources(objs):
    """Column name (as the XMLA connector names it) -> {(dataset, column)} physical origins.
    Calculations resolve transitively through the measures their MDX references."""
    src = {}
    def add(name, ds, col):
        if ds and col:
            src.setdefault(name, set()).add((ds, col))
    for name, o in objs.get("metric", {}).items():
        add(name, o.get("dataset"), o.get("column"))
    for dname, d in objs.get("dimension", {}).items():
        for la in d.get("level_attributes", []) or []:
            for c in [la.get("name_column")] + list(la.get("key_columns") or []):
                add(f"{dname}.{la['unique_name']}", la.get("dataset"), c)
        for h in d.get("hierarchies", []) or []:
            for lvl in h.get("levels", []) or []:
                for sa in lvl.get("secondary_attributes", []) or []:
                    for c in [sa.get("name_column")] + list(sa.get("key_columns") or []):
                        add(f"{dname}.{sa['unique_name']}", sa.get("dataset"), c)
                for m in lvl.get("metrics", []) or []:
                    add(m["unique_name"], m.get("dataset"), m.get("column"))
    calcs = objs.get("metric_calc", {})
    def resolve(name, seen):
        if name in src or name in seen:
            return src.get(name, set())
        seen.add(name)
        out = set()
        for r in measure_refs(calcs.get(name, {})):
            out |= resolve(r, seen)
        if out:
            src[name] = out
        return out
    for name in calcs:
        resolve(name, set())
    for t in ("model", "composite_model"):                   # role-played dimensions
        for mo in objs.get(t, {}).values():
            for rel in mo.get("relationships", []) or []:
                rp, to = rel.get("role_play"), rel.get("to") or {}
                dim = to.get("dimension") if isinstance(to, dict) else None
                if rp and dim and "{0}" in rp:
                    for key in [k for k in src if k.startswith(dim + ".")]:
                        src.setdefault(f"{rp.format(dim)}.{rp.format(key[len(dim) + 1:])}", src[key])
    return src


def column_lineage(s, svc, dm_fqn, table_fqn, table_id, dataset_names, objs, sources):
    """columnsLineage for one table->data model edge: every data model column whose physical
    origin is a column of this table (through one of the datasets reading it)."""
    dm = s.get(f"{OM_URL}/v1/dashboard/datamodels/name/{quote(dm_fqn, safe='')}", params={"fields": "columns"}, timeout=60)
    tb = s.get(f"{OM_URL}/v1/tables/{table_id}", params={"fields": "columns"}, timeout=60)
    if dm.status_code != 200 or tb.status_code != 200:
        return []
    tcols = {c["name"].lower(): c["fullyQualifiedName"] for c in tb.json().get("columns", [])}
    out = []
    for c in dm.json().get("columns", []):
        froms = sorted({tcols[col.lower()] for ds, col in sources.get(c["name"], ())
                        if ds in dataset_names and col.lower() in tcols})
        if froms:
            out.append({"fromColumns": froms, "toColumn": c["fullyQualifiedName"]})
    return out


def datamodel_fqn(svc, name):
    """OM quotes FQN segments that contain a dot: svc.model."<catalog>.<cube>"."""
    return f'{svc}.model."{name}"' if "." in name else f"{svc}.model.{name}"


def _md_code(expr):
    return "**MDX**\n```mdx\n" + str(expr).strip() + "\n```"


def sml_field_docs(objs):
    """Column name (as the XMLA connector names it) -> markdown description derived from
    the SML definition: the object's own description, else a generated one, plus the
    defining details (source column, format, hierarchy, MDX)."""
    docs = {}
    for name, o in objs.get("metric", {}).items():
        src = f"`{o.get('dataset')}.{o.get('column')}`" if o.get("dataset") else ""
        text = o.get("description") or f"{(o.get('calculation_method') or 'metric').title()} of {src}".rstrip()
        details = " · ".join(x for x in (
            f"{o.get('calculation_method')} of {src}" if src and o.get("description") else "",
            f"format `{o['format']}`" if o.get("format") else "",
            f"unrelated dimensions: {o['unrelated_dimensions_handling']}" if o.get("unrelated_dimensions_handling") else "") if x)
        docs[name] = text + (f"\n\n{details}" if details else "")
    for name, o in objs.get("metric_calc", {}).items():
        text = o.get("description") or "MDX calculation"
        fmt = f"\n\nformat `{o['format']}`" if o.get("format") else ""
        refs = [metric_obj(objs, r).get("label", r) for r in measure_refs(o) if r != name]
        dep = ("\n\n**Depends on** " + ", ".join(f"`{r}`" for r in dict.fromkeys(refs))) if refs else ""
        docs[name] = text + fmt + dep + ("\n\n" + _md_code(o["expression"]) if o.get("expression") else "")
    for dname, d in objs.get("dimension", {}).items():
        dlabel = d.get("label", dname)
        in_hier = {}                                   # level unique_name -> hierarchies containing it
        for h in d.get("hierarchies", []) or []:
            hlabel = h.get("label", h.get("unique_name", ""))
            for lvl in h.get("levels", []) or []:
                in_hier.setdefault(lvl.get("unique_name"), []).append(hlabel)
                for sa in lvl.get("secondary_attributes", []) or []:
                    src = f"`{sa.get('dataset')}.{sa.get('name_column')}`" if sa.get("dataset") else ""
                    text = sa.get("description") or f"Secondary attribute of level *{lvl.get('unique_name')}* in *{dlabel}*"
                    details = " · ".join(x for x in (f"hierarchy *{hlabel}*", f"from {src}" if src else "") if x)
                    docs[f"{dname}.{sa['unique_name']}"] = f"{text}\n\n{details}"
                for m in lvl.get("metrics", []) or []:            # metrical attributes surface as measures
                    src = f"`{m.get('dataset')}.{m.get('column')}`" if m.get("dataset") else ""
                    docs[m["unique_name"]] = (f"{(m.get('calculation_method') or 'metric').title()} of {src} "
                                              f"(metrical attribute on level *{lvl.get('unique_name')}* of *{dlabel}*)")
        for la in d.get("level_attributes", []) or []:
            src = f"`{la.get('dataset')}.{la.get('name_column')}`" if la.get("dataset") else ""
            hiers = ", ".join(f"*{h}*" for h in in_hier.get(la.get("unique_name"), []))
            text = la.get("description") or f"Level *{la.get('label', la['unique_name'])}* of dimension *{dlabel}*"
            details = " · ".join(x for x in (f"hierarchy {hiers}" if hiers else "", f"from {src}" if src else "",
                                             f"time unit {la['time_unit']}" if la.get("time_unit") else "") if x)
            docs[f"{dname}.{la['unique_name']}"] = f"{text}\n\n{details}" if details else text
    # role-played dimensions: a model relationship with role_play "Order {0}" exposes
    # "Date Dimension.rpt_Year" as "Order Date Dimension.Order rpt_Year"
    for t in ("model", "composite_model"):
        for mo in objs.get(t, {}).values():
            for rel in mo.get("relationships", []) or []:
                rp, to = rel.get("role_play"), rel.get("to") or {}
                dim = to.get("dimension") if isinstance(to, dict) else None
                if not rp or not dim or "{0}" not in rp:
                    continue
                role = rp.replace(" {0}", "").replace("{0}", "").strip()
                for key, text in list(docs.items()):
                    if key.startswith(dim + "."):
                        attr = key[len(dim) + 1:]
                        docs.setdefault(f"{rp.format(dim)}.{rp.format(attr)}",
                                        f"{text}\n\nrole-played as *{role}* ({rp.format(dim)})")
    return docs


def enrich_catalog(s, svc, catalog, objs):
    """Describe every data model of the catalog (incl. composite models) and every column
    from the SML definitions. Idempotent: only differing descriptions are patched."""
    docs = sml_field_docs(objs)
    model_docs = {n: o.get("description") for t in ("model", "composite_model")
                  for n, o in objs.get(t, {}).items() if o.get("description")}
    r = s.get(f"{OM_URL}/v1/dashboard/datamodels", params={"service": svc, "limit": 100, "fields": "columns"},
              timeout=60); r.raise_for_status()
    for dm in r.json().get("data", []):
        if dm.get("project") != catalog:
            continue
        cube = dm["name"].split(".", 1)[1] if dm["name"].startswith(f"{catalog}.") else dm["name"]
        ops, missing = [], 0
        if model_docs.get(cube) and (dm.get("description") or "") != model_docs[cube]:
            ops.append({"op": "add" if dm.get("description") is None else "replace",
                        "path": "/description", "value": model_docs[cube]})
        for i, c in enumerate(dm.get("columns", [])):
            want = docs.get(c["name"])
            if not want:
                missing += 1; continue
            if (c.get("description") or "").strip() != want.strip():
                ops.append({"op": "add" if c.get("description") is None else "replace",
                            "path": f"/columns/{i}/description", "value": want})
        if ops:
            pr = s.patch(f"{OM_URL}/v1/dashboard/datamodels/{dm['id']}", json=ops, timeout=120,
                         headers={"Content-Type": "application/json-patch+json"})
            print(f"  described {dm['name']!r}: {len(ops)} change(s) -> {pr.status_code}"
                  + (f" ({missing} column(s) not in SML)" if missing else "")); pr.raise_for_status()
        else:
            print(f"  described {dm['name']!r}: up to date" + (f" ({missing} column(s) not in SML)" if missing else ""))


def model_columns(model, objs):
    """Columns = model metrics (+ referenced dimension attributes)."""
    cols = []
    for m in model.get("metrics", []):
        o = metric_obj(objs, m["unique_name"])
        cols.append({"name": m["unique_name"], "displayName": o.get("label", m["unique_name"]),
                     "description": sml_field_docs(objs).get(m["unique_name"], "Metric"),
                     "dataType": "DOUBLE", "dataTypeDisplay": "metric", "tags": [], "children": []})
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
    dims = [d if isinstance(d, str) else d.get("unique_name") for d in model.get("dimensions", []) or []]
    for rel in model.get("relationships", []) or []:            # dimensions joined by the model
        to = rel.get("to") or {}
        if isinstance(to, dict) and to.get("dimension"):
            dims.append(to["dimension"])
        if isinstance(rel.get("from"), dict) and rel["from"].get("dataset"):
            names.add(rel["from"]["dataset"])
    for dname in dict.fromkeys(dims):
        dim = objs.get("dimension", {}).get(dname, {})
        for la in dim.get("level_attributes", []) or []:
            if la.get("dataset"): names.add(la["dataset"])
        for h in dim.get("hierarchies", []) or []:
            for lvl in h.get("levels", []) or []:
                for sa in lvl.get("secondary_attributes", []) or []:
                    if sa.get("dataset"): names.add(sa["dataset"])
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
    members = {}                                          # data model name -> SML models it is made of
    for name, model in objs.get("model", {}).items():
        members[name] = [model]
    for name, cm in objs.get("composite_model", {}).items():
        members[name] = [objs["model"][m] for m in cm.get("models") or [] if m in objs.get("model", {})]
    for name, mods in members.items():
        dsets = {d for m in mods for d in model_datasets(m, objs) if d in objs.get("dataset", {})}
        tables = [t for d in dsets for t in table_fqns(objs["dataset"][d], objs, a.warehouse_service)]
        uniq = {t.lower(): t for t in tables}            # SQL refs differ in case from connection values
        cols = [c for m in mods for c in model_columns(m, objs)]
        plan.append((name, mods[0] if mods else {}, cols, sorted(uniq.values()), dsets))
    sources = field_sources(objs)
    for name, _, cols, tables, _d in plan:
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
    # the service is normally owned by the XMLA connector (om_connectors.atscale_xmla) —
    # never overwrite its connection; only create a placeholder when it does not exist
    if s.get(f"{OM_URL}/v1/services/dashboardServices/name/{svc}", timeout=60).status_code == 404:
        call("PUT", "/v1/services/dashboardServices", json={
            "name": svc, "serviceType": "CustomDashboard",
            "description": f"AtScale semantic layer ({a.env})",
            "connection": {"config": {"type": "CustomDashboard", "sourcePythonClass": "atscale.none"}}})
        print(f"service {svc!r} created (placeholder connection — point it at the XMLA connector)")
    for name, model, cols, tables, dsets in plan:
        dm_name = f"{a.catalog}.{name}" if a.catalog else name
        # the XMLA connector (om_connectors.atscale_xmla) owns the data models once it has
        # run; only create one here if it is missing, never overwrite its columns
        r0 = s.get(f"{OM_URL}/v1/dashboard/datamodels/name/{quote(datamodel_fqn(svc, dm_name), safe='')}", timeout=60)
        if r0.status_code == 200:
            dm = r0.json(); print(f"model {dm_name!r} exists (connector-owned) — lineage only")
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
            # datasets that read this table (by table name, case-insensitive)
            tname = fqn.rsplit(".", 1)[1].lower()
            ds_here = {d for d in dsets if any(t.rsplit(".", 1)[1].lower() == tname
                                                 for t in table_fqns(objs["dataset"][d], objs, a.warehouse_service))}
            cl = column_lineage(s, svc, datamodel_fqn(svc, dm_name), fqn, r.json()["id"], ds_here, objs, sources)
            edge = {"fromEntity": {"id": r.json()["id"], "type": "table"},
                    "toEntity": {"id": dm["id"], "type": "dashboardDataModel"}}
            if cl:
                edge["lineageDetails"] = {"columnsLineage": cl, "source": "DashboardLineage"}
            call("PUT", "/v1/lineage", json={"edge": edge})
            print(f"  lineage {fqn} -> {svc}.{dm_name} ({len(cl)} column mappings)")
    if a.catalog:
        print("describing data models + columns from the SML definitions:")
        enrich_catalog(s, svc, a.catalog, objs)
    return 0


if __name__ == "__main__":
    sys.exit(main())
