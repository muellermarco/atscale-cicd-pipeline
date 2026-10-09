# om_connectors — OpenMetadata custom connectors

## `atscale_xmla` — AtScale / SSAS-compatible XMLA endpoint → dashboard service

OpenMetadata (open source) has no cube connector; this is a **CustomDashboard**
connector that reads the standard XMLA `DISCOVER` rowsets, so it works against
AtScale's XMLA endpoint and against a real SSAS instance alike.

| XMLA | OpenMetadata |
|---|---|
| catalog | Dashboard (`<service>.<catalog>`) |
| cube | DashboardDataModel (`<service>.model.<catalog>.<cube>`, project = catalog) |
| measure / hierarchy level | column (`dataTypeDisplay` = `measure` / `level`) |

Warehouse lineage (cube → tables) is **not** in XMLA; `tools/om_sync.py` adds it
at release time from the SML sources (it creates the data model only if the
connector hasn't yet, and always upserts the lineage edges).

### Runtime

The class must be importable by the ingestion runtime (the pipeline Airflow
image, see `atscale-cicd-infrastructure/.../airflow-image/Dockerfile`):

```
pip install --no-deps "git+https://github.com/muellermarco/atscale-cicd-pipeline.git@main#subdirectory=om_connectors"
```

### Service (UI → Add New Service → Dashboard → CustomDashboard)

- Source Python Class: `om_connectors.atscale_xmla.AtScaleXmlaSource`
- Connection options:
  - `xmla_url_env` = `ATSCALE_XMLA_URL` (env var on the Airflow pods; AtScale
    embeds the auth token in the URL path, so keep it out of the service config), **or**
  - `xmla_url` = the full URL
  - `catalogs` = optional comma-separated allow-list
  - `verify_ssl` = `true` to verify TLS (default `false`, demo certs are self-signed)

**Test Connection** runs a real `DBSCHEMA_CATALOGS` discover.

Log noise to expect: `Error importing connection class for CustomDashboard` — the
framework first looks for a stock connection spec, finds none for custom
connectors, and falls back to this module's `get_connection`/`test_connection`.

### Tests

```
pip install -e . pytest && pytest tests/test_parse.py                 # offline
OM_URL=http://host:8585/api OM_TOKEN=<bot jwt> python tests/e2e_fake_xmla.py   # full workflow, canned XMLA
```
