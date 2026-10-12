# om_connectors — OpenMetadata custom connector for AtScale

OpenMetadata (open source) has no cube connector; this is a **CustomDashboard**
connector that lands an AtScale semantic layer as a dashboard service, read from
the [container API](https://documentation.atscale.com/container-api/export-catalog-representation)
(`GET /v1/catalogs` + `GET /v1/catalogs/{id}/export`):

| AtScale | OpenMetadata |
|---|---|
| catalog | Dashboard (`<service>.<catalog>`) |
| model / cube (and perspective) | DashboardDataModel (`<service>.model.<catalog>.<model>`, project = catalog) |
| measure / calculated member, hierarchy level / secondary attribute | column (`dataTypeDisplay` = `measure` / `level`), named `<measure>` / `<dimension>.<attribute>` |
| dataset's warehouse table → model | lineage edge with column mappings (for the services in `dbServicePrefixes`) |

The export is the *published definition*; what the engine exposes per model follows
from the joins, which `atscale_export.py` replays (join-level reach, role-play naming
such as `Order Date Dimension.Order Year`, snowflakes, SCD extras, perspectives,
visibility). The result matches the engine's own XMLA view column for column (it was
verified cube by cube against all prod catalogs when the XMLA connector was retired).

### Runtime

The class must be importable by the ingestion runtime (the pipeline Airflow
image, see `atscale-cicd-infrastructure/.../airflow-image/Dockerfile`):

```
pip install --no-deps "git+https://github.com/muellermarco/atscale-cicd-pipeline.git@main#subdirectory=om_connectors"
```

### Service (UI → Add New Service → Dashboard → CustomDashboard)

- Source Python Class: `om_connectors.atscale_api.AtScaleApiSource`
- Connection options:
  - `api_url` = the AtScale host, e.g. `https://prod.example.com` (the API is served at
    `/v1/...`, *not* under `/api`), or `api_url_env` (default `ATSCALE_API_URL`)
  - `api_token_env` = env var holding a Design Center API token (profile icon → Account →
    API Token; it does not expire), default `ATSCALE_API_TOKEN` — or `api_token` inline
  - `catalogs` = optional comma-separated allow-list of catalog names
  - `verify_ssl` = `true` to verify TLS (default `false`, demo certs are self-signed)
  - `connections` = optional `<AtScale connection id>=<OM database service>[;...]`
    (e.g. `Snowflake Partner=snowflake_partner;gbq=bigquery_partner`) so a dataset's
    table is only looked up in the matching database service
- Lineage: set the metadata agent's *Lineage Information → Database Service Prefixes*
  (`sourceConfig.lineageInformation.dbServicePrefixes`) to the warehouse services already
  ingested in OpenMetadata; without it no lineage is emitted.

**Test Connection** runs a real `GET /v1/catalogs`.

Log noise to expect: `Error importing connection class for CustomDashboard` — the
framework first looks for a stock connection spec, finds none for custom
connectors, and falls back to this module's `get_connection`/`test_connection`.

`tools/om_sync.py` (release time) still adds the SML descriptions / MDX and creates
data models + lineage only where the connector has not run yet.

### Tests

```
pip install -e . pytest && pytest tests/                                        # offline: parser rules, client, source + lineage
OM_URL=http://host:8585/api OM_TOKEN=<bot jwt> python tests/e2e_fake_api.py        # full workflow against a live OpenMetadata, canned export
```
