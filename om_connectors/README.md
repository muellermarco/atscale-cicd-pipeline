# om_connectors

An OpenMetadata custom connector that loads AtScale semantic models into OpenMetadata, with lineage to the warehouse tables.

OpenMetadata (open source) has no connector for AtScale or for cubes. This package adds a CustomDashboard connector. It reads the published catalogs from the AtScale [container API](https://documentation.atscale.com/container-api/export-catalog-representation) (`GET /v1/catalogs` and `GET /v1/catalogs/{id}/export`) and creates these OpenMetadata entities:

| AtScale | OpenMetadata |
|---|---|
| Catalog | Dashboard (`<service>.<catalog>`) |
| Model or cube (and perspective) | DashboardDataModel (`<service>.model.<catalog>.<model>`, project = catalog) |
| Measure or calculated member, hierarchy level or secondary attribute | Column (`dataTypeDisplay` = `measure` or `level`), named `<measure>` or `<dimension>.<attribute>` |
| Warehouse table of a dataset, to the model | Lineage edge with column mappings (for the services in `dbServicePrefixes`) |

The export is the published definition of the catalog. The columns that the engine shows for each model depend on the joins, and `atscale_export.py` replays them: reach through joins, role-play names such as `Order Date Dimension.Order Year`, snowflake dimensions, SCD extras, perspectives, and visibility. The result matches the engine's own XMLA view column for column. This was checked cube by cube against all catalogs of a live instance.

## Technologies

- Python 3.10 or later (`requires-python` in `pyproject.toml`)
- OpenMetadata server 2.0.4.0 and `openmetadata-ingestion` 2.0.4.0. The two versions must match.
- Apache Airflow 3.2.2 as the ingestion runtime
- AtScale container API (`/v1` at the host root)

## Install the connector in the ingestion runtime

The connector class must be importable in the OpenMetadata ingestion runtime. In the demo environment, this is the pipeline Airflow image. Its Dockerfile is `infrastructure/helm/azure/airflow-image/Dockerfile` in the private repository `atscale-cicd-infrastructure`.

1. Install `openmetadata-ingestion==2.0.4.0` in the runtime, if it is not there.
2. Install the connector without dependencies:

   ```sh
   pip install --no-deps "git+https://github.com/muellermarco/atscale-cicd-pipeline.git@main#subdirectory=om_connectors"
   ```

   Result: `python -c "import om_connectors.atscale_api"` runs with no error.

`--no-deps` keeps pip from changing the Airflow dependencies. The runtime already supplies `openmetadata-ingestion` and `requests`.

## Add the AtScale service in OpenMetadata

1. Create a Design Center API token in AtScale: profile icon, **Account**, **API Token**.
2. Put the token in an environment variable of the ingestion runtime, for example `ATSCALE_API_TOKEN`.
3. In OpenMetadata, select **Add New Service**, **Dashboard**, **CustomDashboard**.
4. Set **Source Python Class** to `om_connectors.atscale_api.AtScaleApiSource`.
5. Add the connection options from the table below.

   **Warning:** `verify_ssl` is `false` by default, because the demo instances use self-signed certificates. Do the next step for every environment that is not a demo.
6. Set `verify_ssl` to `true`.
7. Select **Test Connection**.
   Result: the test runs a real `GET /v1/catalogs` and passes.
8. In the metadata agent, set **Lineage Information**, **Database Service Prefixes** (`sourceConfig.lineageInformation.dbServicePrefixes`) to the warehouse services in OpenMetadata.
   Result: the next run creates lineage edges. Without this setting, the connector creates no lineage.

### Connection options

| Name | Purpose | Default | Required |
|---|---|---|---|
| `api_url` | AtScale host, for example `https://<atscale-host>`. The API is at `/v1/...`, not under `/api`. | None | Yes, or `api_url_env` |
| `api_url_env` | Environment variable that holds the host | `ATSCALE_API_URL` | No |
| `api_token_env` | Environment variable that holds the Design Center API token. Use this, not `api_token`. | `ATSCALE_API_TOKEN` | Yes, or `api_token` |
| `api_token` | The token itself, stored in the service configuration. Avoid it. | None | No |
| `catalogs` | Comma-separated allow-list of catalog names | All catalogs | No |
| `verify_ssl` | `true` turns on TLS certificate checks | `false` | No |
| `connections` | Maps AtScale connections to OpenMetadata database services, `<AtScale connection id>=<OM database service>[;...]`, for example `Snowflake=snowflake_prod;gbq=bigquery_prod`. A dataset's table is then looked up only in the matching service. | None | No |

## Branding

The **Add New Service** page has no AtScale tile. A tile needs a native connector in OpenMetadata itself, so the service stays a CustomDashboard.

To show the AtScale logo instead of the generic dashboard icon, `tools/om_sync.py` sets two fields on the service on every run:

- `displayName`: `AtScale`
- `style`: `iconURL` points to the raw GitHub URL of [`assets/atscale.svg`](om_connectors/assets/atscale.svg), and `color` is `#0047BB`

If the service does not exist, the script creates it. Otherwise it changes only these two fields with a JSON Patch, never the connection. The OpenMetadata UI uses the service's own `style.iconURL` before the type icon, on the services list and on asset headers. To override the values, set `OM_SERVICE_DISPLAY_NAME`, `OM_SERVICE_ICON_URL`, and `OM_SERVICE_COLOR`.

At release time, `tools/om_sync.py` also adds the SML descriptions and MDX expressions. It creates data models and lineage only where the connector has not run yet.

## Expected log message

The log shows `Error importing connection class for CustomDashboard`. This is normal. The framework first looks for a stock connection specification, finds none for a custom connector, and then uses this module's `get_connection` and `test_connection`.

## Run the tests

1. Open the `om_connectors` folder.
2. Install the OpenMetadata ingestion package: `pip install "openmetadata-ingestion==2.0.4.0"`.
   The test `tests/test_source.py` imports the `metadata` package.
3. Install the connector and pytest: `pip install -e . pytest`.
4. Run the offline tests: `pytest tests/`.
   Result: the tests for the parser rules, the API client, the source, and the lineage pass.
5. To test the full workflow against a live OpenMetadata with a canned export, run:

   ```sh
   OM_URL=http://<om-host>:8585/api OM_TOKEN=<bot-jwt> python tests/e2e_fake_api.py
   ```

   Result: the script ingests the canned catalog into OpenMetadata.
