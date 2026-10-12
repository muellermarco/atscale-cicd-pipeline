"""OpenMetadata custom dashboard connector for AtScale, on the container API.

OpenMetadata has no cube entity, so the semantic layer lands as a dashboard
service — the shape the Tableau / Power BI connectors use:

    catalog                      -> Dashboard             (name = catalog)
    model / cube / perspective   -> DashboardDataModel    (name = "<catalog>.<model>")
    measures + hierarchy levels  -> columns of the data model
                                    ("<measure>", "<dimension>.<attribute>")

read from the published catalog definitions
(https://documentation.atscale.com/container-api/export-catalog-representation):

    GET /v1/catalogs                      published catalogs and their models
    GET /v1/catalogs/{id}/export          the catalog definition (Accept: application/json)

``atscale_export.CatalogExport`` turns an export into the flat column list (the
same names the engine reports over XMLA). The export also names every dataset's
warehouse table, so warehouse lineage (table -> data model, with column
mappings) is emitted for the database services listed under
``sourceConfig.lineageInformation.dbServicePrefixes``.

Service type CustomDashboard, sourcePythonClass
``om_connectors.atscale_api.AtScaleApiSource``. connectionOptions:

    api_url        AtScale host, e.g. https://prod.example.com (the API lives at /v1,
                   not under /api)
    api_url_env    env var holding the host instead (default ATSCALE_API_URL)
    api_token      Design Center API token (Account -> API Token; does not expire)
    api_token_env  env var holding the token instead (default ATSCALE_API_TOKEN) —
                   preferred, keeps the token out of the service config
    catalogs       optional comma-separated allow-list of catalog names
    verify_ssl     "true" to verify TLS (default "false": self-signed demo certs)
    connections    optional "<AtScale connection id>=<OM database service>[;...]"
                   so lineage for a dataset is only looked up in the matching
                   database service (default: every service in dbServicePrefixes)
"""

from __future__ import annotations

import os
import traceback
from collections.abc import Iterable
from typing import Any, Optional
from urllib.parse import quote, urlsplit

import requests

from metadata.generated.schema.api.data.createDashboard import CreateDashboardRequest
from metadata.generated.schema.api.data.createDashboardDataModel import (
    CreateDashboardDataModelRequest,
)
from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.generated.schema.entity.automations.workflow import (
    Workflow as AutomationWorkflow,
)
from metadata.generated.schema.entity.data.dashboardDataModel import (
    DashboardDataModel,
    DataModelType,
)
from metadata.generated.schema.entity.data.table import Column, DataType, Table
from metadata.generated.schema.entity.services.connections.dashboard.customDashboardConnection import (
    CustomDashboardConnection,
)
from metadata.generated.schema.entity.services.connections.testConnectionResult import (
    TestConnectionResult,
)
from metadata.generated.schema.metadataIngestion.workflow import Source as WorkflowSource
from metadata.generated.schema.type.basic import (
    EntityName,
    FullyQualifiedEntityName,
    Markdown,
    SourceUrl,
)
from metadata.generated.schema.type.entityLineage import ColumnLineage
from metadata.ingestion.api.models import Either, StackTraceError
from metadata.ingestion.api.steps import InvalidSourceException
from metadata.ingestion.connections.test_connections import (
    TestConnectionStep,
    _test_connection_steps,
)
from metadata.ingestion.lineage.sql_lineage import get_column_fqn, search_table_entities
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.source.dashboard.dashboard_service import DashboardServiceSource
from metadata.utils import fqn
from metadata.utils.constants import THREE_MIN
from metadata.utils.filters import filter_by_datamodel
from metadata.utils.logger import ingestion_logger

from om_connectors.atscale_export import CatalogExport

logger = ingestion_logger()


class AtScaleApiClient:
    """Minimal container-API client: list catalogs, export one."""

    def __init__(self, base_url: str, token: str, verify_ssl: bool = False, timeout: int = 300):
        if not base_url:
            raise ValueError("api_url is empty — set connectionOptions.api_url or api_url_env")
        if not token:
            raise ValueError("api token is empty — set connectionOptions.api_token or api_token_env")
        self.base_url = base_url.rstrip("/")
        if self.base_url.endswith("/api"):                 # the container API is served at /v1, not /api/v1
            self.base_url = self.base_url[:-4]
        self.verify_ssl = verify_ssl
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {token}", "Accept": "application/json"})

    @property
    def host_url(self) -> str:
        u = urlsplit(self.base_url)
        return f"{u.scheme}://{u.netloc}"

    def _get(self, path: str) -> Any:
        resp = self.session.get(f"{self.base_url}{path}", timeout=self.timeout, verify=self.verify_ssl)
        if resp.status_code == 401:
            raise RuntimeError(f"AtScale API {path}: 401 unauthorized — check the API token")
        resp.raise_for_status()
        return resp.json()

    def catalogs(self) -> list[dict]:
        """Published catalogs: id, name, caption, publishedAt, publishedBy, models[]."""
        data = self._get("/v1/catalogs")
        if not isinstance(data, list):
            raise RuntimeError(f"AtScale API /v1/catalogs: unexpected payload {str(data)[:200]}")
        return sorted(data, key=lambda c: c.get("name", ""))

    def export(self, catalog_id: str) -> dict:
        data = self._get(f"/v1/catalogs/{quote(catalog_id, safe='')}/export")
        if not isinstance(data, dict) or "cubes" not in data:
            raise RuntimeError(f"AtScale API export of {catalog_id}: unexpected payload {str(data)[:200]}")
        return data


def connection_options(connection: CustomDashboardConnection) -> dict[str, str]:
    opts = connection.connectionOptions.root if connection.connectionOptions else {}
    return {k: str(v) for k, v in (opts or {}).items()}


# --- module-level hooks the OpenMetadata importer resolves for custom connectors
def get_connection(connection: CustomDashboardConnection) -> AtScaleApiClient:
    opts = connection_options(connection)
    url = opts.get("api_url") or os.environ.get(opts.get("api_url_env", "ATSCALE_API_URL"), "")
    token = opts.get("api_token") or os.environ.get(opts.get("api_token_env", "ATSCALE_API_TOKEN"), "")
    return AtScaleApiClient(url, token, verify_ssl=opts.get("verify_ssl", "false").lower() == "true")


def test_connection(
    metadata: OpenMetadata,
    client: AtScaleApiClient,
    service_connection: CustomDashboardConnection,
    automation_workflow: Optional[AutomationWorkflow] = None,
    timeout_seconds: Optional[int] = THREE_MIN,
) -> TestConnectionResult:
    """Real check: list the published catalogs the token can see.

    The stock ``test_connection_steps`` needs a server-side TestConnectionDefinition,
    which OpenMetadata does not ship for CustomDashboard — so the step is declared
    here and run through the same step runner (it also reports back to the
    automation workflow, which is what the UI's Test Connection button shows).
    """
    def _catalogs():
        cats = client.catalogs()
        if not cats:
            raise RuntimeError("AtScale API reachable but lists no published catalogs")
        logger.info(f"AtScale catalogs: {[c.get('name') for c in cats]}")

    steps = [TestConnectionStep(
        name="ListCatalogs",
        description="GET /v1/catalogs against the AtScale container API",
        mandatory=True,
        function=_catalogs,
        error_message="Could not list catalogs — check api_url (host without /api), the API token and TLS",
        short_circuit=True,
    )]
    return _test_connection_steps(metadata, steps, automation_workflow)


def build_columns(entries: Iterable[tuple[str, str, DataType, str, str]]) -> list[Column]:
    """(name, display name, data type, kind, description) -> Column list. Measures and
    levels share one namespace; a repeated name gets a ' (n)' suffix."""
    seen: set[str] = set()
    cols: list[Column] = []
    for name, display, dtype, kind, description in entries:
        base, n = name, 2
        while name in seen:
            name = f"{base} ({n})"
            n += 1
        seen.add(name)
        cols.append(Column(
            name=name[:256], displayName=display or name, dataType=dtype, dataTypeDisplay=kind,
            description=Markdown(description) if description else None, ordinalPosition=len(cols) + 1))
    return cols


class AtScaleApiSource(DashboardServiceSource):
    """Catalog -> Dashboard, model / perspective -> DashboardDataModel, from catalog exports."""

    client: AtScaleApiClient

    @classmethod
    def create(cls, config_dict: dict, metadata: OpenMetadata, pipeline_name: Optional[str] = None):
        config: WorkflowSource = WorkflowSource.model_validate(config_dict)
        connection = config.serviceConnection.root.config
        if not isinstance(connection, CustomDashboardConnection):
            raise InvalidSourceException(f"Expected CustomDashboardConnection, but got {connection}")
        return cls(config, metadata)

    def __init__(self, config: WorkflowSource, metadata: OpenMetadata):
        super().__init__(config, metadata)
        self.options = connection_options(self.service_connection)
        allow = self.options.get("catalogs", "")
        self._catalog_allow = {c.strip() for c in allow.split(",") if c.strip()}

    # --- dashboards = catalogs ---------------------------------------------
    def get_dashboards_list(self) -> Optional[list[Any]]:
        return [c for c in self.client.catalogs()
                if not self._catalog_allow or c.get("name", "") in self._catalog_allow]

    def get_dashboard_name(self, dashboard: Any) -> str:
        return dashboard.get("name", "")

    def get_dashboard_details(self, dashboard: Any) -> dict:
        export = CatalogExport(self.client.export(dashboard["id"]))
        models = [{"name": m["name"], "caption": m["caption"], "description": m["description"], "model": m}
                  for m in export.models()]
        published = " · ".join(x for x in (
            f"published {dashboard['publishedAt']}" if dashboard.get("publishedAt") else "",
            f"by {dashboard['publishedBy']}" if dashboard.get("publishedBy") else "") if x)
        return {
            "catalog": dashboard.get("name", ""), "caption": dashboard.get("caption") or dashboard.get("name", ""),
            "description": (f"AtScale catalog `{dashboard.get('name', '')}` — {len(models)} model(s) "
                            f"(incl. perspectives), from the container API export. "
                            f"Each model is listed as a data model of this entry." + (f" {published}." if published else "")),
            "models": models, "export": export, "api": dashboard,
        }

    def yield_dashboard(self, dashboard_details: Any) -> Iterable[Either[CreateDashboardRequest]]:
        catalog = dashboard_details["catalog"]
        try:
            request = CreateDashboardRequest(
                name=EntityName(catalog),
                displayName=dashboard_details["caption"],
                description=Markdown(dashboard_details["description"]),
                sourceUrl=SourceUrl(self.client.host_url),
                service=FullyQualifiedEntityName(self.context.get().dashboard_service),
            )
            yield Either(right=request)
            self.register_record(dashboard_request=request)
        except Exception as exc:
            yield Either(left=StackTraceError(name=catalog, error=f"Error creating catalog entry [{catalog}]: {exc}",
                                              stackTrace=traceback.format_exc()))

    def yield_dashboard_chart(self, dashboard_details: Any) -> Iterable:
        yield from ()                                       # models have no charts

    # --- data models = models / perspectives -----------------------------------
    def yield_datamodel(self, dashboard_details: Any) -> Iterable[Either[CreateDashboardDataModelRequest]]:
        catalog = dashboard_details["catalog"]
        export: CatalogExport = dashboard_details["export"]
        for model in dashboard_details["models"]:
            name = model["name"]
            if filter_by_datamodel(self.source_config.dataModelFilterPattern, name):
                self.status.filter(name, "Data model filtered out.")
                continue
            try:
                request = CreateDashboardDataModelRequest(
                    name=EntityName(f"{catalog}.{name}"),
                    displayName=model.get("caption") or name,
                    description=Markdown(model.get("description") or f"Model `{name}` of catalog `{catalog}`"),
                    project=catalog,
                    sourceUrl=SourceUrl(self.client.host_url),
                    service=FullyQualifiedEntityName(self.context.get().dashboard_service),
                    columns=self.model_columns(export, model),
                    dataModelType=DataModelType.SupersetDataModel.value,   # OM has no generic/cube type
                )
                yield Either(right=request)
                self.register_record_datamodel(datamodel_request=request)
            except Exception as exc:
                yield Either(left=StackTraceError(name=f"{catalog}.{name}",
                                                  error=f"Error yielding model [{name}]: {exc}",
                                                  stackTrace=traceback.format_exc()))

    @staticmethod
    def model_columns(export: CatalogExport, model: dict) -> list[Column]:
        return build_columns(
            (c["name"], c["display"], getattr(DataType, c["dtype"], DataType.STRING), c["kind"], c["description"])
            for c in export.columns(model["model"]))

    # --- warehouse lineage ------------------------------------------------------
    def _connection_services(self) -> dict[str, str]:
        """connectionOptions.connections: 'Snowflake Partner=snowflake_partner;gbq=bigquery'."""
        out = {}
        for part in self.options.get("connections", "").replace(",", ";").split(";"):
            if "=" in part:
                k, v = part.split("=", 1)
                out[k.strip()] = v.strip()
        return out

    def _datamodel_entity(self, catalog: str, name: str) -> Optional[DashboardDataModel]:
        dm_fqn = fqn.build(self.metadata, entity_type=DashboardDataModel,
                           service_name=self.context.get().dashboard_service, data_model_name=f"{catalog}.{name}")
        return self.metadata.get_by_name(entity=DashboardDataModel, fqn=dm_fqn) if dm_fqn else None

    def yield_dashboard_lineage_details(self, dashboard_details: Any,
                                        db_service_prefix: Optional[str] = None) -> Iterable[Either[AddLineageRequest]]:
        """table -> data model edges for every warehouse table a model's datasets read,
        with column lineage from the physical columns behind each measure / level."""
        if not db_service_prefix:
            logger.debug("AtScale lineage skipped: no dbServicePrefixes configured")
            return
        service, database, schema, table_filter = self.parse_db_service_prefix(db_service_prefix)
        export: CatalogExport = dashboard_details["export"]
        catalog = dashboard_details["catalog"]
        services = self._connection_services()
        for model in dashboard_details["models"]:
            try:
                yield from self._model_lineage(export, catalog, model, service, database, schema, table_filter, services)
            except Exception as exc:
                yield Either(left=StackTraceError(
                    name=f"{catalog}.{model['name']}",
                    error=f"Error building lineage for model [{model['name']}]: {exc}",
                    stackTrace=traceback.format_exc()))

    def _model_lineage(self, export: CatalogExport, catalog: str, model: dict, service: str,
                       database: Optional[str], schema: Optional[str], table_filter: Optional[str],
                       services: dict[str, str]) -> Iterable[Either[AddLineageRequest]]:
        datamodel = self._datamodel_entity(catalog, model["name"])
        if not datamodel:
            logger.warning(f"lineage: data model {catalog}.{model['name']} not found in OpenMetadata yet")
            return
        columns = export.columns(model["model"])
        for ds in export.datasets(model["model"]):
            if not ds["table"]:
                continue                                   # SQL datasets: no single table to point at
            wanted = services.get(ds["connection"])
            if wanted and wanted != service:
                continue
            if database and ds["database"] and database.lower() != ds["database"].lower():
                continue
            if schema and ds["schema"] and schema.lower() != ds["schema"].lower():
                continue
            if table_filter and table_filter.lower() != ds["table"].lower():
                continue
            tables: list[Table] = search_table_entities(
                self.metadata, service, ds["database"] or database, ds["schema"] or schema, ds["table"]) or []
            if not tables:
                logger.debug(f"lineage: {service} table {ds['database']}.{ds['schema']}.{ds['table']} not in OpenMetadata")
                continue
            for table in tables:
                column_lineage = self._column_lineage_for(table, datamodel, columns, ds["id"])
                yield self._get_add_lineage_request(to_entity=datamodel, from_entity=table,
                                                    column_lineage=column_lineage or None)

    def _column_lineage_for(self, table: Table, datamodel: DashboardDataModel, columns: list[dict],
                            dataset_id: str) -> list[ColumnLineage]:
        out = []
        for col in columns:
            phys = sorted({c for ds_id, c in col["sources"] if ds_id == dataset_id})
            froms = [f for f in (get_column_fqn(table_entity=table, column=c) for c in phys) if f]
            to = self._get_data_model_column_fqn(data_model_entity=datamodel, column=col["name"])
            if froms and to:
                out.append(ColumnLineage(fromColumns=froms, toColumn=to))
        return out
