"""OpenMetadata custom dashboard connector for an XMLA endpoint — AtScale, or any
SSAS-compatible server (SSAS speaks the same DISCOVER rowsets).

Mapping (OpenMetadata has no "cube" entity, so the semantic layer lands as a
dashboard service, the same shape the Tableau/Power BI connectors use):

    XMLA catalog  -> Dashboard             (name = catalog)
    cube          -> DashboardDataModel    (name = "<catalog>.<cube>", displayName = cube)
    measures + hierarchy levels -> columns of the data model

Service type CustomDashboard, sourcePythonClass
``om_connectors.atscale_xmla.AtScaleXmlaSource``. connectionOptions:

    xmla_url      full XMLA URL. AtScale embeds the auth token in the path, so
                  prefer xmla_url_env to keep it out of the service config.
    xmla_url_env  name of an env var on the ingestion runtime holding the URL
    catalogs      optional comma-separated allow-list of catalogs
    verify_ssl    "true" to verify TLS (default "false": self-signed demo certs)

Warehouse lineage (cube -> tables) is not derivable from XMLA; the release-time
sync (tools/om_sync.py) adds it from the SML sources.
"""

from __future__ import annotations

import os
import traceback
from collections.abc import Iterable
from typing import Any, Optional
from urllib.parse import urlsplit
from xml.etree import ElementTree as ET
from xml.sax.saxutils import escape

import requests

from metadata.generated.schema.api.data.createDashboard import CreateDashboardRequest
from metadata.generated.schema.api.data.createDashboardDataModel import (
    CreateDashboardDataModelRequest,
)
from metadata.generated.schema.entity.automations.workflow import (
    Workflow as AutomationWorkflow,
)
from metadata.generated.schema.entity.data.dashboardDataModel import DataModelType
from metadata.generated.schema.entity.data.table import Column, DataType
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
from metadata.ingestion.api.models import Either, StackTraceError
from metadata.ingestion.api.steps import InvalidSourceException
from metadata.ingestion.connections.test_connections import (
    TestConnectionStep,
    _test_connection_steps,
)
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.source.dashboard.dashboard_service import DashboardServiceSource
from metadata.utils.constants import THREE_MIN
from metadata.utils.filters import filter_by_datamodel
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()

_SOAP = "{http://schemas.xmlsoap.org/soap/envelope/}"
_ROWSET = "{urn:schemas-microsoft-com:xml-analysis:rowset}"

# OLE DB DBTYPE codes as reported in DATA_TYPE / LEVEL_DBTYPE
_DBTYPE_TO_OM = {
    "2": DataType.SMALLINT, "3": DataType.INT, "4": DataType.FLOAT, "5": DataType.DOUBLE,
    "6": DataType.DECIMAL, "7": DataType.DATETIME, "8": DataType.STRING, "11": DataType.BOOLEAN,
    "14": DataType.DECIMAL, "16": DataType.TINYINT, "17": DataType.TINYINT, "18": DataType.SMALLINT,
    "19": DataType.INT, "20": DataType.BIGINT, "21": DataType.BIGINT, "129": DataType.STRING,
    "130": DataType.STRING, "131": DataType.DECIMAL, "133": DataType.DATE, "134": DataType.TIME,
    "135": DataType.DATETIME,
}


def _strip_brackets(unique_name: str) -> str:
    """'[Product Dimension].[Product Hierarchy]' -> 'Product Dimension.Product Hierarchy'."""
    return ".".join(p.strip("[]") for p in unique_name.split("].[")) if unique_name else ""


class XmlaClient:
    """Minimal XMLA Discover client (SOAP over HTTP)."""

    def __init__(self, url: str, verify_ssl: bool = False, timeout: int = 120):
        if not url:
            raise ValueError("xmla_url is empty — set connectionOptions.xmla_url or xmla_url_env")
        self.url = url
        self.verify_ssl = verify_ssl
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction": '"urn:schemas-microsoft-com:xml-analysis:Discover"',
        })

    @property
    def host_url(self) -> str:
        u = urlsplit(self.url)
        return f"{u.scheme}://{u.netloc}"

    def discover(self, request_type: str, restrictions: Optional[dict] = None,
                 properties: Optional[dict] = None) -> list[dict]:
        r = "".join(f"<{k}>{escape(str(v))}</{k}>" for k, v in (restrictions or {}).items())
        p = "".join(f"<{k}>{escape(str(v))}</{k}>" for k, v in {"Format": "Tabular", **(properties or {})}.items())
        body = (
            '<?xml version="1.0" encoding="utf-8"?>'
            '<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><soap:Body>'
            '<Discover xmlns="urn:schemas-microsoft-com:xml-analysis">'
            f"<RequestType>{request_type}</RequestType>"
            f"<Restrictions><RestrictionList>{r}</RestrictionList></Restrictions>"
            f"<Properties><PropertyList>{p}</PropertyList></Properties>"
            "</Discover></soap:Body></soap:Envelope>"
        )
        resp = self.session.post(self.url, data=body.encode(), timeout=self.timeout, verify=self.verify_ssl)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        fault = root.find(f".//{_SOAP}Fault")
        if fault is not None:
            raise RuntimeError(f"XMLA {request_type} fault: {''.join(fault.itertext()).strip()[:500]}")
        return [{c.tag.replace(_ROWSET, ""): (c.text or "") for c in row} for row in root.iter(f"{_ROWSET}row")]

    # --- rowsets the connector needs ---------------------------------------
    def catalogs(self) -> list[str]:
        return sorted({r["CATALOG_NAME"] for r in self.discover("DBSCHEMA_CATALOGS") if r.get("CATALOG_NAME")})

    def cubes(self, catalog: str) -> list[dict]:
        rows = self.discover("MDSCHEMA_CUBES", {"CATALOG_NAME": catalog}, {"Catalog": catalog})
        return [r for r in rows if r.get("CUBE_TYPE", "CUBE").upper() == "CUBE"]

    def measures(self, catalog: str, cube: str) -> list[dict]:
        return self.discover("MDSCHEMA_MEASURES", {"CATALOG_NAME": catalog, "CUBE_NAME": cube}, {"Catalog": catalog})

    def levels(self, catalog: str, cube: str) -> list[dict]:
        rows = self.discover("MDSCHEMA_LEVELS", {"CATALOG_NAME": catalog, "CUBE_NAME": cube}, {"Catalog": catalog})
        return [r for r in rows if r.get("LEVEL_NUMBER", "1") != "0"]          # skip the (All) level


def _options(connection: CustomDashboardConnection) -> dict:
    opts = connection.connectionOptions.root if connection.connectionOptions else {}
    return {k: str(v) for k, v in (opts or {}).items()}


# --- module-level hooks the OpenMetadata importer resolves for custom connectors
def get_connection(connection: CustomDashboardConnection) -> XmlaClient:
    opts = _options(connection)
    url = opts.get("xmla_url") or os.environ.get(opts.get("xmla_url_env", "ATSCALE_XMLA_URL"), "")
    return XmlaClient(url, verify_ssl=opts.get("verify_ssl", "false").lower() == "true")


def test_connection(
    metadata: OpenMetadata,
    client: XmlaClient,
    service_connection: CustomDashboardConnection,
    automation_workflow: Optional[AutomationWorkflow] = None,
    timeout_seconds: Optional[int] = THREE_MIN,
) -> TestConnectionResult:
    """Real check: a DISCOVER of the catalogs the token can see.

    The stock ``test_connection_steps`` needs a server-side TestConnectionDefinition,
    which OpenMetadata does not ship for CustomDashboard — so the steps are declared
    here and run through the same step runner (it also reports back to the
    automation workflow, which is what the UI's Test Connection button shows).
    """
    def _catalogs():
        cats = client.catalogs()
        if not cats:
            raise RuntimeError("XMLA endpoint reachable but exposes no catalogs")
        logger.info(f"XMLA catalogs: {cats}")

    steps = [TestConnectionStep(
        name="DiscoverCatalogs",
        description="DBSCHEMA_CATALOGS discover against the XMLA endpoint",
        mandatory=True,
        function=_catalogs,
        error_message="Could not discover catalogs — check the XMLA URL (incl. embedded token) and TLS",
        short_circuit=True,
    )]
    return _test_connection_steps(metadata, steps, automation_workflow)


class AtScaleXmlaSource(DashboardServiceSource):
    """Catalog -> Dashboard, cube -> DashboardDataModel."""

    client: XmlaClient

    @classmethod
    def create(cls, config_dict: dict, metadata: OpenMetadata, pipeline_name: Optional[str] = None):
        config: WorkflowSource = WorkflowSource.model_validate(config_dict)
        connection = config.serviceConnection.root.config
        if not isinstance(connection, CustomDashboardConnection):
            raise InvalidSourceException(f"Expected CustomDashboardConnection, but got {connection}")
        return cls(config, metadata)

    def __init__(self, config: WorkflowSource, metadata: OpenMetadata):
        super().__init__(config, metadata)
        allow = _options(self.service_connection).get("catalogs", "")
        self._catalog_allow = {c.strip() for c in allow.split(",") if c.strip()}

    # --- dashboards = catalogs ---------------------------------------------
    def get_dashboards_list(self) -> Optional[list[Any]]:
        cats = self.client.catalogs()
        return [c for c in cats if not self._catalog_allow or c in self._catalog_allow]

    def get_dashboard_name(self, dashboard: Any) -> str:
        return dashboard

    def get_dashboard_details(self, dashboard: Any) -> Any:
        return {"catalog": dashboard, "cubes": self.client.cubes(dashboard)}

    def yield_dashboard(self, dashboard_details: Any) -> Iterable[Either[CreateDashboardRequest]]:
        catalog = dashboard_details["catalog"]
        try:
            request = CreateDashboardRequest(
                name=EntityName(catalog),
                displayName=catalog,
                description=Markdown(
                    f"AtScale/XMLA catalog `{catalog}` — {len(dashboard_details['cubes'])} cube(s). "
                    "Each cube is listed as a data model of this entry."),
                sourceUrl=SourceUrl(self.client.host_url),
                service=FullyQualifiedEntityName(self.context.get().dashboard_service),
            )
            yield Either(right=request)
            self.register_record(dashboard_request=request)
        except Exception as exc:
            yield Either(left=StackTraceError(name=catalog, error=f"Error creating catalog entry [{catalog}]: {exc}",
                                              stackTrace=traceback.format_exc()))

    def yield_dashboard_chart(self, dashboard_details: Any) -> Iterable:
        yield from ()                                       # cubes have no charts

    def yield_dashboard_lineage_details(self, dashboard_details: Any,
                                        db_service_prefix: Optional[str] = None) -> Iterable:
        yield from ()                                       # warehouse lineage comes from om_sync.py

    # --- data models = cubes -----------------------------------------------
    def yield_datamodel(self, dashboard_details: Any) -> Iterable[Either[CreateDashboardDataModelRequest]]:
        catalog = dashboard_details["catalog"]
        for cube in dashboard_details["cubes"]:
            cube_name = cube.get("CUBE_NAME", "")
            if filter_by_datamodel(self.source_config.dataModelFilterPattern, cube_name):
                self.status.filter(cube_name, "Data model filtered out.")
                continue
            try:
                columns = self._columns(catalog, cube_name)
                request = CreateDashboardDataModelRequest(
                    name=EntityName(f"{catalog}.{cube_name}"),
                    displayName=cube.get("CUBE_CAPTION") or cube_name,
                    description=Markdown(cube.get("DESCRIPTION") or f"Cube `{cube_name}` of catalog `{catalog}`"),
                    project=catalog,
                    sourceUrl=SourceUrl(self.client.host_url),
                    service=FullyQualifiedEntityName(self.context.get().dashboard_service),
                    columns=columns,
                    dataModelType=DataModelType.SupersetDataModel.value,   # OM has no generic/cube type
                )
                yield Either(right=request)
                self.register_record_datamodel(datamodel_request=request)
            except Exception as exc:
                yield Either(left=StackTraceError(name=f"{catalog}.{cube_name}",
                                                  error=f"Error yielding cube [{cube_name}]: {exc}",
                                                  stackTrace=traceback.format_exc()))

    def _columns(self, catalog: str, cube: str) -> list[Column]:
        seen: set[str] = set()
        cols: list[Column] = []

        def add(name: str, display: str, dtype: DataType, kind: str, description: str) -> None:
            base, n = name, 2
            while name in seen:                              # measures and levels share one namespace
                name = f"{base} ({n})"; n += 1
            seen.add(name)
            cols.append(Column(
                name=name[:256], displayName=display or name, dataType=dtype, dataTypeDisplay=kind,
                description=Markdown(description) if description else None, ordinalPosition=len(cols) + 1))

        for m in self.client.measures(catalog, cube):
            if m.get("MEASURE_IS_VISIBLE", "true").lower() == "false":
                continue
            desc = m.get("DESCRIPTION") or ""
            extra = " · ".join(x for x in (
                f"measure group: {m['MEASUREGROUP_NAME']}" if m.get("MEASUREGROUP_NAME") else "",
                f"format: {m['DEFAULT_FORMAT_STRING']}" if m.get("DEFAULT_FORMAT_STRING") else "",
                f"expression: {m['EXPRESSION']}" if m.get("EXPRESSION") else "") if x)
            add(m.get("MEASURE_NAME", ""), m.get("MEASURE_CAPTION", ""),
                _DBTYPE_TO_OM.get(m.get("DATA_TYPE", ""), DataType.DOUBLE), "measure",
                " — ".join(x for x in (desc, extra) if x))

        for lvl in self.client.levels(catalog, cube):
            if lvl.get("LEVEL_IS_VISIBLE", "true").lower() == "false":
                continue
            dim = _strip_brackets(lvl.get("DIMENSION_UNIQUE_NAME", ""))
            hier = _strip_brackets(lvl.get("HIERARCHY_UNIQUE_NAME", "")).split(".")[-1]
            add(f"{dim}.{lvl.get('LEVEL_NAME', '')}", lvl.get("LEVEL_CAPTION", ""),
                _DBTYPE_TO_OM.get(lvl.get("LEVEL_DBTYPE", ""), DataType.STRING), "level",
                " — ".join(x for x in (lvl.get("DESCRIPTION") or "", f"hierarchy: {hier}" if hier else "") if x))
        return cols
