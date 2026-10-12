"""AtScaleApiSource through the real OpenMetadata source machinery with the API client
and the OpenMetadata server mocked: dashboards, data models, lineage."""
from unittest import mock

import om_connectors.atscale_api as api
from om_connectors.atscale_api import AtScaleApiClient, AtScaleApiSource
from metadata.generated.schema.entity.data.dashboardDataModel import DashboardDataModel
from metadata.generated.schema.entity.data.table import Column, DataType, Table
from metadata.generated.schema.type.entityReference import EntityReference

from tests.test_export import EXPORT

CATALOGS = [{"id": "cat1", "name": "Demo Catalog", "caption": "Demo", "publishedAt": "2026-10-12T16:33:57Z",
             "publishedBy": "admin", "models": []},
            {"id": "cat2", "name": "Other", "caption": "Other", "models": []}]
CONFIG = {
    "type": "customdashboard", "serviceName": "atscale-test",
    "serviceConnection": {"config": {
        "type": "CustomDashboard", "sourcePythonClass": "om_connectors.atscale_api.AtScaleApiSource",
        "connectionOptions": {"api_url": "https://fake.example", "api_token": "t", "catalogs": "Demo Catalog",
                              "connections": "wh=snowflake_partner"}}},
    "sourceConfig": {"config": {"type": "DashboardMetadata",
                                "lineageInformation": {"dbServicePrefixes": ["snowflake_partner", "other_svc"]}}},
}


def _source():
    metadata = mock.Mock()
    with mock.patch.object(AtScaleApiClient, "catalogs", return_value=CATALOGS), \
         mock.patch.object(AtScaleApiClient, "export", return_value=EXPORT):
        src = AtScaleApiSource.create(CONFIG, metadata)
        src.context.get().dashboard_service = "atscale-test"
        dashboards = src.get_dashboards_list()
        details = src.get_dashboard_details(dashboards[0])
    return src, metadata, dashboards, details


def test_dashboards_and_datamodels():
    src, _, dashboards, details = _source()
    assert [src.get_dashboard_name(d) for d in dashboards] == ["Demo Catalog"]        # allow-list applied
    dash = list(src.yield_dashboard(details))[0].right
    assert dash.name.root == "Demo Catalog" and dash.displayName == "Demo"
    assert "published 2026-10-12T16:33:57Z · by admin" in dash.description.root
    dms = [e.right for e in src.yield_datamodel(details)]
    assert [d.name.root for d in dms] == ["Demo Catalog.Sales", "Demo Catalog.Sales - No PII"]
    assert dms[0].project == "Demo Catalog" and dms[0].description.root == "Sales model"
    cols = {c.name.root: c for c in dms[0].columns}
    assert cols["Sales Amount"].dataType == DataType.DOUBLE and cols["Sales Amount"].dataTypeDisplay == "measure"
    assert cols["Ship Date Dimension.Ship Year"].dataType == DataType.BIGINT and cols["Ship Date Dimension.Ship Year"].displayName == "Ship Year"
    assert len(dms[1].columns) == len(dms[0].columns) - 2                               # perspective hides two


def _entities(dm_request, export, model):
    svc = EntityReference(id="00000000-0000-0000-0000-000000000009", type="dashboardService")
    dm = DashboardDataModel(
        id="00000000-0000-0000-0000-000000000001", name=dm_request.name, dataModelType=dm_request.dataModelType, service=svc,
        fullyQualifiedName=f'atscale-test.model."{dm_request.name.root}"',
        columns=[Column(name=c.name, dataType=c.dataType, fullyQualifiedName=f'atscale-test.model."{dm_request.name.root}".{c.name.root}')
                 for c in dm_request.columns])
    physical = {d["table"]: d["columns"] for d in export.datasets(model)}

    def search(md, service, database, schema, table):
        fq = f"{service}.{database}.{schema}.{table}"
        return [Table(id="00000000-0000-0000-0000-000000000002", name=table, fullyQualifiedName=fq,
                      columns=[Column(name=c, dataType=DataType.STRING, fullyQualifiedName=f"{fq}.{c}") for c in physical[table]])]
    return dm, search


def test_lineage():
    src, metadata, _, details = _source()
    dm_request = list(src.yield_datamodel(details))[0].right
    dm, search = _entities(dm_request, details["export"], details["models"][0]["model"])
    metadata.get_by_name = mock.Mock(return_value=dm)
    with mock.patch.object(api, "search_table_entities", side_effect=search) as st, \
         mock.patch.object(api.fqn, "build", return_value="atscale-test.model.x"):
        edges = [e.right.edge for e in src.yield_dashboard_lineage_details(details, "snowflake_partner")]
        assert st.call_args_list[0].args[1:] == ("snowflake_partner", "DB", "SCH", "FACT_SALES")
        by_table = {e.fromEntity.fullyQualifiedName.rsplit(".", 1)[1]: e for e in edges}
        assert set(by_table) >= {"FACT_SALES", "DIM_DATE", "DIM_CUSTOMER", "DIM_GENDER", "DIM_GEO"}
        assert all(e.toEntity.fullyQualifiedName == 'atscale-test.model."Demo Catalog.Sales"' for e in edges)
        prefix = 'atscale-test.model."Demo Catalog.Sales".'
        fact = {cl.toColumn.root.removeprefix(prefix): [f.root for f in cl.fromColumns]
                for cl in by_table["FACT_SALES"].lineageDetails.columnsLineage}
        assert fact["Sales Amount"] == ["snowflake_partner.DB.SCH.FACT_SALES.SALES_AMOUNT"]
        assert fact["Ship Date Dimension.Ship Day"] == ["snowflake_partner.DB.SCH.FACT_SALES.SHIP_DAY"]
        assert fact["Date Dimension.Day"] == ["snowflake_partner.DB.SCH.FACT_SALES.DAY"]
        assert "Avg Price" not in fact                                                   # calculated: no physical column
        # the connections map pins connection 'wh' to snowflake_partner: nothing for other services
        assert list(src.yield_dashboard_lineage_details(details, "other_svc")) == []
        assert list(src.yield_dashboard_lineage_details(details, None)) == []
        # database filter in the prefix
        assert list(src.yield_dashboard_lineage_details(details, "snowflake_partner.OTHER_DB")) == []
        narrowed = list(src.yield_dashboard_lineage_details(details, "snowflake_partner.db.sch.dim_geo"))
        assert len(narrowed) == 2                                                       # one edge per model: Sales + its perspective
