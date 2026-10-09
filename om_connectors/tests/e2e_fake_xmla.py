"""End-to-end check of the connector through the real OpenMetadata ingestion
workflow, with the XMLA client replaced by canned MDSCHEMA rowsets — so the
whole topology (service -> data models -> dashboard -> lineage) is exercised
against a live OpenMetadata without touching an XMLA endpoint.

  OM_URL=http://host:8585/api OM_TOKEN=<jwt> python e2e_fake_xmla.py [service-name]

Creates/updates the service (default atscale-xmla-e2e); delete it afterwards.
"""
import os
import sys

import om_connectors.atscale_xmla as mod

ROWS = {
    "DBSCHEMA_CATALOGS": [{"CATALOG_NAME": "Sales Insights - Snowflake_main"}],
    "MDSCHEMA_CUBES": [
        {"CATALOG_NAME": "Sales Insights - Snowflake_main", "CUBE_NAME": "Sales", "CUBE_CAPTION": "Sales",
         "CUBE_TYPE": "CUBE", "DESCRIPTION": "Internet sales model"},
        {"CATALOG_NAME": "Sales Insights - Snowflake_main", "CUBE_NAME": "Inventory", "CUBE_CAPTION": "Inventory",
         "CUBE_TYPE": "CUBE", "DESCRIPTION": ""},
    ],
    "MDSCHEMA_MEASURES": [
        {"CUBE_NAME": "Sales", "MEASURE_NAME": "salesamount1", "MEASURE_CAPTION": "Sales Amount",
         "MEASURE_UNIQUE_NAME": "[Measures].[salesamount1]", "DATA_TYPE": "5", "DESCRIPTION": "Sum of sales",
         "MEASUREGROUP_NAME": "Sales Metrics", "DEFAULT_FORMAT_STRING": "$#,##0", "MEASURE_IS_VISIBLE": "true"},
        {"CUBE_NAME": "Sales", "MEASURE_NAME": "orderquantity1", "MEASURE_CAPTION": "Order Quantity",
         "MEASURE_UNIQUE_NAME": "[Measures].[orderquantity1]", "DATA_TYPE": "20", "DESCRIPTION": "",
         "MEASUREGROUP_NAME": "Sales Metrics", "DEFAULT_FORMAT_STRING": "", "MEASURE_IS_VISIBLE": "true"},
        {"CUBE_NAME": "Sales", "MEASURE_NAME": "hidden1", "MEASURE_CAPTION": "Hidden", "DATA_TYPE": "5",
         "MEASURE_IS_VISIBLE": "false"},
        {"CUBE_NAME": "Inventory", "MEASURE_NAME": "stock", "MEASURE_CAPTION": "Stock", "DATA_TYPE": "3",
         "MEASUREGROUP_NAME": "Inventory", "MEASURE_IS_VISIBLE": "true"},
    ],
    "MDSCHEMA_LEVELS": [
        {"CUBE_NAME": "Sales", "DIMENSION_UNIQUE_NAME": "[Product Dimension]",
         "HIERARCHY_UNIQUE_NAME": "[Product Dimension].[Product Hierarchy]", "LEVEL_NAME": "(All)",
         "LEVEL_CAPTION": "(All)", "LEVEL_NUMBER": "0", "LEVEL_DBTYPE": "130", "LEVEL_IS_VISIBLE": "true"},
        {"CUBE_NAME": "Sales", "DIMENSION_UNIQUE_NAME": "[Product Dimension]",
         "HIERARCHY_UNIQUE_NAME": "[Product Dimension].[Product Hierarchy]", "LEVEL_NAME": "Product Category",
         "LEVEL_CAPTION": "Product Category", "LEVEL_NUMBER": "1", "LEVEL_DBTYPE": "130",
         "DESCRIPTION": "Product Sub Category", "LEVEL_IS_VISIBLE": "true"},
        {"CUBE_NAME": "Sales", "DIMENSION_UNIQUE_NAME": "[Date Dimension]",
         "HIERARCHY_UNIQUE_NAME": "[Date Dimension].[Date Hierarchy]", "LEVEL_NAME": "Year",
         "LEVEL_CAPTION": "Year", "LEVEL_NUMBER": "1", "LEVEL_DBTYPE": "3", "LEVEL_IS_VISIBLE": "true"},
        {"CUBE_NAME": "Inventory", "DIMENSION_UNIQUE_NAME": "[Product Dimension]",
         "HIERARCHY_UNIQUE_NAME": "[Product Dimension].[Product Hierarchy]", "LEVEL_NAME": "Product Category",
         "LEVEL_CAPTION": "Product Category", "LEVEL_NUMBER": "1", "LEVEL_DBTYPE": "130", "LEVEL_IS_VISIBLE": "true"},
    ],
}


def fake_discover(self, request_type, restrictions=None, properties=None):
    rows = ROWS[request_type]
    cube = (restrictions or {}).get("CUBE_NAME")
    return [r for r in rows if not cube or r.get("CUBE_NAME") == cube]


mod.XmlaClient.discover = fake_discover

service = sys.argv[1] if len(sys.argv) > 1 else "atscale-xmla-e2e"
config = {
    "source": {
        "type": "customdashboard",
        "serviceName": service,
        "serviceConnection": {"config": {
            "type": "CustomDashboard",
            "sourcePythonClass": "om_connectors.atscale_xmla.AtScaleXmlaSource",
            "connectionOptions": {"xmla_url": "https://fake.example/engine/xmla/token"},
        }},
        "sourceConfig": {"config": {"type": "DashboardMetadata"}},
    },
    "sink": {"type": "metadata-rest", "config": {}},
    "workflowConfig": {
        "loggerLevel": "INFO",
        "openMetadataServerConfig": {
            "hostPort": os.environ["OM_URL"],
            "authProvider": "openmetadata",
            "securityConfig": {"jwtToken": os.environ["OM_TOKEN"]},
        },
    },
}

from metadata.workflow.metadata import MetadataWorkflow  # noqa: E402

wf = MetadataWorkflow.create(config)
wf.execute()
wf.print_status()
wf.raise_from_status()
wf.stop()
print("E2E OK")
