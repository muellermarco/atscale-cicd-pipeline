"""End-to-end check of the connector through the real OpenMetadata ingestion
workflow, with the AtScale API client replaced by a canned catalog export — so the
whole topology (service -> dashboard -> data models -> lineage) is exercised
against a live OpenMetadata without touching AtScale.

  OM_URL=http://host:8585/api OM_TOKEN=<jwt> python tests/e2e_fake_api.py [service-name] [db-service-prefix ...]

Creates/updates the service (default atscale-api-e2e); delete it afterwards.
"""
import os
import sys

import om_connectors.atscale_api as mod
from tests.test_export import EXPORT

mod.AtScaleApiClient.catalogs = lambda self: [{"id": "cat1", "name": EXPORT["name"], "caption": "Demo", "models": []}]
mod.AtScaleApiClient.export = lambda self, catalog_id: EXPORT

service = sys.argv[1] if len(sys.argv) > 1 else "atscale-api-e2e"
prefixes = sys.argv[2:]
config = {
    "source": {
        "type": "customdashboard",
        "serviceName": service,
        "serviceConnection": {"config": {
            "type": "CustomDashboard",
            "sourcePythonClass": "om_connectors.atscale_api.AtScaleApiSource",
            "connectionOptions": {"api_url": "https://fake.example", "api_token": "fake"},
        }},
        "sourceConfig": {"config": {"type": "DashboardMetadata",
                                    **({"lineageInformation": {"dbServicePrefixes": prefixes}} if prefixes else {})}},
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
