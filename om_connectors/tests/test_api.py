"""Offline checks of the container-API client and connection hooks."""
import os
from unittest import mock

import pytest

from om_connectors.atscale_api import AtScaleApiClient, get_connection


def _resp(payload, status=200):
    r = mock.Mock()
    r.status_code = status
    r.json = lambda: payload
    r.raise_for_status = lambda: None
    return r


def test_client_lists_sorted_catalogs_and_exports():
    c = AtScaleApiClient("https://x.example.com/api/", "tok")
    assert c.base_url == "https://x.example.com" and c.host_url == "https://x.example.com"
    assert c.session.headers["Authorization"] == "Bearer tok" and c.session.headers["Accept"] == "application/json"
    cats = [{"id": "2", "name": "b"}, {"id": "1", "name": "a"}]
    with mock.patch.object(c.session, "get", side_effect=[_resp(cats), _resp({"name": "a", "cubes": ""})]) as get:
        assert [x["name"] for x in c.catalogs()] == ["a", "b"]
        assert c.export("id with space")["name"] == "a"
        assert get.call_args_list[0].args[0] == "https://x.example.com/v1/catalogs"
        assert get.call_args_list[1].args[0] == "https://x.example.com/v1/catalogs/id%20with%20space/export"


def test_client_errors():
    c = AtScaleApiClient("https://x.example.com", "tok")
    with mock.patch.object(c.session, "get", return_value=_resp({"statusCode": 401}, 401)):
        with pytest.raises(RuntimeError, match="401"):
            c.catalogs()
    with mock.patch.object(c.session, "get", return_value=_resp({"oops": 1})):
        with pytest.raises(RuntimeError, match="unexpected payload"):
            c.catalogs()
    with pytest.raises(ValueError, match="api_url"):
        AtScaleApiClient("", "tok")
    with pytest.raises(ValueError, match="token"):
        AtScaleApiClient("https://x", "")


def test_get_connection_reads_env():
    from metadata.generated.schema.entity.services.connections.dashboard.customDashboardConnection import (
        CustomDashboardConnection,
    )
    conn = CustomDashboardConnection.model_validate({
        "type": "CustomDashboard", "sourcePythonClass": "om_connectors.atscale_api.AtScaleApiSource",
        "connectionOptions": {"api_url_env": "T_URL", "api_token_env": "T_TOKEN", "verify_ssl": "true"}})
    with mock.patch.dict(os.environ, {"T_URL": "https://h.example.com/", "T_TOKEN": "secret"}):
        c = get_connection(conn)
    assert c.base_url == "https://h.example.com" and c.verify_ssl is True
    assert c.session.headers["Authorization"] == "Bearer secret"
