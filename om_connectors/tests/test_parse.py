"""Offline checks: SOAP rowset parsing, fault handling, column building."""
from unittest import mock

from om_connectors.atscale_xmla import XmlaClient, _strip_brackets

ROWSET = b"""<?xml version="1.0"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><soap:Body>
<DiscoverResponse xmlns="urn:schemas-microsoft-com:xml-analysis"><return>
<root xmlns="urn:schemas-microsoft-com:xml-analysis:rowset">
<row><CATALOG_NAME>Sales Insights - Snowflake_main</CATALOG_NAME><DESCRIPTION/></row>
<row><CATALOG_NAME>Other &amp; Co</CATALOG_NAME></row>
</root></return></DiscoverResponse></soap:Body></soap:Envelope>"""

FAULT = b"""<?xml version="1.0"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><soap:Body>
<soap:Fault><faultcode>XMLAnalysisError.0xc10e0002</faultcode><faultstring>Unknown catalog</faultstring></soap:Fault>
</soap:Body></soap:Envelope>"""


def _resp(content):
    r = mock.Mock(); r.content = content; r.raise_for_status = lambda: None
    return r


def test_parse_rows():
    c = XmlaClient("https://x/engine/xmla/t")
    with mock.patch.object(c.session, "post", return_value=_resp(ROWSET)) as post:
        assert c.catalogs() == ["Other & Co", "Sales Insights - Snowflake_main"]
        body = post.call_args.kwargs["data"].decode()
        assert "<RequestType>DBSCHEMA_CATALOGS</RequestType>" in body and "<Format>Tabular</Format>" in body


def test_restriction_escaping():
    c = XmlaClient("https://x/engine/xmla/t")
    with mock.patch.object(c.session, "post", return_value=_resp(ROWSET)) as post:
        c.cubes("A & B <c>")
        body = post.call_args.kwargs["data"].decode()
        assert "<CATALOG_NAME>A &amp; B &lt;c&gt;</CATALOG_NAME>" in body and "<Catalog>A &amp; B &lt;c&gt;</Catalog>" in body


def test_fault_raises():
    c = XmlaClient("https://x/engine/xmla/t")
    with mock.patch.object(c.session, "post", return_value=_resp(FAULT)):
        try:
            c.catalogs(); raise AssertionError("expected fault")
        except RuntimeError as e:
            assert "Unknown catalog" in str(e)


def test_host_and_brackets():
    assert XmlaClient("https://prod.example.com/engine/xmla/abc").host_url == "https://prod.example.com"
    assert _strip_brackets("[Product Dimension].[Product Hierarchy]") == "Product Dimension.Product Hierarchy"
    assert _strip_brackets("[Measures]") == "Measures"
