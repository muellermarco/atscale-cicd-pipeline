"""Offline checks of atscale_export.CatalogExport on a compact synthetic export that
exercises every rule: join-level reach, role-play, snowflake pointers, hidden
dimensions, shared / metrical / calculated measures, SCD levels, perspectives,
cube-local dimensions, physical types and dataset resolution."""
import json

from om_connectors.atscale_export import CatalogExport


def key(i, **p):
    return {"id": i, "properties": {"visible": True, **p}}


def ka(i, name, k, **p):
    return {"id": i, "name": name, "key-ref": k, "properties": {"visible": True, "caption": p.pop("caption", name), **p}}


def level(ka_id, secondary=(), props=None, **extra):
    lvl = {"properties": {"visible": True, **(props or {})}, "primary-attribute": ka_id}
    if secondary:
        lvl["keyed-attribute-ref"] = list(secondary)
    lvl.update(extra)
    return lvl


def hierarchy(i, name, levels, **p):
    return {"id": i, "name": name, "properties": {"visible": True, "caption": name, **p}, "level": levels}


def dimension(i, name, hierarchies, **p):
    return {"id": i, "name": name, "properties": {"visible": True, "caption": name, **p}, "hierarchy": hierarchies}


def dataset(i, table, columns, key_refs, attr_refs):
    return {"id": i, "properties": {}, "physical": {
                "connection": {"id": "wh"}, "table": {"database": "DB", "schema": "SCH", "name": table},
                "column": [{"name": c, "type": t} for c, t in columns]},
            "logical": {"key-ref": key_refs, "attribute-ref": attr_refs}}


def kref(k, col, **extra):
    return {"id": k, "column": col, "unique": "false", "complete": "true", **extra}


def aref(a, col):
    return {"id": a, "column": col, "complete": "true"}


def measure(i, name, kind="measure", folder="Metrics", **p):
    t = {"measure": {"default-aggregation": "SUM"}} if kind == "measure" else {kind: {}}
    return {"id": i, "name": name, "properties": {"visible": True, "caption": name, "folder": folder, "type": t, **p}}


EXPORT = {
    "name": "Demo Catalog", "version": 3,
    "attributes": {
        "attribute-key": [key("k_year"), key("k_month"), key("k_day"), key("k_week"), key("k_weekend"),
                          key("k_segment"), key("k_customer"), key("k_gender"), key("k_geo"), key("k_industry"),
                          key("k_country"), key("k_city"), key("k_hidden"), key("k_eff_start"), key("k_eff_end")],
        "keyed-attribute": [
            ka("a_year", "Year", "k_year"), ka("a_month", "Month", "k_month"), ka("a_day", "Day", "k_day"),
            ka("a_week", "Week", "k_week"), ka("a_weekend", "Is Weekend", "k_weekend"),
            ka("a_segment", "Segment", "k_segment"), ka("a_customer", "Customer", "k_customer"),
            ka("a_gender_ptr", "Gender", "k_gender"), ka("a_geo_ptr", "GeoKey", "k_geo", visible=False),
            ka("a_industry", "Industry", "k_industry"),
            ka("a_country", "Country", "k_country"), ka("a_city", "City", "k_city"),
            ka("a_hidden", "Secret", "k_hidden"),
            ka("a_eff_start", "Customer Effective Start", "k_eff_start"),
            ka("a_eff_end", "Customer Effective End", "k_eff_end"),
        ],
        "attribute": [measure("m_list_price", "List Price", folder="Product Metrics")],       # shared metric
        "virtual-filter-attribute": [{"id": "v_asof", "name": "Customer As-Of", "sql-type": "Int",
                                      "properties": {"visible": True, "caption": "Customer As-Of"}}],
    },
    "dimensions": {"dimension": [
        dimension("d_date", "Date Dimension", [
            hierarchy("h_date", "Date Hierarchy", [level("a_year"), level("a_month"),
                                                   level("a_day", [{"attribute-id": "a_weekend", "properties": ""}])]),
            hierarchy("h_week", "Week Hierarchy", [level("a_year"), level("a_week"), level("a_day")]),
        ], **{"dimension-type": "Time"}),
        dimension("d_customer", "Customer Dimension", [
            hierarchy("h_customer", "Customer Hierarchy", [
                level("a_segment"),
                level("a_customer", [{"attribute-id": "a_gender_ptr", "ref-id": "R_gender", "properties": ""},
                                     {"attribute-id": "a_geo_ptr", "ref-id": "R_geo", "properties": ""}],
                      props={"slowly-changing-dimension": {
                          "effective-start-date": {"keyed-attribute-ref": "a_eff_start", "has-nulls": "false"},
                          "effective-end-date": {"keyed-attribute-ref": "a_eff_end", "has-nulls": "false"},
                          "as-of-date": {"virtual-filter-attribute-ref": "v_asof"},
                          "is-latest": {"virtual-filter-attribute-ref": "v_missing"}}},
                      **{"attribute-ref": {"attribute-id": "m_list_price", "properties": ""}})]),
            hierarchy("h_industry", "Industry Hierarchy", [level("a_industry")]),      # no fact key reaches it
        ]),
        dimension("d_gender", "Gender Dimension", [hierarchy("h_gender", "Gender Hierarchy", [level("a_gender_ptr")])]),
        dimension("d_geo", "Geography Dimension", [hierarchy("h_geo", "Geo Hierarchy", [level("a_country"), level("a_city")])]),
        dimension("d_hidden", "Hidden Dimension", [hierarchy("h_hidden", "Hidden Hierarchy", [level("a_hidden")])], visible=False),
    ]},
    "data-sets": {"data-set": [
        dataset("ds_date", "DIM_DATE", [("YEAR", "Long"), ("MONTH", "Long"), ("DAY", "Date"), ("WEEK", "Long"), ("WEEKEND", "Boolean")],
                [kref("k_year", "YEAR"), kref("k_month", "MONTH"), kref("k_day", "DAY"), kref("k_week", "WEEK"), kref("k_weekend", "WEEKEND")],
                [aref("a_year", "YEAR"), aref("a_month", "MONTH"), aref("a_day", "DAY"), aref("a_week", "WEEK"), aref("a_weekend", "WEEKEND")]),
        dataset("ds_customer", "DIM_CUSTOMER",
                [("SEGMENT", "String"), ("CUSTOMER_KEY", "Long"), ("GENDER", "String"), ("GEO_KEY", "Long"),
                 ("INDUSTRY", "String"), ("EFF_START", "Date"), ("EFF_END", "Date"), ("LIST_PRICE", "Double")],
                [kref("k_segment", "SEGMENT"), kref("k_customer", "CUSTOMER_KEY"), kref("k_industry", "INDUSTRY"),
                 kref("k_gender", "GENDER", **{"ref-path": {"ref": {"id": "R_gender"}}}),
                 kref("k_city", "GEO_KEY", **{"ref-path": {"ref": {"id": "R_geo"}}}),
                 kref("k_eff_start", "EFF_START"), kref("k_eff_end", "EFF_END")],
                [aref("a_segment", "SEGMENT"), aref("a_customer", "CUSTOMER_KEY"), aref("a_gender_ptr", "GENDER"),
                 aref("a_industry", "INDUSTRY"), aref("a_eff_start", "EFF_START"), aref("a_eff_end", "EFF_END"),
                 aref("m_list_price", "LIST_PRICE")]),
        dataset("ds_gender", "DIM_GENDER", [("GENDER", "String")], [kref("k_gender", "GENDER")], [aref("a_gender_ptr", "GENDER")]),
        dataset("ds_geo", "DIM_GEO", [("COUNTRY", "String"), ("CITY_KEY", "Long")],
                [kref("k_country", "COUNTRY"), kref("k_city", "CITY_KEY")], [aref("a_country", "COUNTRY"), aref("a_city", "CITY_KEY")]),
        dataset("ds_fact", "FACT_SALES", [("DAY", "Date"), ("SHIP_DAY", "Date"), ("CUSTOMER_KEY", "Long"),
                                          ("SALES_AMOUNT", "Double"), ("COLOR", "String")], [], []),
        dataset("ds_unused", "UNUSED", [("X", "String")], [kref("k_hidden", "X")], [aref("a_hidden", "X")]),
    ]},
    "calculated-members": {"calculated-member": [
        {"id": "c_avg", "name": "Avg Price", "expression": "[Measures].[Sales Amount] / [Measures].[Customer Count]",
         "properties": {"visible": True, "caption": "Avg Price", "folder": "Metrics", "formatting": {"format-string": "#,##0.00"}}},
        {"id": "c_other", "name": "Not Referenced", "expression": "1", "properties": {"visible": True}},
    ]},
    "cubes": {"cube": {                                   # single cube -> bare object, not a list
        "id": "cube_sales", "name": "Sales", "properties": {"visible": True, "caption": "Sales", "description": "Sales model"},
        "attributes": {
            "attribute-key": [key("k_color")],
            "keyed-attribute": [ka("a_color", "Color", "k_color")],
            "attribute": [measure("m_sales", "Sales Amount"), measure("m_customers", "Customer Count", kind="count-distinct"),
                          measure("m_hidden", "Hidden Measure", visible=False)],
        },
        "dimensions": {"dimension": dimension("d_color", "Color Dimension", hierarchy("h_color", "Color Hierarchy", level("a_color")))},
        "data-sets": {"data-set-ref": {"id": "ds_fact", "logical": {
            "key-ref": [kref("k_day", "DAY"),
                        kref("k_day", "SHIP_DAY", **{"ref-path": {"new-ref": {"ref-naming": "Ship {0}", "attribute-id": "a_day", "ref-id": "RP1"}}}),
                        kref("k_customer", "CUSTOMER_KEY"), kref("k_color", "COLOR")],
            "attribute-ref": [aref("m_sales", "SALES_AMOUNT"), aref("m_customers", "CUSTOMER_KEY"), aref("a_color", "COLOR")]}}},
        "calculated-members": {"calculated-member-ref": {"id": "c_avg"}},
    }},
    "perspectives": {"perspective": {
        "id": "p_nopii", "name": "Sales - No PII", "cube-ref": "cube_sales",
        "flat-attributes": {"flat-attribute-ref": {"properties": {"visible": False}, "id": "m_customers"}},
        "flat-dimensions": {"flat-dimension-ref": {"properties": {"visible": True}, "id": "d_customer",
                            "flat-hierarchy-ref": {"properties": {"visible": True}, "id": "h_customer",
                                                   "flat-level-ref": {"properties": {"visible": False}, "primary-attribute": "a_customer"}}}},
        "calculated-members": "",
    }},
}


def _export():
    return CatalogExport(json.loads(json.dumps(EXPORT)))       # deep copy: the parser must not need to mutate


def _by_name(cols):
    return {c["name"]: c for c in cols}


def test_models_lists_cubes_then_perspectives():
    ex = _export()
    assert [(m["name"], m["perspective"] is not None) for m in ex.models()] == [("Sales", False), ("Sales - No PII", True)]
    assert ex.models()[0]["description"] == "Sales model"


def test_measures():
    ex = _export()
    cols = ex.columns(ex.models()[0])
    measures = [c["name"] for c in cols if c["kind"] == "measure"]
    assert measures == ["Avg Price", "Customer Count", "List Price", "Sales Amount"]     # sorted; hidden + unreferenced dropped
    by = _by_name(cols)
    assert by["Sales Amount"]["dtype"] == "DOUBLE" and by["Sales Amount"]["sources"] == {("ds_fact", "SALES_AMOUNT")}
    assert by["Customer Count"]["dtype"] == "BIGINT"
    assert "expression: [Measures].[Sales Amount]" in by["Avg Price"]["description"] and "format: #,##0.00" in by["Avg Price"]["description"]
    assert "metrical attribute on Customer Dimension.Customer Hierarchy" in by["List Price"]["description"]


def test_levels_reach_roleplay_snowflake_and_hidden():
    ex = _export()
    cols = ex.columns(ex.models()[0])
    levels = {c["name"] for c in cols if c["kind"] == "level"}
    assert levels == {
        # Date Dimension joined at Day: both hierarchies, Day shared, Is Weekend secondary
        "Date Dimension.Year", "Date Dimension.Month", "Date Dimension.Day", "Date Dimension.Is Weekend", "Date Dimension.Week",
        # the same key with role-play naming
        "Ship Date Dimension.Ship Year", "Ship Date Dimension.Ship Month", "Ship Date Dimension.Ship Day",
        "Ship Date Dimension.Ship Is Weekend", "Ship Date Dimension.Ship Week",
        # Customer joined at Customer: Industry hierarchy unreachable, pointer attributes replaced by the
        # snowflaked dimensions, SCD extras exposed
        "Customer Dimension.Segment", "Customer Dimension.Customer",
        "Customer Dimension.Customer Effective Start", "Customer Dimension.Customer Effective End", "Customer Dimension.Customer As-Of",
        "Gender Dimension.Gender", "Geography Dimension.Country", "Geography Dimension.City",
        # cube-local
        "Color Dimension.Color",
    }
    by = _by_name(cols)
    assert by["Ship Date Dimension.Ship Year"]["display"] == "Ship Year"
    assert "hierarchy: Ship Date Hierarchy" in by["Ship Date Dimension.Ship Year"]["description"]
    assert by["Date Dimension.Year"]["dtype"] == "BIGINT" and by["Date Dimension.Is Weekend"]["dtype"] == "BOOLEAN"
    assert by["Customer Dimension.Customer As-Of"]["dtype"] == "INT"
    assert "Date Hierarchy, Week Hierarchy" in by["Date Dimension.Day"]["description"]      # merged, not duplicated
    assert by["Date Dimension.Day"]["sources"] == {("ds_date", "DAY"), ("ds_fact", "DAY")}
    assert by["Ship Date Dimension.Ship Day"]["sources"] == {("ds_date", "DAY"), ("ds_fact", "SHIP_DAY")}
    assert len(cols) == len({c["name"] for c in cols})


def test_perspective_hides_level_and_measure():
    ex = _export()
    cols = _by_name(ex.columns(ex.models()[1]))
    assert "Customer Dimension.Customer" not in cols and "Customer Dimension.Segment" in cols
    assert "Customer Count" not in cols and "Sales Amount" in cols


def test_datasets_for_lineage():
    ex = _export()
    ds = ex.datasets(ex.models()[0])
    assert ds[0]["table"] == "FACT_SALES"                                            # fact datasets first
    assert {d["table"] for d in ds} == {"FACT_SALES", "DIM_DATE", "DIM_CUSTOMER", "DIM_GENDER", "DIM_GEO"}   # UNUSED not read
    assert ds[0]["connection"] == "wh" and ds[0]["database"] == "DB" and ds[0]["schema"] == "SCH"
    assert next(d for d in ds if d["table"] == "DIM_DATE")["columns"]["YEAR"] == "Long"


def test_empty_and_bare_collections():
    ex = CatalogExport({"name": "empty", "attributes": "", "dimensions": "", "data-sets": "", "calculated-members": "",
                        "cubes": {"cube": {"id": "c", "name": "Bare", "attributes": "", "dimensions": "", "data-sets": ""}}})
    assert [m["name"] for m in ex.models()] == ["Bare"]
    assert ex.columns(ex.models()[0]) == [] and ex.datasets(ex.models()[0]) == []
