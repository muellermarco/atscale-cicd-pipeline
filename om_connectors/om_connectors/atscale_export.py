"""Read-only view over an AtScale catalog export (container API
``GET /v1/catalogs/{id}/export``, ``Accept: application/json``).

The export is the published catalog definition: shared dimensions, attributes
and datasets at the top, one ``cube`` per model (with its own degenerate
dimensions, measures and dataset references) and perspectives on top of cubes.
This module resolves it into the same flat column list the XMLA connector
derives from ``MDSCHEMA_MEASURES`` / ``MDSCHEMA_LEVELS``:

    measure / calculated member  -> "<name>"                     (kind "measure")
    hierarchy level / secondary  -> "<dimension>.<attribute>"    (kind "level")

so a data model keeps the same columns whichever connector produced it.

What the engine exposes per cube is not written down in the export; it follows
from the joins, which this module replays:

* a shared dimension is reachable through a fact key (``data-set-ref`` ->
  ``key-ref``) that is the key of one of its hierarchy levels; the hierarchy is
  exposed from the top down to that level, hierarchies no key reaches are not;
* a key-ref with ``ref-path.new-ref`` is a role-play: the dimension, hierarchy,
  level names and captions take the ``ref-naming`` pattern ("Order {0}");
* a secondary attribute carrying a ``ref-id`` is a snowflake pointer, not a
  column: the dimension dataset's key-ref whose ``ref-path.ref`` names that id
  gives the key (and so the level) of the dimension joined underneath;
* a level with ``slowly-changing-dimension`` settings also exposes its effective
  start / end attributes and the as-of-date virtual filter attribute;
* perspectives are the base cube minus everything they flag ``visible: false``.

No OpenMetadata dependency here, so the mapping is unit-testable offline.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Optional

MEASURE_KINDS = ("measure", "count-distinct", "count-nonnull")

# physical column types of the export -> OpenMetadata DataType names
PHYSICAL_TYPES = {
    "string": "STRING", "varchar": "VARCHAR", "char": "CHAR", "text": "TEXT",
    "long": "BIGINT", "bigint": "BIGINT", "int": "INT", "integer": "INT", "short": "SMALLINT",
    "double": "DOUBLE", "float": "FLOAT", "decimal": "DECIMAL", "numeric": "DECIMAL",
    "boolean": "BOOLEAN", "date": "DATE", "datetime": "DATETIME", "timestamp": "TIMESTAMP",
    "time": "TIME",
}


def _list(x: Any) -> list:
    """The export serialises one-element collections as a bare object and empty ones as ''."""
    if isinstance(x, list):
        return x
    return [] if x in (None, "", {}) else [x]


def _props(o: Any) -> dict:
    p = o.get("properties") if isinstance(o, dict) else None
    return p if isinstance(p, dict) else {}


def _visible(o: Any) -> bool:
    return _props(o).get("visible", True) is not False


def _fmt(naming: Optional[str], name: str) -> str:
    """Role-play naming: 'Order {0}' applied to 'Date Dimension' -> 'Order Date Dimension'."""
    return naming.replace("{0}", name) if naming and "{0}" in naming else (f"{naming} {name}" if naming else name)


class CatalogExport:
    """One exported catalog. ``models()`` lists cubes and perspectives, ``columns(model)``
    the flat column list, ``sources(model)`` / ``datasets(model)`` the physical origins
    (for warehouse lineage)."""

    def __init__(self, doc: dict):
        self.doc = doc
        self.name: str = doc.get("name", "")
        attrs = doc.get("attributes") or {}
        self.keys = {k["id"]: k for k in _list(attrs.get("attribute-key"))}
        self.keyed = {k["id"]: k for k in _list(attrs.get("keyed-attribute"))}
        self.measures = {k["id"]: k for k in _list(attrs.get("attribute"))}          # shared metrics
        self.virtual = {k["id"]: k for k in _list(attrs.get("virtual-filter-attribute"))}
        self.dims = {d["id"]: d for d in _list((doc.get("dimensions") or {}).get("dimension"))}
        self.dataset_defs = {d["id"]: d for d in _list((doc.get("data-sets") or {}).get("data-set"))}
        self.calcs = {c["id"]: c for c in _list((doc.get("calculated-members") or {}).get("calculated-member"))}
        self.cubes = {c["id"]: c for c in _list((doc.get("cubes") or {}).get("cube"))}
        self.perspectives = {p["id"]: p for p in _list((doc.get("perspectives") or {}).get("perspective"))}

        # attribute-key id -> [(dimension id, hierarchy idx, level idx)] over the shared dimensions
        self.key_levels: dict[str, list[tuple[str, int, int]]] = {}
        for dim in self.dims.values():
            for hi, h in enumerate(_list(dim.get("hierarchy"))):
                for li, lvl in enumerate(_list(h.get("level"))):
                    ka = self.keyed.get(lvl.get("primary-attribute"))
                    if ka:
                        self.key_levels.setdefault(ka.get("key-ref", ""), []).append((dim["id"], hi, li))

        # physical origins: attribute id -> {(dataset id, column)}; attribute-key id -> the
        # dimension datasets' key columns; (attribute-key id, role-play naming) -> the fact
        # key columns of that join (so "Ship Day" traces to SHIP_DAY, not to DAY)
        self.attr_sources: dict[str, set[tuple[str, str]]] = {}
        self.dim_key_sources: dict[str, set[tuple[str, str]]] = {}
        self.key_sources: dict[tuple[str, Optional[str]], set[tuple[str, str]]] = {}
        self.ref_targets: dict[str, list[dict]] = {}             # snowflake ref-id -> key-refs naming it
        for ds in self.dataset_defs.values():
            self._index_logical(ds["id"], ds.get("logical") or {}, fact=False)
        for cube in self.cubes.values():
            for ref in _list((cube.get("data-sets") or {}).get("data-set-ref")):
                self._index_logical(ref["id"], ref.get("logical") or {}, fact=True)

    def _index_logical(self, ds_id: str, logical: dict, fact: bool) -> None:
        for r in _list(logical.get("attribute-ref")):
            for col in _list(r.get("column")):
                self.attr_sources.setdefault(r["id"], set()).add((ds_id, col))
        for r in _list(logical.get("key-ref")):
            for col in _list(r.get("column")):
                if fact:
                    self.key_sources.setdefault((r["id"], self._naming(r)), set()).add((ds_id, col))
                else:
                    self.dim_key_sources.setdefault(r["id"], set()).add((ds_id, col))
            rp = r.get("ref-path")
            ref_id = (rp.get("ref") or {}).get("id") if isinstance(rp, dict) and isinstance(rp.get("ref"), dict) else None
            if ref_id:
                self.ref_targets.setdefault(ref_id, []).append(r)

    # --- models ---------------------------------------------------------------
    def models(self) -> list[dict]:
        """Cubes and perspectives, in export order. A perspective carries its base cube."""
        out = []
        for cube in self.cubes.values():
            p = _props(cube)
            out.append({"id": cube["id"], "name": cube.get("name", ""), "caption": p.get("caption") or cube.get("name", ""),
                        "description": p.get("description") or "", "visible": _visible(cube),
                        "cube": cube, "perspective": None})
        for per in self.perspectives.values():
            cube = self.cubes.get(per.get("cube-ref", ""))
            if not cube:
                continue
            p = _props(per)
            out.append({"id": per["id"], "name": per.get("name", ""), "caption": p.get("caption") or per.get("name", ""),
                        "description": p.get("description") or f"Perspective of model `{cube.get('name', '')}`",
                        "visible": _visible(per), "cube": cube, "perspective": per})
        return out

    # --- columns --------------------------------------------------------------
    def columns(self, model: dict) -> list[dict]:
        """Flat columns of a model: dicts with name, display, kind ('measure' | 'level'),
        dtype (OpenMetadata DataType name), description, sources {(dataset id, column)}."""
        cube, hidden = model["cube"], self._hidden(model.get("perspective"))
        levels, metrical = self._level_columns(cube, hidden)
        measures = list(self._measure_columns(cube, hidden)) + metrical
        return self._dedupe(sorted(measures, key=lambda c: c["name"].lower()) + levels)

    @staticmethod
    def _dedupe(cols: list[dict]) -> list[dict]:
        """A level shared by two hierarchies (Day in a month and a week hierarchy) is one
        column; a measure listed twice (cube and dimension level) likewise."""
        out: dict[str, dict] = {}
        for c in cols:
            prev = out.get(c["name"])
            if prev is None:
                out[c["name"]] = c
            elif c["description"] and c["description"] not in prev["description"]:
                extra = c["description"].rsplit("hierarchy: ", 1)[-1] if "hierarchy: " in c["description"] else ""
                prev["description"] = f"{prev['description']}, {extra}" if extra and "hierarchy: " in prev["description"] else prev["description"]
                prev["sources"] |= c["sources"]
        return list(out.values())

    def _measure_columns(self, cube: dict, hidden: set[str]) -> Iterable[dict]:
        local = {a["id"]: a for a in _list((cube.get("attributes") or {}).get("attribute"))}
        referenced = {r.get("id") for ref in _list((cube.get("data-sets") or {}).get("data-set-ref"))
                      for r in _list((ref.get("logical") or {}).get("attribute-ref"))}
        shared = {i: self.measures[i] for i in referenced if i in self.measures and i not in local}
        for a in list(local.values()) + list(shared.values()):
            if not _visible(a) or a["id"] in hidden:
                continue
            col = self._measure_column(a)
            if col:
                yield col
        for r in _list((cube.get("calculated-members") or {}).get("calculated-member-ref")):
            c = self.calcs.get(r.get("id", ""))
            if not c or not _visible(c) or c["id"] in hidden:
                continue
            p = _props(c)
            yield {"name": c.get("name", ""), "display": p.get("caption") or c.get("name", ""), "kind": "measure",
                   "dtype": "DOUBLE", "sources": set(),
                   "description": self._describe(p, (self._folder(p), self._format(p),
                                                     f"expression: {c['expression']}" if c.get("expression") else ""))}

    def _measure_column(self, a: dict, note: str = "") -> Optional[dict]:
        p = _props(a)
        kind = next(iter(p.get("type") or {}), "")
        if kind not in MEASURE_KINDS:
            return None
        agg = (p.get("type") or {}).get(kind) or {}
        agg_name = agg.get("default-aggregation") if isinstance(agg, dict) else None
        srcs = set(self.attr_sources.get(a["id"], set()))
        dtype = "BIGINT" if kind != "measure" else self._column_type(srcs, "DOUBLE")
        return {"name": a.get("name", ""), "display": p.get("caption") or a.get("name", ""), "kind": "measure",
                "dtype": dtype, "sources": srcs,
                "description": self._describe(p, (f"aggregation: {agg_name or kind}", self._folder(p),
                                                  self._format(p), note))}

    def _level_columns(self, cube: dict, hidden: set[str]) -> tuple[list[dict], list[dict]]:
        local_keyed = {a["id"]: a for a in _list((cube.get("attributes") or {}).get("keyed-attribute"))}
        keyed = {**self.keyed, **local_keyed}
        cols: list[dict] = []
        metrical: list[dict] = []

        def emit(dim: dict, naming: Optional[str], reach: Optional[dict[int, int]]) -> None:
            if not _visible(dim) or dim["id"] in hidden:
                return
            dname = _fmt(naming, dim.get("name", ""))
            for hi, h in enumerate(_list(dim.get("hierarchy"))):
                if reach is not None and hi not in reach:
                    continue
                if not _visible(h) or h.get("id") in hidden:
                    continue
                hname = _fmt(naming, h.get("name", ""))
                levels = _list(h.get("level"))
                if reach is not None:
                    levels = levels[: reach[hi] + 1]
                for lvl in levels:
                    if not _visible(lvl):
                        continue
                    ka = keyed.get(lvl.get("primary-attribute", ""))
                    if ka and _visible(ka) and ka["id"] not in hidden:
                        cols.append(self._level_column(dname, hname, naming, ka))
                    for sec in _list(lvl.get("keyed-attribute-ref")):
                        ka2 = keyed.get(sec.get("attribute-id", ""))
                        if not ka2 or not _visible(ka2) or ka2["id"] in hidden or sec.get("ref-id"):
                            continue                                              # ref-id: snowflake pointer
                        cols.append(self._level_column(dname, hname, naming, ka2))
                    cols.extend(self._scd_columns(dname, hname, naming, lvl, keyed, hidden))
                    for mref in _list(lvl.get("attribute-ref")):                # metrical attribute
                        m = self.measures.get(mref.get("attribute-id", ""))
                        if m and _visible(m) and m["id"] not in hidden:
                            metrical.append(self._measure_column(m, f"metrical attribute on {dname}.{hname}"))

        for (dim_id, naming), reach in self._joined(cube).items():
            emit(self.dims[dim_id], naming, reach)
        for dim in _list((cube.get("dimensions") or {}).get("dimension")):            # degenerate, cube-local
            emit(dim, None, None)
        return cols, metrical

    def _joined(self, cube: dict) -> dict[tuple[str, Optional[str]], dict[int, int]]:
        """(dimension id, role-play naming) -> {hierarchy idx: deepest joined level idx},
        following fact keys into shared dimensions and from there along snowflake keys."""
        out: dict[tuple[str, Optional[str]], dict[int, int]] = {}
        pending: list[tuple[str, Optional[str]]] = []
        for ref in _list((cube.get("data-sets") or {}).get("data-set-ref")):
            for r in _list((ref.get("logical") or {}).get("key-ref")):
                pending.append((r.get("id", ""), self._naming(r)))
        seen: set[tuple[str, Optional[str]]] = set()
        while pending:
            kid, naming = pending.pop()
            if (kid, naming) in seen:
                continue
            seen.add((kid, naming))
            for dim_id, hi, li in self.key_levels.get(kid, []):
                reach = out.setdefault((dim_id, naming), {})
                reach[hi] = max(reach.get(hi, -1), li)
                levels = _list(_list(self.dims[dim_id].get("hierarchy"))[hi].get("level"))[: li + 1]
                for lvl in levels:
                    for sec in _list(lvl.get("keyed-attribute-ref")):
                        for r in self.ref_targets.get(sec.get("ref-id", ""), []):
                            pending.append((r.get("id", ""), self._naming(r) or naming))
        return out

    def _scd_columns(self, dname: str, hname: str, naming: Optional[str], lvl: dict, keyed: dict,
                     hidden: set[str]) -> list[dict]:
        """Slowly-changing level: effective start / end attributes and the as-of-date filter."""
        scd = _props(lvl).get("slowly-changing-dimension")
        if not isinstance(scd, dict):
            return []
        out = []
        for part in ("effective-start-date", "effective-end-date"):
            ka = keyed.get((scd.get(part) or {}).get("keyed-attribute-ref", "")) if isinstance(scd.get(part), dict) else None
            if ka and _visible(ka) and ka["id"] not in hidden:
                out.append(self._level_column(dname, hname, naming, ka))
        as_of = scd.get("as-of-date") if isinstance(scd.get("as-of-date"), dict) else {}
        va = self.virtual.get(as_of.get("virtual-filter-attribute-ref", ""))
        if va and _visible(va) and va["id"] not in hidden:
            p = _props(va)
            out.append({"name": f"{dname}.{_fmt(naming, va.get('name', ''))}",
                        "display": _fmt(naming, p.get("caption") or va.get("name", "")), "kind": "level",
                        "dtype": PHYSICAL_TYPES.get(str(va.get("sql-type", "")).lower(), "STRING"), "sources": set(),
                        "description": self._describe(p, (f"hierarchy: {hname}",
                                                          "as-of-date filter of a slowly changing dimension"))})
        return out

    @staticmethod
    def _naming(key_ref: dict) -> Optional[str]:
        rp = key_ref.get("ref-path")
        new = rp.get("new-ref") if isinstance(rp, dict) else None
        return new.get("ref-naming") if isinstance(new, dict) else None

    def _level_column(self, dname: str, hname: str, naming: Optional[str], ka: dict) -> dict:
        p = _props(ka)
        kid = ka.get("key-ref", "")
        srcs = (set(self.attr_sources.get(ka["id"], set())) | self.dim_key_sources.get(kid, set())
                | self.key_sources.get((kid, naming), set()))
        return {"name": f"{dname}.{_fmt(naming, ka.get('name', ''))}",
                "display": _fmt(naming, p.get("caption") or ka.get("name", "")), "kind": "level",
                "dtype": self._column_type(self.attr_sources.get(ka["id"], set()), "STRING"), "sources": srcs,
                "description": self._describe(p, (f"hierarchy: {hname}", self._folder(p)))}

    # --- perspectives ---------------------------------------------------------
    @staticmethod
    def _hidden(perspective: Optional[dict]) -> set[str]:
        """Ids (dimension / hierarchy / attribute / calculated member) a perspective hides."""
        hidden: set[str] = set()
        if not perspective:
            return hidden

        def walk(o: Any) -> None:
            if isinstance(o, dict):
                if _props(o).get("visible") is False:
                    hidden.update(v for k in ("id", "primary-attribute", "attribute-id") if (v := o.get(k)))
                for v in o.values():
                    walk(v)
            elif isinstance(o, list):
                for v in o:
                    walk(v)
        walk({k: v for k, v in perspective.items() if k != "id"})
        return hidden

    # --- physical ---------------------------------------------------------------
    def datasets(self, model: dict) -> list[dict]:
        """Warehouse datasets a model reads: fact datasets of the cube plus the datasets of
        every column's origin. Each: id, name, connection, database, schema, table, columns."""
        ids: dict[str, None] = {}
        for ref in _list((model["cube"].get("data-sets") or {}).get("data-set-ref")):
            ids.setdefault(ref.get("id", ""))
        for col in self.columns(model):
            for ds_id, _ in col["sources"]:
                ids.setdefault(ds_id)
        return [self.dataset(i) for i in ids if i in self.dataset_defs]

    def dataset(self, ds_id: str) -> dict:
        ds = self.dataset_defs[ds_id]
        phys = ds.get("physical") or {}
        table = phys.get("table") or {}
        conn = phys.get("connection") or {}
        return {"id": ds_id, "name": ds.get("name") or table.get("name", ""),
                "connection": conn.get("id", "") if isinstance(conn, dict) else str(conn),
                "database": table.get("database", ""), "schema": table.get("schema", ""),
                "table": table.get("name", ""), "sql": phys.get("sql") or phys.get("query") or "",
                "columns": self._physical_columns(ds)}

    @staticmethod
    def _physical_columns(ds: dict) -> dict[str, str]:
        phys = ds.get("physical") or {}
        out = {c.get("name", ""): c.get("type", "") for c in _list(phys.get("column"))}
        for mc in _list(phys.get("map-column")):
            out.update({c.get("name", ""): c.get("type", "") for c in _list((mc.get("columns") or {}).get("column"))})
        return out

    def _column_type(self, sources: set[tuple[str, str]], default: str) -> str:
        for ds_id, col in sorted(sources):
            ds = self.dataset_defs.get(ds_id)
            if ds:
                t = self._physical_columns(ds).get(col, "")
                if t:
                    return PHYSICAL_TYPES.get(t.lower(), default)
        return default

    # --- descriptions -------------------------------------------------------------
    @staticmethod
    def _describe(p: dict, extras: Iterable[str]) -> str:
        extra = " · ".join(x for x in extras if x)
        return " — ".join(x for x in (p.get("description") or "", extra) if x)

    @staticmethod
    def _folder(p: dict) -> str:
        return f"folder: {p['folder']}" if p.get("folder") else ""

    @staticmethod
    def _format(p: dict) -> str:
        f = p.get("formatting") or {}
        fmt = f.get("format-string") or f.get("named-format") if isinstance(f, dict) else None
        return f"format: {fmt}" if fmt else ""
