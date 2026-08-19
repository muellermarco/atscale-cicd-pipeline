#!/usr/bin/env python3
"""Consistency validator for the generated SML repo."""
import os, re, glob, sys
import yaml

REPO = sys.argv[1] if len(sys.argv) > 1 else "."
errors, warns = [], []

objs = {'dataset': {}, 'dimension': {}, 'metric': {}, 'metric_calc': {}, 'model': {},
        'composite_model': {}, 'connection': {}, 'catalog': {}}
for path in glob.glob(os.path.join(REPO, '**', '*.yml'), recursive=True):
    try:
        with open(path) as f:
            o = yaml.safe_load(f)
    except Exception as ex:
        errors.append(f"YAML parse error {path}: {ex}")
        continue
    if not isinstance(o, dict) or 'object_type' not in o:
        continue  # non-SML yaml (package.yml, config/, ...)
    ot = o.get('object_type')
    if ot not in objs:
        warns.append(f"{path}: unknown object_type {ot}")
        continue
    uq = o.get('unique_name')
    if uq in objs[ot]:
        errors.append(f"duplicate {ot} unique_name: {uq}")
    objs[ot][uq] = (o, path)

ds_cols = {}
for uq, (o, p) in objs['dataset'].items():
    cols = {c['name'] for c in o.get('columns', [])}
    ds_cols[uq] = cols
    if o.get('connection_id') not in objs['connection']:
        errors.append(f"dataset {uq}: unknown connection {o.get('connection_id')}")
    if not o.get('sql') and not o.get('table'):
        errors.append(f"dataset {uq}: neither sql nor table")

dim_levels = {}     # dim unique -> set of level attribute uniques
dim_hiers = {}      # dim unique -> set of hierarchy uniques
dim_secondary = {}  # dim unique -> set of secondary attr uniques
for uq, (o, p) in objs['dimension'].items():
    las = {}
    for la in o.get('level_attributes', []):
        if la['unique_name'] in las:
            errors.append(f"dim {uq}: duplicate level attr {la['unique_name']}")
        las[la['unique_name']] = la
        d = la.get('dataset')
        if d not in ds_cols:
            errors.append(f"dim {uq} level {la['unique_name']}: unknown dataset {d}")
            continue
        for c in [la.get('name_column')] + la.get('key_columns', []) + ([la['sort_column']] if la.get('sort_column') else []):
            if c not in ds_cols[d]:
                errors.append(f"dim {uq} level {la['unique_name']}: column {c} not in dataset {d}")
    dim_levels[uq] = set(las)
    hs = set()
    secs = set()
    for h in o.get('hierarchies', []):
        if h['unique_name'] in hs:
            errors.append(f"dim {uq}: duplicate hierarchy {h['unique_name']}")
        hs.add(h['unique_name'])
        for lv in h.get('levels', []):
            if lv['unique_name'] not in las:
                errors.append(f"dim {uq} hier {h['unique_name']}: level {lv['unique_name']} not in level_attributes")
            for sa in lv.get('secondary_attributes', []):
                if sa['unique_name'] in secs or sa['unique_name'] in las:
                    errors.append(f"dim {uq}: duplicate attribute {sa['unique_name']}")
                secs.add(sa['unique_name'])
                d = sa.get('dataset')
                if d not in ds_cols:
                    errors.append(f"dim {uq} sec {sa['unique_name']}: unknown dataset {d}")
                    continue
                for c in [sa.get('name_column')] + sa.get('key_columns', []) + ([sa['sort_column']] if sa.get('sort_column') else []):
                    if c not in ds_cols[d]:
                        errors.append(f"dim {uq} sec {sa['unique_name']}: column {c} not in dataset {d}")
    dim_hiers[uq] = hs
    dim_secondary[uq] = secs
    # levels referenced by relationships within dimension files
    for r in o.get('relationships', []):
        fd = r.get('from', {}).get('dataset')
        if fd not in ds_cols:
            errors.append(f"dim {uq} rel {r.get('unique_name')}: unknown from dataset {fd}")
        else:
            for jc in r['from'].get('join_columns', []):
                if jc not in ds_cols[fd]:
                    errors.append(f"dim {uq} rel {r.get('unique_name')}: join col {jc} not in {fd}")
        td = r.get('to', {}).get('dimension')
        tl = r.get('to', {}).get('level')
        # td may be absent for intra-dimension snowflake joins (to.level only)
        if td is not None and td not in objs['dimension']:
            errors.append(f"dim {uq} rel {r.get('unique_name')}: unknown to dimension {td}")
        # to-level checked after all dims loaded (below)

# second pass for embedded rel levels
for uq, (o, p) in objs['dimension'].items():
    for r in o.get('relationships', []):
        td = r.get('to', {}).get('dimension')
        tl = r.get('to', {}).get('level')
        if td is None:
            td = uq  # intra-dimension snowflake join
        if td in dim_levels and tl not in dim_levels[td]:
            errors.append(f"dim {uq} rel {r.get('unique_name')}: level {tl} not in dimension {td}")

for uq, (o, p) in objs['metric'].items():
    d = o.get('dataset')
    if d not in ds_cols:
        errors.append(f"metric {uq}: unknown dataset {d}")
    elif o.get('column') not in ds_cols[d]:
        errors.append(f"metric {uq}: column {o.get('column')} not in {d}")

measure_names = set(objs['metric']) | set(objs['metric_calc'])
for uq, (o, p) in objs['metric_calc'].items():
    expr = o.get('expression', '')
    if expr.startswith('/* TODO'):
        continue
    for m in re.finditer(r'\[Measures\]\.\[([^\]]+)\]', expr):
        if m.group(1) not in measure_names:
            errors.append(f"calc {uq}: references unknown measure {m.group(1)}")
    for m in re.finditer(r'\[([^\]]+)\]\.\[([^\]]+)\]\.&', expr):
        duq, attr = m.group(1), m.group(2)
        if duq == 'Measures':
            continue
        if duq not in objs['dimension']:
            errors.append(f"calc {uq}: unknown dimension {duq}")
        elif attr not in dim_levels.get(duq, set()) | dim_secondary.get(duq, set()):
            errors.append(f"calc {uq}: unknown attribute {attr} in {duq}")

for uq, (o, p) in objs['model'].items():
    for r in o.get('relationships', []):
        fd = r['from'].get('dataset')
        if fd not in ds_cols:
            errors.append(f"model rel {r['unique_name']}: unknown dataset {fd}")
        else:
            for jc in r['from'].get('join_columns', []):
                if jc not in ds_cols[fd]:
                    errors.append(f"model rel {r['unique_name']}: join col {jc} not in {fd}")
        td, tl = r['to'].get('dimension'), r['to'].get('level')
        if td not in dim_levels:
            errors.append(f"model rel {r['unique_name']}: unknown dimension {td}")
        elif tl not in dim_levels[td]:
            errors.append(f"model rel {r['unique_name']}: level {tl} not in {td}")
    for mref in o.get('metrics', []):
        if mref['unique_name'] not in measure_names:
            errors.append(f"model: unknown metric {mref['unique_name']}")
    for d in o.get('dimensions', []):
        if d not in objs['dimension']:
            errors.append(f"model: unknown degenerate dimension {d}")
    for persp in o.get('perspectives', []):
        for mn in persp.get('metrics', []):
            if mn not in measure_names:
                errors.append(f"perspective {persp['unique_name']}: unknown metric {mn}")
        for de in persp.get('dimensions', []):
            dn = de['name']
            if dn not in objs['dimension']:
                errors.append(f"perspective {persp['unique_name']}: unknown dimension {dn}")
                continue
            for h in de.get('hierarchies', []):
                if h['name'] not in dim_hiers[dn]:
                    errors.append(f"perspective {persp['unique_name']}: unknown hierarchy {h['name']} in {dn}")
            for sa in de.get('secondary_attributes', []):
                if sa not in dim_secondary[dn]:
                    errors.append(f"perspective {persp['unique_name']}: unknown secondary attr {sa} in {dn}")

# composite model checks
for uq, (o, p) in objs['composite_model'].items():
    member_metrics = set()
    for mref in o.get('models', []):
        if mref not in objs['model']:
            errors.append(f"composite {uq}: unknown model {mref}")
        else:
            member_metrics |= {m['unique_name'] for m in objs['model'][mref][0].get('metrics', [])}
    for mref in o.get('metrics', []):
        nm = mref['unique_name']
        if nm not in objs['metric_calc']:
            errors.append(f"composite {uq}: metric {nm} is not a metric_calc")
            continue
        expr = objs['metric_calc'][nm][0].get('expression', '')
        for m in re.finditer(r'\[Measures\]\.\[([^\]]+)\]', expr):
            if m.group(1) not in member_metrics | {x['unique_name'] for x in o.get('metrics', [])}:
                errors.append(f"composite {uq}: calc {nm} references {m.group(1)} not present in member models")

# every metric/calc should be referenced by some model or composite
referenced = set()
for uq, (o, p) in objs['model'].items():
    referenced |= {m['unique_name'] for m in o.get('metrics', [])}
for uq, (o, p) in objs['composite_model'].items():
    referenced |= {m['unique_name'] for m in o.get('metrics', [])}
for nm in (set(objs['metric']) | set(objs['metric_calc'])) - referenced:
    errors.append(f"metric/calc not referenced by any model: {nm}")

print(f"objects: " + ", ".join(f"{k}={len(v)}" for k, v in objs.items()))
print(f"errors: {len(errors)}  warnings: {len(warns)}")
for w in warns[:20]:
    print("WARN:", w)
for e in errors[:80]:
    print("ERR:", e)
sys.exit(1 if errors else 0)
