#!/usr/bin/env python3
"""Copy only the SML files of a model repo into a staging folder.

sml-cli validate parses every YAML file under its root and has no exclude
option, so non-SML YAML (Kubernetes manifests, report configs) breaks it.

Inputs:  SOURCE  repo folder that holds catalog.yml (or the older atscale.yml)
         TARGET  empty staging folder (created if missing)
Output:  TARGET holds package.yml and every YAML file that is an SML object.
         A YAML file is staged when any document has an object_type key, or
         when it does not parse but has an object_type line, so broken SML
         still reaches the validator. Dot folders and packages/ are skipped.
Run:     python stage_sml.py <SOURCE> <TARGET>
"""
import logging
import re
import shutil
import sys
from pathlib import Path

import yaml

log = logging.getLogger("stage_sml")
OBJECT_TYPE_LINE = re.compile(r"^object_type\s*:", re.MULTILINE)
SKIP_DIRS = {"packages", "node_modules"}
CATALOG_FILES = ("catalog.yml", "atscale.yml")


def is_sml(path: Path) -> bool:
    text = path.read_text(encoding="utf-8")
    try:
        docs = list(yaml.safe_load_all(text))
    except yaml.YAMLError:
        return bool(OBJECT_TYPE_LINE.search(text))
    return any(isinstance(d, dict) and "object_type" in d for d in docs)


def skipped(rel: Path) -> bool:
    return any(p.startswith(".") or p in SKIP_DIRS for p in rel.parts[:-1])


def stage(source: Path, target: Path) -> int:
    if not any((source / name).is_file() for name in CATALOG_FILES):
        raise SystemExit(f"no {' or '.join(CATALOG_FILES)} in {source}")
    target.mkdir(parents=True, exist_ok=True)
    staged = 0
    for path in sorted(source.rglob("*")):
        rel = path.relative_to(source)
        if path.suffix not in (".yml", ".yaml") or not path.is_file() or skipped(rel):
            continue
        if rel != Path("package.yml") and not is_sml(path):
            log.info("skip non-SML %s", rel)
            continue
        (target / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target / rel)
        staged += 1
    return staged


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if len(sys.argv) != 3:
        raise SystemExit("usage: stage_sml.py <SOURCE> <TARGET>")
    source, target = Path(sys.argv[1]).resolve(), Path(sys.argv[2]).resolve()
    if target == source or source in target.parents:
        raise SystemExit("TARGET must be outside SOURCE")
    log.info("staged %d SML files from %s to %s", stage(source, target), source, target)


if __name__ == "__main__":
    main()
