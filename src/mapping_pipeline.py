# Mapping Factory — generated Spark Declarative Pipeline (Deploy-as-Pipeline).
#
# GENERIC + config-driven. This file is IDENTICAL for every mapping — it carries NO
# mapping-specific SQL. At runtime it reads the mapping config from a Unity Catalog
# table (mf_pipeline_config), so the code stays small and a platform team can enhance
# it without touching per-mapping details. Regenerate the CONFIG from Mapping Factory;
# only edit THIS code for framework-level changes.
#
# Per target it builds up to three tables:
#   <target>              — clean rows (pass all rules, or only warn-level violations)
#   <target>_quarantine   — rows that violate a FAIL-severity rule (kept OUT of target)
#   (audit)               — one row per (rule, run) appended to mf_dq_audit for metrics
#
# DQ routing: each rule has an action of 'warn' or 'fail'. A row failing a FAIL rule is
# quarantined; a row that only trips WARN rules still lands in the target. Every
# violation (warn + fail) is counted in the audit table.

import json

import pyspark.pipelines as dlt
from pyspark.sql import functions as F

# --- locate + load the config from a UC table (NOT inlined in this code) -----
# The deploy/test flow writes the config row; `mf.config_key` selects which mapping,
# `mf.run_id` (optional) pins an exact revision. Defaults resolve the latest row.
_CFG_CATALOG = spark.conf.get("mf.config_catalog", spark.conf.get("mf.catalog", None))   # noqa: F821
_CFG_SCHEMA = spark.conf.get("mf.config_schema", spark.conf.get("mf.schema", None))       # noqa: F821
_CONFIG_TABLE = spark.conf.get("mf.config_table",                                          # noqa: F821
                               f"{_CFG_CATALOG}.{_CFG_SCHEMA}.mf_pipeline_config")
_CONFIG_KEY = spark.conf.get("mf.config_key", None)   # noqa: F821
try:
    _RUN_ID = spark.conf.get("mf.run_id", None)       # noqa: F821
except Exception:
    _RUN_ID = None
_AUDIT_TABLE = spark.conf.get("mf.audit_table",                                            # noqa: F821
                              f"{_CFG_CATALOG}.{_CFG_SCHEMA}.mf_dq_audit")


def _load_config():
    """Read the mapping config row (base64 JSON) for this pipeline from the UC table."""
    import base64
    df = spark.read.table(_CONFIG_TABLE)   # noqa: F821
    if _CONFIG_KEY:
        df = df.filter(F.col("config_key") == _CONFIG_KEY)
    if _RUN_ID:
        df = df.filter(F.col("run_id") == _RUN_ID)
    row = df.orderBy(F.col("created_at").desc()).limit(1).collect()
    if not row:
        raise ValueError(f"No pipeline config found in {_CONFIG_TABLE} "
                         f"(config_key={_CONFIG_KEY}, run_id={_RUN_ID})")
    raw = row[0]["config_json"]
    try:
        return json.loads(base64.b64decode(raw).decode())
    except Exception:
        return json.loads(raw)  # tolerate plain JSON


CFG = _load_config()

CATALOG = spark.conf.get("mf.catalog", CFG.get("catalog"))   # noqa: F821
SCHEMA = spark.conf.get("mf.schema", CFG.get("schema"))      # noqa: F821
TABLE_TYPE = CFG.get("table_type", "streaming_table")
PIPELINE_NAME = CFG.get("pipeline_name", "mapping_pipeline")

_INPUTS = CFG.get("inputs", {})
_JOINS = CFG.get("joins", [])
_TARGETS = CFG.get("targets", [])

print(f"[MappingFactory] pipeline={PIPELINE_NAME} inputs={list(_INPUTS)} "
      f"joins={len(_JOINS)} targets={[t.get('target_name') for t in _TARGETS]}")


def _fqn(name):
    return f"{CATALOG}.{SCHEMA}.{name}"


def _source_relation(name):
    """Inline a source as a derived-table subquery aliased by its LOGICAL name, exposing
    every column BARE and QUALIFIED (`Table.col`) so transform SQL resolves either.
    A subquery (not a TEMP VIEW) because SDP forbids DDL inside a @dlt.table query def."""
    meta = _INPUTS[name]
    cols = meta.get("columns", [])
    proj = ", ".join([f"`{c}`" for c in cols] + [f"`{c}` AS `{name}.{c}`" for c in cols]) or "*"
    src = meta.get("table") or _fqn(name)
    return f"(SELECT {proj} FROM {src}) AS `{name}`"


def _join_on(a, b):
    """ON clause joining table `b` to already-included table `a`, or None."""
    for j in _JOINS:
        lt, rt = j.get("left_table"), j.get("right_table")
        if lt == a and rt == b:
            return f"`{a}`.`{j['left_col']}` = `{b}`.`{j['right_col']}`"
        if rt == a and lt == b:
            return f"`{a}`.`{j['right_col']}` = `{b}`.`{j['left_col']}`"
    return None


def _bridge_path(target, included):
    """BFS the join graph for the shortest path from any INCLUDED table to `target`,
    returning the connector tables to add (ending at `target`). Lets a table that isn't
    DIRECTLY joinable to the grain still be reached via intermediates — otherwise a
    transform/DQ rule referencing it hits UNRESOLVED_COLUMN."""
    from collections import deque
    adj = {}
    for j in _JOINS:
        lt, rt = j.get("left_table"), j.get("right_table")
        if not lt or not rt or lt == rt:
            continue
        adj.setdefault(lt, set()).add(rt)
        adj.setdefault(rt, set()).add(lt)
    q = deque([[s] for s in included])
    visited = set(included)
    while q:
        path = q.popleft()
        for nxt in adj.get(path[-1], ()):
            if nxt in visited:
                continue
            newpath = path + [nxt]
            if nxt == target:
                return newpath[1:]
            visited.add(nxt)
            q.append(newpath)
    return None


def _from_clause(source_tables):
    tables = [t for t in source_tables if t in _INPUTS] or list(_INPUTS)
    if not tables:
        raise ValueError("no source tables for target")
    included = [tables[0]]
    parts = [_source_relation(tables[0])]

    def _add(tbl):
        on = next((_join_on(a, tbl) for a in included if _join_on(a, tbl)), None)
        if on:
            parts.append(f"LEFT JOIN {_source_relation(tbl)} ON {on}")
            included.append(tbl)
            return True
        return False

    for tbl in tables[1:]:
        if tbl in included or _add(tbl):
            continue
        # Not directly joinable — bridge through intermediate join tables instead of
        # silently dropping it (which caused UNRESOLVED_COLUMN in the DQ flow).
        for hop in (_bridge_path(tbl, included) or []):
            if hop not in included and hop in _INPUTS:
                _add(hop)
    return " ".join(parts)


def _select_sql(cfg_t):
    items = [f"{c['sql']} AS `{c['target_field']}`" for c in cfg_t.get("columns", []) if c.get("sql")]
    if not items:
        items = ["1 AS `_empty`"]
    return f"SELECT {', '.join(items)} FROM {_from_clause(cfg_t.get('source_tables') or list(_INPUTS))}"


def _usable_rules(validations):
    """Resolve each validation to (name, logic, action) with the predicate parse-checked
    against a 0-row probe so one malformed LLM predicate is skipped, never fatal."""
    from pyspark.sql import SparkSession
    spark_ = SparkSession.getActiveSession()
    out = []
    for i, v in enumerate(validations or []):
        logic = v.get("logic")
        if not logic:
            continue
        name = v.get("name") or f"rule_{i}"
        action = "fail" if (v.get("action") or "warn").lower() == "fail" else "warn"
        out.append({"name": name, "logic": logic, "action": action})
    return out


def _sanitize(rule_name):
    import re
    return "r_" + re.sub(r"[^0-9A-Za-z]+", "_", str(rule_name)).strip("_")


def _make_target(cfg_t):
    name = cfg_t["target_name"]
    rules = _usable_rules(cfg_t.get("validations", []))
    fail_rules = [r for r in rules if r["action"] == "fail"]
    dq_name = f"{name}_dq"
    q_name = f"{name}_quarantine"

    # <target>_dq: transformed rows + one boolean col per rule (_rule_<safe>) + a
    # _quarantine flag (true iff any FAIL rule is violated). Persisted so the app can
    # roll it up into the append-only mf_dq_audit table after the run. The clean target
    # and the quarantine table both derive from this.
    def _build_dq(_cfg=cfg_t, _rules=rules):
        df = spark.sql(_select_sql(_cfg))  # noqa: F821
        quarantine = F.lit(False)
        for r in _rules:
            try:
                cond = F.coalesce(F.expr(r["logic"]), F.lit(False))  # NULL predicate -> violation
                df.select(cond.alias("_probe")).limit(0).collect()   # force resolution now
            except Exception as ex:  # noqa: BLE001 — skip an unparseable rule, don't fail the run
                print(f"[MappingFactory] {name}: skipping unparseable rule '{r['name']}': {str(ex)[:160]}")
                continue
            df = df.withColumn(f"_rule_{_sanitize(r['name'])}", cond)
            if r["action"] == "fail":
                quarantine = quarantine | (~cond)
        return df.withColumn("_quarantine", quarantine)
    _build_dq.__name__ = f"build_{dq_name}"
    dlt.table(name=dq_name, comment=f"{name}: rows + per-rule DQ booleans + _quarantine flag")(_build_dq)

    # Clean target: rows NOT quarantined, internal marker cols dropped. Materialization per config.
    def _build_target(_dq=dq_name):
        df = dlt.read(_dq).filter(~F.col("_quarantine"))
        drop = [c for c in df.columns if c.startswith("_rule_")] + ["_quarantine"]
        return df.drop(*drop)
    _build_target.__name__ = f"build_{name}"
    deco = dlt.materialized_view(name=name) if TABLE_TYPE == "materialized_view" else dlt.table(name=name)
    deco(_build_target)

    # Quarantine table: rows failing a FAIL rule, plus which FAIL rules each row broke.
    if fail_rules:
        def _build_quarantine(_dq=dq_name, _fail=fail_rules):
            df = dlt.read(_dq).filter(F.col("_quarantine"))
            # _failed_rules = array of the FAIL-rule names this row violated (NULLs removed).
            marks = F.array(*[F.when(~F.col(f"_rule_{_sanitize(r['name'])}"), F.lit(r["name"])) for r in _fail])
            df = df.withColumn("_failed_rules", F.array_except(marks, F.array(F.lit(None))))
            drop = [c for c in df.columns if c.startswith("_rule_")] + ["_quarantine"]
            return (df.drop(*drop)
                      .withColumn("_quarantined_at", F.current_timestamp())
                      .withColumn("_pipeline", F.lit(PIPELINE_NAME))
                      .withColumn("_target", F.lit(name)))
        _build_quarantine.__name__ = f"build_{q_name}"
        dlt.table(name=q_name, comment=f"{name}: rows quarantined by a FAIL-severity rule")(_build_quarantine)


for _t in _TARGETS:
    _make_target(_t)
