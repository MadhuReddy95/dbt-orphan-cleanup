"""
orphan_report.py  --  dbt orphan detector for Databricks (read-only report + results table)

Finds ORPHANED warehouse tables/views: objects left behind when a dbt model is renamed,
removed, or relocated to another schema. It diffs the dbt manifest against what actually exists
in Unity Catalog, classifies each leftover by GIT EVIDENCE, prints a summary, and writes the
full classified list to a results table you can query.

The problem: the naive approach (drop everything in the warehouse that is not in the dbt graph)
is dangerous, because plenty of tables are *supposed* to exist without being dbt models:
ingestion/CDC tables, manually created tables, other teams' objects. Dropping on that signal
risks deleting live data. This tool avoids that with three gates:

  1. keep-set includes SOURCES (not just models/seeds/snapshots) -> external tables dbt knows
     about are never flagged.
  2. git-history gate -> a table is only an orphan if a model file by that name once existed in
     git. Something that was never a `.sql` model can never be called an orphan.
  3. report, never drop -> this tool only classifies and writes a results table.

Every leftover runs through six checks, FIRST MATCH WINS (the order is load-bearing):
  1 ARTIFACT                - dbt/DLT internal (*__dbt_backup, __materialization_*, event_log_*)
  2 KEEP                    - full address schema.table is in the dbt graph (incl. sources)
  3 ORPHAN_RELOCATED        - name is an active model, but it now builds in a different schema
  4 DISABLED_UNBUILT        - a .sql by that name exists now, but is not active (disabled)
  5 ORPHAN_DELETED_RENAMED  - a .sql by that name existed in git history, now gone (droppable)
  6 NON_DBT                 - never a model file (external/manual) -> keep

It reads only SELECT / DESCRIBE DETAIL; the ONLY write is the results table. It never drops or
renames any scanned object.

Setup:
    pip install databricks-sdk
    export DATABRICKS_WAREHOUSE_ID=<your_sql_warehouse_id>   # auth via ~/.databrickscfg
    dbt parse                                                # produces target/manifest.json

Usage:
    python orphan_report.py <catalog> <schema_like> <output_table>
    python orphan_report.py main 'analytics%' main.meta.orphan_report
"""

import json
import os
import re
import subprocess
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient

if len(sys.argv) < 4:
    sys.exit("usage: python orphan_report.py <catalog> <schema_like> <output_table>")

WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID")
if not WAREHOUSE_ID:
    sys.exit("Set DATABRICKS_WAREHOUSE_ID to your SQL warehouse id (auth via ~/.databrickscfg).")

CATALOG = sys.argv[1]
SCHEMA_LIKE = sys.argv[2]
OUTPUT_TABLE = sys.argv[3]
MANIFEST_PATH = os.environ.get("DBT_MANIFEST", "target/manifest.json")
RUN_TS = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

# These args are interpolated into SQL. CATALOG/OUTPUT_TABLE are identifiers (can't be
# parameter-bound), so validate to a safe charset; SCHEMA_LIKE is a string literal, validated
# loosely here and escaped at use via sql_lit().
if not re.fullmatch(r"[A-Za-z0-9_]+", CATALOG):
    sys.exit(f"Invalid catalog '{CATALOG}': letters, digits, underscore only.")
if not re.fullmatch(r"[A-Za-z0-9_.]+", OUTPUT_TABLE):
    sys.exit(f"Invalid output table '{OUTPUT_TABLE}': use catalog.schema.table (alnum / _ / . only).")
if not re.fullmatch(r"[A-Za-z0-9_%.]+", SCHEMA_LIKE):
    sys.exit(f"Invalid schema pattern '{SCHEMA_LIKE}': alnum, underscore, %, . only.")

# dbt / DLT internal objects that are never orphans (auto-keep, never flag)
ARTIFACT_SUFFIXES = ("__dbt_backup", "__dbt_tmp")
ARTIFACT_PREFIXES = ("__materialization_", "event_log_")

w = WorkspaceClient()


def is_artifact(name):
    n = name.lower()
    return n.endswith(ARTIFACT_SUFFIXES) or n.startswith(ARTIFACT_PREFIXES)


def run_sql(stmt):
    r = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID, statement=stmt, wait_timeout="50s"
    )
    while r.status.state.value in ("PENDING", "RUNNING"):
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state.value != "SUCCEEDED":
        raise RuntimeError(f"{r.status.state.value}: {getattr(r.status, 'error', None)}")
    cols = [c.name for c in r.manifest.schema.columns] if r.manifest and r.manifest.schema else []
    return cols, (r.result.data_array or [])


def git_out(args):
    return subprocess.run(["git"] + args, capture_output=True, text=True).stdout


def is_model_path(p):
    p = p.strip().replace("\\", "/")
    return p.endswith(".sql") and "/models/" in ("/" + p)


def basename_no_ext(p):
    return os.path.basename(p.strip())[:-4].lower()


def sql_lit(v):
    if v is None:
        return "NULL"
    if isinstance(v, (int, float)):
        return repr(v)
    return "'" + str(v).replace("'", "''") + "'"


# ---- 1) keep-set from the manifest -----------------------------------------
if not os.path.exists(MANIFEST_PATH):
    sys.exit(f"{MANIFEST_PATH} not found. Run `dbt parse` first (or set DBT_MANIFEST).")

with open(MANIFEST_PATH) as f:
    man = json.load(f)

keep = set()
# relname -> {schemas where an ACTIVE dbt resource of that name lives}; lets us tell a
# relocation orphan (active model now in a different schema) from a current/disabled one.
active_names = defaultdict(set)
for node in man.get("nodes", {}).values():
    if node.get("resource_type") in ("model", "seed", "snapshot"):
        sch = (node.get("schema") or "").lower()
        rel = (node.get("alias") or node.get("name") or "").lower()
        keep.add(f"{sch}.{rel}")
        active_names[rel].add(sch)
for src in man.get("sources", {}).values():
    sch = (src.get("schema") or "").lower()
    rel = (src.get("identifier") or src.get("name") or "").lower()
    keep.add(f"{sch}.{rel}")  # sources are in keep, but NOT in active_names (never "relocated")

# ---- 2) git evidence: current + ever-existed model basenames ----------------
files_now = {basename_no_ext(l) for l in git_out(["ls-files", "*.sql"]).splitlines() if is_model_path(l)}
files_ever = {
    basename_no_ext(l)
    for l in git_out(["log", "--all", "--pretty=format:", "--name-only"]).splitlines()
    if is_model_path(l)
}

# ---- 3) what actually exists in the warehouse ------------------------------
_, rows = run_sql(f"""
    SELECT table_schema, table_name, table_type, CAST(last_altered AS STRING) AS last_altered
    FROM {CATALOG}.information_schema.tables
    WHERE table_schema LIKE {sql_lit(SCHEMA_LIKE)}
    ORDER BY table_schema, table_name
""")

# ---- 4) classify: six checks, first match wins (order is load-bearing) ------
keep_recs, relocated, git_confirmed, disabled_unbuilt, non_dbt, artifacts = [], [], [], [], [], []
lives_now = {}
for sch, name, ttype, last_alt in rows:
    rec = (sch, name, ttype, last_alt)
    base = name.lower()
    if is_artifact(name):                              # 1
        artifacts.append(rec)
    elif f"{sch.lower()}.{base}" in keep:              # 2  full address
        keep_recs.append(rec)
    elif base in active_names:                         # 3  name active, different schema
        relocated.append(rec)
        lives_now[(sch, name)] = ", ".join(sorted(active_names[base]))
    elif base in files_now:                            # 4  file present, not active
        disabled_unbuilt.append(rec)
    elif base in files_ever:                           # 5  file gone -> droppable orphan
        git_confirmed.append(rec)
    else:                                              # 6  never a model -> keep
        non_dbt.append(rec)

orphans = git_confirmed + relocated

# ---- 5) size lookup for the actionable set (read-only) ----------------------
def get_size_mb(rec):
    sch, name = rec[0], rec[1]
    try:
        cols, data = run_sql(f"DESCRIBE DETAIL {CATALOG}.{sch}.{name}")
        if data:
            d = dict(zip(cols, data[0]))
            b = d.get("sizeInBytes")
            return round(int(b) / 1_048_576, 1) if b is not None else None
    except Exception:
        return None
    return None

sizes = {}
if orphans:
    with ThreadPoolExecutor(max_workers=8) as ex:
        for rec, mb in zip(orphans, ex.map(get_size_mb, orphans)):
            sizes[(rec[0], rec[1])] = mb

# ---- 6) console summary ----------------------------------------------------
print(f"\nOrphan report  |  {CATALOG}  schema LIKE '{SCHEMA_LIKE}'\n" + "=" * 78)
print(f"Tables/views scanned : {len(rows)}")
print(f"  KEEP (in dbt graph)                          : {len(keep_recs)}")
print(f"  ORPHAN - deleted/renamed model (review)      : {len(git_confirmed)}")
print(f"  ORPHAN - relocated to another schema (review): {len(relocated)}")
print(f"  DISABLED/UNBUILT (file exists, not active)   : {len(disabled_unbuilt)}")
print(f"  NON-DBT (never a model -> keep)              : {len(non_dbt)}")
print(f"  dbt/DLT artifacts (auto-keep)                : {len(artifacts)}")

def fmt(rec, with_size=False, note=None):
    sch, name, ttype, last_alt = rec
    extra = ""
    if with_size:
        mb = sizes.get((sch, name))
        extra = f"  {mb} MB" if mb is not None else "  (size n/a)"
    tail = f"  -> now in: {note}" if note else ""
    return f"  {sch}.{name}  [{ttype}]  last_altered={last_alt}{extra}{tail}"

if git_confirmed:
    print(f"\n--- ORPHANS: deleted/renamed model ({len(git_confirmed)}) ---")
    for rec in git_confirmed:
        print(fmt(rec, with_size=True))
if relocated:
    print(f"\n--- ORPHANS: relocated to another schema ({len(relocated)}) ---")
    for rec in relocated:
        print(fmt(rec, with_size=True, note=lives_now.get((rec[0], rec[1]))))
if disabled_unbuilt:
    print(f"\n--- DISABLED/UNBUILT: investigate, do NOT drop ({len(disabled_unbuilt)}) ---")
    for rec in disabled_unbuilt:
        print(fmt(rec))
if non_dbt:
    print(f"\n--- NON-DBT: never a dbt model -> keep ({len(non_dbt)}) ---")
    for rec in non_dbt:
        print(fmt(rec))

# ---- 7) write results table (the only write; scanned data is untouched) -----
def to_row(rec, bucket, reloc=None):
    sch, name, ttype, last_alt = rec
    return (CATALOG, sch, name, ttype, bucket, last_alt, sizes.get((sch, name)), reloc, RUN_TS)

all_rows = (
    [to_row(r, "KEEP") for r in keep_recs]
    + [to_row(r, "ORPHAN_DELETED_RENAMED") for r in git_confirmed]
    + [to_row(r, "ORPHAN_RELOCATED", lives_now.get((r[0], r[1]))) for r in relocated]
    + [to_row(r, "DISABLED_UNBUILT") for r in disabled_unbuilt]
    + [to_row(r, "NON_DBT") for r in non_dbt]
    + [to_row(r, "ARTIFACT") for r in artifacts]
)

COLS = ("scan_catalog,table_schema,table_name,object_type,bucket,"
        "last_altered,size_mb,relocated_to,generated_at")
schema_fqn = OUTPUT_TABLE.rsplit(".", 1)[0]
run_sql(f"CREATE SCHEMA IF NOT EXISTS {schema_fqn}")
run_sql(f"""
    CREATE OR REPLACE TABLE {OUTPUT_TABLE} (
        scan_catalog STRING, table_schema STRING, table_name STRING,
        object_type  STRING, bucket       STRING, last_altered STRING,
        size_mb      DOUBLE, relocated_to STRING, generated_at STRING
    )
""")
for i in range(0, len(all_rows), 200):
    chunk = all_rows[i:i + 200]
    values = ",\n".join("(" + ",".join(sql_lit(c) for c in row) + ")" for row in chunk)
    run_sql(f"INSERT INTO {OUTPUT_TABLE} ({COLS}) VALUES {values}")

print(f"\n{len(orphans)} actionable orphan(s). Wrote {len(all_rows)} classified rows to "
      f"{OUTPUT_TABLE} (generated_at={RUN_TS} UTC).")
print(f"Query: SELECT * FROM {OUTPUT_TABLE} "
      f"WHERE bucket LIKE 'ORPHAN%' ORDER BY size_mb DESC NULLS LAST")
print("No scanned object was dropped or renamed.")
