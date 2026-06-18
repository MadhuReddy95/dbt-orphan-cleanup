# dbt Orphan cleanup

Find **orphaned** warehouse tables/views on Databricks: objects left behind when a dbt model is
renamed, removed, or relocated to another schema. The tool diffs the dbt manifest against what
actually exists in Unity Catalog, classifies each leftover by **git evidence**, prints a
summary, and writes the full classified list to a results table you can query.

It reads only the scanned data (`SELECT` from `information_schema` + `DESCRIBE DETAIL`); its only
write is the results table. It never drops or renames any scanned object.

## Why not just "drop what isn't in the dbt graph"?

Because that is dangerous. Plenty of tables are *supposed* to exist without being dbt models:
ingestion/CDC feeds, manually created tables, other teams' objects. Dropping everything absent
from the dbt graph risks deleting live data. This tool avoids that with three gates.

## The 3 gates

1. **Keep-set includes sources.** The keep-set is built from the dbt manifest:
   models + seeds + snapshots + **sources**. Including sources means the external tables dbt
   references but does not build are never flagged.
2. **Git-history gate.** A table is only called an orphan if a model file by that name once
   existed in git. Something that was never a `.sql` model can never be flagged. This separates
   "former dbt model, now stale" from "never was dbt."
3. **Report, never drop.** The tool only classifies and writes a results table. Removal is a
   separate, opt-in, reversible step you run yourself.

## How it classifies (six checks, first match wins)

Every leftover table runs through six checks, in order. **First match wins, and the order is
load-bearing.**

The tool builds four lists first:

- **keep**: `schema.table` of everything dbt builds now (models, seeds, snapshots) plus
  **sources**. Full address, not just the name.
- **active-names**: bare names of models that are still active, and which schema(s) they live in
  now. (Sources are NOT in this list: only models/seeds/snapshots.)
- **files-now**: model `.sql` files that exist in the repo today (`git ls-files`).
- **files-ever**: model `.sql` files that ever existed in git history (`git log --all`).

Then, for each warehouse table:

| # | Check | Bucket | Meaning |
|---|---|---|---|
| 1 | dbt/DLT internal? (`*__dbt_backup`, `__materialization_*`, `event_log_`) | **ARTIFACT** | auto-keep |
| 2 | full address `schema.table` in **keep**? | **KEEP** | dbt builds exactly this, here |
| 3 | name in **active-names**, but a different schema? | **ORPHAN_RELOCATED** | model moved; this copy is stale |
| 4 | a `.sql` by that name in **files-now**, not active? | **DISABLED/UNBUILT** | turned off; investigate, do not drop |
| 5 | a `.sql` by that name in **files-ever**, now gone? | **ORPHAN_DELETED_RENAMED** | the real droppable leftover |
| 6 | none of the above | **NON_DBT** | never a model (external/manual); keep |

That is exactly the classify loop:

```python
for table in warehouse_tables:
    if   is_artifact(name):              bucket = "ARTIFACT"                # 1
    elif f"{schema}.{name}" in keep:     bucket = "KEEP"                    # 2  full address
    elif name in active_names:           bucket = "ORPHAN_RELOCATED"        # 3  name hit, other schema
    elif name in files_now:              bucket = "DISABLED_UNBUILT"        # 4  file here, turned off
    elif name in files_ever:             bucket = "ORPHAN_DELETED_RENAMED"  # 5  file gone
    else:                                bucket = "NON_DBT"                 # 6
```

Two details that matter:

- **Order is load-bearing.** Disabled (check 4) is tested *before* deleted-orphan (check 5): a
  turned-off-but-still-present model must never be called a droppable orphan. Relocated (3) beats
  disabled (4) beats deleted (5). Reorder these and you mislabel tables.
- **Sources are in `keep` but not in `active-names`.** So a source table can be KEEP (check 2)
  but can never become a RELOCATED orphan (check 3). That asymmetry is deliberate: sources are
  external, so the tool must never suggest one moved or went stale.

## Setup

```bash
pip install databricks-sdk
export DATABRICKS_WAREHOUSE_ID=<your_sql_warehouse_id>   # auth via ~/.databrickscfg
dbt parse                                                # produces target/manifest.json
```

Run it from inside your dbt project (so `git` sees the model history and `target/manifest.json`
exists). The manifest must be generated for the target whose schemas you are scanning, so the
keep-set's `schema.table` addresses line up with the warehouse.

## Usage

```bash
python orphan_report.py <catalog> <schema_like> <output_table>

# example
python orphan_report.py main 'analytics%' main.meta.orphan_report
```

`catalog`, `schema` (SQL `LIKE`), and the output table are positional args, so the same tool
points at any scope without code changes. Each arg is validated to a safe charset before it
touches SQL.

## Output

The console prints the bucketed summary; the full classified list is written
(`CREATE OR REPLACE` each run) to the results table you pass as the 3rd arg:

| column | notes |
|---|---|
| scan_catalog, table_schema, table_name, object_type | the scanned object |
| bucket | KEEP / ORPHAN_DELETED_RENAMED / ORPHAN_RELOCATED / DISABLED_UNBUILT / NON_DBT / ARTIFACT |
| last_altered | from `information_schema` |
| size_mb | populated for the actionable orphan buckets |
| relocated_to | for ORPHAN_RELOCATED, the schema the active model now builds in |
| generated_at | run timestamp (UTC) |

```sql
SELECT * FROM main.meta.orphan_report
WHERE bucket LIKE 'ORPHAN%' ORDER BY size_mb DESC NULLS LAST
```

## Roadmap

- **Phase 1 (this tool):** read-only reporter.
- **Phase 2:** quarantine: rename confirmed orphans to `_orphan_*` (or move to a `_to_drop`
  schema) and soak for a few days. Reversible, opt-in.
- **Phase 3:** drop after the soak. Manual, opt-in. Never an automatic post-hook.

Scope ladder: start with a dev/personal schema, then one production schema, never the whole
warehouse at once.

## License

MIT
