# Oracle CPQ → Databricks snapshot sync (Roy G/CPQ)

Built 4 Nov 2026. Org `ConnectFasterInc`, project `Roy G/CPQ`, Snaplex `ConnectFasterInc/rt/cloud/dev`.

## Pipelines
| Pipeline | Role |
|---|---|
| CPQ_00_Setup_Databricks | Creates schemas cpq_raw / cpq_silver / cpq_ops, tables and views (param `rebuild`) |
| CPQ_10_Sync_DataTable | Orchestrator for one deployed CPQ Data Table (`tableName`, `keyColumns`) |
| CPQ_11_Sync_PriceBook | Orchestrator for `/priceBooks` (key `variableName`) |
| CPQ_18_Extract_To_Stage | Child: pages a CPQ collection into cpq_ops.stg_snapshot |
| CPQ_19_Apply_Snapshot | Child: diff vs silver → change log (I/U/D) → MERGE silver, audit, control |
| CPQ_19T_Test_Apply | Test harness (no CPQ needed): steps seed, change, mass_delete, incomplete, resurrect, cleanup |

Config: `pipelines/cpq_config.expr` (uploaded to the project): cpqBaseUrl, restVersion (v19), pageSize,
catalog (blank = account default), deleteGuardPct (5), deleteGuardMax (500).

## Databricks objects
- cpq_ops.stg_snapshot: per-run landing
- cpq_raw.snapshot_changes: append-only change log, op I/U/D
- cpq_silver.snapshot_rows: current state, soft delete (is_deleted, deleted_at), Change Data Feed on
- cpq_silver.v_price_book (typed), v_custom_price and v_custom_discount (payload JSON until columns are known)
- cpq_ops.sync_audit (one row per run), cpq_ops.sync_control (latest status per object)

## Delete safety
Deletes are applied only if staged rows = CPQ totalResults AND would_delete ≤ deleteGuardMax AND
≤ deleteGuardPct % of active rows. Otherwise status OK_DELETES_DEFERRED_INCOMPLETE / OK_DELETES_BLOCKED_GUARD.
`forceDeletes=true` overrides the guard only.

## Build
`sh build/translate_all.sh` (edit build/build_assets.py, not pipelines/*.py), then re-import the *.min.slp files.

## Open items
- Both shared EMEA SE Databricks accounts return HTTP 403 (token) on 4 Nov 2026; Setup not yet run.
- CPQ HTTP Client account must be attached on CPQ_18 (1 snap), CPQ_10 and CPQ_11 (1 snap each).
- Set cpqBaseUrl in cpq_config.expr; confirm data table names and keyColumns with the customer.
- Tasks (every 5–15 min, one per object, no overlap) still to create.
