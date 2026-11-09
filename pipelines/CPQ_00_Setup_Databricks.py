"""
Pipeline Name: CPQ_00_Setup_Databricks

Pipeline Summary: Creates the Databricks objects for the Oracle CPQ snapshot sync in the catalog set in
cpq_config.expr (blank = the account's current catalog): schemas cpq_raw (change log), cpq_silver (current
state + typed views) and cpq_ops (staging, audit, control). Idempotent. rebuild=true first DROPS the three
cpq_* schemas (and only those). Output: the current catalog and row counts.

(#snap_0) TransformJSONGenerator: one start document
(#snap_1) DatabricksMultiExecute: optional drops, then CREATE ... IF NOT EXISTS / CREATE OR REPLACE VIEW
(#snap_2) TransformGroupByN: collapses the statement results to one document
(#snap_3) DatabricksExecute: reports current_catalog() and row counts (pipeline output)

Configuration Parameters:
- rebuild: 'true' drops cpq_raw, cpq_silver and cpq_ops first (default 'false')
"""

from slpy.modules.Pipeline.Pipeline import Pipeline
from slpy.modules.Pipeline.utils import Expr
from slpy.modules.Pipeline.param_table.ParamTable import ParamTable
from slpy.modules.Pipeline.expression_libraries.ExpressionLibraries import ExpressionLibraries
from slpy.modules.Snap import Account
from slpy.modules.Pipeline.utils.open_view import OPEN_VIEW
from slpy.modules.Snap.TransformJSONGenerator import TransformJSONGenerator
from slpy.modules.Snap.DatabricksMultiExecute import DatabricksMultiExecute
from slpy.modules.Snap.TransformGroupByN import TransformGroupByN
from slpy.modules.Snap.DatabricksExecute import DatabricksExecute

p = Pipeline(label='CPQ_00_Setup_Databricks')
p.param_table = ParamTable(param=[
    ParamTable.ParamItem(capture=True, key='rebuild', value='false', data_type='string')
])
p.expression_libraries = ExpressionLibraries(expression_library=[
    ExpressionLibraries.ExpressionLibraryItem(path='cpq_config.expr', as_='cfg')
])
snap_0 = TransformJSONGenerator(
    label='Start',
    editable_content='[{"start": true}]',
    array_elements_as_documents=True
)
snap_1 = DatabricksMultiExecute(
    label='Create schemas, tables and views',
    executable_during_suggest=False,
    sql_statements=[
        DatabricksMultiExecute.SqlStatementsItem(sql_statement=Expr('_rebuild == \'true\' ? ("DROP SCHEMA IF EXISTS " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_silver CASCADE") : "SELECT 1"')),
        DatabricksMultiExecute.SqlStatementsItem(sql_statement=Expr('_rebuild == \'true\' ? ("DROP SCHEMA IF EXISTS " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_raw CASCADE") : "SELECT 1"')),
        DatabricksMultiExecute.SqlStatementsItem(sql_statement=Expr('_rebuild == \'true\' ? ("DROP SCHEMA IF EXISTS " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_ops CASCADE") : "SELECT 1"')),
        DatabricksMultiExecute.SqlStatementsItem(sql_statement=Expr('"CREATE SCHEMA IF NOT EXISTS " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_raw COMMENT \'Oracle CPQ change log (bronze)\'"')),
        DatabricksMultiExecute.SqlStatementsItem(sql_statement=Expr('"CREATE SCHEMA IF NOT EXISTS " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_silver COMMENT \'Oracle CPQ current state (silver)\'"')),
        DatabricksMultiExecute.SqlStatementsItem(sql_statement=Expr('"CREATE SCHEMA IF NOT EXISTS " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_ops COMMENT \'Oracle CPQ sync staging, control and audit\'"')),
        DatabricksMultiExecute.SqlStatementsItem(sql_statement=Expr('"CREATE TABLE IF NOT EXISTS " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_ops.stg_snapshot (run_id STRING, object_name STRING, cpq_id STRING, payload STRING, business_key STRING, row_hash STRING) COMMENT \'Per-run landing area for full CPQ snapshots\'"')),
        DatabricksMultiExecute.SqlStatementsItem(sql_statement=Expr('"CREATE TABLE IF NOT EXISTS " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_raw.snapshot_changes (run_id STRING, object_name STRING, business_key STRING, cpq_id STRING, op STRING, row_hash STRING, payload STRING, changed_at TIMESTAMP) COMMENT \'Append-only change log: op I (insert or undelete), U (update), D (delete)\'"')),
        DatabricksMultiExecute.SqlStatementsItem(sql_statement=Expr('"CREATE TABLE IF NOT EXISTS " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_silver.snapshot_rows (object_name STRING, business_key STRING, cpq_id STRING, row_hash STRING, payload STRING, is_deleted BOOLEAN, deleted_at TIMESTAMP, first_seen_at TIMESTAMP, last_changed_at TIMESTAMP, last_run_id STRING) COMMENT \'Current state of every snapshot-synced CPQ object; soft deletes\' TBLPROPERTIES (delta.enableChangeDataFeed = true)"')),
        DatabricksMultiExecute.SqlStatementsItem(sql_statement=Expr('"CREATE TABLE IF NOT EXISTS " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_ops.sync_audit (run_id STRING, object_name STRING, finished_at TIMESTAMP, status STRING, expected_total BIGINT, staged_rows BIGINT, distinct_keys BIGINT, inserted_rows BIGINT, updated_rows BIGINT, deleted_rows BIGINT, would_delete BIGINT, deletes_allowed BOOLEAN, message STRING) COMMENT \'One row per object per sync run\'"')),
        DatabricksMultiExecute.SqlStatementsItem(sql_statement=Expr('"CREATE TABLE IF NOT EXISTS " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_ops.sync_control (object_name STRING, last_run_id STRING, last_run_at TIMESTAMP, last_status STRING, last_message STRING, watermark STRING) COMMENT \'Latest sync state per object (watermark used later by quote syncs)\'"')),
        DatabricksMultiExecute.SqlStatementsItem(sql_statement=Expr('"CREATE OR REPLACE VIEW " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_silver.v_price_book AS SELECT CAST(cpq_id AS BIGINT) AS id, business_key AS variable_name, payload:[\'name\'] AS name, payload:[\'description\'] AS description, coalesce(payload:[\'status\'][\'lookupCode\'], payload:[\'status\']) AS status_code, payload:[\'status\'][\'displayValue\'] AS status_display, CAST(payload:[\'dateAdded\'] AS TIMESTAMP) AS date_added, CAST(payload:[\'dateModified\'] AS TIMESTAMP) AS date_modified, CAST(payload:[\'orderNumber\'] AS INT) AS order_number, is_deleted, deleted_at, first_seen_at, last_changed_at, last_run_id FROM " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_silver.snapshot_rows WHERE object_name = \'PriceBook\'"')),
        DatabricksMultiExecute.SqlStatementsItem(sql_statement=Expr('"CREATE OR REPLACE VIEW " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_silver.v_custom_price AS SELECT business_key, CAST(cpq_id AS BIGINT) AS cpq_id, payload, is_deleted, deleted_at, first_seen_at, last_changed_at, last_run_id FROM " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_silver.snapshot_rows WHERE object_name = \'CustomPrice\'"')),
        DatabricksMultiExecute.SqlStatementsItem(sql_statement=Expr('"CREATE OR REPLACE VIEW " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_silver.v_custom_discount AS SELECT business_key, CAST(cpq_id AS BIGINT) AS cpq_id, payload, is_deleted, deleted_at, first_seen_at, last_changed_at, last_run_id FROM " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_silver.snapshot_rows WHERE object_name = \'CustomDiscount\'"'))
    ],
    pm_account=Account(ref_class_id='com-snaplogic-snap-api-sql-accounts-databricks-databricksdatabaseaccount', label='Databricks_EMEA-SE_sql-warehouse-SERVERLESS')
)
snap_2 = TransformGroupByN(
    label='Wait for all statements',
    group_size=1000000,
    target_field='results'
)
snap_3 = DatabricksExecute(
    label='Report catalog and row counts',
    sql_statement=Expr('"SELECT current_catalog() AS current_catalog, current_user() AS run_as, (SELECT count(*) FROM " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_silver.snapshot_rows) AS silver_rows, (SELECT count(*) FROM " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_ops.sync_control) AS control_rows, (SELECT count(*) FROM " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_ops.sync_audit) AS audit_rows"'),
    query_type='Read',
    executable_during_suggest=False,
    pm_account=Account(ref_class_id='com-snaplogic-snap-api-sql-accounts-databricks-databricksdatabaseaccount', label='Databricks_EMEA-SE_sql-warehouse-SERVERLESS')
)
p.connect(src=snap_0, dst=snap_1, src_view_id='output0', dst_view_id='input0', src_output_type='document', dst_input_type='document')
p.connect(src=snap_1, dst=snap_2, src_view_id='output0', dst_view_id='input0', src_output_type='document', dst_input_type='document')
p.connect(src=snap_2, dst=snap_3, src_view_id='output0', dst_view_id='input0', src_output_type='document', dst_input_type='document')
p.connect(src=snap_3, dst=OPEN_VIEW.OUTPUT, src_view_id='output0', dst_view_id='OPENVIEW.OUTPUT', src_output_type='document', dst_input_type='document')
