"""
Pipeline Name: CPQ_19_Apply_Snapshot

Pipeline Summary: Child pipeline. Compares the full snapshot staged for runId with the current state in
cpq_silver.snapshot_rows and applies inserts, updates and (soft) deletes. Every change is first appended to
cpq_raw.snapshot_changes (op I/U/D); silver is then MERGEd from that change log. Deletes are applied only when
the snapshot is complete (staged rows = expectedTotal reported by CPQ) and within the delete guard
(would_delete <= deleteGuardMax and <= deleteGuardPct % of active rows), unless forceDeletes=true.
Writes cpq_ops.sync_audit and cpq_ops.sync_control and clears staging. Output: the audit row.

(#snap_0) TransformJSONGenerator: start document
(#snap_1) DatabricksExecute: business_key from keyColumns (payload:['col'] joined by '|', or cpq_id) and sha256 row_hash
(#snap_2) DatabricksExecute: staged_rows, distinct_keys, active_rows, would_delete
(#snap_3/4) TransformMapper: completeness, guard, status, message
(#snap_5) DatabricksMultiExecute: change log I/U, change log D (if allowed), MERGE silver, audit, control, cleanup
(#snap_6) TransformGroupByN: one document after all statements
(#snap_7) DatabricksExecute: audit row (pipeline output, returned to the parent)

Configuration Parameters:
- runId, objectName, keyColumns, expectedTotal: set by the parent (-1 = unknown, deletes deferred)
- deleteGuardPct / deleteGuardMax: blank = cpq_config.expr defaults (5 % / 500 rows)
- forceDeletes: 'true' overrides the guard (never the completeness check)
"""

from slpy.modules.Pipeline.Pipeline import Pipeline
from slpy.modules.Pipeline.utils import Expr
from slpy.modules.Pipeline.param_table.ParamTable import ParamTable
from slpy.modules.Pipeline.expression_libraries.ExpressionLibraries import ExpressionLibraries
from slpy.modules.Snap import Account
from slpy.modules.Pipeline.utils.open_view import OPEN_VIEW
from slpy.modules.Snap.TransformJSONGenerator import TransformJSONGenerator
from slpy.modules.Snap.DatabricksExecute import DatabricksExecute
from slpy.modules.Snap.TransformMapper import TransformMapper
from slpy.modules.Snap.DatabricksMultiExecute import DatabricksMultiExecute
from slpy.modules.Snap.TransformGroupByN import TransformGroupByN

p = Pipeline(label='CPQ_19_Apply_Snapshot')
p.param_table = ParamTable(param=[
    ParamTable.ParamItem(capture=True, key='runId', value='', data_type='string'),
    ParamTable.ParamItem(capture=True, key='objectName', value='', data_type='string'),
    ParamTable.ParamItem(capture=True, key='keyColumns', value='', data_type='string'),
    ParamTable.ParamItem(capture=True, key='expectedTotal', value='-1', data_type='string'),
    ParamTable.ParamItem(capture=True, key='deleteGuardPct', value='', data_type='string'),
    ParamTable.ParamItem(capture=True, key='deleteGuardMax', value='', data_type='string'),
    ParamTable.ParamItem(capture=True, key='forceDeletes', value='false', data_type='string')
])
p.expression_libraries = ExpressionLibraries(expression_library=[
    ExpressionLibraries.ExpressionLibraryItem(path='cpq_config.expr', as_='cfg')
])
snap_0 = TransformJSONGenerator(
    label='Start',
    editable_content='[{"start": true}]',
    array_elements_as_documents=True
)
snap_1 = DatabricksExecute(
    label='Compute business_key and row_hash',
    sql_statement=Expr('"UPDATE " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_ops.stg_snapshot SET business_key = " + (lib.cfg.keyExpr(_keyColumns)) + ", row_hash = sha2(payload, 256) WHERE run_id = " + (lib.cfg.sqlStr(_runId))'),
    query_type='Write',
    executable_during_suggest=False,
    pm_account=Account(ref_class_id='com-snaplogic-snap-api-sql-accounts-databricks-databricksdatabaseaccount', label='Databricks_EMEA-SE_sql-warehouse-SERVERLESS')
)
snap_2 = DatabricksExecute(
    label='Completeness and delete-guard counts',
    sql_statement=Expr('"SELECT (SELECT count(*) FROM " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_ops.stg_snapshot WHERE run_id = " + (lib.cfg.sqlStr(_runId)) + ") AS staged_rows, (SELECT count(DISTINCT business_key) FROM " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_ops.stg_snapshot WHERE run_id = " + (lib.cfg.sqlStr(_runId)) + ") AS distinct_keys, (SELECT count(*) FROM " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_silver.snapshot_rows WHERE object_name = " + (lib.cfg.sqlStr(_objectName)) + " AND NOT is_deleted) AS active_rows, (SELECT count(*) FROM (SELECT business_key FROM " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_silver.snapshot_rows WHERE object_name = " + (lib.cfg.sqlStr(_objectName)) + " AND NOT is_deleted) t LEFT ANTI JOIN (SELECT business_key FROM " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_ops.stg_snapshot WHERE run_id = " + (lib.cfg.sqlStr(_runId)) + ") s ON t.business_key = s.business_key) AS would_delete"'),
    query_type='Read',
    executable_during_suggest=False,
    pm_account=Account(ref_class_id='com-snaplogic-snap-api-sql-accounts-databricks-databricksdatabaseaccount', label='Databricks_EMEA-SE_sql-warehouse-SERVERLESS')
)
snap_3 = TransformMapper(
    label='Evaluate completeness and guard',
    pass_through=True,
    null_safe_access=False,
    transformations=TransformMapper.Transformations(
        mapping_root='$',
        mapping_table=[
            TransformMapper.Transformations.MappingTableItem(expression=Expr('parseInt(_expectedTotal)'), target_path='$expected'),
            TransformMapper.Transformations.MappingTableItem(expression=Expr('parseInt(_expectedTotal) >= 0 && $staged_rows == parseInt(_expectedTotal)'), target_path='$complete'),
            TransformMapper.Transformations.MappingTableItem(expression=Expr("$would_delete <= (_deleteGuardMax == '' ? lib.cfg.deleteGuardMax : parseInt(_deleteGuardMax)) && ($active_rows == 0 || $would_delete * 100 <= (_deleteGuardPct == '' ? lib.cfg.deleteGuardPct : parseFloat(_deleteGuardPct)) * $active_rows)"), target_path='$within_guard')
        ]
    )
)
snap_4 = TransformMapper(
    label='Decide deletes and status',
    pass_through=True,
    null_safe_access=False,
    transformations=TransformMapper.Transformations(
        mapping_root='$',
        mapping_table=[
            TransformMapper.Transformations.MappingTableItem(expression=Expr("$complete && ($within_guard || _forceDeletes == 'true')"), target_path='$allow'),
            TransformMapper.Transformations.MappingTableItem(expression=Expr("!$complete ? 'OK_DELETES_DEFERRED_INCOMPLETE' : (($within_guard || _forceDeletes == 'true') ? 'OK' : 'OK_DELETES_BLOCKED_GUARD')"), target_path='$status'),
            TransformMapper.Transformations.MappingTableItem(expression=Expr("'expected=' + $expected + ' staged=' + $staged_rows + ' distinct_keys=' + $distinct_keys + ' active=' + $active_rows + ' would_delete=' + $would_delete + ($distinct_keys != $staged_rows ? ' WARNING duplicate business keys - check keyColumns' : '')"), target_path='$message')
        ]
    )
)
snap_5 = DatabricksMultiExecute(
    label='Log changes, merge silver, audit, control, clean staging',
    executable_during_suggest=False,
    sql_statements=[
        DatabricksMultiExecute.SqlStatementsItem(sql_statement=Expr('"INSERT INTO " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_raw.snapshot_changes (run_id, object_name, business_key, cpq_id, op, row_hash, payload, changed_at) SELECT s.run_id, s.object_name, s.business_key, s.cpq_id, CASE WHEN t.business_key IS NULL OR t.is_deleted THEN \'I\' ELSE \'U\' END, s.row_hash, s.payload, current_timestamp() FROM (SELECT * FROM " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_ops.stg_snapshot WHERE run_id = " + (lib.cfg.sqlStr(_runId)) + " QUALIFY row_number() OVER (PARTITION BY business_key ORDER BY cpq_id DESC) = 1) s LEFT JOIN (SELECT * FROM " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_silver.snapshot_rows WHERE object_name = " + (lib.cfg.sqlStr(_objectName)) + ") t ON t.business_key = s.business_key WHERE t.business_key IS NULL OR t.is_deleted OR t.row_hash <> s.row_hash"')),
        DatabricksMultiExecute.SqlStatementsItem(sql_statement=Expr('"INSERT INTO " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_raw.snapshot_changes (run_id, object_name, business_key, cpq_id, op, row_hash, payload, changed_at) SELECT " + (lib.cfg.sqlStr(_runId)) + ", " + (lib.cfg.sqlStr(_objectName)) + ", t.business_key, t.cpq_id, \'D\', t.row_hash, t.payload, current_timestamp() FROM (SELECT * FROM " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_silver.snapshot_rows WHERE object_name = " + (lib.cfg.sqlStr(_objectName)) + " AND NOT is_deleted) t LEFT ANTI JOIN (SELECT business_key FROM " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_ops.stg_snapshot WHERE run_id = " + (lib.cfg.sqlStr(_runId)) + ") s ON t.business_key = s.business_key WHERE " + ($allow ? \'true\' : \'false\')')),
        DatabricksMultiExecute.SqlStatementsItem(sql_statement=Expr('"MERGE INTO " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_silver.snapshot_rows t USING (SELECT * FROM " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_raw.snapshot_changes WHERE run_id = " + (lib.cfg.sqlStr(_runId)) + " AND object_name = " + (lib.cfg.sqlStr(_objectName)) + ") c ON t.object_name = c.object_name AND t.business_key = c.business_key WHEN MATCHED AND c.op = \'D\' THEN UPDATE SET t.is_deleted = true, t.deleted_at = c.changed_at, t.last_changed_at = c.changed_at, t.last_run_id = c.run_id WHEN MATCHED THEN UPDATE SET t.cpq_id = c.cpq_id, t.row_hash = c.row_hash, t.payload = c.payload, t.is_deleted = false, t.deleted_at = NULL, t.last_changed_at = c.changed_at, t.last_run_id = c.run_id WHEN NOT MATCHED AND c.op <> \'D\' THEN INSERT (object_name, business_key, cpq_id, row_hash, payload, is_deleted, deleted_at, first_seen_at, last_changed_at, last_run_id) VALUES (c.object_name, c.business_key, c.cpq_id, c.row_hash, c.payload, false, NULL, c.changed_at, c.changed_at, c.run_id)"')),
        DatabricksMultiExecute.SqlStatementsItem(sql_statement=Expr('"INSERT INTO " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_ops.sync_audit (run_id, object_name, finished_at, status, expected_total, staged_rows, distinct_keys, inserted_rows, updated_rows, deleted_rows, would_delete, deletes_allowed, message) SELECT " + (lib.cfg.sqlStr(_runId)) + ", " + (lib.cfg.sqlStr(_objectName)) + ", current_timestamp(), " + (lib.cfg.sqlStr($status)) + ", " + (\'\' + $expected) + ", " + (\'\' + $staged_rows) + ", " + (\'\' + $distinct_keys) + ", count_if(op = \'I\'), count_if(op = \'U\'), count_if(op = \'D\'), " + (\'\' + $would_delete) + ", " + ($allow ? \'true\' : \'false\') + ", " + (lib.cfg.sqlStr($message)) + " FROM " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_raw.snapshot_changes WHERE run_id = " + (lib.cfg.sqlStr(_runId)) + " AND object_name = " + (lib.cfg.sqlStr(_objectName))')),
        DatabricksMultiExecute.SqlStatementsItem(sql_statement=Expr('"MERGE INTO " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_ops.sync_control t USING (SELECT " + (lib.cfg.sqlStr(_objectName)) + " AS object_name) s ON t.object_name = s.object_name WHEN MATCHED THEN UPDATE SET t.last_run_id = " + (lib.cfg.sqlStr(_runId)) + ", t.last_run_at = current_timestamp(), t.last_status = " + (lib.cfg.sqlStr($status)) + ", t.last_message = " + (lib.cfg.sqlStr($message)) + " WHEN NOT MATCHED THEN INSERT (object_name, last_run_id, last_run_at, last_status, last_message, watermark) VALUES (" + (lib.cfg.sqlStr(_objectName)) + ", " + (lib.cfg.sqlStr(_runId)) + ", current_timestamp(), " + (lib.cfg.sqlStr($status)) + ", " + (lib.cfg.sqlStr($message)) + ", NULL)"')),
        DatabricksMultiExecute.SqlStatementsItem(sql_statement=Expr('"DELETE FROM " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_ops.stg_snapshot WHERE object_name = " + (lib.cfg.sqlStr(_objectName))'))
    ],
    pm_account=Account(ref_class_id='com-snaplogic-snap-api-sql-accounts-databricks-databricksdatabaseaccount', label='Databricks_EMEA-SE_sql-warehouse-SERVERLESS')
)
snap_6 = TransformGroupByN(
    label='Wait for all statements',
    group_size=1000000,
    target_field='results'
)
snap_7 = DatabricksExecute(
    label='Return run summary',
    sql_statement=Expr('"SELECT * FROM " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_ops.sync_audit WHERE run_id = " + (lib.cfg.sqlStr(_runId)) + " AND object_name = " + (lib.cfg.sqlStr(_objectName))'),
    query_type='Read',
    executable_during_suggest=False,
    pm_account=Account(ref_class_id='com-snaplogic-snap-api-sql-accounts-databricks-databricksdatabaseaccount', label='Databricks_EMEA-SE_sql-warehouse-SERVERLESS')
)
p.connect(src=snap_0, dst=snap_1, src_view_id='output0', dst_view_id='input0', src_output_type='document', dst_input_type='document')
p.connect(src=snap_1, dst=snap_2, src_view_id='output0', dst_view_id='input0', src_output_type='document', dst_input_type='document')
p.connect(src=snap_2, dst=snap_3, src_view_id='output0', dst_view_id='input0', src_output_type='document', dst_input_type='document')
p.connect(src=snap_3, dst=snap_4, src_view_id='output0', dst_view_id='input0', src_output_type='document', dst_input_type='document')
p.connect(src=snap_4, dst=snap_5, src_view_id='output0', dst_view_id='input0', src_output_type='document', dst_input_type='document')
p.connect(src=snap_5, dst=snap_6, src_view_id='output0', dst_view_id='input0', src_output_type='document', dst_input_type='document')
p.connect(src=snap_6, dst=snap_7, src_view_id='output0', dst_view_id='input0', src_output_type='document', dst_input_type='document')
p.connect(src=snap_7, dst=OPEN_VIEW.OUTPUT, src_view_id='output0', dst_view_id='OPENVIEW.OUTPUT', src_output_type='document', dst_input_type='document')
