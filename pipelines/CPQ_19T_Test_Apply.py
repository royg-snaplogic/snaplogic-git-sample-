"""
Pipeline Name: CPQ_19T_Test_Apply

Pipeline Summary: Test harness for CPQ_19_Apply_Snapshot without needing Oracle CPQ. Stages a synthetic
snapshot for object TEST_SNAPSHOT (key column k), runs CPQ_19 and compares status/inserted/updated/deleted
with the expected result. Run the steps in order: seed, change, mass_delete, incomplete, resurrect, cleanup.
  seed        A,B,C           -> OK 3/0/0
  change      A,B(v2),D       -> OK 1/1/1 (D new, B updated, C deleted; 33 % <= 50 % guard)
  mass_delete A               -> OK_DELETES_BLOCKED_GUARD (would delete 2 of 3)
  incomplete  A, expected 2   -> OK_DELETES_DEFERRED_INCOMPLETE
  resurrect   A,B(v2),C,D     -> OK 1/0/0 (C undeleted)
  cleanup     removes all TEST_SNAPSHOT rows
Output: one document with step, actual, expected, passed.
"""

from slpy.modules.Pipeline.Pipeline import Pipeline
from slpy.modules.Pipeline.utils import Expr
from slpy.modules.Pipeline.param_table.ParamTable import ParamTable
from slpy.modules.Pipeline.expression_libraries.ExpressionLibraries import ExpressionLibraries
from slpy.modules.Snap import Account
from slpy.modules.Pipeline.utils.open_view import OPEN_VIEW
from slpy.modules.Snap.TransformJSONGenerator import TransformJSONGenerator
from slpy.modules.Snap.FlowFilter import FlowFilter
from slpy.modules.Snap.TransformMapper import TransformMapper
from slpy.modules.Snap.DatabricksInsert import DatabricksInsert
from slpy.modules.Snap.TransformGroupByN import TransformGroupByN
from slpy.modules.Snap.FlowPipelineExecute import FlowPipelineExecute
from slpy.modules.Snap.DatabricksMultiExecute import DatabricksMultiExecute

p = Pipeline(label='CPQ_19T_Test_Apply')
p.param_table = ParamTable(param=[
    ParamTable.ParamItem(capture=True, key='step', value='seed', data_type='string')
])
p.expression_libraries = ExpressionLibraries(expression_library=[
    ExpressionLibraries.ExpressionLibraryItem(path='cpq_config.expr', as_='cfg')
])
snap_0 = TransformJSONGenerator(
    label='Synthetic snapshot rows',
    editable_content='[{"k": "A", "v": 1, "steps": ["seed", "change", "mass_delete", "incomplete", "resurrect"]}, {"k": "B", "v": 1, "steps": ["seed"]}, {"k": "B", "v": 2, "steps": ["change", "resurrect"]}, {"k": "C", "v": 1, "steps": ["seed", "resurrect"]}, {"k": "D", "v": 1, "steps": ["change", "resurrect"]}]',
    array_elements_as_documents=True
)
snap_1 = FlowFilter(
    label='Rows present at this step',
    filter_expression=Expr("$steps.indexOf(_step) != -1")
)
snap_2 = TransformMapper(
    label='Shape staging row',
    pass_through=False,
    null_safe_access=False,
    transformations=TransformMapper.Transformations(
        mapping_root='$',
        mapping_table=[
            TransformMapper.Transformations.MappingTableItem(expression=Expr('pipe.ruuid'), target_path='$run_id'),
            TransformMapper.Transformations.MappingTableItem(expression=Expr("'TEST_SNAPSHOT'"), target_path='$object_name'),
            TransformMapper.Transformations.MappingTableItem(expression=Expr('$k'), target_path='$cpq_id'),
            TransformMapper.Transformations.MappingTableItem(expression=Expr("JSON.stringify($.filter((v, k) => k != 'steps'))"), target_path='$payload')
        ]
    )
)
snap_3 = DatabricksInsert(
    label='Insert into cpq_ops.stg_snapshot',
    database_name='cpq_ops',
    table_name='stg_snapshot',
    executable_during_suggest=False,
    pm_account=Account(ref_class_id='com-snaplogic-snap-api-sql-accounts-databricks-databricksdatabaseaccount', label='Databricks_EMEA-SE_sql-warehouse-SERVERLESS')
)
snap_4 = TransformGroupByN(
    label='Wait for staging',
    group_size=1000000,
    target_field='results'
)
snap_5 = FlowPipelineExecute(
    label='Apply snapshot (CPQ_19)',
    pipeline='CPQ_19_Apply_Snapshot',
    snaplex_governor='LOCAL_NODE',
    params=[
        FlowPipelineExecute.ParamsItem(param_name='runId', param_value=Expr('pipe.ruuid')),
        FlowPipelineExecute.ParamsItem(param_name='objectName', param_value=Expr("'TEST_SNAPSHOT'")),
        FlowPipelineExecute.ParamsItem(param_name='keyColumns', param_value=Expr("'k'")),
        FlowPipelineExecute.ParamsItem(param_name='expectedTotal', param_value=Expr("'' + (_step == 'seed' ? 3 : (_step == 'change' ? 3 : (_step == 'mass_delete' ? 1 : (_step == 'incomplete' ? 2 : 4))))")),
        FlowPipelineExecute.ParamsItem(param_name='deleteGuardPct', param_value=Expr("'50'")),
        FlowPipelineExecute.ParamsItem(param_name='deleteGuardMax', param_value=Expr("'500'")),
        FlowPipelineExecute.ParamsItem(param_name='forceDeletes', param_value=Expr("'false'"))
    ],
    reuse=False,
    batch_size=1,
    pool_size=1,
    executable_during_suggest=False
)
snap_6 = TransformMapper(
    label='Actual vs expected',
    pass_through=False,
    null_safe_access=False,
    transformations=TransformMapper.Transformations(
        mapping_root='$',
        mapping_table=[
            TransformMapper.Transformations.MappingTableItem(expression=Expr('_step'), target_path='$step'),
            TransformMapper.Transformations.MappingTableItem(expression=Expr("$status + '/' + $inserted_rows + '/' + $updated_rows + '/' + $deleted_rows"), target_path='$actual'),
            TransformMapper.Transformations.MappingTableItem(expression=Expr("_step == 'seed' ? 'OK/3/0/0' : (_step == 'change' ? 'OK/1/1/1' : (_step == 'mass_delete' ? 'OK_DELETES_BLOCKED_GUARD/0/0/0' : (_step == 'incomplete' ? 'OK_DELETES_DEFERRED_INCOMPLETE/0/0/0' : 'OK/1/0/0')))"), target_path='$expected'),
            TransformMapper.Transformations.MappingTableItem(expression=Expr('$message'), target_path='$message')
        ]
    )
)
snap_7 = TransformMapper(
    label='Verdict',
    pass_through=True,
    null_safe_access=False,
    transformations=TransformMapper.Transformations(
        mapping_root='$',
        mapping_table=[
            TransformMapper.Transformations.MappingTableItem(expression=Expr('$actual == $expected'), target_path='$passed')
        ]
    )
)
snap_8 = TransformJSONGenerator(
    label='Cleanup trigger',
    editable_content='[{"start": true}]',
    array_elements_as_documents=True
)
snap_9 = FlowFilter(
    label='Only when step=cleanup',
    filter_expression=Expr("_step == 'cleanup'")
)
snap_10 = DatabricksMultiExecute(
    label='Delete TEST_SNAPSHOT rows',
    executable_during_suggest=False,
    sql_statements=[
        DatabricksMultiExecute.SqlStatementsItem(sql_statement=Expr('"DELETE FROM " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_silver.snapshot_rows WHERE object_name = " + ("\'TEST_SNAPSHOT\'")')),
        DatabricksMultiExecute.SqlStatementsItem(sql_statement=Expr('"DELETE FROM " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_raw.snapshot_changes WHERE object_name = " + ("\'TEST_SNAPSHOT\'")')),
        DatabricksMultiExecute.SqlStatementsItem(sql_statement=Expr('"DELETE FROM " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_ops.sync_audit WHERE object_name = " + ("\'TEST_SNAPSHOT\'")')),
        DatabricksMultiExecute.SqlStatementsItem(sql_statement=Expr('"DELETE FROM " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_ops.sync_control WHERE object_name = " + ("\'TEST_SNAPSHOT\'")')),
        DatabricksMultiExecute.SqlStatementsItem(sql_statement=Expr('"DELETE FROM " + (lib.cfg.prefix(lib.cfg.catalog)) + "cpq_ops.stg_snapshot WHERE object_name = " + ("\'TEST_SNAPSHOT\'")'))
    ],
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
p.connect(src=snap_8, dst=snap_9, src_view_id='output0', dst_view_id='input0', src_output_type='document', dst_input_type='document')
p.connect(src=snap_9, dst=snap_10, src_view_id='output0', dst_view_id='input0', src_output_type='document', dst_input_type='document')
