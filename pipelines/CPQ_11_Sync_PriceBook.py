"""
Pipeline Name: CPQ_11_Sync_PriceBook

Pipeline Summary: Full-snapshot sync of Oracle CPQ Price Books (standard Pricing object,
GET /rest/{v}/priceBooks) to Databricks with insert, update and soft-delete detection, keyed on
variableName. The endpoint takes no query parameters, so the expected count is the item count when
hasMore=false (otherwise -1, which defers deletes). Typed view: cpq_silver.v_price_book.
Note: priceBooks returns price book headers only; item prices (price models) are a separate object.

(#snap_0) TransformJSONGenerator: start
(#snap_1) TransformMapper: run context
(#snap_2) FlowPipelineExecute: CPQ_18_Extract_To_Stage (paged=false)
(#snap_3) APISuiteHTTPClient: GET priceBooks again for the expected count
(#snap_4) TransformMapper: expectedTotal
(#snap_5) FlowPipelineExecute: CPQ_19_Apply_Snapshot; its audit row is this pipeline's output

Configuration Parameters:
- keyColumns: default 'variableName'
- deleteGuardPct, deleteGuardMax, forceDeletes: passed to CPQ_19
"""

from slpy.modules.Pipeline.Pipeline import Pipeline
from slpy.modules.Pipeline.utils import Expr
from slpy.modules.Pipeline.param_table.ParamTable import ParamTable
from slpy.modules.Pipeline.expression_libraries.ExpressionLibraries import ExpressionLibraries
from slpy.modules.Snap import Account
from slpy.modules.Pipeline.utils.open_view import OPEN_VIEW
from slpy.modules.Snap.TransformJSONGenerator import TransformJSONGenerator
from slpy.modules.Snap.TransformMapper import TransformMapper
from slpy.modules.Snap.FlowPipelineExecute import FlowPipelineExecute
from slpy.modules.Snap.APISuiteHTTPClient import APISuiteHTTPClient

p = Pipeline(label='CPQ_11_Sync_PriceBook')
p.param_table = ParamTable(param=[
    ParamTable.ParamItem(capture=True, key='keyColumns', value='variableName', data_type='string'),
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
snap_1 = TransformMapper(
    label='Run context',
    pass_through=False,
    null_safe_access=False,
    transformations=TransformMapper.Transformations(
        mapping_root='$',
        mapping_table=[
            TransformMapper.Transformations.MappingTableItem(expression=Expr('pipe.ruuid'), target_path='$runId'),
            TransformMapper.Transformations.MappingTableItem(expression=Expr("'PriceBook'"), target_path='$objectName'),
            TransformMapper.Transformations.MappingTableItem(expression=Expr("'priceBooks'"), target_path='$resourcePath')
        ]
    )
)
snap_2 = FlowPipelineExecute(
    label='Extract to staging (CPQ_18)',
    pipeline='CPQ_18_Extract_To_Stage',
    snaplex_governor='LOCAL_NODE',
    params=[
        FlowPipelineExecute.ParamsItem(param_name='runId', param_value=Expr('pipe.ruuid')),
        FlowPipelineExecute.ParamsItem(param_name='objectName', param_value=Expr("'PriceBook'")),
        FlowPipelineExecute.ParamsItem(param_name='resourcePath', param_value=Expr("'priceBooks'")),
        FlowPipelineExecute.ParamsItem(param_name='paged', param_value=Expr("'false'"))
    ],
    reuse=False,
    batch_size=1,
    pool_size=1,
    executable_during_suggest=False
)
snap_3 = APISuiteHTTPClient(
    label='Ask CPQ for the expected record count',
    http_method='GET',
    uri=Expr("lib.cfg.restUrl(lib.cfg.cpqBaseUrl, lib.cfg.restVersion, 'priceBooks')"),
    header=[
        APISuiteHTTPClient.HeaderItem(header_name_prop='Accept', header_val_prop='application/json')
    ],
    client_properties=APISuiteHTTPClient.ClientProperties(
        output_props=APISuiteHTTPClient.ClientProperties.OutputProps(
            response_type='JSON',
            error_resp_to_errorview=True
        ),
        retry=APISuiteHTTPClient.ClientProperties.Retry(
            max_retry=3,
            retry_interval=10,
            auto_retry429=True,
            use_retry_after_header=True,
            retry_condition='Connection Errors and Server Errors (5xx)'
        )
    )
)
snap_4 = TransformMapper(
    label='Expected total',
    pass_through=False,
    null_safe_access=True,
    transformations=TransformMapper.Transformations(
        mapping_root='$',
        mapping_table=[
            TransformMapper.Transformations.MappingTableItem(expression=Expr('$entity.hasMore == false && $entity.items != null ? $entity.items.length : -1'), target_path='$expectedTotal')
        ]
    )
)
snap_5 = FlowPipelineExecute(
    label='Apply snapshot (CPQ_19)',
    pipeline='CPQ_19_Apply_Snapshot',
    snaplex_governor='LOCAL_NODE',
    params=[
        FlowPipelineExecute.ParamsItem(param_name='runId', param_value=Expr('pipe.ruuid')),
        FlowPipelineExecute.ParamsItem(param_name='objectName', param_value=Expr("'PriceBook'")),
        FlowPipelineExecute.ParamsItem(param_name='keyColumns', param_value=Expr('_keyColumns')),
        FlowPipelineExecute.ParamsItem(param_name='expectedTotal', param_value=Expr("'' + $expectedTotal")),
        FlowPipelineExecute.ParamsItem(param_name='deleteGuardPct', param_value=Expr('_deleteGuardPct')),
        FlowPipelineExecute.ParamsItem(param_name='deleteGuardMax', param_value=Expr('_deleteGuardMax')),
        FlowPipelineExecute.ParamsItem(param_name='forceDeletes', param_value=Expr('_forceDeletes'))
    ],
    reuse=False,
    batch_size=1,
    pool_size=1,
    executable_during_suggest=False
)
p.connect(src=snap_0, dst=snap_1, src_view_id='output0', dst_view_id='input0', src_output_type='document', dst_input_type='document')
p.connect(src=snap_1, dst=snap_2, src_view_id='output0', dst_view_id='input0', src_output_type='document', dst_input_type='document')
p.connect(src=snap_2, dst=snap_3, src_view_id='output0', dst_view_id='input0', src_output_type='document', dst_input_type='document')
p.connect(src=snap_3, dst=snap_4, src_view_id='output0', dst_view_id='input0', src_output_type='document', dst_input_type='document')
p.connect(src=snap_4, dst=snap_5, src_view_id='output0', dst_view_id='input0', src_output_type='document', dst_input_type='document')
p.connect(src=snap_5, dst=OPEN_VIEW.OUTPUT, src_view_id='output0', dst_view_id='OPENVIEW.OUTPUT', src_output_type='document', dst_input_type='document')
