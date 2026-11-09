"""
Pipeline Name: CPQ_18_Extract_To_Stage

Pipeline Summary: Child pipeline. Reads every record of one Oracle CPQ REST collection (all pages, ordered by
id) and lands it in cpq_ops.stg_snapshot tagged with the parent's runId. Each record is stored as JSON
(payload) minus its 'links' element. Called by CPQ_10_Sync_DataTable and CPQ_11_Sync_PriceBook.

(#snap_0) APISuiteHTTPClient: GET {cpqBaseUrl}/rest/{restVersion}/{resourcePath}; when paged=true adds
          limit/offset/orderby=id:asc and follows hasMore. CPQ account must be attached (Basic or OAuth2).
(#snap_1) TransformJSONSplitter: splits $entity.items
(#snap_2) TransformMapper: run_id, object_name, cpq_id, payload
(#snap_3) DatabricksInsert: cpq_ops.stg_snapshot

Configuration Parameters:
- runId, objectName: set by the parent
- resourcePath: e.g. customCustomPrice (deployed data table) or priceBooks
- paged: 'true' for endpoints supporting limit/offset (data tables); 'false' for priceBooks
"""

from slpy.modules.Pipeline.Pipeline import Pipeline
from slpy.modules.Pipeline.utils import Expr
from slpy.modules.Pipeline.param_table.ParamTable import ParamTable
from slpy.modules.Pipeline.expression_libraries.ExpressionLibraries import ExpressionLibraries
from slpy.modules.Snap import Account
from slpy.modules.Pipeline.utils.open_view import OPEN_VIEW
from slpy.modules.Snap.APISuiteHTTPClient import APISuiteHTTPClient
from slpy.modules.Snap.TransformJSONSplitter import TransformJSONSplitter
from slpy.modules.Snap.TransformMapper import TransformMapper
from slpy.modules.Snap.DatabricksInsert import DatabricksInsert

p = Pipeline(label='CPQ_18_Extract_To_Stage')
p.param_table = ParamTable(param=[
    ParamTable.ParamItem(capture=True, key='runId', value='', data_type='string'),
    ParamTable.ParamItem(capture=True, key='objectName', value='', data_type='string'),
    ParamTable.ParamItem(capture=True, key='resourcePath', value='', data_type='string'),
    ParamTable.ParamItem(capture=True, key='paged', value='true', data_type='string')
])
p.expression_libraries = ExpressionLibraries(expression_library=[
    ExpressionLibraries.ExpressionLibraryItem(path='cpq_config.expr', as_='cfg')
])
snap_0 = APISuiteHTTPClient(
    label='GET CPQ collection (all pages)',
    http_method='GET',
    uri=Expr("lib.cfg.restUrl(lib.cfg.cpqBaseUrl, lib.cfg.restVersion, _resourcePath) + (_paged == 'true' ? lib.cfg.pageQuery(lib.cfg.pageSize, 0) : '')"),
    header=[
        APISuiteHTTPClient.HeaderItem(header_name_prop='Accept', header_val_prop='application/json')
    ],
    pagination=APISuiteHTTPClient.Pagination(
        has_next=Expr("_paged == 'true' && $entity.hasMore == true"),
        override_uri=True,
        next_url=Expr('lib.cfg.restUrl(lib.cfg.cpqBaseUrl, lib.cfg.restVersion, _resourcePath) + lib.cfg.pageQuery(lib.cfg.pageSize, parseInt($entity.offset) + parseInt($entity.count))'),
        pagination_interval=0
    ),
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
snap_1 = TransformJSONSplitter(
    label='One document per record',
    path='$entity.items',
    null_safe_access=True
)
snap_2 = TransformMapper(
    label='Shape staging row',
    pass_through=False,
    null_safe_access=True,
    transformations=TransformMapper.Transformations(
        mapping_root='$',
        mapping_table=[
            TransformMapper.Transformations.MappingTableItem(expression=Expr('_runId'), target_path='$run_id'),
            TransformMapper.Transformations.MappingTableItem(expression=Expr('_objectName'), target_path='$object_name'),
            TransformMapper.Transformations.MappingTableItem(expression=Expr("'' + $id"), target_path='$cpq_id'),
            TransformMapper.Transformations.MappingTableItem(expression=Expr("JSON.stringify($.filter((v, k) => k != 'links'))"), target_path='$payload')
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
p.connect(src=snap_0, dst=snap_1, src_view_id='output0', dst_view_id='input0', src_output_type='document', dst_input_type='document')
p.connect(src=snap_1, dst=snap_2, src_view_id='output0', dst_view_id='input0', src_output_type='document', dst_input_type='document')
p.connect(src=snap_2, dst=snap_3, src_view_id='output0', dst_view_id='input0', src_output_type='document', dst_input_type='document')
