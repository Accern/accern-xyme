from typing import Any, Optional, List, Dict, Tuple, Union
from typing_extensions import TypedDict, Literal


DagDef = TypedDict('DagDef', {
    "company": str,
    "default_input_key": Optional[str],
    "default_output_key": Optional[str],
    "high_priority": bool,
    "name": str,
    "nodes": List['NodeDef'],
    "queue_mng": Optional[str],
    "state": str,
})
NodeDef = TypedDict('NodeDef', {
    "blobs": Dict[str, str],
    "id": str,
    "inputs": Dict[str, Tuple[str, str]],
    "kind": str,
    "name": str,
    "params": Dict[str, Any],
}, total=False)
S3BucketSettings = TypedDict('S3BucketSettings', {
    "api_version": Optional[str],
    "aws_access_key_id": str,
    "aws_secret_access_key": str,
    "aws_session_token": Optional[str],
    "buckets": Dict[str, str],
    "endpoint_url": Optional[str],
    "region_name": Optional[str],
    "use_ssl": Optional[bool],
    "verify": Optional[bool],
})
ESConnectorSettings = TypedDict('ESConnectorSettings', {
    "host": str,
    "password": str,
})
SettingsObj = TypedDict('SettingsObj', {
    "s3": Dict[str, S3BucketSettings],
    "triton": Dict[str, S3BucketSettings],
    "es": Dict[str, ESConnectorSettings],
}, total=False)
TaskType = Literal[
    "node:cpubig",
    "node:cpusmall",
]
QueueType = Literal[
    "parallel:kafka",
    "parallel:queue",
]
QueueStatus = Literal[
    "waiting",
    "running",
    "result",
    "error",
    "void",
]
TaskStatus = Literal[
    "blocked",
    "waiting",
    "running",
    "complete",
    "eos",
    "paused",
    "error",
    "unknown",
    "virtual",
    "queue",
]
ParamType = Literal[
    "bool",
    "choice_list",
    "choice_set",
    "choice",
    "col",
    "date",
    "int",
    "list_col",
    "list_str",
    "list_tup_str",
    "mapping_col_num",
    "mapping_col_type",
    "mapping_int_int",
    "mapping_str_str",
    "mapping_str_tup_str_int",
    "mapping_tup_int_float",
    "mapping_tup_int_tup_int_int_int_int",
    "mapping_tup_str_str",
    "num",
    "opt_col",
    "opt_coltype",
    "opt_date",
    "opt_int",
    "opt_scol",
    "opt_str",
    "pair_num",
    "scol",
    "str",
    "col_or_mapping_str_col",
]
InstanceStatus = Literal[
    "busy_queue",
    "busy_task",
    "data_queue",
    "off",
    "ready_queue",
    "ready_task",
]
ParamDef = TypedDict('ParamDef', {
    "name": str,
    "help": str,
    "type": ParamType,
    "required": bool,
    "default": Optional[Union[str, float, int, List[str]]],
})
ParamDefs = Dict[str, ParamDef]
ModelParamDefs = Dict[str, ParamDefs]
Backends = TypedDict('Backends', {
    "logger": List[str],
    "status_emitter": List[str],
    "progress": str,
    "queue_mng": Dict[str, str],
    "task_mng": Dict[str, str],
    "executor_mng": str,
    "mtype": str,
    "ns": str,
})
VersionResponse = TypedDict('VersionResponse', {
    "api_version": int,
    "backends": Optional[Backends],
    "caller_api_version": int,
    "time": str,
    "xyme_version_full": str,
    "xyme_version": str,
})
ReadNode = TypedDict('ReadNode', {
    "redis_key": str,
    "result_uri": Optional[str],
})
NodeDefInfo = TypedDict('NodeDefInfo', {
    "name": str,
    "desc": str,
    "input_keys": List[str],
    "output_keys": List[str],
    "task_types": Optional[List[TaskType]],
    "queue_types": Optional[List[QueueType]],
    "blob_types": Dict[str, Union[str, List[str]]],
    "params": ParamDefs,
})
NodeStatus = TypedDict('NodeStatus', {
    "status": TaskStatus,
})
NodeChunk = TypedDict('NodeChunk', {
    "chunk": int,
})
NodeState = TypedDict('NodeState', {
    "status": TaskStatus,
    "chunk": int,
})
NodeTypes = TypedDict('NodeTypes', {
    "types": List[str],
    "info": Dict[str, NodeDefInfo],
})
NodeInfo = TypedDict('NodeInfo', {
    "id": str,
    "name": str,
    "type": str,
    "blobs": Dict[str, str],
    "inputs": Dict[str, Tuple[str, str]],
    "state": Optional[int],
    "config_error": Optional[str],
})
DagList = TypedDict('DagList', {
    "cur_time": float,
    "dags": List[Tuple[str, Optional[float], Optional[float]]],
})
KnownBlobs = TypedDict('KnownBlobs', {
    "cur_time": float,
    "blobs": List[Tuple[str, Optional[float]]],
})
DagInfo = TypedDict('DagInfo', {
    "company": str,
    "high_priority": bool,
    "ins": List[str],
    "name": str,
    "nodes": List[NodeInfo],
    "outs": List[Tuple[str, str]],
    "queue_mng": Optional[str],
    "state": str,
})
BlobInit = TypedDict('BlobInit', {
    "blob": str,
})
DagInit = TypedDict('DagInit', {
    "dag": str,
})
DagCreate = TypedDict('DagCreate', {
    "dag": str,
    "nodes": List[str],
    "warnings": List[str],
})
NamespaceUpdateSettings = TypedDict('NamespaceUpdateSettings', {
    "namespace": str,
    "settings": SettingsObj,
})
DagReload = TypedDict('DagReload', {
    "dag": str,
    "when": float,
})
Timing = TypedDict('Timing', {
    "name": str,
    "total": float,
    "quantity": int,
    "avg": float,
})
Timings = TypedDict('Timings', {
    "times": List[Timing],
})
NodeTiming = TypedDict('NodeTiming', {
    "node_name": str,
    "node_total": float,
    "node_avg": float,
    "fns": List[Timing],
})
TimingResult = TypedDict('TimingResult', {
    "dag_total": float,
    "nodes": List[Tuple[str, NodeTiming]],
})
InCursors = TypedDict('InCursors', {
    "cursors": Dict[str, int],
})
CSVBlobResponse = TypedDict('CSVBlobResponse', {
    "count": int,
    "csv": str,
    "pos": int,
    "tmp": bool,
})
CSVOp = TypedDict('CSVOp', {
    "count": int,
    "pos": int,
    "tmp": bool,
})
NamespaceList = TypedDict('NamespaceList', {
    "namespaces": List[str],
})
NodeCustomCode = TypedDict('NodeCustomCode', {
    "code": str,
})
NodeCustomImports = TypedDict('NodeCustomImports', {
    "modules": List[List[str]],
})
AllowedCustomImports = TypedDict('AllowedCustomImports', {
    "modules": List[str],
})
JSONBlobResponse = TypedDict('JSONBlobResponse', {
    "count": int,
    "json": str,
})
JSONBlobAppendResponse = TypedDict('JSONBlobAppendResponse', {
    "count": int,
})
NodeUserColumnsResponse = TypedDict('NodeUserColumnsResponse', {
    "user_columns": List[str],
})
ModelParamsResponse = TypedDict('ModelParamsResponse', {
    "model_params": ModelParamDefs,
})
ModelSetupResponse = TypedDict('ModelSetupResponse', {
    "model_info": Dict[str, str],
})
BlobFilesResponse = TypedDict('BlobFilesResponse', {
    "files": List[str],
})
UploadFilesResponse = TypedDict('UploadFilesResponse', {
    "uri": Optional[str],
    "pos": int,
    "files": List[str],
})
DagDupResponse = TypedDict('DagDupResponse', {
    "dag": str,
})
FlushAllQueuesResponse = TypedDict('FlushAllQueuesResponse', {
    "success": bool,
})
WorkerScale = TypedDict('WorkerScale', {
    "success": bool,
})
SetNamedSecret = TypedDict('SetNamedSecret', {
    "replaced": bool,
})
KafkaTopics = TypedDict('KafkaTopics', {
    "topics": Dict[str, Optional[str]],
    "create": bool,
})
KafkaMessage = TypedDict('KafkaMessage', {
    "messages": Dict[str, str],
})
KafkaOffsets = TypedDict('KafkaOffsets', {
    "error": int,
    "input": int,
    "output": int,
})
KafkaGroup = TypedDict('KafkaGroup', {
    "group": str,
    "dag": str,
    "reset": Optional[str],
})
ThroughputDict = TypedDict('ThroughputDict', {
    "throughput": float,
    "max": float,
    "min": float,
    "stddev": float,
    "segments": int,
    "count": int,
    "total": float,
})
KafkaThroughput = TypedDict('KafkaThroughput', {
    "dag": str,
    "input": ThroughputDict,
    "output": ThroughputDict,
    "faster": Literal["input", "output", "both"],
    "errors": int,
})
PutNodeBlob = TypedDict('PutNodeBlob', {
    "key": str,
    "new_uri": str,
})
BlobOwner = TypedDict('BlobOwner', {
    "owner_dag": Optional[str],
    "owner_node": str,
})
CopyBlob = TypedDict('CopyBlob', {
    "new_uri": str,
})
QueueStatsResponse = TypedDict('QueueStatsResponse', {
    "active": int,
    "error": int,
    "extras": Dict[str, int],
    "need": bool,
    "queue_count": int,
    "restarted": int,
    "results": int,
    "start_blocker": bool,
    "threshold": int,
    "total": int,
    "workers": int,
})
MinimalQueueStatsResponse = TypedDict('MinimalQueueStatsResponse', {
    "active": int,
    "total": int,
})
DynamicStatusResponse = TypedDict('DynamicStatusResponse', {
    "status": Dict[str, QueueStatus],
})
DynamicResults = TypedDict('DynamicResults', {
    "results": List[Any],
})
TritonModelsResponse = TypedDict('TritonModelsResponse', {
    "models": List[str],
})
CacheStats = TypedDict('CacheStats', {
    "total": int,
})
QueueMode = TypedDict('QueueMode', {
    "mode": str,
})
ModelReleaseResponse = TypedDict('ModelReleaseResponse', {
    "release": Optional[int],
})
ESQueryResponse = TypedDict('ESQueryResponse', {
    "query": Dict[str, Any],
})
