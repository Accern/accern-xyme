# pylint: disable=invalid-name
from typing import Any, Optional, List, Dict, Tuple
from typing_extensions import TypedDict, Literal


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
    "parallel",
]
ParamType = Literal[
    "str",
    "int",
    "num",
    "mapping_str_str",
    "list_str",
]
InstanceStatus = Literal[
    "busy_queue",
    "busy_task",
    "ready_queue",
    "ready_task",
]
ParamDef = TypedDict('ParamDef', {
    "name": str,
    "help": str,
    "type": ParamType,
    "required": bool,
    "default": Optional[Any],
})
ParamDefs = Dict[str, ParamDef]
MaintenanceResponse = TypedDict('MaintenanceResponse', {
    "is_maintenance": bool,
})
VersionResponse = TypedDict('VersionResponse', {
    "xyme_version": str,
    "time": str,
    "caller_api_version": int,
    "api_version": int,
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
    "task_types": Optional[List[str]],
    "blob_types": Dict[str, str],
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
PipelineList = TypedDict('PipelineList', {
    "pipelines": List[str],
})
PipelineInfo = TypedDict('PipelineInfo', {
    "company": str,
    "high_priority": bool,
    "ins": List[str],
    "is_parallel": bool,
    "name": str,
    "nodes": List[NodeInfo],
    "outs": List[Tuple[str, str]],
    "settings": Dict[str, Any],
    "state": str,
})
BlobInit = TypedDict('BlobInit', {
    "blob": str,
})
PipelineInit = TypedDict('PipelineInit', {
    "pipeline": str,
})
PipelineDupResponse = TypedDict('PipelineDupResponse', {
    "pipeline": str,
})
PipelineCreate = TypedDict('PipelineCreate', {
    "pipeline": str,
    "nodes": List[str],
    "warnings": List[str],
})
NodeDef = TypedDict('NodeDef', {
    "id": str,
    "kind": str,
    "inputs": Dict[str, Tuple[str, str]],
    "blobs": Dict[str, str],
    "params": Dict[str, Any],
}, total=False)
PipelineDef = TypedDict('PipelineDef', {
    "name": str,
    "company": str,
    "nodes": List[NodeDef],
    "state": str,
    "high_priority": bool,
    "is_parallel": bool,
}, total=False)
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
    "pipe_total": float,
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
CSVList = TypedDict('CSVList', {
    "csvs": List[str],
})
JobList = TypedDict('JobList', {
    "jobs": List[str],
})
JobInfoEntries = TypedDict('JobInfoEntries', {
    "data": Optional[Tuple[str, str]],
    "performance": Optional[Tuple[str, str]],
    "predictions": Optional[Tuple[str, str]],
    "sources": List[str],
})
JobInfo = TypedDict('JobInfo', {
    "id": str,
    "name": str,
    "owner": str,
    "company": str,
    "high_priority": bool,
    "is_parallel": bool,
    "entry_points": JobInfoEntries,
})
CustomCodeResponse = TypedDict('CustomCodeResponse', {
    "code": str,
})
CustomImportsResponse = TypedDict('CustomImportsResponse', {
    "modules": List[str],
})
JSONBlobResponse = TypedDict('JSONBlobResponse', {
    "count": int,
    "json": str,
})
UserColumnsResponse = TypedDict('UserColumnsResponse', {
    "user_columns": List[str],
})
ModelSetupResponse = TypedDict('ModelSetupResponse', {
    "model_info": Dict[str, Any],
})
ModelParamsResponse = TypedDict('ModelParamsResponse', {
    "model_params": Dict[str, Any],
})
FlushAllQueuesResponse = TypedDict('FlushAllQueuesResponse', {
    "success": bool,
})
PutNodeBlob = TypedDict('PutNodeBlob', {
    "key": str,
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
QueueStatus = Literal[
    "waiting",
    "running",
    "result",
    "error",
    "void",
]
DynamicStatusResponse = TypedDict('DynamicStatusResponse', {
    "status": Dict[str, QueueStatus],
})
DynamicResults = TypedDict('DynamicResults', {
    "results": List[Any],
})
