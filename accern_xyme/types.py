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
]
ParamType = Literal[
    "str",
    "int",
    "num",
    "mapping_str_str",
    "list_str",
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
    "publisher": str,
})
NodeDefInfo = TypedDict('NodeDefInfo', {
    "name": str,
    "desc": str,
    "input_keys": List[str],
    "output_keys": List[str],
    "task_type": Optional[str],
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
    "state_key": Optional[str],
    "type": str,
    "blobs": Dict[str, str],
    "inputs": Dict[str, Tuple[str, str]],
    "state": Optional[int],
    "config_error": bool,
})
PipelineList = TypedDict('PipelineList', {
    "pipelines": List[str],
})
PipelineInfo = TypedDict('PipelineInfo', {
    "name": str,
    "state_publisher": Optional[str],
    "notify_publisher": Optional[str],
    "company": str,
    "nodes": List[NodeInfo],
    "state": str,
    "high_priority": bool,
    "settings": Dict[str, Any],
})
PipelineInit = TypedDict('PipelineInit', {
    "pipeline": str,
})
PipelineCreate = TypedDict('PipelineCreate', {
    "pipeline": str,
    "nodes": List[str],
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
JobInfoEntries = TypedDict('JobInfoEntries', {  # pylint: disable=invalid-name
    "data": Optional[Tuple[str, str]],
    "performance": Optional[Tuple[str, str]],
    "predictions": Optional[Tuple[str, str]],
    "sources": List[str],
})
JobInfo = TypedDict('JobInfo', {  # pylint: disable=invalid-name
    "id": str,
    "name": str,
    "owner": str,
    "company": str,
    "high_priority": bool,
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
