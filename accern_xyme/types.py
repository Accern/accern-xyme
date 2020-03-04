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
