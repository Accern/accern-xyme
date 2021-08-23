import io
import pandas as pd
from .types import AllowedCustomImports as AllowedCustomImports, BlobFilesResponse as BlobFilesResponse, BlobInit as BlobInit, BlobOwner as BlobOwner, BlobTypeResponse as BlobTypeResponse, BlobURIResponse as BlobURIResponse, CacheStats as CacheStats, CopyBlob as CopyBlob, DagCreate as DagCreate, DagDef as DagDef, DagDupResponse as DagDupResponse, DagInfo as DagInfo, DagInit as DagInit, DagList as DagList, DagPrettyNode as DagPrettyNode, DagReload as DagReload, DagStatus as DagStatus, DeleteBlobResponse as DeleteBlobResponse, DynamicResults as DynamicResults, DynamicStatusResponse as DynamicStatusResponse, ESQueryResponse as ESQueryResponse, FlushAllQueuesResponse as FlushAllQueuesResponse, InCursors as InCursors, InstanceStatus as InstanceStatus, JSONBlobAppendResponse as JSONBlobAppendResponse, KafkaGroup as KafkaGroup, KafkaMessage as KafkaMessage, KafkaOffsets as KafkaOffsets, KafkaThroughput as KafkaThroughput, KafkaTopicNames as KafkaTopicNames, KafkaTopics as KafkaTopics, KnownBlobs as KnownBlobs, MinimalQueueStatsResponse as MinimalQueueStatsResponse, ModelInfo as ModelInfo, ModelParamsResponse as ModelParamsResponse, ModelReleaseResponse as ModelReleaseResponse, ModelVersionResponse as ModelVersionResponse, NamespaceList as NamespaceList, NamespaceUpdateSettings as NamespaceUpdateSettings, NodeChunk as NodeChunk, NodeCustomCode as NodeCustomCode, NodeCustomImports as NodeCustomImports, NodeDef as NodeDef, NodeDefInfo as NodeDefInfo, NodeInfo as NodeInfo, NodeState as NodeState, NodeStatus as NodeStatus, NodeTiming as NodeTiming, NodeTypeResponse as NodeTypeResponse, NodeTypes as NodeTypes, NodeUserColumnsResponse as NodeUserColumnsResponse, PrettyResponse as PrettyResponse, PutNodeBlob as PutNodeBlob, QueueMode as QueueMode, QueueStatsResponse as QueueStatsResponse, QueueStatus as QueueStatus, ReadNode as ReadNode, S3Config as S3Config, SetNamedSecret as SetNamedSecret, SettingsObj as SettingsObj, TaskStatus as TaskStatus, Timing as Timing, TimingResult as TimingResult, Timings as Timings, TritonModelsResponse as TritonModelsResponse, UUIDResponse as UUIDResponse, UploadFilesResponse as UploadFilesResponse, VersionResponse as VersionResponse, WorkerScale as WorkerScale
from .util import ByteResponse as ByteResponse, ServerSideError as ServerSideError, async_compute as async_compute, content_to_csv_bytes as content_to_csv_bytes, df_to_csv_bytes as df_to_csv_bytes, get_age as get_age, get_file_hash as get_file_hash, get_file_upload_chunk_size as get_file_upload_chunk_size, get_max_retry as get_max_retry, get_progress_bar as get_progress_bar, get_retry_sleep as get_retry_sleep, has_graph_easy as has_graph_easy, interpret_ctype as interpret_ctype, is_jupyter as is_jupyter, maybe_json_loads as maybe_json_loads, merge_ctype as merge_ctype, safe_opt_num as safe_opt_num
from io import BytesIO
from requests import Response as Response
from typing import Any, Callable, Dict, IO, Iterable, Iterator, List, Optional, Set, Tuple, Union, overload
from typing_extensions import Literal as Literal

WVD: Any
API_VERSION: int
DEFAULT_NAMESPACE: str
METHOD_DELETE: str
METHOD_FILE: str
METHOD_GET: str
METHOD_LONGPOST: str
METHOD_POST: str
METHOD_PUT: str
PREFIX: str
INPUT_CSV_EXT: str
INPUT_TSV_EXT: str
INPUT_ZIP_EXT: str
INPUT_EXT: Any
FUNC = Callable[[Any], Any]
CUSTOM_NODE_TYPES: Any
NO_RETRY: Any

class AccessDenied(Exception): ...

class LegacyVersion(Exception):
    def __init__(self, api_version: int) -> None: ...
    def get_api_version(self) -> int: ...

class XYMEClient:
    def __init__(self, url: str, token: Optional[str], namespace: str): ...
    def get_api_version(self) -> int: ...
    def set_auto_refresh(self, is_auto_refresh: bool) -> None: ...
    def is_auto_refresh(self) -> bool: ...
    def refresh(self) -> None: ...
    def bulk_operation(self) -> Iterator[bool]: ...
    def request_bytes(self, method: str, path: str, args: Dict[str, Any], files: Optional[Dict[str, BytesIO]] = ..., add_prefix: bool = ..., add_namespace: bool = ..., api_version: Optional[int] = ...) -> Tuple[BytesIO, str]: ...
    def request_json(self, method: str, path: str, args: Dict[str, Any], add_prefix: bool = ..., add_namespace: bool = ..., files: Optional[Dict[str, IO[bytes]]] = ..., api_version: Optional[int] = ...) -> Dict[str, Any]: ...
    def get_server_version(self) -> VersionResponse: ...
    def get_namespaces(self) -> List[str]: ...
    def get_dags(self) -> List[str]: ...
    def get_dag_ages(self) -> List[Dict[str, Optional[str]]]: ...
    def get_dag_times(self, retrieve_times: bool = ...) -> Tuple[float, List[DagStatus]]: ...
    def get_dag(self, dag_uri: str) -> DagHandle: ...
    def get_blob_handle(self, uri: str, is_full: bool = ...) -> BlobHandle: ...
    def get_node_defs(self) -> Dict[str, NodeDefInfo]: ...
    def create_new_blob(self, blob_type: str) -> str: ...
    def get_blob_owner(self, blob_uri: str) -> BlobOwner: ...
    def set_blob_owner(self, blob_uri: str, dag_id: Optional[str] = ..., node_id: Optional[str] = ..., external_owner: bool = ...) -> BlobOwner: ...
    def create_new_dag(self, username: Optional[str] = ..., dagname: Optional[str] = ..., index: Optional[int] = ...) -> str: ...
    def get_blob_type(self, blob_uri: str) -> BlobTypeResponse: ...
    def get_csv_blob(self, blob_uri: str) -> CSVBlobHandle: ...
    def get_custom_code_blob(self, blob_uri: str) -> CustomCodeBlobHandle: ...
    def get_json_blob(self, blob_uri: str) -> JSONBlobHandle: ...
    def duplicate_dag(self, dag_uri: str, dest_uri: Optional[str] = ..., copy_nonowned_blobs: Optional[bool] = ..., retain_nonowned_blobs: bool = ..., warnings_io: Optional[IO[Any]] = ...) -> str: ...
    def set_dag(self, dag_uri: str, defs: DagDef, warnings_io: Optional[IO[Any]] = ...) -> DagHandle: ...
    def set_settings(self, config_token: str, settings: SettingsObj) -> SettingsObj: ...
    def get_settings(self) -> SettingsObj: ...
    def get_allowed_custom_imports(self) -> AllowedCustomImports: ...
    @overload
    def check_queue_stats(self, dag: Optional[str], minimal: Literal[True]) -> MinimalQueueStatsResponse: ...
    @overload
    def check_queue_stats(self, dag: Optional[str], minimal: Literal[False]) -> QueueStatsResponse: ...
    @overload
    def check_queue_stats(self, dag: Optional[str], minimal: bool) -> Union[MinimalQueueStatsResponse, QueueStatsResponse]: ...
    def get_instance_status(self, dag_uri: Optional[str] = ..., node_id: Optional[str] = ...) -> Dict[InstanceStatus, int]: ...
    def get_queue_mode(self) -> str: ...
    def set_queue_mode(self, mode: str) -> str: ...
    def flush_all_queue_data(self) -> None: ...
    def get_cache_stats(self) -> CacheStats: ...
    def reset_cache(self) -> CacheStats: ...
    def create_kafka_error_topic(self) -> KafkaTopics: ...
    def get_kafka_error_topic(self) -> str: ...
    def delete_kafka_error_topic(self) -> KafkaTopics: ...
    def read_kafka_errors(self, offset: str = ...) -> List[str]: ...
    def get_named_secrets(self, config_token: Optional[str] = ..., show_values: bool = ...) -> Dict[str, Optional[str]]: ...
    def set_named_secret(self, config_token: str, key: str, value: str) -> bool: ...
    def get_error_logs(self) -> str: ...
    def get_known_blobs(self, blob_type: Optional[str] = ..., connector: Optional[str] = ...) -> List[str]: ...
    def get_known_blob_ages(self, blob_type: Optional[str] = ..., connector: Optional[str] = ...) -> List[Tuple[str, str]]: ...
    def get_known_blob_times(self, retrieve_times: bool, blob_type: Optional[str] = ..., connector: Optional[str] = ...) -> Tuple[float, List[Tuple[str, Optional[float]]]]: ...
    def get_triton_models(self) -> List[str]: ...
    @staticmethod
    def read_dvc(path: str, repo: str, rev: Optional[str] = ..., warnings_io: Optional[IO[Any]] = ...) -> Any: ...
    @staticmethod
    def load_json(json_path: str) -> Dict[str, Any]: ...
    @classmethod
    def load_s3_config(cls, config_path: str) -> S3Config: ...
    @classmethod
    def download_s3_from_file(cls, dest_path: str, config_path: str) -> None: ...
    @staticmethod
    def download_s3(dest_path: str, config: S3Config) -> None: ...
    def get_uuid(self) -> str: ...
    def delete_blobs(self, blob_uris: List[str]) -> DeleteBlobResponse: ...

class DagHandle:
    def __init__(self, client: XYMEClient, dag_uri: str) -> None: ...
    def refresh(self) -> None: ...
    def get_info(self) -> DagInfo: ...
    def get_nodes(self) -> List[str]: ...
    def get_node(self, node_name: str) -> NodeHandle: ...
    def get_uri(self) -> str: ...
    def get_name(self) -> str: ...
    def get_company(self) -> str: ...
    def get_state_type(self) -> str: ...
    def get_timing(self, blacklist: Optional[List[str]] = ...) -> TimingResult: ...
    def is_high_priority(self) -> bool: ...
    def is_queue(self) -> bool: ...
    def get_queue_mng(self) -> Optional[str]: ...
    def get_ins(self) -> List[str]: ...
    def get_outs(self) -> List[Tuple[str, str]]: ...
    def bulk_operation(self) -> Iterator[bool]: ...
    def set_dag(self, defs: DagDef) -> None: ...
    def dynamic_model(self, inputs: List[Any], format_method: str = ..., no_cache: bool = ...) -> List[Any]: ...
    def dynamic_list(self, inputs: List[Any], input_key: Optional[str] = ..., output_key: Optional[str] = ..., split_th: Optional[int] = ..., max_threads: int = ..., format_method: str = ..., force_keys: bool = ..., no_cache: bool = ...) -> List[Any]: ...
    def dynamic(self, input_data: BytesIO) -> ByteResponse: ...
    def dynamic_obj(self, input_obj: Any) -> ByteResponse: ...
    def dynamic_async(self, input_data: List[BytesIO]) -> List[ComputationHandle]: ...
    def set_dynamic_error_message(self, msg: Optional[str]) -> None: ...
    def get_dynamic_error_message(self) -> Optional[str]: ...
    def dynamic_async_obj(self, input_data: List[Any]) -> List[ComputationHandle]: ...
    def get_dynamic_result(self, value_id: str) -> ByteResponse: ...
    def get_dynamic_status(self, value_ids: List[ComputationHandle]) -> Dict[ComputationHandle, QueueStatus]: ...
    def get_dynamic_bulk(self, input_data: List[BytesIO], max_buff: int = ..., block_size: int = ..., num_threads: int = ...) -> Iterable[ByteResponse]: ...
    def get_dynamic_bulk_obj(self, input_data: List[Any], max_buff: int = ..., block_size: int = ..., num_threads: int = ...) -> Iterable[ByteResponse]: ...
    def pretty(self, nodes_only: bool = ..., allow_unicode: bool = ..., method: Optional[str] = ..., fields: Optional[List[str]] = ..., output_format: Optional[str] = ..., display: Optional[IO[Any]] = ...) -> Optional[str]: ...
    def pretty_obj(self, nodes_only: bool = ..., allow_unicode: bool = ..., fields: Optional[List[str]] = ...) -> List[DagPrettyNode]: ...
    def get_def(self, full: bool = ...) -> DagDef: ...
    def set_attr(self, attr: str, value: Any) -> None: ...
    def set_name(self, value: str) -> None: ...
    def set_company(self, value: str) -> None: ...
    def set_state(self, value: str) -> None: ...
    def set_high_priority(self, value: bool) -> None: ...
    def set_queue_mng(self, value: Optional[str]) -> None: ...
    @overload
    def check_queue_stats(self, minimal: Literal[True]) -> MinimalQueueStatsResponse: ...
    @overload
    def check_queue_stats(self, minimal: Literal[False]) -> QueueStatsResponse: ...
    @overload
    def check_queue_stats(self, minimal: bool) -> Union[MinimalQueueStatsResponse, QueueStatsResponse]: ...
    def scale_worker(self, replicas: int) -> int: ...
    def reload(self, timestamp: Optional[float] = ...) -> float: ...
    def get_kafka_input_topic(self, postfix: str = ...) -> str: ...
    def get_kafka_output_topic(self) -> str: ...
    def set_kafka_topic_partitions(self, num_partitions: int, postfix: str = ..., large_input_retention: bool = ..., no_output: bool = ...) -> KafkaTopics: ...
    def post_kafka_objs(self, input_objs: List[Any]) -> List[str]: ...
    def post_kafka_msgs(self, input_data: List[BytesIO], postfix: str = ...) -> List[str]: ...
    def read_kafka_output(self, offset: str = ..., max_rows: int = ...) -> Optional[ByteResponse]: ...
    def get_kafka_offsets(self, alive: bool, postfix: Optional[str] = ...) -> KafkaOffsets: ...
    def get_kafka_throughput(self, postfix: Optional[str] = ..., segment_interval: float = ..., segments: int = ...) -> KafkaThroughput: ...
    def get_kafka_group(self) -> KafkaGroup: ...
    def set_kafka_group(self, group_id: Optional[str] = ..., reset: Optional[str] = ..., **kwargs: Any) -> KafkaGroup: ...
    def delete(self) -> DeleteBlobResponse: ...
    def __hash__(self) -> int: ...
    def __eq__(self, other: object) -> bool: ...
    def __ne__(self, other: object) -> bool: ...

class NodeHandle:
    def __init__(self, client: XYMEClient, dag: DagHandle, node_id: str, node_name: str, kind: str) -> None: ...
    def as_owner(self) -> BlobOwner: ...
    @staticmethod
    def from_node_info(client: XYMEClient, dag: DagHandle, node_info: NodeInfo, prev: Optional[NodeHandle]) -> NodeHandle: ...
    def update_info(self, node_info: NodeInfo) -> None: ...
    def get_dag(self) -> DagHandle: ...
    def get_id(self) -> str: ...
    def get_name(self) -> str: ...
    def get_type(self) -> str: ...
    def get_node_def(self) -> NodeDefInfo: ...
    def get_inputs(self) -> Set[str]: ...
    def get_input(self, key: str) -> Tuple[NodeHandle, str]: ...
    def get_status(self) -> TaskStatus: ...
    def has_config_error(self) -> bool: ...
    def get_config_error(self) -> Optional[str]: ...
    def get_blobs(self) -> List[str]: ...
    def get_blob_handles(self) -> Dict[str, BlobHandle]: ...
    def get_blob_handle(self, key: str) -> BlobHandle: ...
    def set_blob_uri(self, key: str, blob_uri: str) -> str: ...
    def get_in_cursor_states(self) -> Dict[str, int]: ...
    def get_highest_chunk(self) -> int: ...
    def get_short_status(self, allow_unicode: bool = ...) -> str: ...
    def get_logs(self) -> str: ...
    def get_timing(self) -> List[Timing]: ...
    def read_blob(self, key: str, chunk: Optional[int], force_refresh: bool) -> BlobHandle: ...
    def read(self, key: str, chunk: Optional[int], force_refresh: bool = ..., filter_id: bool = ...) -> Optional[ByteResponse]: ...
    def read_all(self, key: str, force_refresh: bool = ..., filter_id: bool = ...) -> Optional[ByteResponse]: ...
    def clear(self) -> NodeState: ...
    def requeue(self) -> NodeState: ...
    def fix_error(self) -> NodeState: ...
    def get_blob_uri(self, blob_key: str, blob_type: str) -> Tuple[str, BlobOwner]: ...
    def get_csv_blob(self, key: str = ...) -> CSVBlobHandle: ...
    def get_json_blob(self, key: str = ...) -> JSONBlobHandle: ...
    def get_custom_code_blob(self, key: str = ...) -> CustomCodeBlobHandle: ...
    def check_custom_code_node(self) -> None: ...
    def set_custom_imports(self, modules: List[List[str]]) -> NodeCustomImports: ...
    def get_custom_imports(self) -> NodeCustomImports: ...
    def set_es_query(self, query: Dict[str, Any]) -> ESQueryResponse: ...
    def get_es_query(self) -> ESQueryResponse: ...
    def get_user_columns(self, key: str) -> NodeUserColumnsResponse: ...
    def get_input_example(self) -> Dict[str, Optional[ByteResponse]]: ...
    def get_def(self) -> NodeDef: ...
    def is_model(self) -> bool: ...
    def ensure_is_model(self) -> None: ...
    def setup_model(self, obj: Dict[str, Any]) -> ModelInfo: ...
    def get_model_params(self) -> ModelParamsResponse: ...
    def __hash__(self) -> int: ...
    def __eq__(self, other: object) -> bool: ...
    def __ne__(self, other: object) -> bool: ...

EMPTY_BLOB_PREFIX: str

class BlobHandle:
    def __init__(self, client: XYMEClient, uri: str, is_full: bool) -> None: ...
    def is_full(self) -> bool: ...
    def is_empty(self) -> bool: ...
    def get_uri(self) -> str: ...
    def get_path(self, *path: str) -> BlobHandle: ...
    def get_parent(self) -> BlobHandle: ...
    def get_ctype(self) -> Optional[str]: ...
    def clear_info_cache(self) -> None: ...
    def get_info(self) -> Dict[str, Any]: ...
    def get_content(self) -> Optional[ByteResponse]: ...
    def list_files(self) -> List[BlobHandle]: ...
    def as_str(self) -> str: ...
    def set_owner(self, owner: NodeHandle) -> BlobOwner: ...
    def get_owner(self) -> BlobOwner: ...
    def set_local_owner(self, owner: BlobOwner) -> None: ...
    def get_owner_dag(self) -> Optional[str]: ...
    def get_owner_node(self) -> str: ...
    def copy_to(self, to_uri: str, new_owner: Optional[NodeHandle] = ..., external_owner: bool = ...) -> BlobHandle: ...
    def download_zip(self, to_path: Optional[str]) -> Optional[io.BytesIO]: ...
    def upload_zip(self, source: Union[str, io.BytesIO]) -> List[BlobHandle]: ...
    def upload_sklike_model_file(self, model_obj: IO[bytes], xcols: List[str], is_clf: bool, model_name: str, maybe_classes: Optional[List[str]] = ..., maybe_range: Optional[Tuple[Optional[float], Optional[float]]] = ..., full_init: bool = ...) -> UploadFilesResponse: ...
    def upload_sklike_model(self, model: Any, xcols: List[str], is_clf: bool, maybe_classes: Optional[List[str]] = ..., maybe_range: Optional[Tuple[Optional[float], Optional[float]]] = ..., full_init: bool = ...) -> UploadFilesResponse: ...
    def convert_model(self, reload: bool = ...) -> ModelReleaseResponse: ...
    def delete(self) -> DeleteBlobResponse: ...
    def get_model_release(self) -> ModelReleaseResponse: ...
    def get_model_version(self) -> ModelVersionResponse: ...
    def copy_model_version(self, read_version: int, write_version: int, overwrite: bool) -> ModelVersionResponse: ...
    def delete_model_version(self, version: int) -> ModelVersionResponse: ...
    def __hash__(self) -> int: ...
    def __eq__(self, other: object) -> bool: ...
    def __ne__(self, other: object) -> bool: ...

class CSVBlobHandle(BlobHandle):
    def finish_csv_upload(self, filename: Optional[str] = ...) -> UploadFilesResponse: ...
    def add_from_file(self, filename: str, progress_bar: Optional[IO[Any]] = ...) -> Optional[UploadFilesResponse]: ...
    def add_from_df(self, df: pd.DataFrame, progress_bar: Optional[IO[Any]] = ...) -> Optional[UploadFilesResponse]: ...
    def add_from_content(self, content: Union[bytes, str, pd.DataFrame], progress_bar: Optional[IO[Any]] = ...) -> Optional[UploadFilesResponse]: ...

class JSONBlobHandle(BlobHandle):
    def __init__(self, client: XYMEClient, uri: str, is_full: bool) -> None: ...
    def get_count(self) -> Optional[int]: ...
    def append_jsons(self, jsons: List[Any], requeue_on_finish: Optional[NodeHandle] = ...) -> None: ...

class CustomCodeBlobHandle(BlobHandle):
    def set_custom_imports(self, modules: List[List[str]]) -> NodeCustomImports: ...
    def get_custom_imports(self) -> NodeCustomImports: ...
    def set_custom_code(self, func: FUNC) -> NodeCustomCode: ...
    def get_custom_code(self) -> NodeCustomCode: ...

class ComputationHandle:
    def __init__(self, dag: DagHandle, value_id: str, get_dyn_error: Callable[[], Optional[str]], set_dyn_error: Callable[[str], None]) -> None: ...
    def has_fetched(self) -> bool: ...
    def get(self) -> ByteResponse: ...
    def get_id(self) -> str: ...
    def __hash__(self) -> int: ...
    def __eq__(self, other: object) -> bool: ...
    def __ne__(self, other: object) -> bool: ...

def create_xyme_client(url: str, token: Optional[str] = ..., namespace: str = ...) -> XYMEClient: ...
