from typing import (
    Any,
    Callable,
    cast,
    Dict,
    IO,
    Iterable,
    Iterator,
    List,
    Optional,
    overload,
    Set,
    TextIO,
    Tuple,
    TYPE_CHECKING,
    Union,
)
import io
import os
import sys
import json
import time
import weakref
import inspect
import textwrap
import threading
import contextlib
import collections
from io import BytesIO, StringIO
import pandas as pd
import requests
from requests import Response
from requests.exceptions import HTTPError, RequestException
from typing_extensions import Literal
import quick_server

from accern_xyme.v3.util import (
    async_compute,
    ByteResponse,
    df_to_csv,
    get_age,
    get_file_hash,
    get_file_upload_chunk_size,
    get_max_retry,
    get_progress_bar,
    get_retry_sleep,
    interpret_ctype,
    merge_ctype,
    safe_opt_num,
    ServerSideError,
)
from accern_xyme.v3.types import (
    BlobInit,
    BlobOwner,
    CacheStats,
    CopyBlob,
    CSVBlobResponse,
    CSVList,
    CSVOp,
    CustomCodeResponse,
    CustomImportsResponse,
    DynamicResults,
    DynamicStatusResponse,
    ESQueryResponse,
    FlushAllQueuesResponse,
    InCursors,
    InstanceStatus,
    JobInfo,
    JobList,
    JSONBlobResponse,
    KafkaGroup,
    KafkaMessage,
    KafkaOffsets,
    KafkaThroughput,
    KafkaTopics,
    ListNamedSecretKeys,
    MaintenanceResponse,
    MinimalQueueStatsResponse,
    ModelParamsResponse,
    ModelReleaseResponse,
    ModelSetupResponse,
    NodeChunk,
    NodeDef,
    NodeDefInfo,
    NodeInfo,
    NodeState,
    NodeStatus,
    NodeTiming,
    NodeTypes,
    PipelineCreate,
    PipelineDef,
    PipelineDupResponse,
    PipelineInfo,
    PipelineInit,
    PipelineList,
    PipelineReload,
    PutNodeBlob,
    QueueMode,
    QueueStatsResponse,
    QueueStatus,
    ReadNode,
    SetNamedSecret,
    TaskStatus,
    Timing,
    TimingResult,
    Timings,
    UserColumnsResponse,
    VersionResponse,
    VisibleBlobs,
    WorkerScale,
)

if TYPE_CHECKING:
    WVD = weakref.WeakValueDictionary[str, 'PipelineHandle']
else:
    WVD = weakref.WeakValueDictionary


API_VERSION = 3


METHOD_DELETE = "DELETE"
METHOD_FILE = "FILE"
METHOD_GET = "GET"
METHOD_LONGPOST = "LONGPOST"
METHOD_POST = "POST"
METHOD_PUT = "PUT"

PREFIX = "/xyme"

INPUT_CSV_EXT = ".csv"
INPUT_TSV_EXT = ".tsv"
INPUT_ZIP_EXT = ".zip"
INPUT_EXT = [INPUT_ZIP_EXT, INPUT_CSV_EXT, INPUT_TSV_EXT]


FUNC = Callable[[Any], Any]
CUSTOM_NODE_TYPES = {
    "custom_data",
    "custom_json",
    "custom_json_to_data",
    "custom_json_join_data",
}
EMBEDDING_MODEL_NODE_TYPES = {
    "dyn_embedding_model",
    "static_embedding_model",
}
MODEL_NODE_TYPES = EMBEDDING_MODEL_NODE_TYPES


class AccessDenied(Exception):
    pass

# *** AccessDenied ***


class LegacyVersion(Exception):
    pass

# *** LegacyVersion ***


class XYMEClientV3:
    def __init__(
            self,
            url: str,
            token: Optional[str]) -> None:
        self._url = url.rstrip("/")
        if token is None:
            token = os.environ.get("XYME_SERVER_TOKEN")
        self._token = token
        self._last_action = time.monotonic()
        self._auto_refresh = True
        self._pipeline_cache: WVD = weakref.WeakValueDictionary()
        self._permissions: Optional[List[str]] = None
        self._node_defs: Optional[Dict[str, NodeDefInfo]] = None

        def get_version() -> int:
            server_version = self.get_server_version()
            try:
                return int(server_version["api_version"])
            except (ValueError, KeyError) as e:
                raise LegacyVersion() from e

        self._api_version = min(get_version(), API_VERSION)

    def get_api_version(self) -> int:
        return self._api_version

    def get_permissions(self) -> List[str]:
        if self._permissions is None:
            raise NotImplementedError("permissions are not implemented")
        assert self._permissions is not None
        return self._permissions

    def set_auto_refresh(self, is_auto_refresh: bool) -> None:
        self._auto_refresh = is_auto_refresh

    def is_auto_refresh(self) -> bool:
        return self._auto_refresh

    def refresh(self) -> None:
        self._node_defs = None

    def _maybe_refresh(self) -> None:
        if self.is_auto_refresh():
            self.refresh()

    @contextlib.contextmanager
    def bulk_operation(self) -> Iterator[bool]:
        old_refresh = self.is_auto_refresh()
        try:
            self.set_auto_refresh(False)
            yield old_refresh
        finally:
            self.set_auto_refresh(old_refresh)

    def _raw_request_bytes(
            self,
            method: str,
            path: str,
            args: Dict[str, Any],
            files: Optional[Dict[str, BytesIO]] = None,
            add_prefix: bool = True,
            api_version: Optional[int] = None) -> Tuple[BytesIO, str]:
        file_resets = {}
        can_reset = True
        if files is not None:
            for (fname, fbuff) in files.items():
                if hasattr(fbuff, "seek"):
                    file_resets[fname] = fbuff.seek(0, io.SEEK_CUR)
                else:
                    can_reset = False

        def reset_files() -> bool:
            if files is None:
                return True
            if not can_reset:
                return False
            for (fname, pos) in file_resets.items():
                files[fname].seek(pos, io.SEEK_SET)
            return True

        retry = 0
        max_retry = get_max_retry()
        while True:
            try:
                try:
                    return self._fallible_raw_request_bytes(
                        method, path, args, files, add_prefix, api_version)
                except HTTPError as e:
                    if e.response.status_code in (403, 404, 500):
                        retry = max_retry
                    raise e
            except RequestException:
                if retry >= max_retry:
                    raise
                if not reset_files():
                    raise
                time.sleep(get_retry_sleep())
            retry += 1

    def _raw_request_str(
            self,
            method: str,
            path: str,
            args: Dict[str, Any],
            add_prefix: bool = True,
            api_version: Optional[int] = None) -> TextIO:
        retry = 0
        max_retry = get_max_retry()
        while True:
            try:
                try:
                    return self._fallible_raw_request_str(
                        method, path, args, add_prefix, api_version)
                except HTTPError as e:
                    if e.response.status_code in (403, 404, 500):
                        retry = max_retry
                    raise e
            except RequestException:
                if retry >= max_retry:
                    raise
                time.sleep(get_retry_sleep())
            retry += 1

    def _raw_request_json(
            self,
            method: str,
            path: str,
            args: Dict[str, Any],
            add_prefix: bool = True,
            files: Optional[Dict[str, IO[bytes]]] = None,
            api_version: Optional[int] = None) -> Dict[str, Any]:
        file_resets = {}
        can_reset = True
        if files is not None:
            for (fname, fbuff) in files.items():
                if hasattr(fbuff, "seek"):
                    file_resets[fname] = fbuff.seek(0, io.SEEK_CUR)
                else:
                    can_reset = False

        def reset_files() -> bool:
            if files is None:
                return True
            if not can_reset:
                return False
            for (fname, pos) in file_resets.items():
                files[fname].seek(pos, io.SEEK_SET)
            return True

        retry = 0
        max_retry = get_max_retry()
        while True:
            try:
                try:
                    return self._fallible_raw_request_json(
                        method, path, args, add_prefix, files, api_version)
                except HTTPError as e:
                    if e.response.status_code in (403, 404, 500):
                        retry = max_retry
                    raise e
            except RequestException:
                if retry >= max_retry:
                    raise
                if not reset_files():
                    raise
                time.sleep(get_retry_sleep())
            retry += 1

    def _fallible_raw_request_bytes(
            self,
            method: str,
            path: str,
            args: Dict[str, Any],
            files: Optional[Dict[str, BytesIO]],
            add_prefix: bool,
            api_version: Optional[int]) -> Tuple[BytesIO, str]:
        prefix = ""
        if add_prefix:
            if api_version is None:
                api_version = self._api_version
            prefix = f"{PREFIX}/v{api_version}"
        url = f"{self._url}{prefix}{path}"
        headers = {
            "authorization": self._token,
        }

        def check_error(req: Response) -> None:
            if req.status_code == 403:
                raise AccessDenied(req.text)
            req.raise_for_status()
            # NOTE: no content type check -- will be handled by interpret_ctype

        if method == METHOD_GET:
            req = requests.get(url, params=args, headers=headers)
            check_error(req)
            return BytesIO(req.content), req.headers["content-type"]
        if method == METHOD_POST:
            req = requests.post(url, json=args, headers=headers)
            check_error(req)
            return BytesIO(req.content), req.headers["content-type"]
        if method == METHOD_FILE:
            if files is None:
                raise ValueError(f"file method must have files: {files}")
            req = requests.post(
                url,
                data=args,
                files={
                    key: (
                        getattr(value, "name", key),
                        value,
                        "application/octet-stream",
                    ) for (key, value) in files.items()
                },
                headers=headers)
            check_error(req)
            return BytesIO(req.content), req.headers["content-type"]
        raise ValueError(f"unknown method {method}")

    def _fallible_raw_request_str(
            self,
            method: str,
            path: str,
            args: Dict[str, Any],
            add_prefix: bool,
            api_version: Optional[int]) -> TextIO:
        prefix = ""
        if add_prefix:
            if api_version is None:
                api_version = self._api_version
            prefix = f"{PREFIX}/v{api_version}"
        url = f"{self._url}{prefix}{path}"
        headers = {
            "authorization": self._token,
        }

        def check_error(req: Response) -> None:
            if req.status_code == 403:
                raise AccessDenied(req.text)
            req.raise_for_status()
            if req.headers["content-type"] == "application/problem+json":
                raise ServerSideError(json.loads(req.text)["errMessage"])

        if method == METHOD_GET:
            req = requests.get(url, params=args, headers=headers)
            check_error(req)
            return StringIO(req.text)
        if method == METHOD_POST:
            req = requests.post(url, json=args, headers=headers)
            check_error(req)
            return StringIO(req.text)
        raise ValueError(f"unknown method {method}")

    def _fallible_raw_request_json(
            self,
            method: str,
            path: str,
            args: Dict[str, Any],
            add_prefix: bool,
            files: Optional[Dict[str, IO[bytes]]],
            api_version: Optional[int]) -> Dict[str, Any]:
        prefix = ""
        if add_prefix:
            if api_version is None:
                api_version = self._api_version
            prefix = f"{PREFIX}/v{api_version}"
        url = f"{self._url}{prefix}{path}"
        headers = {
            "authorization": self._token,
        }
        if method != METHOD_FILE and files is not None:
            raise ValueError(
                f"files are only allow for post (got {method}): {files}")
        req = None

        def check_error(req: Response) -> None:
            if req.status_code == 403:
                raise AccessDenied(req.text)
            req.raise_for_status()
            if req.headers["content-type"] == "application/problem+json":
                raise ServerSideError(json.loads(req.text)["errMessage"])

        try:
            if method == METHOD_GET:
                req = requests.get(url, params=args, headers=headers)
                check_error(req)
                return json.loads(req.text)
            if method == METHOD_FILE:
                if files is None:
                    raise ValueError(f"file method must have files: {files}")
                req = requests.post(
                    url,
                    data=args,
                    files={
                        key: (
                            getattr(value, "name", key),
                            value,
                            "application/octet-stream",
                        ) for (key, value) in files.items()
                    },
                    headers=headers)
                check_error(req)
                return json.loads(req.text)
            if method == METHOD_POST:
                req = requests.post(url, json=args, headers=headers)
                check_error(req)
                return json.loads(req.text)
            if method == METHOD_PUT:
                req = requests.put(url, json=args, headers=headers)
                check_error(req)
                return json.loads(req.text)
            if method == METHOD_DELETE:
                req = requests.delete(url, json=args, headers=headers)
                check_error(req)
                return json.loads(req.text)
            if method == METHOD_LONGPOST:
                args["token"] = self._token
                try:
                    res = quick_server.worker_request(url, args)
                    if "errMessage" in res:
                        raise ServerSideError(res["errMessage"])
                    return res
                except quick_server.WorkerError as e:
                    if e.get_status_code() == 403:
                        raise AccessDenied(e.args) from e
                    raise e
            raise ValueError(f"unknown method {method}")
        except json.decoder.JSONDecodeError as e:
            if req is None:
                raise
            raise ValueError(req.text) from e

    def request_bytes(
            self,
            method: str,
            path: str,
            args: Dict[str, Any],
            files: Optional[Dict[str, BytesIO]] = None,
            add_prefix: bool = True,
            api_version: Optional[int] = None) -> Tuple[BytesIO, str]:
        return self._raw_request_bytes(
            method, path, args, files, add_prefix, api_version)

    def _request_json(
            self,
            method: str,
            path: str,
            args: Dict[str, Any],
            add_prefix: bool = True,
            files: Optional[Dict[str, IO[bytes]]] = None,
            api_version: Optional[int] = None) -> Dict[str, Any]:
        return self._raw_request_json(
            method, path, args, add_prefix, files, api_version)

    def get_server_version(self) -> VersionResponse:
        return cast(VersionResponse, self._raw_request_json(
            METHOD_GET, f"{PREFIX}/v{API_VERSION}/version", {
            }, add_prefix=False))

    def set_maintenance_mode(
            self, is_maintenance: bool) -> MaintenanceResponse:
        """Set the maintenance mode of the server

        Args:
            is_maintenance (bool): If the server should be in maintenance mode.

        Returns:
            MaintenanceResponse: MaintenanceResponse object.
        """
        return cast(MaintenanceResponse, self._request_json(
            METHOD_PUT, "/maintenance", {
                "is_maintenance": is_maintenance,
            }))

    def get_maintenance_mode(self) -> MaintenanceResponse:
        return cast(MaintenanceResponse, self._request_json(
            METHOD_GET, "/maintenance", {}))

    def get_pipelines(self) -> List[str]:
        return [
            res[0]
            for res in self.get_pipeline_times(retrieve_times=False)[1]
        ]

    def get_pipeline_ages(self) -> List[Tuple[str, str, str]]:
        cur_time, pipelines = self.get_pipeline_times(retrieve_times=True)
        return [
            (pipe_id, get_age(cur_time, oldest), get_age(cur_time, latest))
            for (pipe_id, oldest, latest) in sorted(pipelines, key=lambda el: (
                safe_opt_num(el[1]), safe_opt_num(el[2]), el[0]))
        ]

    def get_pipeline_times(
            self,
            retrieve_times: bool) -> Tuple[
                float, List[Tuple[str, Optional[float], Optional[float]]]]:
        obj = {}
        if retrieve_times:
            obj["retrieve_times"] = "1"
        res = cast(PipelineList, self._request_json(
            METHOD_GET, "/pipelines", obj))
        return res["cur_time"], res["pipelines"]

    def get_pipeline(self, pipe_id: str) -> 'PipelineHandle':
        res = self._pipeline_cache.get(pipe_id)
        if res is not None:
            return res
        res = PipelineHandle(self, pipe_id)
        self._pipeline_cache[pipe_id] = res
        return res

    def get_node_defs(self) -> Dict[str, NodeDefInfo]:
        self._maybe_refresh()
        if self._node_defs is not None:
            return self._node_defs
        res = cast(NodeTypes, self._request_json(
            METHOD_GET, "/node_types", {}))["info"]
        self._node_defs = res
        return res

    def create_new_blob(self, blob_type: str) -> str:
        return cast(BlobInit, self._request_json(
            METHOD_POST, "/blob_init", {
                "type": blob_type,
            }))["blob"]

    def create_new_pipeline(
            self,
            username: Optional[str] = None,
            pipename: Optional[str] = None,
            index: Optional[int] = None) -> str:
        return cast(PipelineInit, self._request_json(
            METHOD_POST, "/pipeline_init", {
                "user": username,
                "name": pipename,
                "index": index,
            }))["pipeline"]

    def duplicate_pipeline(
            self, pipe_id: str, dest_id: Optional[str] = None) -> str:
        args = {
            "pipeline": pipe_id,
        }
        if dest_id is not None:
            args["dest"] = dest_id
        return cast(PipelineDupResponse, self._request_json(
            METHOD_POST, "/pipeline_dup", args))["pipeline"]

    def set_pipeline(
            self,
            pipe_id: str,
            defs: PipelineDef,
            warnings_io: Optional[IO[Any]] = sys.stderr) -> 'PipelineHandle':
        pipe_create = cast(PipelineCreate, self._request_json(
            METHOD_POST, "/pipeline_create", {
                "pipeline": pipe_id,
                "defs": defs,
            }))
        pipe_id = pipe_create["pipeline"]
        if warnings_io is not None:
            warnings = pipe_create["warnings"]
            if len(warnings) > 1:
                warnings_io.write(
                    f"{len(warnings)} warnings while "
                    f"setting pipeline {pipe_id}:\n")
            elif len(warnings) == 1:
                warnings_io.write(
                    f"Warning while setting pipeline {pipe_id}:\n")
            for warn in warnings:
                warnings_io.write(f"{warn}\n")
            if warnings:
                warnings_io.flush()
        return self.get_pipeline(pipe_id)

    def update_settings(
            self, pipe_id: str, settings: Dict[str, Any]) -> 'PipelineHandle':
        pipe_id = cast(PipelineCreate, self._request_json(
            METHOD_POST, "/update_pipeline_settings", {
                "pipeline": pipe_id,
                "settings": settings,
            }))["pipeline"]
        return self.get_pipeline(pipe_id)

    def get_csvs(self) -> List[str]:
        return cast(CSVList, self._request_json(
            METHOD_GET, "/csvs", {
            }))["csvs"]

    def add_csv(self, csv_blob_id: str) -> List[str]:
        return cast(CSVList, self._request_json(
            METHOD_PUT, "/csvs", {
                "blob": csv_blob_id,
            }))["csvs"]

    def remove_csv(self, csv_blob_id: str) -> List[str]:
        return cast(CSVList, self._request_json(
            METHOD_DELETE, "/csvs", {
                "blob": csv_blob_id,
            }))["csvs"]

    def get_jobs(self) -> List[str]:
        return cast(JobList, self._request_json(
            METHOD_GET, "/jobs", {
            }))["jobs"]

    def remove_job(self, job_id: str) -> List[str]:
        return cast(JobList, self._request_json(
            METHOD_DELETE, "/job", {
                "job": job_id,
            }))["jobs"]

    def create_job(self) -> JobInfo:
        return cast(JobInfo, self._request_json(
            METHOD_POST, "/job_init", {}))

    def get_job(self, job_id: str) -> JobInfo:
        return cast(JobInfo, self._request_json(
            METHOD_GET, "/job", {
                "job": job_id,
            }))

    def set_job(self, job: JobInfo) -> JobInfo:
        return cast(JobInfo, self._request_json(
            METHOD_PUT, "/job", {
                "job": job,
            }))

    def get_allowed_custom_imports(self) -> CustomImportsResponse:
        return cast(CustomImportsResponse, self._request_json(
            METHOD_GET, "/allowed_custom_imports", {}))

    @overload
    def check_queue_stats(  # pylint: disable=no-self-use
            self,
            pipeline: Optional[str],
            minimal: Literal[True]) -> MinimalQueueStatsResponse:
        ...

    @overload
    def check_queue_stats(  # pylint: disable=no-self-use
            self,
            pipeline: Optional[str],
            minimal: Literal[False]) -> QueueStatsResponse:
        ...

    @overload
    def check_queue_stats(  # pylint: disable=no-self-use
            self,
            pipeline: Optional[str],
            minimal: bool) -> Union[
                MinimalQueueStatsResponse, QueueStatsResponse]:
        ...

    def check_queue_stats(
            self,
            pipeline: Optional[str] = None,
            minimal: bool = False) -> Union[
                MinimalQueueStatsResponse, QueueStatsResponse]:
        if minimal:
            return cast(MinimalQueueStatsResponse, self._request_json(
                METHOD_GET, "/queue_stats", {
                    "pipeline": pipeline,
                    "minimal": 1,
                }))
        return cast(QueueStatsResponse, self._request_json(
            METHOD_GET, "/queue_stats", {
                "pipeline": pipeline,
                "minimal": 0,
            }))

    def get_instance_status(
            self,
            pipe_id: Optional[str] = None,
            node_id: Optional[str] = None) -> Dict[InstanceStatus, int]:
        return cast(Dict[InstanceStatus, int], self._request_json(
            METHOD_GET, "/instance_status", {
                "pipeline": pipe_id,
                "node": node_id,
            }))

    def get_queue_mode(self) -> str:
        return cast(QueueMode, self._request_json(
            METHOD_GET, "/queue_mode", {}))["mode"]

    def set_queue_mode(self, mode: str) -> str:
        return cast(QueueMode, self._request_json(
            METHOD_PUT, "/queue_mode", {
                "mode": mode,
            }))["mode"]

    def flush_all_queue_data(self) -> None:

        def do_flush() -> bool:
            res = cast(FlushAllQueuesResponse, self._request_json(
                METHOD_POST, "/flush_all_queues", {}))
            return bool(res["success"])

        while do_flush():  # we flush until there is nothing to flush anymore
            time.sleep(1.0)

    def get_cache_stats(self) -> CacheStats:
        return cast(CacheStats, self._request_json(
            METHOD_GET, "/cache_stats", {}))

    def reset_cache(self) -> CacheStats:
        return cast(CacheStats, self._request_json(
            METHOD_POST, "/cache_reset", {}))

    def create_kafka_error_topic(self) -> KafkaTopics:
        return cast(KafkaTopics, self._request_json(
            METHOD_POST, "/kafka_topics", {
                "num_partitions": 1,
            }))

    def delete_kafka_error_topic(self) -> KafkaTopics:
        return cast(KafkaTopics, self._request_json(
            METHOD_POST, "/kafka_topics", {
                "num_partitions": 0,
            }))

    def read_kafka_errors(self, offset: str = "current") -> List[str]:
        return cast(List[str], self._request_json(
            METHOD_GET, "/kafka_msg", {
                "offset": offset,
            }))

    def get_named_secret_keys(self) -> List[str]:
        return cast(ListNamedSecretKeys, self._request_json(
            METHOD_GET, "/named_secrets", {}))["keys"]

    def set_named_secret(self, key: str, value: str) -> bool:
        return cast(SetNamedSecret, self._request_json(
            METHOD_PUT, "/named_secrets", {
                "key": key,
                "value": value,
            }))["replaced"]

    def get_error_logs(self) -> str:
        with self._raw_request_str(METHOD_GET, "/error_logs", {}) as fin:
            return fin.read()


# *** XYMEClientV3 ***


class PipelineHandle:
    def __init__(
            self,
            client: XYMEClientV3,
            pipe_id: str) -> None:
        self._client = client
        self._pipe_id = pipe_id
        self._name: Optional[str] = None
        self._company: Optional[str] = None
        self._state: Optional[str] = None
        self._is_high_priority: Optional[bool] = None
        self._queue_mng: Optional[str] = None
        self._nodes: Dict[str, NodeHandle] = {}
        self._node_lookup: Dict[str, str] = {}
        self._settings: Optional[Dict[str, Any]] = None
        self._dynamic_error: Optional[str] = None
        self._ins: Optional[List[str]] = None
        self._outs: Optional[List[Tuple[str, str]]] = None

    def refresh(self) -> None:
        self._name = None
        self._company = None
        self._state = None
        self._is_high_priority = None
        self._queue_mng = None
        self._ins = None
        self._outs = None
        # NOTE: we don't reset nodes

    def _maybe_refresh(self) -> None:
        if self._client.is_auto_refresh():
            self.refresh()

    def _maybe_fetch(self) -> None:
        if self._name is None:
            self._fetch_info()

    def get_info(self) -> PipelineInfo:
        return cast(PipelineInfo, self._client._request_json(
            METHOD_GET, "/pipeline_info", {
                "pipeline": self._pipe_id,
            }))

    def _fetch_info(self) -> None:
        info = self.get_info()
        self._name = info["name"]
        self._company = info["company"]
        self._state = info["state"]
        self._is_high_priority = info["high_priority"]
        self._queue_mng = info["queue_mng"]
        self._settings = info["settings"]
        self._ins = info["ins"]
        self._outs = [(el[0], el[1]) for el in info["outs"]]
        old_nodes = {} if self._nodes is None else self._nodes
        self._nodes = {
            node["id"]: NodeHandle.from_node_info(
                self._client, self, node, old_nodes.get(node["id"]))
            for node in info["nodes"]
        }
        self._node_lookup = {
            node["name"]: node["id"]
            for node in info["nodes"]
            if node["name"] is not None
        }

    def get_nodes(self) -> List[str]:
        self._maybe_refresh()
        self._maybe_fetch()
        return list(self._nodes.keys())

    def get_node(self, node_id: str) -> 'NodeHandle':
        self._maybe_refresh()
        self._maybe_fetch()
        node_id = self._node_lookup.get(node_id, node_id)
        return self._nodes[node_id]

    def get_id(self) -> str:
        return self._pipe_id

    def get_name(self) -> str:
        self._maybe_refresh()
        self._maybe_fetch()
        assert self._name is not None
        return self._name

    def get_company(self) -> str:
        self._maybe_refresh()
        self._maybe_fetch()
        assert self._company is not None
        return self._company

    def get_state_type(self) -> str:
        self._maybe_refresh()
        self._maybe_fetch()
        assert self._state is not None
        return self._state

    def get_settings(self) -> Dict[str, Any]:
        self._maybe_refresh()
        self._maybe_fetch()
        assert self._settings is not None
        return self._settings

    def get_timing(
                self,
                blacklist: Optional[List[str]] = None,
                ) -> Optional[TimingResult]:
        blist = [] if blacklist is None else blacklist
        node_timing: Dict[str, NodeTiming] = {}
        nodes = self.get_nodes()

        def get_filterd_times(
                node_time: List[Timing]) -> Tuple[float, float, List[Timing]]:
            fns = []
            node_total = 0.0
            for value in node_time:
                if value["name"] not in blist:
                    fns.append(value)
                    node_total += value["total"]
            if not fns:
                return (0, 0, fns)
            return (node_total, node_total / len(fns), fns)

        pipe_total = 0.0
        for node in nodes:
            node_get = self.get_node(node)
            node_time = node_get.get_timing()
            node_name = node_get.get_node_def()["name"]
            node_id = node_get.get_id()
            node_total, avg_time, fns = get_filterd_times(node_time)
            node_timing[node_id] = {
                "node_name": node_name,
                "node_total": node_total,
                "node_avg": avg_time,
                "fns": fns,
            }
            pipe_total += node_total
        node_timing_sorted = sorted(
            node_timing.items(),
            key=lambda x: x[1]["node_total"],
            reverse=True)
        return {
            "pipe_total": pipe_total,
            "nodes": node_timing_sorted,
        }

    def is_high_priority(self) -> bool:
        self._maybe_refresh()
        self._maybe_fetch()
        assert self._is_high_priority is not None
        return self._is_high_priority

    def is_queue(self) -> bool:
        self._maybe_refresh()
        self._maybe_fetch()
        return self._queue_mng is not None

    def get_queue_mng(self) -> Optional[str]:
        self._maybe_refresh()
        self._maybe_fetch()
        return self._queue_mng

    def get_ins(self) -> List[str]:
        self._maybe_refresh()
        self._maybe_fetch()
        assert self._ins is not None
        return self._ins

    def get_outs(self) -> List[Tuple[str, str]]:
        self._maybe_refresh()
        self._maybe_fetch()
        assert self._outs is not None
        return self._outs

    @contextlib.contextmanager
    def bulk_operation(self) -> Iterator[bool]:
        with self._client.bulk_operation() as do_refresh:
            if do_refresh:
                self.refresh()
            yield do_refresh

    def set_pipeline(self, defs: PipelineDef) -> None:
        self._client.set_pipeline(self.get_id(), defs)

    def update_settings(self, settings: Dict[str, Any]) -> None:
        self._client.update_settings(self.get_id(), settings)

    def dynamic_model(
            self,
            inputs: List[Any],
            format_method: str = "simple",
            no_cache: bool = False) -> List[Any]:
        res = cast(DynamicResults, self._client._request_json(
            METHOD_POST, "/dynamic_model", {
                "format": format_method,
                "inputs": inputs,
                "no_cache": no_cache,
                "pipeline": self._pipe_id,
            }))
        return res["results"]

    def dynamic_list(
            self,
            inputs: List[Any],
            input_key: Optional[str] = None,
            output_key: Optional[str] = None,
            split_th: Optional[int] = 1000,
            max_threads: int = 50,
            format_method: str = "simple",
            force_keys: bool = False,
            no_cache: bool = False) -> List[Any]:
        if split_th is None or len(inputs) <= split_th:
            res = cast(DynamicResults, self._client._request_json(
                METHOD_POST, "/dynamic_list", {
                    "force_keys": force_keys,
                    "format": format_method,
                    "input_key": input_key,
                    "inputs": inputs,
                    "no_cache": no_cache,
                    "output_key": output_key,
                    "pipeline": self._pipe_id,
                }))
            return res["results"]
        split_num: int = split_th
        assert split_num > 0
        res_arr: List[Any] = [None] * len(inputs)
        exc: List[Optional[BaseException]] = [None]
        active_ths: Set[threading.Thread] = set()

        def compute_half(cur: List[Any], offset: int) -> None:
            if exc[0] is not None:
                return
            if len(cur) <= split_num:
                try:
                    cur_res = self.dynamic_list(
                        cur,
                        input_key=input_key,
                        output_key=output_key,
                        split_th=None,
                        max_threads=max_threads,
                        format_method=format_method,
                        force_keys=force_keys,
                        no_cache=no_cache)
                    res_arr[offset:offset + len(cur_res)] = cur_res
                except BaseException as e:  # pylint: disable=broad-except
                    exc[0] = e
                return
            half_ix: int = len(cur) // 2
            args_first = (cur[:half_ix], offset)
            args_second = (cur[half_ix:], offset + half_ix)
            if len(active_ths) < max_threads:
                comp_th = threading.Thread(
                    target=compute_half, args=args_first)
                active_ths.add(comp_th)
                comp_th.start()
                compute_half(*args_second)
                comp_th.join()
                active_ths.remove(comp_th)
            else:
                compute_half(*args_first)
                compute_half(*args_second)

        compute_half(inputs, 0)
        for remain_th in active_ths:
            remain_th.join()
        raise_e = exc[0]
        try:
            if isinstance(raise_e, BaseException):
                raise raise_e  # pylint: disable=raising-bad-type
        except RequestException as e:
            raise ValueError(
                "request error while processing. processing time per batch "
                "might be too large. try reducing split_th") from e
        return res_arr

    def dynamic(self, input_data: BytesIO) -> ByteResponse:
        cur_res, ctype = self._client.request_bytes(
            METHOD_FILE, "/dynamic", {
                "pipeline": self._pipe_id,
            }, files={
                "file": input_data,
            })
        return interpret_ctype(cur_res, ctype)

    def dynamic_obj(self, input_obj: Any) -> ByteResponse:
        bio = BytesIO(json.dumps(
            input_obj,
            separators=(",", ":"),
            indent=None,
            sort_keys=True).encode("utf-8"))
        return self.dynamic(bio)

    def dynamic_async(
            self, input_data: List[BytesIO]) -> List['ComputationHandle']:
        names = [f"file{pos}" for pos in range(len(input_data))]
        res: Dict[str, str] = self._client._request_json(
            METHOD_FILE, "/dynamic_async", {
                "pipeline": self._pipe_id,
            }, files=dict(zip(names, input_data)))
        return [
            ComputationHandle(
                self,
                res[name],
                self.get_dynamic_error_message,
                self.set_dynamic_error_message)
            for name in names]

    def set_dynamic_error_message(self, msg: Optional[str]) -> None:
        self._dynamic_error = msg

    def get_dynamic_error_message(self) -> Optional[str]:
        return self._dynamic_error

    def dynamic_async_obj(
            self, input_data: List[Any]) -> List['ComputationHandle']:
        return self.dynamic_async([
            BytesIO(json.dumps(
                input_obj,
                separators=(",", ":"),
                indent=None,
                sort_keys=True).encode("utf-8"))
            for input_obj in input_data
        ])

    def get_dynamic_result(self, data_id: str) -> ByteResponse:
        try:
            cur_res, ctype = self._client.request_bytes(
                METHOD_GET, "/dynamic_result", {
                    "pipeline": self._pipe_id,
                    "id": data_id,
                })
        except HTTPError as e:
            if e.response.status_code == 404:
                raise KeyError(f"data_id {data_id} does not exist") from e
            raise e
        return interpret_ctype(cur_res, ctype)

    def get_dynamic_status(
            self,
            data_ids: List['ComputationHandle'],
    ) -> Dict['ComputationHandle', QueueStatus]:
        res = cast(DynamicStatusResponse, self._client._request_json(
            METHOD_POST, "/dynamic_status", {
                "data_ids": [data_id.get_id() for data_id in data_ids],
                "pipeline": self._pipe_id,
            }))
        status = res["status"]
        hnd_map = {data_id.get_id(): data_id for data_id in data_ids}
        return {
            hnd_map[key]: cast(QueueStatus, value)
            for key, value in status.items()
        }

    def get_dynamic_bulk(
            self,
            input_data: List[BytesIO],
            max_buff: int = 4000,
            block_size: int = 5,
            num_threads: int = 20) -> Iterable[ByteResponse]:

        def get(hnd: 'ComputationHandle') -> ByteResponse:
            return hnd.get()

        success = False
        try:
            yield from async_compute(
                input_data,
                self.dynamic_async,
                get,
                lambda: self.check_queue_stats(minimal=True),
                self.get_dynamic_status,
                max_buff,
                block_size,
                num_threads)
            success = True
        finally:
            if success:
                self.set_dynamic_error_message(None)

    def get_dynamic_bulk_obj(
            self,
            input_data: List[Any],
            max_buff: int = 4000,
            block_size: int = 5,
            num_threads: int = 20) -> Iterable[ByteResponse]:

        def get(hnd: 'ComputationHandle') -> ByteResponse:
            return hnd.get()

        success = False
        try:
            yield from async_compute(
                input_data,
                self.dynamic_async_obj,
                get,
                lambda: self.check_queue_stats(minimal=True),
                self.get_dynamic_status,
                max_buff,
                block_size,
                num_threads)
            success = True
        finally:
            if success:
                self.set_dynamic_error_message(None)

    def pretty(
            self, nodes_only: bool = False, allow_unicode: bool = True) -> str:
        nodes = [
            self.get_node(node_id)
            for node_id in sorted(self.get_nodes())
        ]
        already: Set[NodeHandle] = set()
        order: List[NodeHandle] = []
        outs: Dict[
            NodeHandle,
            List[Tuple[NodeHandle, str, str]],
        ] = collections.defaultdict(list)
        start_pipe = "├" if allow_unicode else "|"
        end_pipe = "├" if allow_unicode else "|"
        before_pipe = "│" if allow_unicode else "|"
        after_pipe = "│" if allow_unicode else "|"
        pipe = "┤" if allow_unicode else "|"
        corner_right = "┐" if allow_unicode else "\\"
        corner_left = "┘" if allow_unicode else "/"
        cont_right = "┬" if allow_unicode else "\\"
        cont_left = "┴" if allow_unicode else "/"
        cont_skip = "─" if allow_unicode else "-"
        cont_pipe = "│" if allow_unicode else "|"
        cont = "┼" if allow_unicode else "-"
        start_left = "└" if allow_unicode else "\\"
        bar = "─" if allow_unicode else "-"
        vsec = "│" if allow_unicode else "|"
        vstart = "├" if allow_unicode else "|"
        vend = "┤" if allow_unicode else "|"
        hsec = "─" if allow_unicode else "-"
        tl = "┌" if allow_unicode else "+"
        tr = "┐" if allow_unicode else "+"
        bl = "└" if allow_unicode else "+"
        br = "┘" if allow_unicode else "+"
        conn_top = "┴" if allow_unicode else "-"
        conn_bottom = "┬" if allow_unicode else "-"
        space = " "
        prefix_len = 2 if nodes_only else 3
        indent = space * prefix_len

        def topo(cur: NodeHandle) -> None:
            if cur in already:
                return
            for in_key in sorted(cur.get_inputs()):
                out_node, out_key = cur.get_input(in_key)
                outs[out_node].append((cur, in_key, out_key))
                topo(out_node)
            already.add(cur)
            order.append(cur)

        for tnode in nodes:
            topo(tnode)

        in_states: Dict[NodeHandle, Dict[str, int]] = {}
        order_lookup = {
            node: pos for (pos, node) in enumerate(order)
        }

        def get_in_state(node: NodeHandle, key: str) -> int:
            if node not in in_states:
                in_states[node] = node.get_in_cursor_states()
            return in_states[node].get(key, 0)

        def draw_in_edges(
                node: NodeHandle,
                cur_edges: List[Tuple[Optional[NodeHandle], str, int]],
                ) -> Tuple[List[Tuple[Optional[NodeHandle], str, int]], str]:
            gap = 0
            prev_gap = 0
            new_edges: List[Tuple[Optional[NodeHandle], str, int]] = []
            segs: List[str] = []
            for edge in cur_edges:
                in_node, in_key, cur_gap = edge
                before_gap = cur_gap
                if in_node == node:
                    in_state = get_in_state(in_node, in_key)
                    cur_str = f"{end_pipe} {in_key} ({in_state}) "
                    new_edges.append((None, in_key, cur_gap))
                else:
                    cur_str = before_pipe if in_node is not None else ""
                    cur_gap += gap
                    gap = 0
                    new_edges.append((in_node, in_key, cur_gap))
                segs.append(f"{space * prev_gap}{cur_str}")
                prev_gap = max(0, before_gap - len(cur_str))
            while new_edges:
                if new_edges[-1][0] is None:
                    new_edges.pop()
                else:
                    break
            return new_edges, "".join(segs)

        def draw_out_edges(
                node: NodeHandle,
                cur_edges: List[Tuple[Optional[NodeHandle], str, int]],
                ) -> Tuple[List[Tuple[Optional[NodeHandle], str, int]], str]:
            new_edges: List[Tuple[Optional[NodeHandle], str, int]] = []
            segs: List[str] = []
            prev_gap = 0
            for edge in cur_edges:
                cur_node, _, cur_gap = edge
                cur_str = after_pipe if cur_node is not None else ""
                segs.append(f"{space * prev_gap}{cur_str}")
                new_edges.append(edge)
                prev_gap = max(0, cur_gap - len(cur_str))
            sout = sorted(
                outs[node], key=lambda e: order_lookup[e[0]], reverse=True)
            for (in_node, in_key, out_key) in sout:
                cur_str = f"{start_pipe} {out_key} "
                in_state = get_in_state(in_node, in_key)
                end_str = f"{end_pipe} {in_key} ({in_state}) "
                segs.append(f"{space * prev_gap}{cur_str}")
                cur_gap = max(len(cur_str), len(end_str))
                new_edges.append((in_node, in_key, cur_gap))
                prev_gap = max(0, cur_gap - len(cur_str))
            return new_edges, "".join(segs)

        def draw() -> List[str]:
            lines: List[str] = []
            edges: List[Tuple[Optional[NodeHandle], str, int]] = []
            for node in order:
                top_gaps = [edge[2] for edge in edges[:-1]]
                top_nodes = [edge[0] for edge in edges]
                same_ids = [edge[0] == node for edge in edges]
                empty_top = [edge[0] is None for edge in edges]
                edges, in_line = draw_in_edges(node, edges)
                in_line = in_line.rstrip()
                if in_line:
                    lines.append(f"{indent}{in_line}")
                edges, out_line = draw_out_edges(node, edges)
                bottom_gaps = [edge[2] for edge in edges[:-1]]
                empty_bottom = [edge[0] is None for edge in edges]
                new_bottom = [
                    eix >= len(top_nodes) or (
                        edge[0] is not None and top_nodes[eix] != edge[0]
                    ) for (eix, edge) in enumerate(edges)
                ]
                line_indents: List[str] = []
                started = False
                had_same = False
                highest_iix = -1
                for (iix, top_gap) in enumerate(top_gaps):
                    if same_ids[iix]:
                        had_same = True
                    if had_same and iix >= len(bottom_gaps):
                        break
                    if not line_indents:
                        line_indents.append(indent)
                    if empty_top[iix]:
                        cur_connect = cont_skip if started else space
                    elif iix >= len(empty_bottom) or empty_bottom[iix]:
                        if started:
                            cur_connect = cont_left
                        else:
                            cur_connect = start_left
                        if len(bottom_gaps) < len(top_gaps):
                            break
                        started = True
                    else:
                        if started:
                            cur_connect = cont_skip
                        else:
                            cur_connect = cont_pipe
                    cur_line = cont_skip if started else space
                    gap_size = top_gap - len(cur_connect)
                    line_indents.append(f"{cur_connect}{cur_line * gap_size}")
                    highest_iix = iix
                if line_indents:
                    line_indents[-1] = line_indents[-1][:-len(indent)]
                if nodes_only:
                    mid = f" {node.get_short_status(allow_unicode)} "
                else:
                    mid = \
                        f"{node.get_short_status(allow_unicode)} " \
                        f"{node.get_name()}({node.get_type()}) " \
                        f"{node.get_highest_chunk()}"
                if len(mid) < prefix_len:
                    mid = f"{mid}{space * (prefix_len - len(mid))}"
                content = f"{vend if started else vsec}{mid}{vsec}"
                node_line = f"{''.join(line_indents)}{content}"
                top_indents: List[str] = []
                bottom_indents: List[str] = []
                for iix in range(highest_iix + 1):
                    top_connect = space if empty_top[iix] else cont_pipe
                    has_bottom = iix >= len(empty_bottom) or empty_bottom[iix]
                    bottom_connect = space if has_bottom else cont_pipe
                    top_gap_size = top_gaps[iix] - len(top_connect)
                    bottom_gap_size = top_gaps[iix] - len(bottom_connect)
                    if not top_indents:
                        top_indents.append(indent)
                    top_indents.append(f"{top_connect}{space * top_gap_size}")
                    if not bottom_indents:
                        bottom_indents.append(indent)
                    bottom_indents.append(
                        f"{bottom_connect}{space * bottom_gap_size}")
                if top_indents:
                    top_indents[-1] = top_indents[-1][:-len(indent)]
                if bottom_indents:
                    bottom_indents[-1] = bottom_indents[-1][:-len(indent)]
                border_len = len(content) - len(tl) - len(tr)
                top_border: List[str] = [tl] + [hsec] * border_len + [tr]
                top_ix = len(indent)
                for iix in range(highest_iix + 1, len(same_ids)):
                    if top_ix >= len(top_border):
                        break
                    if same_ids[iix]:
                        top_border[top_ix] = conn_top
                    if iix >= len(top_gaps):
                        break
                    top_ix += top_gaps[iix]
                bottom_border: List[str] = [bl] + [hsec] * border_len + [br]
                bottom_ix = len(indent)
                for iix in range(highest_iix + 1, len(new_bottom)):
                    if bottom_ix >= len(bottom_border):
                        break
                    if new_bottom[iix]:
                        bottom_border[bottom_ix] = conn_bottom
                    if iix >= len(bottom_gaps):
                        break
                    bottom_ix += bottom_gaps[iix]
                node_top = f"{''.join(top_indents)}{''.join(top_border)}"
                node_bottom = \
                    f"{''.join(bottom_indents)}{''.join(bottom_border)}"
                total_gap_top = sum(top_gaps) - len(node_line)
                total_gap_bottom = sum(bottom_gaps) - len(node_line)
                if total_gap_bottom > total_gap_top:
                    connector = corner_right
                    more_gaps = bottom_gaps
                    top_conn = space
                    bottom_conn = cont_pipe
                else:
                    connector = corner_left
                    more_gaps = top_gaps
                    top_conn = cont_pipe
                    bottom_conn = space
                if total_gap_bottom == total_gap_top:
                    connector = pipe
                    more_gaps = bottom_gaps
                    top_conn = cont_pipe
                    bottom_conn = cont_pipe
                total_gap = max(total_gap_bottom, total_gap_top)
                if total_gap >= -prefix_len:
                    bar_len = total_gap + prefix_len
                    full_bar = list(bar * bar_len)
                    full_top = list(space * bar_len)
                    full_bottom = list(space * bar_len)
                    bar_ix = prefix_len - len(node_line)
                    for (before_gap_ix, bar_gap) in enumerate(more_gaps):
                        bar_ix += bar_gap
                        if bar_ix < 0:
                            continue
                        if bar_ix >= len(full_bar):
                            break
                        gap_ix = before_gap_ix + 1
                        if gap_ix < len(same_ids) and not same_ids[gap_ix]:
                            mid_connector = cont_skip
                            mid_top = cont_pipe
                            mid_bottom = cont_pipe
                        else:
                            mid_connector = cont
                            mid_top = cont_pipe
                            mid_bottom = cont_pipe
                        adj_ix = bar_ix - prefix_len
                        if total_gap_bottom >= adj_ix > total_gap_top:
                            mid_connector = cont_right
                            mid_top = space
                        elif total_gap_bottom < adj_ix <= total_gap_top:
                            if not empty_top[gap_ix]:
                                mid_connector = cont_left
                            else:
                                mid_top = space
                            mid_bottom = space
                        full_bar[bar_ix] = mid_connector
                        full_top[bar_ix] = mid_top
                        full_bottom[bar_ix] = mid_bottom
                    node_line = \
                        f"{node_line[:-len(vsec)]}{vstart}" \
                        f"{''.join(full_bar)}{connector}"
                    node_top = f"{node_top}{''.join(full_top)}{top_conn}"
                    node_bottom = \
                        f"{node_bottom}{''.join(full_bottom)}{bottom_conn}"
                lines.append(node_top.rstrip())
                lines.append(node_line)
                lines.append(node_bottom.rstrip())
                out_line = out_line.rstrip()
                if out_line:
                    lines.append(f"{indent}{out_line}")
            return lines

        return "\n".join(draw())

    def get_def(
            self,
            full: bool = True,
            warnings_io: Optional[IO[Any]] = sys.stderr) -> PipelineDef:
        res = cast(PipelineDef, self._client._request_json(
            METHOD_GET, "/pipeline_def", {
                "pipeline": self.get_id(),
                "full": 1 if full else 0,
            }))
        # look for warnings

        def s3_warnings(
                kind: str,
                settings: Dict[str, Dict[str, Any]],
                warnings: List[str]) -> None:
            s3_settings = settings.get(kind, {})
            for (key, s3_setting) in s3_settings.items():
                warnings.extend((
                    f"{kind}:{key}: {warn}"
                    for warn in s3_setting.get("warnings", [])
                ))

        if warnings_io is not None:
            settings = res.get("settings", {})
            warnings: List[str] = []
            s3_warnings("s3", settings, warnings)
            s3_warnings("triton", settings, warnings)
            if len(warnings) > 1:
                warnings_io.write(
                    f"{len(warnings)} warnings while "
                    f"reconstructing settings:\n")
            elif len(warnings) == 1:
                warnings_io.write(
                    "Warning while reconstructing settings:\n")
            for warn in warnings:
                warnings_io.write(f"{warn}\n")
            if warnings:
                warnings_io.flush()
        return res

    def set_attr(
            self,
            attr: str,
            value: Any) -> None:
        pipe_def = self.get_def()
        pipe_def[attr] = value  # type: ignore
        self._client.set_pipeline(self.get_id(), pipe_def)

    def set_name(self, value: str) -> None:
        self.set_attr("name", value)

    def set_company(self, value: str) -> None:
        self.set_attr("company", value)

    def set_state(self, value: str) -> None:
        self.set_attr("state", value)

    def set_high_priority(self, value: bool) -> None:
        self.set_attr("high_priority", value)

    def set_queue_mng(self, value: Optional[str]) -> None:
        self.set_attr("queue_mng", value)

    def get_visible_blobs(self) -> List[str]:
        return [
            res[0]
            for res in self.get_visible_blob_times(retrieve_times=False)[1]
        ]

    def get_visible_blob_ages(self) -> List[Tuple[str, str]]:
        cur_time, visible = self.get_visible_blob_times(retrieve_times=True)
        return [
            (blob_id, get_age(cur_time, blob_time))
            for (blob_id, blob_time) in sorted(visible, key=lambda el: (
                safe_opt_num(el[1]), el[0]))
        ]

    def get_visible_blob_times(self, retrieve_times: bool) -> Tuple[
            float, List[Tuple[str, Optional[float]]]]:
        res = cast(VisibleBlobs, self._client._request_json(
            METHOD_GET, "/visible_blobs", {
                "pipeline": self.get_id(),
                "retrieve_times": int(retrieve_times),
            }))
        return res["cur_time"], res["visible"]

    @overload
    def check_queue_stats(  # pylint: disable=no-self-use
            self, minimal: Literal[True]) -> MinimalQueueStatsResponse:
        ...

    @overload
    def check_queue_stats(  # pylint: disable=no-self-use
            self, minimal: Literal[False]) -> QueueStatsResponse:
        ...

    @overload
    def check_queue_stats(  # pylint: disable=no-self-use
            self,
            minimal: bool) -> Union[
                MinimalQueueStatsResponse, QueueStatsResponse]:
        ...

    def check_queue_stats(self, minimal: bool) -> Union[
            MinimalQueueStatsResponse, QueueStatsResponse]:
        pipe_id: Optional[str] = self.get_id()
        return self._client.check_queue_stats(pipe_id, minimal=minimal)

    def scale_worker(self, replicas: int) -> bool:
        return cast(WorkerScale, self._client._request_json(
            METHOD_PUT, "/worker", {
                "pipeline": self.get_id(),
                "replicas": replicas,
                "task": None,
            }))["success"]

    def reload(self, timestamp: Optional[float] = None) -> float:
        return cast(PipelineReload, self._client._request_json(
            METHOD_PUT, "/pipeline_reload", {
                "pipeline": self.get_id(),
                "timestamp": timestamp,
            }))["when"]

    def set_kafka_topic_partitions(
            self,
            num_partitions: int,
            large_input_retention: bool = False) -> KafkaTopics:
        return cast(KafkaTopics, self._client._request_json(
            METHOD_POST, "/kafka_topics", {
                "pipeline": self.get_id(),
                "num_partitions": num_partitions,
                "large_input_retention": large_input_retention,
            }))

    def post_kafka_objs(self, input_objs: List[Any]) -> List[str]:
        bios = [
            BytesIO(json.dumps(
                input_obj,
                separators=(",", ":"),
                indent=None,
                sort_keys=True).encode("utf-8"))
            for input_obj in input_objs
        ]
        return self.post_kafka_msgs(bios)

    def post_kafka_msgs(self, input_data: List[BytesIO]) -> List[str]:
        names = [f"file{pos}" for pos in range(len(input_data))]
        res = cast(KafkaMessage, self._client._request_json(
            METHOD_FILE, "/kafka_msg", {
                "pipeline": self._pipe_id,
            }, files=dict(zip(names, input_data))))
        msgs = res["messages"]
        return [msgs[key] for key in names]

    def read_kafka_output(
            self,
            offset: str = "current",
            max_rows: int = 100) -> Optional[ByteResponse]:
        offset_str = [offset]

        def read_single() -> Tuple[ByteResponse, str]:
            cur, read_ctype = self._client.request_bytes(
                METHOD_GET, "/kafka_msg", {
                    "pipeline": self.get_id(),
                    "offset": offset_str[0],
                })
            offset_str[0] = "current"
            return interpret_ctype(cur, read_ctype), read_ctype

        if max_rows <= 1:
            return read_single()[0]

        res: List[ByteResponse] = []
        ctype: Optional[str] = None
        while True:
            val, cur_ctype = read_single()
            if val is None:
                break
            if ctype is None:
                ctype = cur_ctype
            elif ctype != cur_ctype:
                raise ValueError(
                    f"inconsistent return types {ctype} != {cur_ctype}")
            res.append(val)
            if len(res) >= max_rows:
                break
        if not res or ctype is None:
            return None
        return merge_ctype(res, ctype)

    def get_kafka_offsets(self, alive: bool) -> KafkaOffsets:
        return cast(KafkaOffsets, self._client._request_json(
            METHOD_GET, "/kafka_offsets", {
                "pipeline": self._pipe_id,
                "alive": int(alive),
            }))

    def get_kafka_throughput(
            self,
            segment_interval: float = 120.0,
            segments: int = 5) -> KafkaThroughput:
        assert segments > 0
        assert segment_interval > 0.0
        offsets = self.get_kafka_offsets(alive=False)
        now = time.monotonic()
        measurements: List[Tuple[int, int, int, float]] = [(
            offsets["input"],
            offsets["output"],
            offsets["error"],
            now,
        )]
        for _ in range(segments):
            prev = now
            while now - prev < segment_interval:
                time.sleep(max(0.0, segment_interval - (now - prev)))
                now = time.monotonic()
            offsets = self.get_kafka_offsets(alive=False)
            measurements.append((
                offsets["input"],
                offsets["output"],
                offsets["error"],
                now,
            ))
        first = measurements[0]
        last = measurements[-1]
        total_input = last[0] - first[0]
        total_output = last[1] - first[1]
        errors = last[2] - first[2]
        total = last[3] - first[3]
        input_segments: List[float] = []
        output_segments: List[float] = []
        cur_input = first[0]
        cur_output = first[1]
        cur_time = first[3]
        for (next_input, next_output, _, next_time) in measurements[1:]:
            seg_time = next_time - cur_time
            input_segments.append((next_input - cur_input) / seg_time)
            output_segments.append((next_output - cur_output) / seg_time)
            cur_input = next_input
            cur_output = next_output
            cur_time = next_time
        inputs = pd.Series(input_segments)
        outputs = pd.Series(output_segments)
        return {
            "pipeline": self._pipe_id,
            "input": {
                "throughput": total_input / total,
                "max": inputs.max(),
                "min": inputs.min(),
                "stddev": inputs.std(),
                "segments": segments,
                "count": total_input,
                "total": total,
            },
            "output": {
                "throughput": total_output / total,
                "max": outputs.max(),
                "min": outputs.min(),
                "stddev": outputs.std(),
                "segments": segments,
                "count": total_output,
                "total": total,
            },
            "faster": "both" if total_input == total_output else (
                "input" if total_input > total_output else "output"),
            "errors": errors,
        }

    def get_kafka_group(self) -> KafkaGroup:
        return cast(KafkaGroup, self._client._request_json(
            METHOD_GET, "/kafka_group", {
                "pipeline": self._pipe_id,
            }))

    def set_kafka_group(
            self,
            group_id: Optional[str] = None,
            reset: Optional[str] = None,
            **kwargs: Any) -> KafkaGroup:
        return cast(KafkaGroup, self._client._request_json(
            METHOD_PUT, "/kafka_group", {
                "pipeline": self._pipe_id,
                "group_id": group_id,
                "reset": reset,
                **kwargs,
            }))

    def __hash__(self) -> int:
        return hash(self._pipe_id)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return self.get_id() == other.get_id()

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def __str__(self) -> str:
        return self._pipe_id

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}[{self._pipe_id}]"

# *** PipelineHandle ***


class NodeHandle:
    def __init__(
            self,
            client: XYMEClientV3,
            pipeline: PipelineHandle,
            node_id: str,
            node_name: str,
            kind: str) -> None:
        self._client = client
        self._pipeline = pipeline
        self._node_id = node_id
        self._node_name = node_name
        self._type = kind
        self._blobs: Dict[str, BlobHandle] = {}
        self._inputs: Dict[str, Tuple[str, str]] = {}
        self._state: Optional[int] = None
        self._config_error: Optional[str] = None

    @staticmethod
    def from_node_info(
            client: XYMEClientV3,
            pipeline: PipelineHandle,
            node_info: NodeInfo,
            prev: Optional['NodeHandle']) -> 'NodeHandle':
        if prev is None:
            res = NodeHandle(
                client,
                pipeline,
                node_info["id"],
                node_info["name"],
                node_info["type"])
        else:
            if prev.get_pipeline() != pipeline:
                raise ValueError(f"{prev.get_pipeline()} != {pipeline}")
            res = prev
        res.update_info(node_info)
        return res

    def update_info(self, node_info: NodeInfo) -> None:
        if self._node_id != node_info["id"]:
            raise ValueError(f"{self._node_id} != {node_info['id']}")
        self._node_name = node_info["name"]
        self._type = node_info["type"]
        self._blobs = {
            key: BlobHandle(
                self._client,
                value,
                is_full=False,
                pipeline=self.get_pipeline())
            for (key, value) in node_info["blobs"].items()
        }
        self._inputs = node_info["inputs"]
        self._state = node_info["state"]
        self._config_error = node_info["config_error"]

    def get_pipeline(self) -> PipelineHandle:
        return self._pipeline

    def get_id(self) -> str:
        return self._node_id

    def get_name(self) -> str:
        return self._node_name

    def get_type(self) -> str:
        return self._type

    def get_node_def(self) -> NodeDefInfo:
        return self._client.get_node_defs()[self.get_type()]

    def get_inputs(self) -> Set[str]:
        return set(self._inputs.keys())

    def get_input(self, key: str) -> Tuple['NodeHandle', str]:
        node_id, out_key = self._inputs[key]
        return self.get_pipeline().get_node(node_id), out_key

    def get_status(self) -> TaskStatus:
        return cast(NodeStatus, self._client._request_json(
            METHOD_GET, "/node_status", {
                "pipeline": self.get_pipeline().get_id(),
                "node": self.get_id(),
            }))["status"]

    def has_config_error(self) -> bool:
        return self._config_error is not None

    def get_config_error(self) -> Optional[str]:
        return self._config_error

    def get_blobs(self) -> List[str]:
        return sorted(self._blobs.keys())

    def get_blob_handles(self) -> Dict[str, 'BlobHandle']:
        return self._blobs

    def get_blob_handle(self, key: str) -> 'BlobHandle':
        return self._blobs[key]

    def set_blob_uri(self, key: str, blob_uri: str) -> str:
        return cast(PutNodeBlob, self._client._request_json(
            METHOD_PUT, "/node_blob", {
                "pipeline": self.get_pipeline().get_id(),
                "node": self.get_id(),
                "blob_key": key,
                "blob_uri": blob_uri,
            }))["new_uri"]

    def get_in_cursor_states(self) -> Dict[str, int]:
        return cast(InCursors, self._client._request_json(
            METHOD_GET, "/node_in_cursors", {
                "pipeline": self.get_pipeline().get_id(),
                "node": self.get_id(),
            }))["cursors"]

    def get_highest_chunk(self) -> int:
        return cast(NodeChunk, self._client._request_json(
            METHOD_GET, "/node_chunk", {
                "pipeline": self.get_pipeline().get_id(),
                "node": self.get_id(),
            }))["chunk"]

    def get_short_status(self, allow_unicode: bool = True) -> str:
        status_map: Dict[TaskStatus, str] = {
            "blocked": "B",
            "waiting": "W",
            "running": "→" if allow_unicode else "R",
            "complete": "✓" if allow_unicode else "C",
            "eos": "X",
            "paused": "P",
            "error": "!",
            "unknown": "?",
            "virtual": "∴" if allow_unicode else "V",
            "queue": "=",
        }
        return status_map[self.get_status()]

    def get_logs(self) -> str:
        with self._client._raw_request_str(
                METHOD_GET, "/node_logs", {
                    "pipeline": self.get_pipeline().get_id(),
                    "node": self.get_id(),
                }) as fin:
            return fin.read()

    def get_timing(self) -> List[Timing]:
        return cast(Timings, self._client._request_json(
            METHOD_GET, "/node_perf", {
                "pipeline": self.get_pipeline().get_id(),
                "node": self.get_id(),
            }))["times"]

    def read_blob(
            self,
            key: str,
            chunk: Optional[int],
            force_refresh: bool) -> 'BlobHandle':
        res = cast(ReadNode, self._client._request_json(
            METHOD_LONGPOST, "/read_node", {
                "pipeline": self.get_pipeline().get_id(),
                "node": self.get_id(),
                "key": key,
                "chunk": chunk,
                "is_blocking": True,
                "force_refresh": force_refresh,
            }))
        uri = res["result_uri"]
        if uri is None:
            raise ValueError(f"uri is None: {res}")
        return BlobHandle(
            self._client,
            uri,
            is_full=True,
            pipeline=self.get_pipeline())

    def read(
            self,
            key: str,
            chunk: Optional[int],
            force_refresh: bool = False) -> Optional[ByteResponse]:
        return self.read_blob(key, chunk, force_refresh).get_content()

    def read_all(
            self,
            key: str,
            force_refresh: bool = False) -> Optional[ByteResponse]:
        self.read(key, chunk=None, force_refresh=force_refresh)
        res: List[ByteResponse] = []
        ctype: Optional[str] = None
        while True:
            blob = self.read_blob(key, chunk=len(res), force_refresh=False)
            cur = blob.get_content()
            if cur is None:
                break
            cur_ctype = blob.get_ctype()
            if ctype is None:
                ctype = cur_ctype
            elif ctype != cur_ctype:
                raise ValueError(
                    f"inconsistent return types {ctype} != {cur_ctype}")
            res.append(cur)
        if not res or ctype is None:
            return None
        return merge_ctype(res, ctype)

    def clear(self) -> NodeState:
        return cast(NodeState, self._client._request_json(
            METHOD_PUT, "/node_state", {
                "pipeline": self.get_pipeline().get_id(),
                "node": self.get_id(),
                "action": "reset",
            }))

    def requeue(self) -> NodeState:
        return cast(NodeState, self._client._request_json(
            METHOD_PUT, "/node_state", {
                "pipeline": self.get_pipeline().get_id(),
                "node": self.get_id(),
                "action": "requeue",
            }))

    def fix_error(self) -> NodeState:
        return cast(NodeState, self._client._request_json(
            METHOD_PUT, "/node_state", {
                "pipeline": self.get_pipeline().get_id(),
                "node": self.get_id(),
                "action": "fix_error",
            }))

    def get_csv_blob(self) -> 'CSVBlobHandle':
        if self.get_type() != "csv_reader":
            raise ValueError("node doesn't have csv blob")
        res = cast(CSVBlobResponse, self._client._request_json(
            METHOD_GET, "/csv_blob", {
                "pipeline": self.get_pipeline().get_id(),
                "node": self.get_id(),
            }))
        return CSVBlobHandle(
            self._client,
            self.get_pipeline(),
            res["csv"],
            res["count"],
            res["pos"],
            res["tmp"])

    def get_json_blob(self) -> 'JSONBlobHandle':
        if self.get_type() != "jsons_reader":
            raise ValueError(
                f"can not append jsons to {self}, expected 'jsons_reader'")
        res = cast(JSONBlobResponse, self._client._request_json(
            METHOD_GET, "/json_blob", {
                "pipeline": self.get_pipeline().get_id(),
                "node": self.get_id(),
            }))
        return JSONBlobHandle(
            self._client,
            self.get_pipeline(),
            res["json"],
            res["count"])

    def check_custom_code_node(self) -> None:
        if not self.get_type() in CUSTOM_NODE_TYPES:
            raise ValueError(f"{self} is not a custom code node.")

    def set_custom_imports(
            self, modules: List[List[str]]) -> CustomImportsResponse:
        self.check_custom_code_node()
        return cast(CustomImportsResponse, self._client._request_json(
            METHOD_PUT, "/custom_imports", {
                "pipeline": self.get_pipeline().get_id(),
                "node": self.get_id(),
                "modules": modules,
            }))

    def get_custom_imports(self) -> CustomImportsResponse:
        self.check_custom_code_node()
        return cast(CustomImportsResponse, self._client._request_json(
            METHOD_GET, "/custom_imports", {
                "pipeline": self.get_pipeline().get_id(),
                "node": self.get_id(),
            }))

    def set_es_query(self, query: Dict[str, Any]) -> ESQueryResponse:
        if self.get_type() != "es_reader":
            raise ValueError(f"{self} is not a es reader node")

        return cast(ESQueryResponse, self._client._request_json(
            METHOD_POST, "/es_query", {
                "pipeline": self.get_pipeline().get_id(),
                "blob": self.get_blob_handle("es").get_uri(),
                "es_query": query,
            },
        ))

    def get_es_query(self) -> ESQueryResponse:
        if self.get_type() != "es_reader":
            raise ValueError(f"{self} is not a es reader node")

        return cast(ESQueryResponse, self._client._request_json(
            METHOD_GET, "/es_query", {
                "pipeline": self.get_pipeline().get_id(),
                "blob": self.get_blob_handle("es").get_uri(),
            },
        ))

    def set_custom_code(self, func: FUNC) -> CustomCodeResponse:
        from RestrictedPython import compile_restricted

        self.check_custom_code_node()

        def fn_as_str(fun: FUNC) -> str:
            body = textwrap.dedent(inspect.getsource(fun))
            res = body + textwrap.dedent(f"""
            result = {fun.__name__}(*data)
            if result is None:
                raise ValueError("{fun.__name__} must return a value")
            """)
            compile_restricted(res, "inline", "exec")
            return res

        raw_code = fn_as_str(func)
        return cast(CustomCodeResponse, self._client._request_json(
            METHOD_PUT, "/custom_code", {
                "pipeline": self.get_pipeline().get_id(),
                "node": self.get_id(),
                "code": raw_code,
            }))

    def get_custom_code(self) -> CustomCodeResponse:
        self.check_custom_code_node()
        return cast(CustomCodeResponse, self._client._request_json(
            METHOD_GET, "/custom_code", {
                "pipeline": self.get_pipeline().get_id(),
                "node": self.get_id(),
            }))

    def get_user_columns(self, key: str) -> UserColumnsResponse:
        return cast(UserColumnsResponse, self._client._request_json(
            METHOD_GET, "/user_columns", {
                "pipeline": self.get_pipeline().get_id(),
                "node": self.get_id(),
                "key": key,
            }))

    def get_input_example(self) -> Dict[str, Optional[ByteResponse]]:
        if self.get_type() != "custom_data":
            raise ValueError(
                "can only load example input data for 'custom' node")
        res = {}
        for key in self.get_inputs():
            input_node, out_key = self.get_input(key)
            df = input_node.read(out_key, 0)
            if df is not None and isinstance(df, pd.DataFrame):
                user_columns = \
                    input_node.get_user_columns(out_key)["user_columns"]
                rmap = {col: col.replace("user_", "") for col in user_columns}
                df = df.loc[:, user_columns].rename(columns=rmap)
            res[key] = df
        return res

    def setup_model(self, obj: Dict[str, Any]) -> Any:
        if self.get_type() not in MODEL_NODE_TYPES:
            raise ValueError(f"{self} is not a model node")
        model_type: str
        if self.get_type() in EMBEDDING_MODEL_NODE_TYPES:
            model_type = "embedding"

        return cast(ModelSetupResponse, self._client._request_json(
            METHOD_PUT, "/model_setup", {
                "pipeline": self.get_pipeline().get_id(),
                "node": self.get_id(),
                "config": obj,
                "model_type": model_type,
            }))

    def get_model_params(self) -> Any:
        return cast(ModelParamsResponse, self._client._request_json(
            METHOD_GET, "/model_params", {
                "pipeline": self.get_pipeline().get_id(),
                "node": self.get_id(),
            }))

    def get_def(self) -> NodeDef:
        return cast(NodeDef, self._client._request_json(
            METHOD_GET, "/node_def", {
                "pipeline": self.get_pipeline().get_id(),
                "node": self.get_id(),
            }))

    def __hash__(self) -> int:
        return hash(self._node_id)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return self.get_id() == other.get_id()

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def __str__(self) -> str:
        return self._node_id

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}[{self._node_id}]"

# *** NodeHandle ***


EMPTY_BLOB_PREFIX = "null://"


class BlobHandle:
    def __init__(
            self,
            client: XYMEClientV3,
            uri: str,
            is_full: bool,
            pipeline: PipelineHandle) -> None:
        self._client = client
        self._uri = uri
        self._is_full = is_full
        self._pipeline = pipeline
        self._ctype: Optional[str] = None

    def is_full(self) -> bool:
        return self._is_full

    def is_empty(self) -> bool:
        return self._uri.startswith(EMPTY_BLOB_PREFIX)

    def get_uri(self) -> str:
        return self._uri

    def get_pipeline(self) -> PipelineHandle:
        return self._pipeline

    def get_ctype(self) -> Optional[str]:
        return self._ctype

    def get_content(self) -> Optional[ByteResponse]:
        if not self.is_full():
            raise ValueError(f"URI must be full: {self}")
        if self.is_empty():
            return None
        fin, ctype = self._client._raw_request_bytes(METHOD_POST, "/uri", {
            "uri": self._uri,
            "pipeline": self.get_pipeline().get_id(),
        })
        self._ctype = ctype
        return interpret_ctype(fin, ctype)

    def list_files(self) -> List['BlobHandle']:
        if self.is_full():
            raise ValueError(f"URI must not be full: {self}")
        resp = self._client._request_json(
            METHOD_GET, "/blob_files", {
                "blob": self._uri,
                "pipeline": self.get_pipeline().get_id(),
            })
        return [
            BlobHandle(
                self._client,
                blob_uri,
                is_full=True,
                pipeline=self._pipeline)
            for blob_uri in resp["files"]
        ]

    def as_str(self) -> str:
        return f"{self.get_uri()}"

    def set_owner(self, new_owner: str) -> str:
        if self.is_full():
            raise ValueError(f"URI must not be full: {self}")
        pipe = self.get_pipeline()
        res = cast(BlobOwner, self._client._request_json(
            METHOD_PUT, "/blob_owner", {
                "pipeline": pipe.get_id(),
                "blob": self._uri,
                "owner": new_owner,
            }))
        return res["owner"]

    def get_owner(self) -> str:
        if self.is_full():
            raise ValueError(f"URI must not be full: {self}")
        pipe = self.get_pipeline()
        res = cast(BlobOwner, self._client._request_json(
            METHOD_GET, "/blob_owner", {
                "pipeline": pipe.get_id(),
                "blob": self._uri,
            }))
        return res["owner"]

    def copy_to(
            self,
            to_uri: str,
            new_owner: Optional[str] = None) -> 'BlobHandle':
        if self.is_full():
            raise ValueError(f"URI must not be full: {self}")
        pipe = self.get_pipeline()
        res = cast(CopyBlob, self._client._request_json(
            METHOD_POST, "/copy_blob", {
                "pipeline": pipe.get_id(),
                "from_uri": self._uri,
                "owner": new_owner,
                "to_uri": to_uri,
            }))
        return BlobHandle(
            self._client, res["new_uri"], is_full=False, pipeline=pipe)

    def download_zip(self, to_path: Optional[str]) -> Optional[io.BytesIO]:
        if self.is_full():
            raise ValueError(f"URI must not be full: {self}")
        cur_res, _ = self._client._raw_request_bytes(
            METHOD_GET, "/download_zip", {
                "blob": self._uri,
                "pipeline": self.get_pipeline().get_id(),
            })
        if to_path is None:
            return io.BytesIO(cur_res.read())
        with open(to_path, "wb") as file_download:
            file_download.write(cur_res.read())
        return None

    def upload_zip(
            self, source: Union[str, io.BytesIO]) -> List['BlobHandle']:
        if isinstance(source, str) or not hasattr(source, "read"):
            with open(f"{source}", "rb") as fin:
                zip_stream = io.BytesIO(fin.read())
        else:
            zip_stream = source

        resp = self._client._request_json(
            METHOD_FILE, "/upload_zip", {
                "blob": self._uri,
                "pipeline": self.get_pipeline().get_id(),
            }, files={
                "file": zip_stream,
            })
        return [
            BlobHandle(
                self._client,
                blob_uri,
                is_full=True,
                pipeline=self._pipeline)
            for blob_uri in resp["files"]
        ]

    def convert_model(self, reload: bool = True) -> ModelReleaseResponse:
        return cast(ModelReleaseResponse, self._client._request_json(
            METHOD_POST, "/convert_model", {
                "blob": self._uri,
                "pipeline": self.get_pipeline().get_id(),
                "reload": reload,
            }))

    def __hash__(self) -> int:
        return hash(self.as_str())

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return self.as_str() == other.as_str()

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def __str__(self) -> str:
        return self.as_str()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}[{self.as_str()}]"

# *** BlobHandle ***


class CSVBlobHandle(BlobHandle):
    def __init__(
            self,
            client: XYMEClientV3,
            pipe: PipelineHandle,
            uri: str,
            count: int,
            pos: int,
            has_tmp: bool) -> None:
        super().__init__(client, uri, is_full=False, pipeline=pipe)
        self._client = client
        self._pipe = pipe
        self._uri = uri
        self._count = count
        self._pos = pos
        self._has_tmp = has_tmp

    def get_uri(self) -> str:
        return self._uri

    def get_count(self) -> int:
        return self._count

    def get_pos(self) -> int:
        return self._pos

    def has_tmp(self) -> bool:
        return self._has_tmp

    def perform_action(
            self,
            action: str,
            additional: Dict[str, Union[str, int]],
            fobj: Optional[IO[bytes]]) -> int:
        args: Dict[str, Union[str, int]] = {
            "blob": self.get_uri(),
            "action": action,
            "pipeline": self.get_pipeline().get_id(),
        }
        args.update(additional)
        if fobj is not None:
            method = METHOD_FILE
            files: Optional[Dict[str, IO[bytes]]] = {
                "file": fobj,
            }
        else:
            method = METHOD_POST
            files = None
        res = cast(CSVOp, self._client._request_json(
            method, "/csv_action", args, files=files))
        self._count = res["count"]
        self._has_tmp = res["tmp"]
        self._pos = res["pos"]
        return self._pos

    def start_data(self, size: int, hash_str: str, ext: str) -> int:
        return self.perform_action("start", {
            "ext": ext,
            "hash": hash_str,
            "size": size,
        }, None)

    def append_data(self, fobj: IO[bytes]) -> int:
        return self.perform_action("append", {}, fobj)

    def finish_data(self) -> None:
        self.perform_action("finish", {}, None)

    def clear_tmp(self) -> None:
        self.perform_action("clear", {}, None)

    def upload_data(
            self,
            file_content: IO[bytes],
            file_ext: str,
            progress_bar: Optional[IO[Any]] = sys.stdout) -> int:
        init_pos = file_content.seek(0, io.SEEK_CUR)
        file_hash = get_file_hash(file_content)
        total_size = file_content.seek(0, io.SEEK_END) - init_pos
        file_content.seek(init_pos, io.SEEK_SET)
        if progress_bar is not None:
            progress_bar.write("Uploading file:\n")
        print_progress = get_progress_bar(out=progress_bar)
        cur_size = self.start_data(total_size, file_hash, file_ext)
        while True:
            print_progress(cur_size / total_size, False)
            buff = file_content.read(get_file_upload_chunk_size())
            if not buff:
                break
            new_size = self.append_data(BytesIO(buff))
            if new_size - cur_size != len(buff):
                raise ValueError(
                    f"incomplete chunk upload n:{new_size} "
                    f"o:{cur_size} b:{len(buff)}")
            cur_size = new_size
        print_progress(cur_size / total_size, True)
        self.finish_data()
        return cur_size

    def add_from_file(
            self,
            filename: str,
            progress_bar: Optional[IO[Any]] = sys.stdout) -> None:
        fname = filename
        if filename.endswith(INPUT_ZIP_EXT):
            fname = filename[:-len(INPUT_ZIP_EXT)]
        ext_pos = fname.rfind(".")
        if ext_pos >= 0:
            ext = filename[ext_pos + 1:]  # full filename
        else:
            raise ValueError("could not determine extension")
        with open(filename, "rb") as fbuff:
            self.upload_data(fbuff, ext, progress_bar)

    def add_from_df(
            self,
            df: pd.DataFrame,
            progress_bar: Optional[IO[Any]] = sys.stdout) -> None:
        io_in = None
        try:
            io_in = df_to_csv(df)
            self.upload_data(io_in, "csv", progress_bar)
        finally:
            if io_in is not None:
                io_in.close()

# *** CSVBlobHandle ***


class JSONBlobHandle(BlobHandle):
    def __init__(
            self,
            client: XYMEClientV3,
            pipe: PipelineHandle,
            uri: str,
            count: int) -> None:
        super().__init__(client, uri, is_full=False, pipeline=pipe)
        self._client = client
        self._pipe = pipe
        self._uri = uri
        self._count = count

    def get_uri(self) -> str:
        return self._uri

    def get_count(self) -> int:
        return self._count

    def append_jsons(self, jsons: List[Any]) -> 'JSONBlobHandle':
        res = self._client._request_json(
            METHOD_PUT, "/json_append", {
                "pipeline": self.get_pipeline().get_id(),
                "blob": self.get_uri(),
                "jsons": jsons,
            })
        self._count = res["count"]
        return self

# *** JSONBlobHandle ***


class ComputationHandle:
    def __init__(
            self,
            pipeline: PipelineHandle,
            data_id: str,
            get_dyn_error: Callable[[], Optional[str]],
            set_dyn_error: Callable[[str], None]) -> None:
        self._pipeline = pipeline
        self._data_id = data_id
        self._value: Optional[ByteResponse] = None
        self._get_dyn_error = get_dyn_error
        self._set_dyn_error = set_dyn_error

    def has_fetched(self) -> bool:
        return self._value is not None

    def get(self) -> ByteResponse:
        try:
            if self._value is None:
                self._value = self._pipeline.get_dynamic_result(self._data_id)
            return self._value
        except ServerSideError as e:
            if self._get_dyn_error() is None:
                self._set_dyn_error(str(e))
            raise e
        except KeyError as e:
            maybe_error = self._get_dyn_error()
            if maybe_error is not None:
                raise ServerSideError(maybe_error) from e
            raise e

    def get_id(self) -> str:
        return self._data_id

    def __str__(self) -> str:
        value = self._value
        if value is None:
            return f"data_id={self._data_id}"
        return f"value({type(value)})={value}"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}[{self.__str__()}]"

    def __hash__(self) -> int:
        return hash(self.get_id())

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return self.get_id() == other.get_id()

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)


# *** ComputationHandle ***


def create_xyme_client_v3(
        url: str, token: Optional[str] = None) -> XYMEClientV3:
    return XYMEClientV3(url, token)
