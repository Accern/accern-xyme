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
import quick_server
import requests
from requests.exceptions import HTTPError, RequestException
from typing_extensions import Literal

from .util import (
    async_compute,
    ByteResponse,
    df_to_csv,
    get_file_hash,
    get_file_upload_chunk_size,
    get_max_retry,
    get_progress_bar,
    get_retry_sleep,
    interpret_ctype,
    merge_ctype,
    ServerSideError,
)
from .types import (
    BlobInit,
    CSVBlobResponse,
    CSVList,
    CSVOp,
    CustomCodeResponse,
    CustomImportsResponse,
    DynamicResults,
    DynamicStatusResponse,
    FlushAllQueuesResponse,
    InCursors,
    InstanceStatus,
    JobInfo,
    JobList,
    JSONBlobResponse,
    MaintenanceResponse,
    MinimalQueueStatsResponse,
    ModelParamsResponse,
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
    PutNodeBlob,
    QueueStatsResponse,
    QueueStatus,
    ReadNode,
    TaskStatus,
    Timing,
    TimingResult,
    Timings,
    UserColumnsResponse,
    VersionResponse,
)

if TYPE_CHECKING:
    WVD = weakref.WeakValueDictionary[str, 'PipelineHandle']
else:
    WVD = weakref.WeakValueDictionary


__version__ = "0.1.4"
# FIXME: async calls, documentation, auth, summary – time it took etc.


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


class XYMEClient:
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
        while True:
            try:
                return self._fallible_raw_request_bytes(
                    method, path, args, files, add_prefix, api_version)
            except RequestException:
                if retry >= get_max_retry():
                    raise
                if not reset_files():
                    raise
                time.sleep(get_retry_sleep())
            except AccessDenied as adex:
                if not reset_files():
                    raise ValueError(
                        "cannot reset file buffers for retry") from adex
                raise adex
            retry += 1

    def _raw_request_str(
            self,
            method: str,
            path: str,
            args: Dict[str, Any],
            add_prefix: bool = True,
            api_version: Optional[int] = None) -> TextIO:
        retry = 0
        while True:
            try:
                return self._fallible_raw_request_str(
                    method, path, args, add_prefix, api_version)
            except RequestException:
                if retry >= get_max_retry():
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
        while True:
            try:
                return self._fallible_raw_request_json(
                    method, path, args, add_prefix, files, api_version)
            except RequestException:
                if retry >= get_max_retry():
                    raise
                if not reset_files():
                    raise
                time.sleep(get_retry_sleep())
            except AccessDenied as adex:
                if not reset_files():
                    raise ValueError(
                        "cannot reset file buffers for retry") from adex
                raise adex
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
        if method == METHOD_GET:
            req = requests.get(url, params=args, headers=headers)
            if req.status_code == 403:
                raise AccessDenied(req.text)
            req.raise_for_status()
            return BytesIO(req.content), req.headers["content-type"]
        if method == METHOD_POST:
            req = requests.post(url, json=args, headers=headers)
            if req.status_code == 403:
                raise AccessDenied(req.text)
            req.raise_for_status()
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
            if req.status_code == 403:
                raise AccessDenied(req.text)
            req.raise_for_status()
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
        if method == METHOD_GET:
            req = requests.get(url, params=args, headers=headers)
            if req.status_code == 403:
                raise AccessDenied(req.text)
            req.raise_for_status()
            return StringIO(req.text)
        if method == METHOD_POST:
            req = requests.post(url, json=args, headers=headers)
            if req.status_code == 403:
                raise AccessDenied(req.text)
            req.raise_for_status()
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
        try:
            if method == METHOD_GET:
                req = requests.get(url, params=args, headers=headers)
                if req.status_code == 403:
                    raise AccessDenied(req.text)
                req.raise_for_status()
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
                if req.status_code == 403:
                    raise AccessDenied(req.text)
                req.raise_for_status()
                return json.loads(req.text)
            if method == METHOD_POST:
                req = requests.post(url, json=args, headers=headers)
                if req.status_code == 403:
                    raise AccessDenied(req.text)
                req.raise_for_status()
                return json.loads(req.text)
            if method == METHOD_PUT:
                req = requests.put(url, json=args, headers=headers)
                if req.status_code == 403:
                    raise AccessDenied(req.text)
                req.raise_for_status()
                return json.loads(req.text)
            if method == METHOD_DELETE:
                req = requests.delete(url, json=args, headers=headers)
                if req.status_code == 403:
                    raise AccessDenied(req.text)
                req.raise_for_status()
                return json.loads(req.text)
            if method == METHOD_LONGPOST:
                args["token"] = self._token
                try:
                    return quick_server.worker_request(url, args)
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
            capture_err: bool,
            add_prefix: bool = True,
            files: Optional[Dict[str, IO[bytes]]] = None,
            api_version: Optional[int] = None,
                ) -> Dict[str, Any]:
        res = self._raw_request_json(
            method, path, args, add_prefix, files, api_version)
        if capture_err and "errMessage" in res and res["errMessage"]:
            raise ValueError(res["errMessage"])
        return res

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
            }, capture_err=False))

    def get_maintenance_mode(self) -> MaintenanceResponse:
        return cast(MaintenanceResponse, self._request_json(
            METHOD_GET, "/maintenance", {}, capture_err=False))

    def get_pipelines(self) -> List[str]:
        return cast(PipelineList, self._request_json(
            METHOD_GET, "/pipelines", {}, capture_err=False))["pipelines"]

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
            METHOD_GET, "/node_types", {}, capture_err=False))["info"]
        self._node_defs = res
        return res

    def create_new_blob(self, blob_type: str) -> str:
        return cast(BlobInit, self._request_json(
            METHOD_POST, "/blob_init", {
                "type": blob_type,
            }, capture_err=False))["blob"]

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
            }, capture_err=False))["pipeline"]

    def duplicate_pipeline(
            self, pipe_id: str, dest_id: Optional[str] = None) -> str:
        args = {
            "pipeline": pipe_id,
        }
        if dest_id is not None:
            args["dest"] = dest_id
        return cast(PipelineDupResponse, self._request_json(
            METHOD_POST, "/pipeline_dup", args, capture_err=False))["pipeline"]

    def set_pipeline(
            self,
            pipe_id: str,
            defs: PipelineDef,
            warnings_io: Optional[IO[Any]] = sys.stderr) -> 'PipelineHandle':
        pipe_create = cast(PipelineCreate, self._request_json(
            METHOD_POST, "/pipeline_create", {
                "pipeline": pipe_id,
                "defs": defs,
            }, capture_err=True))
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
            warnings_io.flush()
        return self.get_pipeline(pipe_id)

    def update_settings(
            self, pipe_id: str, settings: Dict[str, Any]) -> 'PipelineHandle':
        pipe_id = cast(PipelineCreate, self._request_json(
            METHOD_POST, "/update_pipeline_settings", {
                "pipeline": pipe_id,
                "settings": settings,
            }, capture_err=True))["pipeline"]
        return self.get_pipeline(pipe_id)

    def get_csvs(self) -> List[str]:
        return cast(CSVList, self._request_json(
            METHOD_GET, "/csvs", {
            }, capture_err=False))["csvs"]

    def add_csv(self, csv_blob_id: str) -> List[str]:
        return cast(CSVList, self._request_json(
            METHOD_PUT, "/csvs", {
                "blob": csv_blob_id,
            }, capture_err=False))["csvs"]

    def remove_csv(self, csv_blob_id: str) -> List[str]:
        return cast(CSVList, self._request_json(
            METHOD_DELETE, "/csvs", {
                "blob": csv_blob_id,
            }, capture_err=False))["csvs"]

    def get_jobs(self) -> List[str]:
        return cast(JobList, self._request_json(
            METHOD_GET, "/jobs", {
            }, capture_err=False))["jobs"]

    def remove_job(self, job_id: str) -> List[str]:
        return cast(JobList, self._request_json(
            METHOD_DELETE, "/job", {
                "job": job_id,
            }, capture_err=False))["jobs"]

    def create_job(self) -> JobInfo:
        return cast(JobInfo, self._request_json(
            METHOD_POST, "/job_init", {
            }, capture_err=False))

    def get_job(self, job_id: str) -> JobInfo:
        return cast(JobInfo, self._request_json(
            METHOD_GET, "/job", {
                "job": job_id,
            }, capture_err=False))

    def set_job(self, job: JobInfo) -> JobInfo:
        return cast(JobInfo, self._request_json(
            METHOD_PUT, "/job", {
                "job": job,
            }, capture_err=False))

    def get_allowed_custom_imports(self) -> CustomImportsResponse:
        return cast(CustomImportsResponse, self._request_json(
            METHOD_GET, "/allowed_custom_imports", {}, capture_err=False))

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
            pipeline: Optional[str],
            minimal: bool) -> Union[
                MinimalQueueStatsResponse, QueueStatsResponse]:
        if minimal:
            return cast(MinimalQueueStatsResponse, self._request_json(
                METHOD_GET, "/queue_stats", {
                    "pipeline": pipeline,
                    "minimal": 1,
                }, capture_err=False))
        return cast(QueueStatsResponse, self._request_json(
            METHOD_GET, "/queue_stats", {
                "pipeline": pipeline,
                "minimal": 0,
            }, capture_err=False))

    def get_instance_status(self) -> Dict[InstanceStatus, int]:
        return cast(Dict[InstanceStatus, int], self._request_json(
            METHOD_GET, "/instance_status", {}, capture_err=False))

    def flush_all_queue_data(self) -> None:

        def do_flush() -> bool:
            res = cast(FlushAllQueuesResponse, self._request_json(
                METHOD_POST, "/flush_all_queues", {}, capture_err=False))
            return bool(res["success"])

        while do_flush():  # we flush until there is nothing to flush anymore
            time.sleep(1.0)


# *** XYMEClient ***


class PipelineHandle:
    def __init__(
            self,
            client: XYMEClient,
            pipe_id: str) -> None:
        self._client = client
        self._pipe_id = pipe_id
        self._name: Optional[str] = None
        self._company: Optional[str] = None
        self._state: Optional[str] = None
        self._is_high_priority: Optional[bool] = None
        self._is_parallel: Optional[bool] = None
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
        self._is_parallel = None
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
            }, capture_err=False))

    def _fetch_info(self) -> None:
        info = self.get_info()
        self._name = info["name"]
        self._company = info["company"]
        self._state = info["state"]
        self._is_high_priority = info["high_priority"]
        self._is_parallel = info["is_parallel"]
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

    def is_parallel(self) -> bool:
        self._maybe_refresh()
        self._maybe_fetch()
        assert self._is_parallel is not None
        return self._is_parallel

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

    def dynamic_list(
            self,
            inputs: List[Any],
            input_key: Optional[str],
            output_key: str,
            split_th: Optional[int] = 1000,
            max_threads: int = 50) -> List[Any]:
        if split_th is None or len(inputs) <= split_th:
            res = cast(DynamicResults, self._client._request_json(
                METHOD_POST, "/dynamic_list", {
                    "pipeline": self._pipe_id,
                    "inputs": inputs,
                    "input_key": input_key,
                    "output_key": output_key,
                }, capture_err=True))
            return res["results"]
        # FIXME: write generic spliterator implementation
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
                        max_threads=max_threads)
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
            }, capture_err=True, files=dict(zip(names, input_data)))
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
            }, capture_err=True))
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

    def pretty(self, allow_unicode: bool = True) -> str:
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
                    cur_str = f"| {in_key} ({get_in_state(in_node, in_key)}) "
                    new_edges.append((None, in_key, cur_gap))
                else:
                    cur_str = "|" if in_node is not None else ""
                    cur_gap += gap
                    gap = 0
                    new_edges.append((in_node, in_key, cur_gap))
                segs.append(f"{' ' * prev_gap}{cur_str}")
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
                cur_str = "|" if cur_node is not None else ""
                segs.append(f"{' ' * prev_gap}{cur_str}")
                new_edges.append(edge)
                prev_gap = max(0, cur_gap - len(cur_str))
            sout = sorted(
                outs[node], key=lambda e: order_lookup[e[0]], reverse=True)
            for (in_node, in_key, out_key) in sout:
                cur_str = f"| {out_key} "
                end_str = f"| {in_key} ({get_in_state(in_node, in_key)}) "
                segs.append(f"{' ' * prev_gap}{cur_str}")
                cur_gap = max(len(cur_str), len(end_str))
                new_edges.append((in_node, in_key, cur_gap))
                prev_gap = max(0, cur_gap - len(cur_str))
            return new_edges, "".join(segs)

        def draw() -> List[str]:
            lines: List[str] = []
            edges: List[Tuple[Optional[NodeHandle], str, int]] = []
            for node in order:
                node_line = \
                    f"{node.get_short_status(allow_unicode)} " \
                    f"{node.get_type()}[{node.get_id()}] " \
                    f"{node.get_highest_chunk()} "
                total_gap_top = max(
                    0, sum((edge[2] for edge in edges[:-1])) - len(node_line))
                edges, in_line = draw_in_edges(node, edges)
                in_line = in_line.rstrip()
                if in_line:
                    lines.append(f"  {in_line}")
                edges, out_line = draw_out_edges(node, edges)
                total_gap_bottom = max(
                    0, sum((edge[2] for edge in edges[:-1])) - len(node_line))
                connector = "\\" if total_gap_bottom > total_gap_top else "/"
                if total_gap_bottom == total_gap_top:
                    connector = "|"
                total_gap = max(total_gap_bottom, total_gap_top)
                if total_gap > 0:
                    node_line = f"{node_line}{'-' * total_gap}--{connector}"
                lines.append(node_line.rstrip())
                out_line = out_line.rstrip()
                if out_line:
                    lines.append(f"  {out_line}")
            return lines

        return "\n".join(draw())

    def get_def(self) -> PipelineDef:
        return cast(PipelineDef, self._client._request_json(
            METHOD_GET, "/pipeline_def", {
                "pipeline": self.get_id(),
            }, capture_err=False))

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
            client: XYMEClient,
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
            client: XYMEClient,
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
            }, capture_err=False))["status"]

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
            }, capture_err=True))["new_uri"]

    def get_in_cursor_states(self) -> Dict[str, int]:
        return cast(InCursors, self._client._request_json(
            METHOD_GET, "/node_in_cursors", {
                "pipeline": self.get_pipeline().get_id(),
                "node": self.get_id(),
            }, capture_err=False))["cursors"]

    def get_highest_chunk(self) -> int:
        return cast(NodeChunk, self._client._request_json(
            METHOD_GET, "/node_chunk", {
                "pipeline": self.get_pipeline().get_id(),
                "node": self.get_id(),
            }, capture_err=False))["chunk"]

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
            "parallel": "=",
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
            }, capture_err=False))["times"]

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
            }, capture_err=False))
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

    def reset(self) -> NodeState:
        return cast(NodeState, self._client._request_json(
            METHOD_PUT, "/node_state", {
                "pipeline": self.get_pipeline().get_id(),
                "node": self.get_id(),
                "action": "reset",
            }, capture_err=False))

    def requeue(self) -> NodeState:
        return cast(NodeState, self._client._request_json(
            METHOD_PUT, "/node_state", {
                "pipeline": self.get_pipeline().get_id(),
                "node": self.get_id(),
                "action": "requeue",
            }, capture_err=False))

    def fix_error(self) -> NodeState:
        return cast(NodeState, self._client._request_json(
            METHOD_PUT, "/node_state", {
                "pipeline": self.get_pipeline().get_id(),
                "node": self.get_id(),
                "action": "fix_error",
            }, capture_err=False))

    def get_csv_blob(self) -> 'CSVBlobHandle':
        if self.get_type() != "csv_reader":
            raise ValueError("node doesn't have csv blob")
        res = cast(CSVBlobResponse, self._client._request_json(
            METHOD_GET, "/csv_blob", {
                "pipeline": self.get_pipeline().get_id(),
                "node": self.get_id(),
            }, capture_err=False))
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
            }, capture_err=False))
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
            }, capture_err=True))

    def get_custom_imports(self) -> CustomImportsResponse:
        self.check_custom_code_node()
        return cast(CustomImportsResponse, self._client._request_json(
            METHOD_GET, "/custom_imports", {
                "pipeline": self.get_pipeline().get_id(),
                "node": self.get_id(),
            }, capture_err=False))

    def set_custom_code(self, func: FUNC) -> CustomCodeResponse:
        from RestrictedPython import compile_restricted

        self.check_custom_code_node()

        def as_str(fun: FUNC) -> str:
            body = textwrap.dedent(inspect.getsource(fun))
            res = body + textwrap.dedent(f"""
            result = {fun.__name__}(data)
            if result is None:
                raise ValueError("{fun.__name__} must return a value")
            """)
            compile_restricted(res, "inline", "exec")
            return res

        raw_code = as_str(func)
        return cast(CustomCodeResponse, self._client._request_json(
            METHOD_PUT, "/custom_code", {
                "pipeline": self.get_pipeline().get_id(),
                "node": self.get_id(),
                "code": raw_code,
            }, capture_err=True))

    def get_custom_code(self) -> CustomCodeResponse:
        self.check_custom_code_node()
        return cast(CustomCodeResponse, self._client._request_json(
            METHOD_GET, "/custom_code", {
                "pipeline": self.get_pipeline().get_id(),
                "node": self.get_id(),
            }, capture_err=False))

    def get_user_columns(self, key: str) -> UserColumnsResponse:
        return cast(UserColumnsResponse, self._client._request_json(
            METHOD_GET, "/user_columns", {
                "pipeline": self.get_pipeline().get_id(),
                "node": self.get_id(),
                "key": key,
            }, capture_err=False))

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
            }, capture_err=True))

    def get_model_params(self) -> Any:
        return cast(ModelParamsResponse, self._client._request_json(
            METHOD_GET, "/model_params", {
                "pipeline": self.get_pipeline().get_id(),
                "node": self.get_id(),
            }, capture_err=True))

    def get_def(self) -> NodeDef:
        return cast(NodeDef, self._client._request_json(
            METHOD_GET, "/node_def", {
                "pipeline": self.get_pipeline().get_id(),
                "node": self.get_id(),
            }, capture_err=False))

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
            client: XYMEClient,
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
            }, capture_err=False)
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
            self,
            from_path: Optional[str],
            from_io: Optional[io.BytesIO]) -> List['BlobHandle']:
        if from_path is not None and from_io is not None:
            raise ValueError("cannot have both from_path and from_io")
        if from_io is not None:
            zip_stream = from_io
        elif from_path is not None:
            with open(from_path, "rb") as fin:
                zip_stream = io.BytesIO(fin.read())
        else:
            raise ValueError("from_path and from_io cannot be both None")

        resp = self._client._request_json(
            METHOD_FILE, "/upload_zip", {
                "blob": self._uri,
                "pipeline": self.get_pipeline().get_id(),
            }, files={
                "file": zip_stream,
            }, capture_err=False)
        return [
            BlobHandle(
                self._client,
                blob_uri,
                is_full=True,
                pipeline=self._pipeline)
            for blob_uri in resp["files"]
        ]

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
            client: XYMEClient,
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
            method, "/csv_action", args, capture_err=False, files=files))
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
            client: XYMEClient,
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
            }, capture_err=True)
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
        if self._value is None:
            return f"data_id={self._data_id}"
        return f"value={self._value}"

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


def create_xyme_client(url: str, token: Optional[str] = None) -> XYMEClient:
    return XYMEClient(url, token)
