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

from .util import (
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
from .types import (
    AllowedCustomImports,
    BlobFilesResponse,
    BlobInit,
    BlobOwner,
    CacheStats,
    CopyBlob,
    CSVBlobResponse,
    DagCreate,
    DagDef,
    DagDupResponse,
    DagInfo,
    DagInit,
    DagList,
    DagReload,
    DynamicResults,
    DynamicStatusResponse,
    ESQueryResponse,
    FlushAllQueuesResponse,
    InCursors,
    InstanceStatus,
    JSONBlobAppendResponse,
    JSONBlobResponse,
    KafkaGroup,
    KafkaMessage,
    KafkaOffsets,
    KafkaThroughput,
    KafkaTopics,
    KnownBlobs,
    MinimalQueueStatsResponse,
    ModelParamsResponse,
    ModelReleaseResponse,
    ModelSetupResponse,
    NamespaceList,
    NamespaceUpdateSettings,
    NodeChunk,
    NodeCustomCode,
    NodeCustomImports,
    NodeDef,
    NodeDefInfo,
    NodeInfo,
    NodeState,
    NodeStatus,
    NodeTiming,
    NodeTypes,
    NodeUserColumnsResponse,
    PutNodeBlob,
    QueueMode,
    QueueStatsResponse,
    QueueStatus,
    ReadNode,
    SetNamedSecret,
    SettingsObj,
    TaskStatus,
    Timing,
    TimingResult,
    Timings,
    TritonModelsResponse,
    UploadFilesResponse,
    VersionResponse,
    WorkerScale,
)

if TYPE_CHECKING:
    WVD = weakref.WeakValueDictionary[str, 'DagHandle']
else:
    WVD = weakref.WeakValueDictionary


API_VERSION = 4
DEFAULT_NAMESPACE = "default"


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


class AccessDenied(Exception):
    pass

# *** AccessDenied ***


class LegacyVersion(Exception):
    def __init__(self, api_version: int) -> None:
        super().__init__(f"expected {API_VERSION} got {api_version}")
        self._api_version = api_version

    def get_api_version(self) -> int:
        return self._api_version

# *** LegacyVersion ***


class XYMEClient:
    def __init__(
            self,
            url: str,
            token: Optional[str],
            namespace: str) -> None:
        self._url = url.rstrip("/")
        if token is None:
            token = os.environ.get("XYME_SERVER_TOKEN")
        self._token = token
        self._namespace = namespace
        self._last_action = time.monotonic()
        self._auto_refresh = True
        self._dag_cache: WVD = weakref.WeakValueDictionary()
        self._node_defs: Optional[Dict[str, NodeDefInfo]] = None

        def get_version() -> int:
            server_version = self.get_server_version()
            try:
                return int(server_version["api_version"])
            except (ValueError, KeyError) as e:
                raise LegacyVersion(1) from e

        api_version = get_version()
        if api_version < API_VERSION:
            raise LegacyVersion(api_version)
        self._api_version = api_version

    def get_api_version(self) -> int:
        return self._api_version

    def set_auto_refresh(self, is_auto_refresh: bool) -> None:
        self._auto_refresh = is_auto_refresh

    def is_auto_refresh(self) -> bool:
        return self._auto_refresh

    def refresh(self) -> None:
        self._node_defs = None

    def _maybe_refresh(self) -> None:
        if self.is_auto_refresh():
            self.refresh()

    # FIXME: Do we still need this?
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
            add_namespace: bool = True,
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
                        method,
                        path,
                        args,
                        files,
                        add_prefix,
                        add_namespace,
                        api_version)
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
            add_namespace: bool = True,
            api_version: Optional[int] = None) -> TextIO:
        retry = 0
        max_retry = get_max_retry()
        while True:
            try:
                try:
                    return self._fallible_raw_request_str(
                        method,
                        path,
                        args,
                        add_prefix,
                        add_namespace,
                        api_version)
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
            add_namespace: bool = True,
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
                        method,
                        path,
                        args,
                        add_prefix,
                        add_namespace,
                        files,
                        api_version)
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
            add_namespace: bool,
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
        if add_namespace:
            args["namespace"] = self._namespace

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
            add_namespace: bool,
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
        if add_namespace:
            args["namespace"] = self._namespace

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
            add_namespace: bool,
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
        if add_namespace:
            args["namespace"] = self._namespace
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
            add_namespace: bool = True,
            api_version: Optional[int] = None) -> Tuple[BytesIO, str]:
        return self._raw_request_bytes(
            method, path, args, files, add_prefix, add_namespace, api_version)

    def request_json(
            self,
            method: str,
            path: str,
            args: Dict[str, Any],
            add_prefix: bool = True,
            add_namespace: bool = True,
            files: Optional[Dict[str, IO[bytes]]] = None,
            api_version: Optional[int] = None) -> Dict[str, Any]:
        return self._raw_request_json(
            method, path, args, add_prefix, add_namespace, files, api_version)

    def get_server_version(self) -> VersionResponse:
        return cast(VersionResponse, self.request_json(
            METHOD_GET,
            f"{PREFIX}/v{API_VERSION}/version",
            {},
            add_prefix=False,
            add_namespace=False))

    def get_namespaces(self) -> List[str]:
        return cast(NamespaceList, self.request_json(
            METHOD_GET, "/namespaces", {}))["namespaces"]

    def get_dags(self) -> List[str]:
        return [
            res[0]
            for res in self.get_dag_times(retrieve_times=False)[1]
        ]

    def get_dag_ages(self) -> List[Tuple[str, str, str]]:
        cur_time, dags = self.get_dag_times(retrieve_times=True)
        return [
            (dag_uri, get_age(cur_time, oldest), get_age(cur_time, latest))
            for (dag_uri, oldest, latest) in sorted(dags, key=lambda el: (
                safe_opt_num(el[1]), safe_opt_num(el[2]), el[0]))
        ]

    def get_dag_times(self, retrieve_times: bool) -> Tuple[
            float, List[Tuple[str, Optional[float], Optional[float]]]]:
        res = cast(DagList, self.request_json(
            METHOD_GET, "/dags", {
                "retrieve_times": int(retrieve_times),
            }))
        return res["cur_time"], res["dags"]

    def get_dag(self, dag_uri: str) -> 'DagHandle':
        res = self._dag_cache.get(dag_uri)
        if res is not None:
            return res
        res = DagHandle(self, dag_uri)
        self._dag_cache[dag_uri] = res
        return res

    def get_blob_handle(self, uri: str, is_full: bool = False) -> 'BlobHandle':
        return BlobHandle(self, uri, is_full=is_full)

    def get_node_defs(self) -> Dict[str, NodeDefInfo]:
        self._maybe_refresh()
        if self._node_defs is not None:
            return self._node_defs
        res = cast(NodeTypes, self.request_json(
            METHOD_GET, "/node_types", {}, add_namespace=False))["info"]
        self._node_defs = res
        return res

    def create_new_blob(self, blob_type: str) -> str:
        return cast(BlobInit, self.request_json(
            METHOD_POST, "/blob_init", {
                "type": blob_type,
            }, add_namespace=False))["blob"]

    def create_new_dag(
            self,
            username: Optional[str] = None,
            dagname: Optional[str] = None,
            index: Optional[int] = None) -> str:
        return cast(DagInit, self.request_json(
            METHOD_POST, "/dag_init", {
                "user": username,
                "name": dagname,
                "index": index,
            }))["dag"]

    def duplicate_dag(
            self, dag_uri: str, dest_uri: Optional[str] = None) -> str:
        args = {
            "dag": dag_uri,
        }
        if dest_uri is not None:
            args["dest"] = dest_uri
        return cast(DagDupResponse, self.request_json(
            METHOD_POST, "/dag_dup", args))["dag"]

    def set_dag(
            self,
            dag_uri: str,
            defs: DagDef,
            warnings_io: Optional[IO[Any]] = sys.stderr) -> 'DagHandle':
        dag_create = cast(DagCreate, self.request_json(
            METHOD_POST, "/dag_create", {
                "dag": dag_uri,
                "defs": defs,
            }))
        dag_uri = dag_create["dag"]
        if warnings_io is not None:
            warnings = dag_create["warnings"]
            if len(warnings) > 1:
                warnings_io.write(
                    f"{len(warnings)} warnings while "
                    f"setting dag {dag_uri}:\n")
            elif len(warnings) == 1:
                warnings_io.write(
                    f"Warning while setting dag {dag_uri}:\n")
            for warn in warnings:
                warnings_io.write(f"{warn}\n")
            if warnings:
                warnings_io.flush()
        return self.get_dag(dag_uri)

    def set_settings(self, settings: SettingsObj) -> SettingsObj:
        return cast(NamespaceUpdateSettings, self.request_json(
            METHOD_POST, "/settings", {
                "settings": settings,
            }))["settings"]

    def get_settings(self) -> SettingsObj:
        return cast(NamespaceUpdateSettings, self.request_json(
            METHOD_GET, "/settings", {}))["settings"]

    def get_allowed_custom_imports(self) -> AllowedCustomImports:
        return cast(AllowedCustomImports, self.request_json(
            METHOD_GET, "/allowed_custom_imports", {}, add_namespace=False))

    @overload
    def check_queue_stats(  # pylint: disable=no-self-use
            self,
            dag: Optional[str],
            minimal: Literal[True]) -> MinimalQueueStatsResponse:
        ...

    @overload
    def check_queue_stats(  # pylint: disable=no-self-use
            self,
            dag: Optional[str],
            minimal: Literal[False]) -> QueueStatsResponse:
        ...

    @overload
    def check_queue_stats(  # pylint: disable=no-self-use
            self,
            dag: Optional[str],
            minimal: bool) -> Union[
                MinimalQueueStatsResponse, QueueStatsResponse]:
        ...

    def check_queue_stats(
            self,
            dag: Optional[str] = None,
            minimal: bool = False) -> Union[
                MinimalQueueStatsResponse, QueueStatsResponse]:
        if minimal:
            return cast(MinimalQueueStatsResponse, self.request_json(
                METHOD_GET, "/queue_stats", {
                    "dag": dag,
                    "minimal": 1,
                }))
        return cast(QueueStatsResponse, self.request_json(
            METHOD_GET, "/queue_stats", {
                "dag": dag,
                "minimal": 0,
            }))

    def get_instance_status(
            self,
            dag_uri: Optional[str] = None,
            node_id: Optional[str] = None) -> Dict[InstanceStatus, int]:
        return cast(Dict[InstanceStatus, int], self.request_json(
            METHOD_GET, "/instance_status", {
                "dag": dag_uri,
                "node": node_id,
            }))

    def get_queue_mode(self) -> str:
        return cast(QueueMode, self.request_json(
            METHOD_GET, "/queue_mode", {}, add_namespace=False))["mode"]

    def set_queue_mode(self, mode: str) -> str:
        return cast(QueueMode, self.request_json(
            METHOD_PUT, "/queue_mode", {
                "mode": mode,
            }, add_namespace=False))["mode"]

    def flush_all_queue_data(self) -> None:

        def do_flush() -> bool:
            res = cast(FlushAllQueuesResponse, self.request_json(
                METHOD_POST, "/flush_all_queues", {}, add_namespace=False))
            return bool(res["success"])

        while do_flush():  # we flush until there is nothing to flush anymore
            time.sleep(1.0)

    def get_cache_stats(self) -> CacheStats:
        return cast(CacheStats, self.request_json(
            METHOD_GET, "/cache_stats", {}, add_namespace=False))

    def reset_cache(self) -> CacheStats:
        return cast(CacheStats, self.request_json(
            METHOD_POST, "/cache_reset", {}, add_namespace=False))

    def create_kafka_error_topic(self) -> KafkaTopics:
        return cast(KafkaTopics, self.request_json(
            METHOD_POST, "/kafka_topics", {
                "num_partitions": 1,
            }))

    def delete_kafka_error_topic(self) -> KafkaTopics:
        return cast(KafkaTopics, self.request_json(
            METHOD_POST, "/kafka_topics", {
                "num_partitions": 0,
            }))

    def read_kafka_errors(self, offset: str = "current") -> List[str]:
        return cast(List[str], self.request_json(
            METHOD_GET, "/kafka_msg", {
                "offset": offset,
            }))

    def get_named_secrets(
            self, show_values: bool = False) -> Dict[str, Optional[str]]:
        return cast(Dict[str, Optional[str]], self.request_json(
            METHOD_GET, "/named_secrets", {
                "show": int(bool(show_values)),
            }))

    def set_named_secret(self, key: str, value: str) -> bool:
        return cast(SetNamedSecret, self.request_json(
            METHOD_PUT, "/named_secrets", {
                "key": key,
                "value": value,
            }))["replaced"]

    def get_error_logs(self) -> str:
        with self._raw_request_str(METHOD_GET, "/error_logs", {}) as fin:
            return fin.read()

    def get_known_blobs(
            self,
            blob_type: Optional[str] = None,
            connector: Optional[str] = None) -> List[str]:
        return [
            res[0]
            for res in self.get_known_blob_times(
                retrieve_times=False,
                blob_type=blob_type,
                connector=connector)[1]
        ]

    def get_known_blob_ages(
            self,
            blob_type: Optional[str] = None,
            connector: Optional[str] = None) -> List[Tuple[str, str]]:
        cur_time, blobs = self.get_known_blob_times(
            retrieve_times=True, blob_type=blob_type, connector=connector)
        return [
            (blob_id, get_age(cur_time, blob_time))
            for (blob_id, blob_time) in sorted(blobs, key=lambda el: (
                safe_opt_num(el[1]), el[0]))
        ]

    def get_known_blob_times(
            self,
            retrieve_times: bool,
            blob_type: Optional[str] = None,
            connector: Optional[str] = None,
            ) -> Tuple[float, List[Tuple[str, Optional[float]]]]:
        obj: Dict[str, Union[int, str]] = {
            "retrieve_times": int(retrieve_times),
        }
        if blob_type is not None:
            obj["blob_type"] = blob_type
        if connector is not None:
            obj["connector"] = connector
        res = cast(KnownBlobs, self.request_json(
            METHOD_GET, "/known_blobs", obj))
        return res["cur_time"], res["blobs"]

    def get_triton_models(self) -> List[str]:
        return cast(TritonModelsResponse, self.request_json(
            METHOD_GET, "/inference_models", {}))["models"]


# *** XYMEClient ***


class DagHandle:
    def __init__(
            self,
            client: XYMEClient,
            dag_uri: str) -> None:
        self._client = client
        self._dag_uri = dag_uri
        self._name: Optional[str] = None
        self._company: Optional[str] = None
        self._state: Optional[str] = None
        self._is_high_priority: Optional[bool] = None
        self._queue_mng: Optional[str] = None
        self._nodes: Dict[str, NodeHandle] = {}
        self._node_lookup: Dict[str, str] = {}
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

    def get_info(self) -> DagInfo:
        return cast(DagInfo, self._client.request_json(
            METHOD_GET, "/dag_info", {
                "dag": self.get_uri(),
            }))

    def _fetch_info(self) -> None:
        info = self.get_info()
        self._name = info["name"]
        self._company = info["company"]
        self._state = info["state"]
        self._is_high_priority = info["high_priority"]
        self._queue_mng = info["queue_mng"]
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

    def get_node(self, node_name: str) -> 'NodeHandle':
        self._maybe_refresh()
        self._maybe_fetch()
        node_id = self._node_lookup.get(node_name, node_name)
        return self._nodes[node_id]

    def get_uri(self) -> str:
        return self._dag_uri

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

    def get_timing(
            self,
            blacklist: Optional[List[str]] = None,
            ) -> TimingResult:
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

        dag_total = 0.0
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
            dag_total += node_total
        node_timing_sorted = sorted(
            node_timing.items(),
            key=lambda x: x[1]["node_total"],
            reverse=True)
        return {
            "dag_total": dag_total,
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

    def set_dag(self, defs: DagDef) -> None:
        self._client.set_dag(self.get_uri(), defs)

    def dynamic_model(
            self,
            inputs: List[Any],
            format_method: str = "simple",
            no_cache: bool = False) -> List[Any]:
        res = cast(DynamicResults, self._client.request_json(
            METHOD_POST, "/dynamic_model", {
                "format": format_method,
                "inputs": inputs,
                "no_cache": no_cache,
                "dag": self.get_uri(),
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
            res = cast(DynamicResults, self._client.request_json(
                METHOD_POST, "/dynamic_list", {
                    "force_keys": force_keys,
                    "format": format_method,
                    "input_key": input_key,
                    "inputs": inputs,
                    "no_cache": no_cache,
                    "output_key": output_key,
                    "dag": self.get_uri(),
                }))
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
                "dag": self.get_uri(),
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
        res: Dict[str, str] = self._client.request_json(
            METHOD_FILE, "/dynamic_async", {
                "dag": self.get_uri(),
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

    def get_dynamic_result(self, value_id: str) -> ByteResponse:
        try:
            cur_res, ctype = self._client.request_bytes(
                METHOD_GET, "/dynamic_result", {
                    "dag": self.get_uri(),
                    "id": value_id,
                })
        except HTTPError as e:
            if e.response.status_code == 404:
                raise KeyError(f"value_id {value_id} does not exist") from e
            raise e
        return interpret_ctype(cur_res, ctype)

    def get_dynamic_status(
            self,
            value_ids: List['ComputationHandle']) -> Dict[
                'ComputationHandle', QueueStatus]:
        res = cast(DynamicStatusResponse, self._client.request_json(
            METHOD_POST, "/dynamic_status", {
                "value_ids": [value_id.get_id() for value_id in value_ids],
                "dag": self.get_uri(),
            }))
        status = res["status"]
        hnd_map = {value_id.get_id(): value_id for value_id in value_ids}
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
        start_vert = "├" if allow_unicode else "|"
        end_vert = "├" if allow_unicode else "|"
        before_vert = "│" if allow_unicode else "|"
        after_vert = "│" if allow_unicode else "|"
        vert = "┤" if allow_unicode else "|"
        corner_right = "┐" if allow_unicode else "\\"
        corner_left = "┘" if allow_unicode else "/"
        cont_right = "┬" if allow_unicode else "\\"
        cont_left = "┴" if allow_unicode else "/"
        cont_skip = "─" if allow_unicode else "-"
        cont_vert = "│" if allow_unicode else "|"
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
                    cur_str = f"{end_vert} {in_key} ({in_state}) "
                    new_edges.append((None, in_key, cur_gap))
                else:
                    cur_str = before_vert if in_node is not None else ""
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
                cur_str = after_vert if cur_node is not None else ""
                segs.append(f"{space * prev_gap}{cur_str}")
                new_edges.append(edge)
                prev_gap = max(0, cur_gap - len(cur_str))
            sout = sorted(
                outs[node], key=lambda e: order_lookup[e[0]], reverse=True)
            for (in_node, in_key, out_key) in sout:
                cur_str = f"{start_vert} {out_key} "
                in_state = get_in_state(in_node, in_key)
                end_str = f"{end_vert} {in_key} ({in_state}) "
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
                            cur_connect = cont_vert
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
                    top_connect = space if empty_top[iix] else cont_vert
                    has_bottom = iix >= len(empty_bottom) or empty_bottom[iix]
                    bottom_connect = space if has_bottom else cont_vert
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
                    bottom_conn = cont_vert
                else:
                    connector = corner_left
                    more_gaps = top_gaps
                    top_conn = cont_vert
                    bottom_conn = space
                if total_gap_bottom == total_gap_top:
                    connector = vert
                    more_gaps = bottom_gaps
                    top_conn = cont_vert
                    bottom_conn = cont_vert
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
                            mid_top = cont_vert
                            mid_bottom = cont_vert
                        else:
                            mid_connector = cont
                            mid_top = cont_vert
                            mid_bottom = cont_vert
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

    def get_def(self, full: bool = True) -> DagDef:
        return cast(DagDef, self._client.request_json(
            METHOD_GET, "/dag_def", {
                "dag": self.get_uri(),
                "full": 1 if full else 0,
            }))

    def set_attr(self, attr: str, value: Any) -> None:
        dag_def = self.get_def()
        dag_def[attr] = value  # type: ignore
        self._client.set_dag(self.get_uri(), dag_def)

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
            self, minimal: bool) -> Union[
                MinimalQueueStatsResponse, QueueStatsResponse]:
        ...

    def check_queue_stats(self, minimal: bool) -> Union[
            MinimalQueueStatsResponse, QueueStatsResponse]:
        return self._client.check_queue_stats(self.get_uri(), minimal=minimal)

    def scale_worker(self, replicas: int) -> bool:
        return cast(WorkerScale, self._client.request_json(
            METHOD_PUT, "/worker", {
                "dag": self.get_uri(),
                "replicas": replicas,
                "task": None,
            }))["success"]

    def reload(self, timestamp: Optional[float] = None) -> float:
        return cast(DagReload, self._client.request_json(
            METHOD_PUT, "/dag_reload", {
                "dag": self.get_uri(),
                "when": timestamp,
            }))["when"]

    def set_kafka_topic_partitions(
            self,
            num_partitions: int,
            large_input_retention: bool = False) -> KafkaTopics:
        return cast(KafkaTopics, self._client.request_json(
            METHOD_POST, "/kafka_topics", {
                "dag": self.get_uri(),
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
        res = cast(KafkaMessage, self._client.request_json(
            METHOD_FILE, "/kafka_msg", {
                "dag": self.get_uri(),
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
                    "dag": self.get_uri(),
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
        return cast(KafkaOffsets, self._client.request_json(
            METHOD_GET, "/kafka_offsets", {
                "dag": self.get_uri(),
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
            "dag": self.get_uri(),
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
        return cast(KafkaGroup, self._client.request_json(
            METHOD_GET, "/kafka_group", {
                "dag": self.get_uri(),
            }))

    def set_kafka_group(
            self,
            group_id: Optional[str] = None,
            reset: Optional[str] = None,
            **kwargs: Any) -> KafkaGroup:
        return cast(KafkaGroup, self._client.request_json(
            METHOD_PUT, "/kafka_group", {
                "dag": self.get_uri(),
                "group_id": group_id,
                "reset": reset,
                **kwargs,
            }))

    def __hash__(self) -> int:
        return hash(self.get_uri())

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return self.get_uri() == other.get_uri()

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def __str__(self) -> str:
        return self.get_uri()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}[{self.get_uri()}]"

# *** DagHandle ***


class NodeHandle:
    def __init__(
            self,
            client: XYMEClient,
            dag: DagHandle,
            node_id: str,
            node_name: str,
            kind: str) -> None:
        self._client = client
        self._dag = dag
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
            dag: DagHandle,
            node_info: NodeInfo,
            prev: Optional['NodeHandle']) -> 'NodeHandle':
        if prev is None:
            res = NodeHandle(
                client,
                dag,
                node_info["id"],
                node_info["name"],
                node_info["type"])
        else:
            if prev.get_dag() != dag:
                raise ValueError(f"{prev.get_dag()} != {dag}")
            res = prev
        res.update_info(node_info)
        return res

    def update_info(self, node_info: NodeInfo) -> None:
        if self._node_id != node_info["id"]:
            raise ValueError(f"{self._node_id} != {node_info['id']}")
        self._node_name = node_info["name"]
        self._type = node_info["type"]
        self._blobs = {
            key: BlobHandle(self._client, value, is_full=False)
            for (key, value) in node_info["blobs"].items()
        }
        self._inputs = node_info["inputs"]
        self._state = node_info["state"]
        self._config_error = node_info["config_error"]

    def get_dag(self) -> DagHandle:
        return self._dag

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
        return self.get_dag().get_node(node_id), out_key

    def get_status(self) -> TaskStatus:
        return cast(NodeStatus, self._client.request_json(
            METHOD_GET, "/node_status", {
                "dag": self.get_dag().get_uri(),
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
        return cast(PutNodeBlob, self._client.request_json(
            METHOD_PUT, "/node_blob", {
                "dag": self.get_dag().get_uri(),
                "node": self.get_id(),
                "blob_key": key,
                "blob_uri": blob_uri,
            }))["new_uri"]

    def get_in_cursor_states(self) -> Dict[str, int]:
        return cast(InCursors, self._client.request_json(
            METHOD_GET, "/node_in_cursors", {
                "dag": self.get_dag().get_uri(),
                "node": self.get_id(),
            }))["cursors"]

    def get_highest_chunk(self) -> int:
        return cast(NodeChunk, self._client.request_json(
            METHOD_GET, "/node_chunk", {
                "dag": self.get_dag().get_uri(),
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
                    "dag": self.get_dag().get_uri(),
                    "node": self.get_id(),
                }) as fin:
            return fin.read()

    def get_timing(self) -> List[Timing]:
        return cast(Timings, self._client.request_json(
            METHOD_GET, "/node_perf", {
                "dag": self.get_dag().get_uri(),
                "node": self.get_id(),
            }))["times"]

    def read_blob(
            self,
            key: str,
            chunk: Optional[int],
            force_refresh: bool) -> 'BlobHandle':
        # FIXME: !!!!!! explicitly repeat on timeout
        dag = self.get_dag()
        res = cast(ReadNode, self._client.request_json(
            METHOD_POST, "/read_node", {
                "dag": dag.get_uri(),
                "node": self.get_id(),
                "key": key,
                "chunk": chunk,
                "is_blocking": True,
                "force_refresh": force_refresh,
            }))
        uri = res["result_uri"]
        if uri is None:
            raise ValueError(f"uri is None: {res}")
        return BlobHandle(self._client, uri, is_full=True)

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
        return cast(NodeState, self._client.request_json(
            METHOD_PUT, "/node_state", {
                "dag": self.get_dag().get_uri(),
                "node": self.get_id(),
                "action": "reset",
            }))

    def requeue(self) -> NodeState:
        return cast(NodeState, self._client.request_json(
            METHOD_PUT, "/node_state", {
                "dag": self.get_dag().get_uri(),
                "node": self.get_id(),
                "action": "requeue",
            }))

    def fix_error(self) -> NodeState:
        return cast(NodeState, self._client.request_json(
            METHOD_PUT, "/node_state", {
                "dag": self.get_dag().get_uri(),
                "node": self.get_id(),
                "action": "fix_error",
            }))

    def get_csv_blob(self) -> 'CSVBlobHandle':
        if self.get_type() != "csv_reader":
            raise ValueError("node doesn't have csv blob")
        dag = self.get_dag()
        res = cast(CSVBlobResponse, self._client.request_json(
            METHOD_GET, "/csv_blob", {
                "dag": dag.get_uri(),
                "node": self.get_id(),
            }))
        owner: BlobOwner = {
            "owner_dag": self.get_dag().get_uri(),
            "owner_node": self.get_id(),
        }
        return CSVBlobHandle(self._client, res["csv"], owner)

    def get_json_blob(self) -> 'JSONBlobHandle':
        if self.get_type() != "jsons_reader":
            raise ValueError(
                f"can not access JSON of {self}, node is not a 'jsons_reader'")
        dag = self.get_dag()
        res = cast(JSONBlobResponse, self._client.request_json(
            METHOD_GET, "/json_blob", {
                "dag": dag.get_uri(),
                "node": self.get_id(),
            }))
        return JSONBlobHandle(self._client, res["json"], res["count"])

    def check_custom_code_node(self) -> None:
        if not self.get_type() in CUSTOM_NODE_TYPES:
            raise ValueError(f"{self} is not a custom code node.")

    def set_custom_imports(
            self, modules: List[List[str]]) -> NodeCustomImports:
        self.check_custom_code_node()
        return cast(NodeCustomImports, self._client.request_json(
            METHOD_PUT, "/custom_imports", {
                "dag": self.get_dag().get_uri(),
                "node": self.get_id(),
                "modules": modules,
            }))

    def get_custom_imports(self) -> NodeCustomImports:
        self.check_custom_code_node()
        return cast(NodeCustomImports, self._client.request_json(
            METHOD_GET, "/custom_imports", {
                "dag": self.get_dag().get_uri(),
                "node": self.get_id(),
            }))

    def set_es_query(self, query: Dict[str, Any]) -> ESQueryResponse:
        if self.get_type() != "es_reader":
            raise ValueError(f"{self} is not an ES reader node")

        return cast(ESQueryResponse, self._client.request_json(
            METHOD_POST, "/es_query", {
                "dag": self.get_dag().get_uri(),
                "blob": self.get_blob_handle("es").get_uri(),
                "es_query": query,
            },
        ))

    def get_es_query(self) -> ESQueryResponse:
        if self.get_type() != "es_reader":
            raise ValueError(f"{self} is not an ES reader node")

        return cast(ESQueryResponse, self._client.request_json(
            METHOD_GET, "/es_query", {
                "dag": self.get_dag().get_uri(),
                "blob": self.get_blob_handle("es").get_uri(),
            },
        ))

    def set_custom_code(self, func: FUNC) -> NodeCustomCode:
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
        return cast(NodeCustomCode, self._client.request_json(
            METHOD_PUT, "/custom_code", {
                "dag": self.get_dag().get_uri(),
                "node": self.get_id(),
                "code": raw_code,
            }))

    def get_custom_code(self) -> NodeCustomCode:
        self.check_custom_code_node()
        return cast(NodeCustomCode, self._client.request_json(
            METHOD_GET, "/custom_code", {
                "dag": self.get_dag().get_uri(),
                "node": self.get_id(),
            }))

    def get_user_columns(self, key: str) -> NodeUserColumnsResponse:
        return cast(NodeUserColumnsResponse, self._client.request_json(
            METHOD_GET, "/user_columns", {
                "dag": self.get_dag().get_uri(),
                "node": self.get_id(),
                "key": key,
            }))

    def get_input_example(self) -> Dict[str, Optional[ByteResponse]]:
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

    def setup_model(self, obj: Dict[str, Any]) -> ModelSetupResponse:
        return cast(ModelSetupResponse, self._client.request_json(
            METHOD_PUT, "/model_setup", {
                "dag": self.get_dag().get_uri(),
                "node": self.get_id(),
                "config": obj,
            }))

    def get_model_params(self) -> ModelParamsResponse:
        return cast(ModelParamsResponse, self._client.request_json(
            METHOD_GET, "/model_params", {
                "dag": self.get_dag().get_uri(),
                "node": self.get_id(),
            }))

    def get_def(self) -> NodeDef:
        return cast(NodeDef, self._client.request_json(
            METHOD_GET, "/node_def", {
                "dag": self.get_dag().get_uri(),
                "node": self.get_id(),
            }))

    def __hash__(self) -> int:
        return hash((self.get_dag(), self.get_id()))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, self.__class__):
            return False
        if self.get_dag() != other.get_dag():
            return False
        return self.get_id() == other.get_id()

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def __str__(self) -> str:
        return self.get_id()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}[{self.get_id()}]"

# *** NodeHandle ***


EMPTY_BLOB_PREFIX = "null://"


class BlobHandle:
    def __init__(
            self,
            client: XYMEClient,
            uri: str,
            is_full: bool) -> None:
        self._client = client
        self._uri = uri
        self._is_full = is_full
        self._ctype: Optional[str] = None
        self._tmp_uri: Optional[str] = None

    def is_full(self) -> bool:
        return self._is_full

    def is_empty(self) -> bool:
        return self._uri.startswith(EMPTY_BLOB_PREFIX)

    def get_uri(self) -> str:
        return self._uri

    def get_ctype(self) -> Optional[str]:
        return self._ctype

    def get_content(self) -> Optional[ByteResponse]:
        if not self.is_full():
            raise ValueError(f"URI must be full: {self}")
        if self.is_empty():
            return None
        sleep_time = 0.1
        sleep_mul = 1.1
        sleep_max = 5.0
        total_time = 60.0
        start_time = time.monotonic()
        while True:
            try:
                fin, ctype = self._client.request_bytes(
                    METHOD_POST, "/uri", {
                        "uri": self.get_uri(),
                    })
                self._ctype = ctype
                return interpret_ctype(fin, ctype)
            except HTTPError as e:
                if e.response.status_code != 404:
                    raise e
                if time.monotonic() - start_time >= total_time:
                    raise e
                time.sleep(sleep_time)
                sleep_time = min(sleep_time * sleep_mul, sleep_max)

    def list_files(self) -> List['BlobHandle']:
        if self.is_full():
            raise ValueError(f"URI must not be full: {self}")
        resp = cast(BlobFilesResponse, self._client.request_json(
            METHOD_GET, "/blob_files", {
                "blob": self.get_uri(),
            }))
        return [
            BlobHandle(self._client, blob_uri, is_full=True)
            for blob_uri in resp["files"]
        ]

    def as_str(self) -> str:
        return f"{self.get_uri()}"

    def set_owner(self, owner: NodeHandle) -> BlobOwner:
        if self.is_full():
            raise ValueError(f"URI must not be full: {self}")
        return cast(BlobOwner, self._client.request_json(
            METHOD_PUT, "/blob_owner", {
                "blob": self.get_uri(),
                "owner_dag": owner.get_dag().get_uri(),
                "owner_node": owner.get_id(),
            }))

    def get_owner(self) -> BlobOwner:
        if self.is_full():
            raise ValueError(f"URI must not be full: {self}")
        return cast(BlobOwner, self._client.request_json(
            METHOD_GET, "/blob_owner", {
                "blob": self.get_uri(),
            }))

    def copy_to(
            self,
            to_uri: str,
            new_owner: Optional[NodeHandle] = None) -> 'BlobHandle':
        if self.is_full():
            raise ValueError(f"URI must not be full: {self}")
        owner_dag = \
            None if new_owner is None else new_owner.get_dag().get_uri()
        owner_node = None if new_owner is None else new_owner.get_id()
        res = cast(CopyBlob, self._client.request_json(
            METHOD_POST, "/copy_blob", {
                "from_uri": self.get_uri(),
                "owner_dag": owner_dag,
                "owner_node": owner_node,
                "to_uri": to_uri,
            }))
        return BlobHandle(self._client, res["new_uri"], is_full=False)

    def download_zip(self, to_path: Optional[str]) -> Optional[io.BytesIO]:
        if self.is_full():
            raise ValueError(f"URI must not be full: {self}")
        cur_res, _ = self._client.request_bytes(
            METHOD_GET, "/download_zip", {
                "blob": self.get_uri(),
            })
        if to_path is None:
            return io.BytesIO(cur_res.read())
        with open(to_path, "wb") as file_download:
            file_download.write(cur_res.read())
        return None

    def _perform_upload_action(
            self,
            action: str,
            additional: Dict[str, Union[str, int]],
            fobj: Optional[IO[bytes]]) -> UploadFilesResponse:
        args: Dict[str, Union[str, int]] = {
            "action": action,
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
        if action == "clear":
            self._tmp_uri = None
        return cast(UploadFilesResponse, self._client.request_json(
            method, "/upload_file", args, files=files))

    def _start_upload(self, size: int, hash_str: str, ext: str) -> str:
        res = self._perform_upload_action(
            "start",
            {
                "target": self.get_uri(),
                "hash": hash_str,
                "size": size,
                "ext": ext,
            },
            fobj=None)
        assert res["uri"] is not None
        return res["uri"]

    def _append_upload(self, uri: str, fobj: IO[bytes]) -> int:
        res = self._perform_upload_action("append", {"uri": uri}, fobj=fobj)
        return res["pos"]

    def _finish_upload_zip(self) -> List[str]:
        uri = self._tmp_uri
        if uri is None:
            raise ValueError("tmp_uri is None")
        res = cast(UploadFilesResponse, self._client.request_json(
            METHOD_POST, "/finish_zip", {"uri": uri}))
        return res["files"]

    def _clear_upload(self) -> None:
        uri = self._tmp_uri
        if uri is None:
            raise ValueError("tmp_uri is None")
        self._perform_upload_action("clear", {"uri": uri}, fobj=None)

    def _upload_file(
            self,
            file_content: IO[bytes],
            ext: str,
            progress_bar: Optional[IO[Any]] = sys.stdout) -> None:
        init_pos = file_content.seek(0, io.SEEK_CUR)
        file_hash = get_file_hash(file_content)
        total_size = file_content.seek(0, io.SEEK_END) - init_pos
        file_content.seek(init_pos, io.SEEK_SET)
        if progress_bar is not None:
            progress_bar.write("Uploading file:\n")
        print_progress = get_progress_bar(out=progress_bar)
        tmp_uri = self._start_upload(total_size, file_hash, ext)
        self._tmp_uri = tmp_uri
        cur_size = 0
        while True:
            print_progress(cur_size / total_size, False)
            buff = file_content.read(get_file_upload_chunk_size())
            if not buff:
                break
            new_size = self._append_upload(tmp_uri, BytesIO(buff))
            if new_size - cur_size != len(buff):
                raise ValueError(
                    f"incomplete chunk upload n:{new_size} "
                    f"o:{cur_size} b:{len(buff)}")
            cur_size = new_size
        print_progress(cur_size / total_size, True)

    def upload_zip(self, source: Union[str, io.BytesIO]) -> List['BlobHandle']:
        files: List[str] = []
        try:
            if isinstance(source, str) or not hasattr(source, "read"):
                with open(f"{source}", "rb") as fin:
                    self._upload_file(fin, ext="zip")
            else:
                self._upload_file(source, ext="zip")
            files = self._finish_upload_zip()
        finally:
            self._clear_upload()
        return [
            BlobHandle(self._client, blob_uri, is_full=True)
            for blob_uri in files
        ]

    def convert_model(self, reload: bool = True) -> ModelReleaseResponse:
        return cast(ModelReleaseResponse, self._client.request_json(
            METHOD_POST, "/convert_model", {
                "blob": self.get_uri(),
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
            client: XYMEClient,
            uri: str,
            owner: BlobOwner) -> None:
        super().__init__(client, uri, is_full=False)
        self._owner = owner

    def finish_csv_upload(self) -> UploadFilesResponse:
        tmp_uri = self._tmp_uri
        assert tmp_uri is not None
        owner = self._owner
        args: Dict[str, Optional[Union[str, int]]] = {
            "tmp_uri": tmp_uri,
            "csv_uri": self.get_uri(),
            "owner_dag": owner["owner_dag"],
            "owner_node": owner["owner_node"],
        }
        return cast(UploadFilesResponse, self._client.request_json(
            METHOD_POST, "/finish_csv", args))

    def add_from_file(
            self,
            filename: str,
            progress_bar: Optional[IO[Any]] = sys.stdout,
            ) -> Optional[UploadFilesResponse]:
        fname = filename
        if filename.endswith(INPUT_ZIP_EXT):
            fname = filename[:-len(INPUT_ZIP_EXT)]
        ext_pos = fname.rfind(".")
        if ext_pos >= 0:
            ext = filename[ext_pos + 1:]  # full filename
        else:
            raise ValueError("could not determine extension")
        try:
            with open(filename, "rb") as fbuff:
                self._upload_file(
                    fbuff,
                    ext=ext,
                    progress_bar=progress_bar)
            return self.finish_csv_upload()
        finally:
            self._clear_upload()

    def add_from_df(
            self,
            df: pd.DataFrame,
            progress_bar: Optional[IO[Any]] = sys.stdout,
            ) -> Optional[UploadFilesResponse]:
        io_in = None
        try:
            io_in = df_to_csv(df)
            self._upload_file(
                io_in,
                ext="csv",
                progress_bar=progress_bar)
            return self.finish_csv_upload()
        finally:
            if io_in is not None:
                io_in.close()
            self._clear_upload()

# *** CSVBlobHandle ***


class JSONBlobHandle(BlobHandle):
    def __init__(
            self,
            client: XYMEClient,
            uri: str,
            count: int) -> None:
        super().__init__(client, uri, is_full=False)
        self._count = count

    def get_count(self) -> int:
        return self._count

    def append_jsons(
            self,
            jsons: List[Any],
            requeue_on_finish: Optional[NodeHandle] = None,
            ) -> 'JSONBlobHandle':
        obj = {
            "blob": self.get_uri(),
            "jsons": jsons,
        }
        if requeue_on_finish is not None:
            obj["dag"] = requeue_on_finish.get_dag().get_uri()
            obj["node"] = requeue_on_finish.get_id()
        res = cast(JSONBlobAppendResponse, self._client.request_json(
            METHOD_PUT, "/json_append", obj))
        self._count = res["count"]
        return self

# *** JSONBlobHandle ***


class ComputationHandle:
    def __init__(
            self,
            dag: DagHandle,
            value_id: str,
            get_dyn_error: Callable[[], Optional[str]],
            set_dyn_error: Callable[[str], None]) -> None:
        self._dag = dag
        self._value_id = value_id
        self._value: Optional[ByteResponse] = None
        self._get_dyn_error = get_dyn_error
        self._set_dyn_error = set_dyn_error

    def has_fetched(self) -> bool:
        return self._value is not None

    def get(self) -> ByteResponse:
        try:
            if self._value is None:
                self._value = self._dag.get_dynamic_result(self._value_id)
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
        return self._value_id

    def __str__(self) -> str:
        value = self._value
        if value is None:
            return f"value_id={self._value_id}"
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


def create_xyme_client(
        url: str,
        token: Optional[str] = None,
        namespace: str = DEFAULT_NAMESPACE) -> XYMEClient:
    try:
        return XYMEClient(url, token, namespace)
    except LegacyVersion as lve:
        api_version = lve.get_api_version()
        if api_version == 3:
            from accern_xyme.v3.accern_xyme import create_xyme_client_v3

            return create_xyme_client_v3(url, token)  # type: ignore
        raise lve
