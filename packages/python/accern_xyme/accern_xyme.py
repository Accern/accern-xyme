import itertools
import math
import shutil
import tempfile
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
    get_args,
    overload,
    Set,
    TextIO,
    Tuple,
    TYPE_CHECKING,
    Union,
)
import io
import os
import pickle
import sys
import json
import time
import weakref
import inspect
import textwrap
import threading
import contextlib
from io import BytesIO, StringIO
from urllib.parse import urlparse, urlunparse
from pathlib import PosixPath, PurePath
from graphviz.backend import ExecutableNotFound
import pandas as pd
import requests
from requests import Response
from requests.exceptions import HTTPError, RequestException
from typing_extensions import Literal
import quick_server

from .util import (
    async_compute,
    ByteResponse,
    compute_parallel,
    content_to_csv_bytes,
    df_to_csv_bytes,
    get_age,
    get_file_hash,
    get_file_upload_chunk_size,
    get_max_retry,
    get_progress_bar,
    get_retry_sleep,
    has_graph_easy,
    interpret_ctype,
    is_jupyter,
    maybe_json_loads,
    merge_ctype,
    safe_opt_num,
    ServerSideError,
    to_bool,
)
from .types import (
    AllowedCustomImports,
    BlobDetails,
    BlobFilesResponse,
    BlobInit,
    BlobOwner,
    BlobTypeResponse,
    BlobURIResponse,
    CacheStats,
    CopyBlob,
    DagCreate,
    DagDupResponse,
    DagInfo,
    DagInit,
    DagList,
    DagPrettyNode,
    DagReload,
    DagStatus,
    DeleteBlobResponse,
    DynamicResults,
    DynamicStatusResponse,
    ESQueryResponse,
    FileMap,
    FlushAllQueuesResponse,
    InCursors,
    InstanceStatus,
    JSONBlobAppendResponse,
    KafkaGroup,
    KafkaMessage,
    KafkaOffsets,
    KafkaThroughput,
    KafkaTopicNames,
    KafkaTopics,
    KnownBlobs,
    MinimalQueueStatsResponse,
    ModelInfo,
    ModelParamsResponse,
    ModelReleaseResponse,
    ModelVersionResponse,
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
    NodeTypeResponse,
    NodeTypes,
    NodeUserColumnsResponse,
    PrettyResponse,
    PutNodeBlob,
    QueueMode,
    QueueStatsResponse,
    QueueStatus,
    ReadNode,
    S3Config,
    SetNamedSecret,
    SettingsObj,
    TaskStatus,
    Timing,
    TimingResult,
    Timings,
    TritonModelsResponse,
    URIPrefix,
    UploadFilesResponse,
    UserDagDef,
    UUIDResponse,
    VersionResponse,
    WorkerScale,
)

if TYPE_CHECKING:
    WVD = weakref.WeakValueDictionary[str, 'DagHandle']
else:
    WVD = weakref.WeakValueDictionary

API_VERSION = 5
MIN_API_VERSION = 4
DEFAULT_URL = "http://localhost:8080"
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


FUNC = Callable[..., Any]
CUSTOM_NODE_TYPES = {
    "custom_data",
    "custom_json",
    "custom_json_to_data",
    "custom_json_join_data",
}
NO_RETRY: List[str] = []  # [METHOD_POST, METHOD_FILE]

CONSUMER_DAG = "dag"
ConsumerType = Literal[
    "err",
    "err_msg",
]
CONSUMER_ERR: ConsumerType = "err"
CONSUMER_ERR_MSG: ConsumerType = "err_msg"
CONSUMER_TYPES: Set[ConsumerType] = set(get_args(ConsumerType))


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


class KafkaErrorMessageState:
    def __init__(self) -> None:
        self._msg_lookup: Dict[str, str] = {}
        self._unmatched: List[str] = []

    def get_msg(self, input_id: str) -> Optional[str]:
        return self._msg_lookup.get(input_id)

    def add_msg(self, input_id: str, msg: str) -> None:
        self._msg_lookup[input_id] = msg

    def get_unmatched(self) -> Iterable[str]:
        unmatched = self._unmatched
        self._unmatched = []
        yield from unmatched

    def add_unmatched(self, msg: str) -> None:
        self._unmatched.append(msg)

# *** KafkaErrorMessageState ***


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

        def get_version() -> Tuple[int, int]:
            server_version = self.get_server_version()
            try:
                return (
                    int(server_version["api_version"]),
                    int(server_version.get("api_version_minor", 0)),
                )
            except (ValueError, KeyError) as e:
                raise LegacyVersion(1) from e

        api_version, api_version_minor = get_version()
        if api_version < MIN_API_VERSION:
            raise LegacyVersion(api_version)
        self._api_version = api_version
        self._api_version_minor = api_version_minor

    def get_api_version(self) -> int:
        return self._api_version

    def get_api_version_minor(self) -> int:
        return self._api_version_minor

    def has_version(self, major: int, minor: int) -> bool:
        if self._api_version > major:
            return True
        return self._api_version == major and self._api_version_minor >= minor

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
                if method in NO_RETRY:
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
                if method in NO_RETRY:
                    raise
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
                if method in NO_RETRY:
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

    def get_version_override(self) -> Dict[str, List[Optional[str]]]:
        server_version = self.get_server_version()
        img_repo = server_version["image_repo"]
        img_tag = server_version["image_tag"]
        return {
            "version": [img_repo, img_tag],
        }

    def get_namespaces(self) -> List[str]:
        return cast(NamespaceList, self.request_json(
            METHOD_GET, "/namespaces", {}))["namespaces"]

    def get_dags(self) -> List[str]:
        return [
            res["dag"]
            for res in self.get_dag_times(retrieve_times=False)[1]
        ]

    def get_dag_ages(self) -> List[Dict[str, Optional[str]]]:
        cur_time, dags = self.get_dag_times(retrieve_times=True)
        return [
            {
                "config_error": dag_status["config_error"],
                "created": get_age(cur_time, dag_status["created"]),
                "dag": dag_status["dag"],
                "deleted": get_age(cur_time, dag_status["deleted"]),
                "latest": get_age(cur_time, dag_status["latest"]),
                "oldest": get_age(cur_time, dag_status["oldest"]),
            }
            for dag_status in sorted(dags, key=lambda el: (
                el["config_error"] is None,
                safe_opt_num(el["oldest"]),
                safe_opt_num(el["latest"]),
                el["dag"]))
        ]

    def get_dag_times(self, retrieve_times: bool = True) -> Tuple[
            float, List[DagStatus]]:
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

    def get_blob_owner(self, blob_uri: str) -> BlobOwner:
        return cast(BlobOwner, self.request_json(
            METHOD_GET, "/blob_owner", {
                "blob": blob_uri,
            }))

    def set_blob_owner(
            self,
            blob_uri: str,
            dag_id: Optional[str] = None,
            node_id: Optional[str] = None,
            external_owner: bool = False) -> BlobOwner:
        return cast(BlobOwner, self.request_json(
            METHOD_PUT, "/blob_owner", {
                "blob": blob_uri,
                "owner_dag": dag_id,
                "owner_node": node_id,
                "external_owner": external_owner,
            }))

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

    def get_blob_type(self, blob_uri: str) -> BlobTypeResponse:
        return cast(BlobTypeResponse, self.request_json(
            METHOD_GET, "/blob_type", {
                "blob_uri": blob_uri,
            },
        ))

    def get_csv_blob(self, blob_uri: str) -> 'CSVBlobHandle':
        blob_type = self.get_blob_type(blob_uri)
        if not blob_type["is_csv"]:
            raise ValueError(f"blob: {blob_uri} is not csv type")
        return CSVBlobHandle(self, blob_uri, is_full=False)

    def get_torch_blob(self, blob_uri: str) -> 'TorchBlobHandle':
        blob_type = self.get_blob_type(blob_uri)
        if not blob_type["is_torch"]:
            raise ValueError(f"blob: {blob_uri} is not torch type")
        return TorchBlobHandle(self, blob_uri, is_full=False)

    def get_custom_code_blob(self, blob_uri: str) -> 'CustomCodeBlobHandle':
        blob_type = self.get_blob_type(blob_uri)
        if not blob_type["is_custom_code"]:
            raise ValueError(f"blob: {blob_uri} is not custom code type")
        return CustomCodeBlobHandle(self, blob_uri, is_full=False)

    def get_json_blob(self, blob_uri: str) -> 'JSONBlobHandle':
        blob_type = self.get_blob_type(blob_uri)
        if not blob_type["is_json"]:
            raise ValueError(f"blob: {blob_uri} is not json type")
        return JSONBlobHandle(self, blob_uri, is_full=False)

    def duplicate_dag(
            self,
            dag_uri: str,
            dest_uri: Optional[str] = None,
            copy_nonowned_blobs: Optional[bool] = None,
            retain_nonowned_blobs: bool = False,
            warnings_io: Optional[IO[Any]] = sys.stderr) -> str:
        if copy_nonowned_blobs is None:
            copy_nonowned_blobs = not retain_nonowned_blobs
        elif warnings_io is not None:
            warnings_io.write(
                "copy_nonowned_blobs is deprecated; "
                "use retain_nonowned_blobs instead\n")
            warnings_io.flush()
        # FIXME: !!!xyme-backend bug!!!
        copy_nonowned_blobs = not copy_nonowned_blobs
        args = {
            "dag": dag_uri,
            "copy_nonowned_blobs": copy_nonowned_blobs,
        }
        if dest_uri is not None:
            args["dest"] = dest_uri
        return cast(DagDupResponse, self.request_json(
            METHOD_POST, "/dag_dup", args))["dag"]

    def set_dag(
            self,
            dag_uri: str,
            defs: UserDagDef,
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

    def set_settings(
            self, config_token: str, settings: SettingsObj) -> SettingsObj:
        return cast(NamespaceUpdateSettings, self.request_json(
            METHOD_POST, "/settings", {
                "settings": settings,
                "config_token": config_token,
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
                    "minimal": True,
                }))
        return cast(QueueStatsResponse, self.request_json(
            METHOD_GET, "/queue_stats", {
                "dag": dag,
                "minimal": False,
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

    def get_kafka_error_topic(self) -> str:
        res = cast(KafkaTopicNames, self.request_json(
            METHOD_GET, "/kafka_topic_names", {}))["error"]
        assert res is not None
        return res

    def get_kafka_error_message_topic(self) -> str:
        res = cast(KafkaTopicNames, self.request_json(
            METHOD_GET, "/kafka_topic_names", {}))["error_msg"]
        assert res is not None
        return res

    def delete_kafka_error_topic(self) -> KafkaTopics:
        return cast(KafkaTopics, self.request_json(
            METHOD_POST, "/kafka_topics", {
                "num_partitions": 0,
            }))

    def read_kafka_errors(
            self,
            consumer_type: str,
            offset: str = "current") -> List[str]:
        if consumer_type not in CONSUMER_TYPES:
            raise ValueError(
                f"consumer_type cannot be {consumer_type} "
                "for reading kafka errors. provide consumer type from "
                f"{CONSUMER_TYPES}")
        return cast(List[str], self.request_json(
            METHOD_GET, "/kafka_msg", {
                "offset": offset,
                "consumer_type": consumer_type,
            }))

    def read_kafka_full_json_errors(
            self,
            input_id_path: List[str],
            err_msg_state: KafkaErrorMessageState,
            ) -> Iterable[Tuple[str, str]]:
        """
        Provides information as to what the error is and what is the
        input id and its associated input message.

        Args:
            input_id_path (List[str]):
                The path of the field to be considered as input_id in the
                input json.
            err_msg_state (KafkaErrorMessageState):
                This will be populated with the mappings of input_ids
                to messages. Also stores any unmatched messages in the
                unmatched list and after filling msg_lookup, check if they
                have matches. Initially an object of KafkaErrorMessageState
                can be passed for this argument.

        Yields:
            Iterable[Tuple[str, Optional[str]]]: the error, the input msg
            associated with the input_id.
        """
        errs = self.read_kafka_errors(consumer_type=CONSUMER_ERR)
        msgs = self.read_kafka_errors(consumer_type=CONSUMER_ERR_MSG)

        def parse_input_id_json(json_str: str) -> Optional[str]:
            try:
                res = json.loads(json_str)
                for path in input_id_path:
                    res = res[path]
                return res
            except json.decoder.JSONDecodeError:
                return None
            except KeyError:
                return None

        def parse_input_id_text(text: str) -> Optional[str]:
            ix = text.find("\ninput_id: ")
            if ix != -1:
                return text[ix + len("\ninput_id: "):]
            return None

        for msg in msgs:
            input_id = parse_input_id_json(msg)
            if input_id is not None:
                err_msg_state.add_msg(input_id, msg)
        for old_err in err_msg_state.get_unmatched():
            input_id = parse_input_id_text(old_err)
            if input_id is None:
                continue
            match = err_msg_state.get_msg(input_id)
            if match is None:
                err_msg_state.add_unmatched(old_err)
            else:
                yield (old_err, match)
        for err in errs:
            input_id = parse_input_id_text(err)
            if input_id is None:
                continue
            match = err_msg_state.get_msg(input_id)
            if match is None:
                err_msg_state.add_unmatched(err)
            else:
                yield (err, match)

    def get_named_secrets(
            self,
            config_token: Optional[str] = None,
            show_values: bool = False) -> Dict[str, Optional[str]]:
        if show_values and config_token is None:
            raise ValueError("config_token must be set to show_values")
        return cast(Dict[str, Optional[str]], self.request_json(
            METHOD_GET, "/named_secrets", {
                "show": int(bool(show_values)),
                "config_token": config_token,
            }))

    def set_named_secret(
            self, config_token: str, key: str, value: str) -> bool:
        return cast(SetNamedSecret, self.request_json(
            METHOD_PUT, "/named_secrets", {
                "key": key,
                "value": value,
                "config_token": config_token,
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

    @staticmethod
    def read_dvc(
            path: str,
            repo: str,
            rev: Optional[str] = "HEAD",
            warnings_io: Optional[IO[Any]] = sys.stderr) -> Any:
        """Reading dvc file content from git tracked DVC project.

        Args:
            path (str):
                File path to read, relative to the root of the repo.
            repo (str):
                specifies the location of the DVC project. It can be a
                github URL or a file system path.
            rev (str):
                Git commit (any revision such as a branch or tag name, or a
                commit hash). If repo is not a Git repo, this option is
                ignored. Default: HEAD.
            warnings_io (optional IO):
                IO stream where the warning will be printed to

        Returns:
            the content of the file.
        """
        from .util import has_dvc

        if not has_dvc():
            if warnings_io is not None:
                warnings_io.write(
                    "Please install dvc https://dvc.org/doc/install")
            return None

        import dvc.api

        res = dvc.api.read(path, repo=repo, rev=rev, mode="r")
        maybe_parse = maybe_json_loads(res)
        if maybe_parse is not None:
            return maybe_parse
        return res

    @staticmethod
    def get_env_str(key: str, default: Optional[str] = None) -> str:
        res = os.getenv(key, default=default)
        if res is None:
            raise ValueError(f"environment variable {key} is not set")
        return f"{res}"

    @staticmethod
    def get_env_int(key: str, default: Optional[int] = None) -> int:
        res = os.getenv(key, default=default)
        if res is None:
            raise ValueError(f"environment variable {key} is not set")
        return int(res)

    @staticmethod
    def get_env_bool(key: str, default: Optional[bool] = None) -> bool:
        res = os.getenv(key, default=default)
        if res is None:
            raise ValueError(f"environment variable {key} is not set")
        return to_bool(res)

    @staticmethod
    def load_json(json_path: str) -> Dict[str, Any]:
        with open(json_path, "r") as fin:
            return json.load(fin)

    @classmethod
    def load_s3_config(cls, config_path: str) -> S3Config:
        return cast(S3Config, cls.load_json(config_path))

    @classmethod
    def download_s3_from_file(
            cls, dest_path: List[str], config_path: str) -> None:
        cls.download_s3(dest_path, cls.load_s3_config(config_path))

    @staticmethod
    def download_s3(dest_path: List[str], config: S3Config) -> None:
        import boto3

        s3 = boto3.client(
            "s3",
            aws_access_key_id=config["accern_aws_key"],
            aws_secret_access_key=config["accern_aws_access_key"])
        assert len(dest_path) == len(config["model_download_path"])
        for (dest, path) in zip(dest_path, config["model_download_path"]):
            s3.download_file(config["model_download_bucket"], path, dest)

    @classmethod
    def upload_s3_from_file(
            cls, source_path: List[str], config_path: str) -> None:
        cls.upload_s3(source_path, cls.load_s3_config(config_path))

    @staticmethod
    def upload_s3(source_path: List[str], config: S3Config) -> None:
        import boto3

        s3 = boto3.client(
            "s3",
            aws_access_key_id=config["accern_aws_key"],
            aws_secret_access_key=config["accern_aws_access_key"])
        assert len(source_path) == len(config["model_download_path"])
        for (source, path) in zip(source_path, config["model_download_path"]):
            s3.upload_file(source, config["model_download_bucket"], path)

    def get_uuid(self) -> str:
        return cast(UUIDResponse, self.request_json(
            METHOD_GET, "/uuid", {}))["uuid"]

    def delete_blobs(self, blob_uris: List[str]) -> DeleteBlobResponse:
        return cast(DeleteBlobResponse, self.request_json(
            METHOD_DELETE, "/blob", {
                "blob_uris": blob_uris,
            },
        ))

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
        self._state_uri: Optional[str] = None
        self._uri_prefix: Optional[URIPrefix] = None
        self._is_high_priority: Optional[bool] = None
        self._queue_mng: Optional[str] = None
        self._version_override: Optional[str] = None
        self._nodes: Dict[str, NodeHandle] = {}
        self._node_lookup: Dict[str, str] = {}
        self._dynamic_error: Optional[str] = None
        self._ins: Optional[List[str]] = None
        self._outs: Optional[List[Tuple[str, str]]] = None
        self._kafka_input_topic: Optional[str] = None
        self._kafka_output_topic: Optional[str] = None

    def refresh(self) -> None:
        self._name = None
        self._company = None
        self._state_uri = None
        self._uri_prefix = None
        self._is_high_priority = None
        self._queue_mng = None
        self._version_override = None
        self._ins = None
        self._outs = None
        self._kafka_input_topic = None
        self._kafka_output_topic = None
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
        self._state_uri = info["state_uri"]
        self._uri_prefix = info["uri_prefix"]
        self._is_high_priority = info["high_priority"]
        self._queue_mng = info["queue_mng"]
        self._version_override = info["version_override"]
        self._ins = info["ins"]
        self._outs = [(el[0], el[1]) for el in info["outs"]]
        self._kafka_input_topic = info["kafka_input_topic"]
        self._kafka_output_topic = info["kafka_output_topic"]
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

    def get_state_uri(self) -> str:
        self._maybe_refresh()
        self._maybe_fetch()
        assert self._state_uri is not None
        return self._state_uri

    def get_version_override(self) -> str:
        self._maybe_refresh()
        self._maybe_fetch()
        assert self._version_override is not None
        return self._version_override

    def get_kafka_topics(self) -> Tuple[Optional[str], Optional[str]]:
        self._maybe_refresh()
        self._maybe_fetch()
        return self._kafka_input_topic, self._kafka_output_topic

    def get_uri_prefix(self) -> URIPrefix:
        self._maybe_refresh()
        self._maybe_fetch()
        assert self._uri_prefix is not None
        return self._uri_prefix

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

    def set_dag(self, defs: UserDagDef) -> None:
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

    def _legacy_dynamic_list(
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
                    cur_res = self._legacy_dynamic_list(
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
        if self._client._api_version < 5:
            return self._legacy_dynamic_list(
                inputs,
                input_key=input_key,
                output_key=output_key,
                split_th=None,
                max_threads=max_threads,
                format_method=format_method,
                force_keys=force_keys,
                no_cache=no_cache)

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

        split_num: int = split_th
        assert split_num > 0

        def compute_dynamic_list(cur: int, _: threading.RLock) -> Any:
            result = None
            try:
                result = self.dynamic_list(
                    inputs[cur:cur + split_num],
                    input_key=input_key,
                    output_key=output_key,
                    split_th=None,
                    max_threads=max_threads,
                    format_method=format_method,
                    force_keys=force_keys,
                    no_cache=no_cache)
            except BaseException as e:  # pylint: disable=broad-except
                try:
                    if isinstance(e, BaseException):
                        raise e
                except RequestException as err:
                    raise ValueError(
                        "request error while processing.") from err
            return result

        tasks = list(range(0, len(inputs), split_num))
        arr = compute_parallel(
            tasks, compute_dynamic_list, None, max_threads)
        return list(itertools.chain(*arr))

    def dynamic(self, input_data: BytesIO) -> Optional[ByteResponse]:
        cur_res, ctype = self._client.request_bytes(
            METHOD_FILE, "/dynamic", {
                "dag": self.get_uri(),
            }, files={
                "file": input_data,
            })
        return interpret_ctype(cur_res, ctype)

    def dynamic_obj(self, input_obj: Any) -> Optional[ByteResponse]:
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

    def get_dynamic_result(self, value_id: str) -> Optional[ByteResponse]:
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
            num_threads: int = 20) -> Iterable[Optional[ByteResponse]]:

        def get(hnd: 'ComputationHandle') -> Optional[ByteResponse]:
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
            num_threads: int = 20) -> Iterable[Optional[ByteResponse]]:

        def get(hnd: 'ComputationHandle') -> Optional[ByteResponse]:
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

    def _pretty(
            self,
            nodes_only: bool,
            allow_unicode: bool,
            method: Optional[str] = "accern",
            fields: Optional[List[str]] = None) -> PrettyResponse:
        args = {
            "dag": self.get_uri(),
            "nodes_only": nodes_only,
            "allow_unicode": allow_unicode,
            "method": method,
        }
        if fields is not None:
            args["fields"] = ",".join(fields)
        return cast(PrettyResponse, self._client.request_json(
            METHOD_GET, "/pretty", args))

    def pretty(
            self,
            nodes_only: bool = False,
            allow_unicode: bool = True,
            method: Optional[str] = "dot",
            fields: Optional[List[str]] = None,
            output_format: Optional[str] = "png",
            display: Optional[IO[Any]] = sys.stdout) -> Optional[str]:

        def render(value: str) -> Optional[str]:
            if display is not None:
                display.write(value)
                display.flush()
                return None
            return value

        graph_str = self._pretty(
            nodes_only=nodes_only,
            allow_unicode=allow_unicode,
            method=method,
            fields=fields)["pretty"]
        if method == "accern":
            return render(graph_str)
        if method == "dot":
            try:
                from graphviz import Source

                graph = Source(graph_str)
                if output_format == "dot":
                    return render(graph_str)
                if output_format == "svg":
                    svg_str = graph.pipe(format="svg")
                    if display is not None:
                        if not is_jupyter():
                            display.write(
                                "Warning: Ipython instance not found.\n")
                            display.write(svg_str)
                            display.flush()
                        else:
                            from IPython.display import display as idisplay
                            from IPython.display import SVG
                            idisplay(SVG(svg_str))
                        return None
                    return svg_str
                if output_format == "png":
                    graph = Source(graph_str)
                    png_str = graph.pipe(format="png")
                    if display is not None:
                        if not is_jupyter():
                            display.write(
                                "Warning: Ipython instance not found.\n")
                            display.write(png_str)
                            display.flush()
                        else:
                            from IPython.display import display as idisplay
                            from IPython.display import Image

                            idisplay(Image(png_str))
                        return None
                    return png_str
                if output_format == "ascii":
                    if not has_graph_easy():
                        return render(graph_str)

                    import subprocess
                    cmd = ["echo", graph_str]
                    with subprocess.Popen(cmd, stdout=subprocess.PIPE) as p1:
                        p2 = subprocess.check_output(
                            ["graph-easy"], stdin=p1.stdout)
                        res = p2.decode("utf-8")
                    return render(res)
                raise ValueError(
                    f"invalid format {output_format}, "
                    "use svg, png, ascii, or dot")
            except ExecutableNotFound as e:
                raise RuntimeError(
                    "use 'brew install graphviz' or use 'method=accern'",
                ) from e
        raise ValueError(
            f"invalid method {method}, use accern or dot")

    def pretty_obj(
            self,
            nodes_only: bool = False,
            allow_unicode: bool = True,
            fields: Optional[List[str]] = None) -> List[DagPrettyNode]:
        return self._pretty(
            nodes_only=nodes_only,
            allow_unicode=allow_unicode,
            fields=fields)["nodes"]

    def get_def(self, full: bool = True) -> UserDagDef:
        return cast(UserDagDef, self._client.request_json(
            METHOD_GET, "/dag_def", {
                "dag": self.get_uri(),
                "full": full,
            }))

    def set_attr(self, attr: str, value: Any) -> None:
        dag_def = self.get_def()
        dag_def[attr] = value  # type: ignore
        self._client.set_dag(self.get_uri(), dag_def)

    def set_name(self, value: str) -> None:
        self.set_attr("name", value)

    def set_company(self, value: str) -> None:
        self.set_attr("company", value)

    def set_state_uri(self, value: str) -> None:
        self.set_attr("state_uri", value)

    def set_uri_prefix(self, value: URIPrefix) -> None:
        self.set_attr("uri_prefix", value)

    def set_high_priority(self, value: bool) -> None:
        self.set_attr("high_priority", value)

    def set_queue_mng(self, value: Optional[str]) -> None:
        self.set_attr("queue_mng", value)

    def set_version_override(self, value: Optional[str]) -> None:
        self.set_attr("version_override", value)

    def set_kafka_input_topic(self, value: Optional[str]) -> None:
        self.set_attr("kafka_input_topic", value)

    def set_kafka_output_topic(self, value: Optional[str]) -> None:
        self.set_attr("kafka_output_topic", value)

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

    def scale_worker(self, replicas: int) -> int:
        return cast(WorkerScale, self._client.request_json(
            METHOD_PUT, "/worker", {
                "dag": self.get_uri(),
                "replicas": replicas,
                "task": None,
            }))["num_replicas"]

    def reload(self, timestamp: Optional[float] = None) -> float:
        return cast(DagReload, self._client.request_json(
            METHOD_PUT, "/dag_reload", {
                "dag": self.get_uri(),
                "when": timestamp,
            }))["when"]

    def get_kafka_input_topic(self, postfix: str = "") -> str:
        res = cast(KafkaTopicNames, self._client.request_json(
            METHOD_GET, "/kafka_topic_names", {
                "dag": self.get_uri(),
                "postfix": postfix,
                "no_output": True,
            }))["input"]
        assert res is not None
        return res

    def get_kafka_output_topic(self) -> str:
        res = cast(KafkaTopicNames, self._client.request_json(
            METHOD_GET, "/kafka_topic_names", {
                "dag": self.get_uri(),
            }))["output"]
        assert res is not None
        return res

    def set_kafka_topic_partitions(
            self,
            num_partitions: int,
            postfix: str = "",
            large_input_retention: bool = False,
            no_output: bool = False) -> KafkaTopics:
        return cast(KafkaTopics, self._client.request_json(
            METHOD_POST, "/kafka_topics", {
                "dag": self.get_uri(),
                "num_partitions": num_partitions,
                "postfix": postfix,
                "large_input_retention": large_input_retention,
                "no_output": no_output,
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

    def post_kafka_msgs(
            self,
            input_data: List[BytesIO],
            postfix: str = "") -> List[str]:
        names = [f"file{pos}" for pos in range(len(input_data))]
        res = cast(KafkaMessage, self._client.request_json(
            METHOD_FILE, "/kafka_msg", {
                "dag": self.get_uri(),
                "postfix": postfix,
            }, files=dict(zip(names, input_data))))
        msgs = res["messages"]
        return [msgs[key] for key in names]

    def read_kafka_output(
            self,
            offset: str = "current",
            max_rows: int = 100) -> Optional[ByteResponse]:
        offset_str = [offset]

        def read_single() -> Tuple[Optional[ByteResponse], str]:
            cur, read_ctype = self._client.request_bytes(
                METHOD_GET, "/kafka_msg", {
                    "dag": self.get_uri(),
                    "offset": offset_str[0],
                    "consumer_type": CONSUMER_DAG,
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

    def get_kafka_offsets(
            self,
            alive: bool,
            postfix: Optional[str] = None) -> KafkaOffsets:
        args = {
            "dag": self.get_uri(),
            "alive": int(alive),
        }
        if postfix is not None:
            args["postfix"] = postfix
        return cast(KafkaOffsets, self._client.request_json(
            METHOD_GET, "/kafka_offsets", args))

    def get_kafka_throughput(
            self,
            postfix: Optional[str] = None,
            segment_interval: float = 120.0,
            segments: int = 5) -> KafkaThroughput:
        assert segments > 0
        assert segment_interval > 0.0
        offsets = self.get_kafka_offsets(postfix=postfix, alive=False)
        now = time.monotonic()
        measurements: List[Tuple[int, int, int, int, float]] = [(
            offsets["input"],
            offsets["output"],
            offsets["error"],
            offsets["error_msg"],
            now,
        )]
        for _ in range(segments):
            prev = now
            while now - prev < segment_interval:
                time.sleep(max(0.0, segment_interval - (now - prev)))
                now = time.monotonic()
            offsets = self.get_kafka_offsets(postfix=postfix, alive=False)
            measurements.append((
                offsets["input"],
                offsets["output"],
                offsets["error"],
                offsets["error_msg"],
                now,
            ))
        first = measurements[0]
        last = measurements[-1]
        total_input = last[0] - first[0]
        total_output = last[1] - first[1]
        errors = last[2] - first[2]
        error_msgs = last[3] - first[3]
        total = last[4] - first[4]
        input_segments: List[float] = []
        output_segments: List[float] = []
        cur_input = first[0]
        cur_output = first[1]
        cur_time = first[4]
        for (next_input, next_output, _, _, next_time) in measurements[1:]:
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
            "error_msgs": error_msgs,
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

    def delete(self) -> DeleteBlobResponse:
        return cast(DeleteBlobResponse, self._client.request_json(
            METHOD_DELETE, "/blob", {
                "blob_uris": [self.get_uri()],
            },
        ))

    def download_full_dag_zip(
            self, to_path: Optional[str]) -> Optional[io.BytesIO]:
        cur_res, _ = self._client.request_bytes(
            METHOD_GET, "/download_dag_zip", {
                "dag": self.get_uri(),
            })
        if to_path is None:
            return io.BytesIO(cur_res.read())
        with open(to_path, "wb") as file_download:
            file_download.write(cur_res.read())
        return None

    def _upload_dag_blobs(
            self,
            tmpdir: str,
            blobs_map: List[BlobDetails],
            max_threads: int) -> List['BlobHandle']:
        blob_handles = []
        for blob in blobs_map:
            blob_file = os.path.join(tmpdir, blob["fname"])
            blob_handle = self._client.get_blob_handle(blob["blob_uri"])
            blob_handles.extend(blob_handle.upload_zip(blob_file, max_threads))
        return blob_handles

    def upload_full_dag_zip(
            self, source: str, max_threads: int = 10) -> List['BlobHandle']:
        dag_blob_handle = self._client.get_blob_handle(self.get_uri())
        blob_handles = []
        with tempfile.TemporaryDirectory() as tmpdir:
            shutil.unpack_archive(source, tmpdir, "zip")
            fmap_json = os.path.join(tmpdir, "filemap.json")
            with open(fmap_json, "r") as fmap:
                file_map: FileMap = json.load(fmap)
            dag_fname = file_map["dag_blob"]
            dag_file = os.path.join(tmpdir, f"{dag_fname}")
            blob_handles.extend(
                dag_blob_handle.upload_zip(dag_file, max_threads))
            blob_handles.extend(self._upload_dag_blobs(
                tmpdir, file_map["blobs"], max_threads))
        return blob_handles

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
        self._is_model: Optional[bool] = None
        self._version_override: Optional[str] = None

    def as_owner(self) -> BlobOwner:
        return {
            "owner_dag": self.get_dag().get_uri(),
            "owner_node": self.get_id(),
        }

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
        if self.get_id() != node_info["id"]:
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
        self._version_override = node_info["version_override"]

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

    def get_version_override(self) -> Optional[str]:
        return self._version_override

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

    def get_index_col(self) -> str:
        return "_index" if self._client.has_version(4, 1) else "index"

    def get_row_id_col(self) -> str:
        return "_row_id" if self._client.has_version(4, 1) else "row_id"

    def read(
            self,
            key: str,
            chunk: Optional[int],
            force_refresh: bool = False,
            filter_id: bool = True) -> Optional[ByteResponse]:
        content = self.read_blob(key, chunk, force_refresh).get_content()
        if filter_id and isinstance(content, pd.DataFrame):
            df: pd.DataFrame = content
            df = df[df[self.get_row_id_col()] >= 0]
            df = df.set_index(self.get_index_col(), drop=True)
            df.index.name = None
            content = df.copy()
        return content

    def read_all(
            self,
            key: str,
            force_refresh: bool = False,
            filter_id: bool = True) -> Optional[ByteResponse]:
        self.read(
            key, chunk=None, force_refresh=force_refresh, filter_id=False)
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
        content = merge_ctype(res, ctype)
        if filter_id and isinstance(content, pd.DataFrame):
            df: pd.DataFrame = content
            df = df[df[self.get_row_id_col()] >= 0]
            df = df.set_index(self.get_index_col(), drop=True)
            df.index.name = None
            content = df.copy()
        return content

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

    def get_blob_uri(
            self, blob_key: str, blob_type: str) -> Tuple[str, BlobOwner]:
        res = cast(BlobURIResponse, self._client.request_json(
            METHOD_GET, "/blob_uri", {
                "dag": self.get_dag().get_uri(),
                "node": self.get_id(),
                "key": blob_key,
                "type": blob_type,
            }))
        return res["uri"], res["owner"]

    def get_csv_blob(self, key: str = "orig") -> 'CSVBlobHandle':
        uri, owner = self.get_blob_uri(key, "csv")
        blob = CSVBlobHandle(self._client, uri, is_full=False)
        blob.set_local_owner(owner)
        return blob

    def get_torch_blob(self, key: str = "orig") -> 'TorchBlobHandle':
        uri, owner = self.get_blob_uri(key, "torch")
        blob = TorchBlobHandle(self._client, uri, is_full=False)
        blob.set_local_owner(owner)
        return blob

    def get_json_blob(self, key: str = "jsons_in") -> 'JSONBlobHandle':
        uri, owner = self.get_blob_uri(key, "json")
        blob = JSONBlobHandle(self._client, uri, is_full=False)
        blob.set_local_owner(owner)
        return blob

    def get_custom_code_blob(
            self, key: str = "custom_code") -> 'CustomCodeBlobHandle':
        uri, owner = self.get_blob_uri(key, "custom_code")
        blob = CustomCodeBlobHandle(self._client, uri, is_full=False)
        blob.set_local_owner(owner)
        return blob

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

    def get_def(self) -> NodeDef:
        return cast(NodeDef, self._client.request_json(
            METHOD_GET, "/node_def", {
                "dag": self.get_dag().get_uri(),
                "node": self.get_id(),
            }))

    # ModelLike Nodes only

    def is_model(self) -> bool:
        if self._is_model is None:
            self._is_model = cast(NodeTypeResponse, self._client.request_json(
                METHOD_GET, "/node_type", {
                    "dag": self.get_dag().get_uri(),
                    "node": self.get_id(),
                }))["is_model"]

        return self._is_model

    def ensure_is_model(self) -> None:
        if not self.is_model():
            raise ValueError(f"{self} is not a model node.")

    def setup_model(self, obj: Dict[str, Any]) -> ModelInfo:
        self.ensure_is_model()
        return cast(ModelInfo, self._client.request_json(
            METHOD_PUT, "/model_setup", {
                "dag": self.get_dag().get_uri(),
                "node": self.get_id(),
                "config": obj,
            }))

    def get_model_params(self) -> ModelParamsResponse:
        self.ensure_is_model()
        return cast(ModelParamsResponse, self._client.request_json(
            METHOD_GET, "/model_params", {
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
        self._owner: Optional[BlobOwner] = None
        self._info: Optional[Dict[str, Any]] = None
        self._parent: Optional[BlobHandle] = None

    def is_full(self) -> bool:
        return self._is_full

    def is_empty(self) -> bool:
        return self._uri.startswith(EMPTY_BLOB_PREFIX)

    def get_uri(self) -> str:
        return self._uri

    def get_path(self, *path: str) -> 'BlobHandle':
        if self.is_full():
            raise ValueError(f"URI must not be full: {self}")
        return BlobHandle(
            self._client, f"{self._uri}/{'/'.join(path)}", is_full=True)

    def get_parent(self) -> 'BlobHandle':
        if self._parent is None:
            uri = urlparse(self._uri)
            path = PurePath(*PosixPath(uri.path).parts[:3])
            new_uri = urlunparse(
                (uri.scheme, uri.netloc, path.as_posix(), None, None, None))
            res = BlobHandle(self._client, new_uri, is_full=False)
            self._parent = res
        return self._parent

    def get_ctype(self) -> Optional[str]:
        return self._ctype

    def clear_info_cache(self) -> None:
        self._info = None

    def get_info(self) -> Dict[str, Any]:
        if self.is_full():
            raise ValueError(f"URI must not be full: {self}")
        if self._info is None:
            info = self.get_path("info.json").get_content()
            assert info is not None
            self._info = cast(Dict[str, Any], info)
        return self._info

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
        if self._owner is None:
            self._owner = self._client.get_blob_owner(self.get_uri())
        return self._owner

    def set_local_owner(self, owner: BlobOwner) -> None:
        self._owner = owner

    def get_owner_dag(self) -> Optional[str]:
        owner = self.get_owner()
        return owner["owner_dag"]

    def get_owner_node(self) -> str:
        owner = self.get_owner()
        return owner["owner_node"]

    def set_model_threshold(
            self,
            threshold: float,
            pos_label: str) -> ModelInfo:
        return cast(ModelInfo, self._client.request_json(
            METHOD_PUT, "/threshold", {
                "blob": self.get_uri(),
                "threshold": threshold,
                "pos_label": pos_label,
            }))

    def del_model_threshold(self) -> ModelInfo:
        return cast(ModelInfo, self._client.request_json(
            METHOD_DELETE, "/threshold", {
                "blob": self.get_uri(),
            }))

    def get_model_info(self) -> ModelInfo:
        return cast(ModelInfo, self._client.request_json(
            METHOD_GET, "/model_info", {
                "blob": self.get_uri(),
            }))

    def copy_to(
            self,
            to_uri: str,
            new_owner: Optional[NodeHandle] = None,
            external_owner: bool = False) -> 'BlobHandle':
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
                "external_owner": external_owner,
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
            additional: Dict[str, Union[str, int, List[int]]],
            fobj: Optional[IO[bytes]]) -> UploadFilesResponse:
        args: Dict[str, Union[str, int, List[int]]] = {
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

    def _legacy_append_upload(
            self,
            uri: str,
            fobj: IO[bytes]) -> int:
        res = self._perform_upload_action(
            "append", {"uri": uri}, fobj=fobj)
        return res["pos"]

    def _append_upload(
            self,
            uri: str,
            fobj: IO[bytes],
            offset: int) -> int:
        res = self._perform_upload_action(
            "append", {"uri": uri, "offset": offset}, fobj=fobj)
        return res["pos"]

    def _finish_upload_zip(self) -> List[str]:
        uri = self._tmp_uri
        if uri is None:
            raise ValueError("tmp_uri is None")
        res = cast(UploadFilesResponse, self._client.request_json(
            METHOD_POST, "/finish_zip", {"uri": uri}))
        return res["files"]

    def _finish_model_upload(
            self,
            xcols: Optional[List[str]],
            ycols: Optional[List[str]],
            is_clf: bool,
            model_name: str,
            version: int,
            model_params: Dict[str, Any],
            delete_later_versions: bool,
            full_init: bool) -> UploadFilesResponse:
        uri = self._tmp_uri
        if uri is None:
            raise ValueError("tmp_uri is None")
        return cast(UploadFilesResponse, self._client.request_json(
            METHOD_POST, "/finish_model_upload", {
                "model_params": model_params,
                "full_init": full_init,
                "is_clf": is_clf,
                "model_uri": self.get_uri(),
                "model_name": model_name,
                "version": version,
                "owner_dag": self.get_owner_dag(),
                "owner_node": self.get_owner_node(),
                "tmp_uri": uri,
                "xcols": xcols,
                "ycols": ycols,
                "delete_later_versions": delete_later_versions,
            }))

    def _clear_upload(self) -> None:
        uri = self._tmp_uri
        if uri is None:
            raise ValueError("tmp_uri is None")
        self._perform_upload_action("clear", {"uri": uri}, fobj=None)

    def _legacy_upload_file(
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
            new_size = self._legacy_append_upload(tmp_uri, BytesIO(buff))
            if new_size - cur_size != len(buff):
                raise ValueError(
                    f"incomplete chunk upload n:{new_size} "
                    f"o:{cur_size} b:{len(buff)}")
            cur_size = new_size
        print_progress(cur_size / total_size, True)

    def _upload_file(
            self,
            file_content: IO[bytes],
            max_threads: int,
            ext: str,
            progress_bar: Optional[IO[Any]] = sys.stdout) -> None:
        if self._client._api_version < 5:
            self._legacy_upload_file(file_content, ext="zip")
            return
        init_pos = file_content.seek(0, io.SEEK_CUR)
        file_hash = get_file_hash(file_content)
        total_size = file_content.seek(0, io.SEEK_END) - init_pos
        file_content.seek(init_pos, io.SEEK_SET)
        if progress_bar is not None:
            progress_bar.write("Uploading file:\n")
        print_progress = get_progress_bar(out=progress_bar)
        tmp_uri = self._start_upload(total_size, file_hash, ext)
        self._tmp_uri = tmp_uri
        upload_chunk_size = get_file_upload_chunk_size()
        total_chunks = math.ceil(total_size / upload_chunk_size)

        begins = [chunk * upload_chunk_size for chunk in range(total_chunks)]

        def compute_upload(
                offset: int,
                compute_lock: threading.RLock) -> None:
            assert self._tmp_uri is not None
            with compute_lock:
                file_content.seek(offset, io.SEEK_SET)
                buff = file_content.read(upload_chunk_size)
            new_size = self._append_upload(
                self._tmp_uri,
                BytesIO(buff),
                offset)
            if new_size != len(buff):
                raise ValueError(
                    f"incomplete chunk upload n:{new_size} "
                    f"b:{len(buff)} offset: {offset}")

        compute_parallel(begins, compute_upload, print_progress, max_threads)

    def upload_zip(
            self,
            source: Union[str, io.BytesIO],
            max_threads: int = 10) -> List['BlobHandle']:
        files: List[str] = []
        try:
            if isinstance(source, str) or not hasattr(source, "read"):
                with open(f"{source}", "rb") as fin:
                    self._upload_file(fin, max_threads, ext="zip")
            else:
                self._upload_file(source, max_threads, ext="zip")
            files = self._finish_upload_zip()
        finally:
            self._clear_upload()
        return [
            BlobHandle(self._client, blob_uri, is_full=True)
            for blob_uri in files
        ]

    def upload_model_file(
            self,
            model_obj: IO[bytes],
            xcols: Optional[List[str]],
            ycols: Optional[List[str]],
            is_clf: bool,
            model_name: str,
            version: int = -1,
            model_params: Optional[Dict[str, Any]] = None,
            delete_later_versions: bool = False,
            max_threads: int = 10,
            full_init: bool = True) -> UploadFilesResponse:
        try:
            self._upload_file(model_obj, max_threads, ext="pkl")
            if model_params is None:
                model_params = {}
            return self._finish_model_upload(
                model_name=model_name,
                version=version,
                model_params=model_params,
                xcols=xcols,
                ycols=ycols,
                is_clf=is_clf,
                delete_later_versions=delete_later_versions,
                full_init=full_init)
        finally:
            self._clear_upload()

    def upload_sklike_model(
            self,
            model: Any,
            xcols: List[str],
            ycols: Optional[List[str]],
            is_clf: bool,
            model_name: Optional[str] = None,
            version: int = -1,
            model_params: Optional[Dict[str, Any]] = None,
            delete_later_versions: bool = False,
            full_init: bool = True) -> UploadFilesResponse:
        if model_name is None:
            try:
                model_name = type(model).__name__
            except Exception as e:
                raise ValueError(f"can not infer model name {model}") from e
        if model_params is None:
            model_params = {}
        try:
            if is_clf and "classes" not in model_params:
                model_params["classes"] = model.classes_
        except Exception as e:
            raise ValueError(f"can not infer classes from {model}") from e
        dump = pickle.dumps(model, pickle.HIGHEST_PROTOCOL)
        with io.BytesIO(dump) as buffer:
            return self.upload_model_file(
                buffer,
                xcols,
                ycols,
                is_clf,
                model_name,
                version,
                model_params,
                delete_later_versions,
                full_init)

    def convert_model(
            self,
            version: Optional[int] = None,
            reload: bool = True) -> ModelReleaseResponse:
        return cast(ModelReleaseResponse, self._client.request_json(
            METHOD_POST, "/convert_model", {
                "blob": self.get_uri(),
                "version": version,
                "reload": reload,
            }))

    def delete(self) -> DeleteBlobResponse:
        return cast(DeleteBlobResponse, self._client.request_json(
            METHOD_DELETE, "/blob", {
                "blob_uris": [self.get_uri()],
            },
        ))

    def get_model_release(self) -> ModelReleaseResponse:
        return cast(ModelReleaseResponse, self._client.request_json(
            METHOD_GET, "/model_release", {
                "blob": self.get_uri(),
            }))

    def get_model_version(self) -> ModelVersionResponse:
        return cast(ModelVersionResponse, self._client.request_json(
            METHOD_GET, "/model_version", {
                "model_uri": self.get_uri(),
            }))

    def _copy_model_version(
            self,
            model_uri: str,
            read_version: Optional[int],
            write_version: int,
            overwrite: bool) -> ModelVersionResponse:
        return cast(ModelVersionResponse, self._client.request_json(
            METHOD_PUT, "/model_version", {
                "model_uri": model_uri,
                "read_version": read_version,
                "write_version": write_version,
                "overwrite": overwrite,
            }))

    def copy_model_version(
            self,
            read_version: int,
            write_version: int,
            overwrite: bool) -> ModelVersionResponse:
        return self._copy_model_version(
            model_uri=self.get_uri(),
            read_version=read_version,
            write_version=write_version,
            overwrite=overwrite)

    def delete_model_version(self, version: int) -> ModelVersionResponse:
        return self._copy_model_version(
            model_uri=self.get_uri(),
            read_version=None,
            write_version=version,
            overwrite=True)

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
    def finish_csv_upload(
            self,
            filename: Optional[str] = None) -> UploadFilesResponse:
        tmp_uri = self._tmp_uri
        if tmp_uri is None:
            raise ValueError("tmp_uri is None")
        args: Dict[str, Optional[Union[str, int]]] = {
            "tmp_uri": tmp_uri,
            "csv_uri": self.get_uri(),
            "owner_dag": self.get_owner_dag(),
            "owner_node": self.get_owner_node(),
            "filename": filename,
        }
        return cast(UploadFilesResponse, self._client.request_json(
            METHOD_POST, "/finish_csv", args))

    def add_from_file(
            self,
            filename: str,
            progress_bar: Optional[IO[Any]] = sys.stdout,
            requeue_on_finish: Optional[NodeHandle] = None,
            max_threads: int = 10) -> Optional[UploadFilesResponse]:
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
                    max_threads,
                    ext=ext,
                    progress_bar=progress_bar)
            return self.finish_csv_upload(filename)
        finally:
            if requeue_on_finish is not None:
                requeue_on_finish.requeue()
            self._clear_upload()

    def add_from_df(
            self,
            df: pd.DataFrame,
            progress_bar: Optional[IO[Any]] = sys.stdout,
            requeue_on_finish: Optional[NodeHandle] = None,
            max_threads: int = 10) -> Optional[UploadFilesResponse]:
        io_in = None
        try:
            io_in = df_to_csv_bytes(df)
            self._upload_file(
                io_in,
                max_threads,
                ext="csv",
                progress_bar=progress_bar)
            return self.finish_csv_upload()
        finally:
            if requeue_on_finish is not None:
                requeue_on_finish.requeue()
            if io_in is not None:
                io_in.close()
            self._clear_upload()

    def add_from_content(
            self,
            content: Union[bytes, str, pd.DataFrame],
            progress_bar: Optional[IO[Any]] = sys.stdout,
            requeue_on_finish: Optional[NodeHandle] = None,
            max_threads: int = 10) -> Optional[UploadFilesResponse]:
        io_in = None
        try:
            io_in = content_to_csv_bytes(content)
            self._upload_file(
                io_in,
                max_threads,
                ext="csv",
                progress_bar=progress_bar)
            return self.finish_csv_upload()
        finally:
            if requeue_on_finish is not None:
                requeue_on_finish.requeue()
            if io_in is not None:
                io_in.close()
            self._clear_upload()

# *** CSVBlobHandle ***


class TorchBlobHandle(BlobHandle):
    def finish_torch_upload(
            self,
            filename: Optional[str] = None) -> UploadFilesResponse:
        tmp_uri = self._tmp_uri
        if tmp_uri is None:
            raise ValueError("tmp_uri is None")
        args: Dict[str, Optional[Union[str, int]]] = {
            "tmp_uri": tmp_uri,
            "torch_uri": self.get_uri(),
            "owner_dag": self.get_owner_dag(),
            "owner_node": self.get_owner_node(),
            "filename": filename,
        }
        return cast(UploadFilesResponse, self._client.request_json(
            METHOD_POST, "/finish_torch", args))

    def add_from_file(
            self,
            filename: str,
            progress_bar: Optional[IO[Any]] = sys.stdout,
            max_threads: int = 10) -> Optional[UploadFilesResponse]:
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
                    max_threads,
                    ext=ext,
                    progress_bar=progress_bar)
            return self.finish_torch_upload(filename)
        finally:
            self._clear_upload()

# *** TorchBlobHandle ***


class JSONBlobHandle(BlobHandle):
    def __init__(
            self,
            client: XYMEClient,
            uri: str,
            is_full: bool) -> None:
        super().__init__(client, uri, is_full)
        self._count: Optional[int] = None

    def get_count(self) -> Optional[int]:
        return self._count

    def append_jsons(
            self,
            jsons: List[Any],
            requeue_on_finish: Optional[NodeHandle] = None,
            ) -> None:
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

# *** JSONBlobHandle ***


class CustomCodeBlobHandle(BlobHandle):
    def set_custom_imports(
            self, modules: List[List[str]]) -> NodeCustomImports:
        return cast(NodeCustomImports, self._client.request_json(
            METHOD_PUT, "/custom_imports", {
                "dag": self.get_owner_dag(),
                "node": self.get_owner_node(),
                "modules": modules,
            }))

    def get_custom_imports(self) -> NodeCustomImports:
        return cast(NodeCustomImports, self._client.request_json(
            METHOD_GET, "/custom_imports", {
                "dag": self.get_owner_dag(),
                "node": self.get_owner_node(),
            }))

    def set_custom_code(self, func: FUNC) -> NodeCustomCode:
        from RestrictedPython import compile_restricted

        def fn_as_str(fun: FUNC) -> str:
            body = textwrap.dedent(inspect.getsource(fun))
            res = body + textwrap.dedent(f"""
            result = {fun.__name__}(*data, **kwargs)
            if result is None:
                raise ValueError("{fun.__name__} must return a value")
            """)
            compile_restricted(res, "inline", "exec")
            return res

        raw_code = fn_as_str(func)
        return cast(NodeCustomCode, self._client.request_json(
            METHOD_PUT, "/custom_code", {
                "dag": self.get_owner_dag(),
                "node": self.get_owner_node(),
                "code": raw_code,
            }))

    def get_custom_code(self) -> NodeCustomCode:
        return cast(NodeCustomCode, self._client.request_json(
            METHOD_GET, "/custom_code", {
                "dag": self.get_owner_dag(),
                "node": self.get_owner_node(),
            }))

# *** CustomCodeBlobHandle ***


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

    def get(self) -> Optional[ByteResponse]:
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


def default_xyme_client() -> XYMEClient:
    return create_xyme_client(
        url=DEFAULT_URL,
        token=os.getenv("XYME_SERVER_TOKEN"),
        namespace=DEFAULT_NAMESPACE)


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
