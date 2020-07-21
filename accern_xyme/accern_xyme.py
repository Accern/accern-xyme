from typing import (
    Any,
    cast,
    Callable,
    Dict,
    IO,
    Iterator,
    List,
    Optional,
    Set,
    TextIO,
    Tuple,
    TYPE_CHECKING,
    Union,
)
import io
import inspect
import os
import sys
import json
import textwrap
import time
import weakref
import contextlib
import collections
from io import BytesIO, StringIO
import pandas as pd
import quick_server
import requests

from .util import (
    df_to_csv,
    get_file_hash,
    get_file_upload_chunk_size,
    get_max_retry,
    get_progress_bar,
    get_retry_sleep,
)
from .types import (
    CSVBlobResponse,
    CSVList,
    CSVOp,
    CustomCodeResponse,
    CustomImportsResponse,
    InCursors,
    JobInfo,
    JobList,
    JSONBlobResponse,
    MaintenanceResponse,
    NodeChunk,
    NodeDefInfo,
    NodeInfo,
    NodeState,
    NodeStatus,
    NodeTypes,
    PipelineCreate,
    PipelineDef,
    PipelineInfo,
    PipelineInit,
    PipelineList,
    ReadNode,
    TaskStatus,
    Timing,
    Timings,
    UserColumnsResponse,
    VersionResponse,
)

if TYPE_CHECKING:
    WVD = weakref.WeakValueDictionary[str, 'PipelineHandle']
else:
    WVD = weakref.WeakValueDictionary


__version__ = "0.1.0"
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
            user: Optional[str],
            password: Optional[str],
            token: Optional[str]) -> None:
        self._url = url.rstrip("/")
        if user is None:
            user = os.environ.get("ACCERN_USER")
        self._user = user
        if password is None:
            password = os.environ.get("ACCERN_PASSWORD")
        self._password = password
        self._token: Optional[str] = token
        self._last_action = time.monotonic()
        self._auto_refresh = True
        self._pipeline_cache: WVD = weakref.WeakValueDictionary()
        self._permissions: Optional[List[str]] = None
        self._node_defs: Optional[Dict[str, NodeDefInfo]] = None

        def get_version() -> int:
            server_version = self.get_server_version()
            try:
                return int(server_version["api_version"])
            except (ValueError, KeyError):
                raise LegacyVersion()

        self._api_version = min(get_version(), API_VERSION)
        self._init()

    def get_api_version(self) -> int:
        return self._api_version

    def _init(self) -> None:
        if self._token is None:
            self._login()
            return
        # FIXME
        # res = cast(UserLogin, self._request_json(
        #     METHOD_GET, "/init", {}, capture_err=False))
        # if not res["success"]:
        #     raise AccessDenied("init was not successful")
        # self._token = res["token"]
        # self._permissions = res["permissions"]

    def get_permissions(self) -> List[str]:
        if self._permissions is None:
            self._init()
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
            api_version: Optional[int] = None) -> BytesIO:
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
            except requests.ConnectionError:
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
            except requests.ConnectionError:
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
            except requests.ConnectionError:
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
            api_version: Optional[int]) -> BytesIO:
        prefix = ""
        if add_prefix:
            if api_version is None:
                api_version = self._api_version
            prefix = f"{PREFIX}/v{api_version}"
        url = f"{self._url}{prefix}{path}"
        if method == METHOD_GET:
            req = requests.get(url, params=args)
            if req.status_code == 403:
                raise AccessDenied(req.text)
            if req.status_code == 200:
                return BytesIO(req.content)
            raise ValueError(
                f"error {req.status_code} in worker request:\n{req.text}")
        if method == METHOD_POST:
            req = requests.post(url, json=args)
            if req.status_code == 403:
                raise AccessDenied(req.text)
            if req.status_code == 200:
                return BytesIO(req.content)
            raise ValueError(
                f"error {req.status_code} in worker request:\n{req.text}")
        if method == METHOD_FILE:
            if files is None:
                raise ValueError(f"file method must have files: {files}")
            req = requests.post(url, data=args, files={
                key: (
                    getattr(value, "name", key),
                    value,
                    "application/octet-stream",
                ) for (key, value) in files.items()
            })
            if req.status_code == 403:
                raise AccessDenied(req.text)
            if req.status_code == 200:
                return BytesIO(req.content)
            raise ValueError(
                f"error {req.status_code} in worker request:\n{req.text}")
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
        if method == METHOD_GET:
            req = requests.get(url, params=args)
            if req.status_code == 403:
                raise AccessDenied(req.text)
            if req.status_code == 200:
                return StringIO(req.text)
            raise ValueError(
                f"error {req.status_code} in worker request:\n{req.text}")
        if method == METHOD_POST:
            req = requests.post(url, json=args)
            if req.status_code == 403:
                raise AccessDenied(req.text)
            if req.status_code == 200:
                return StringIO(req.text)
            raise ValueError(
                f"error {req.status_code} in worker request:\n{req.text}")
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
        if method != METHOD_FILE and files is not None:
            raise ValueError(
                f"files are only allow for post (got {method}): {files}")
        req = None
        try:
            if method == METHOD_GET:
                req = requests.get(url, params=args)
                if req.status_code == 403:
                    raise AccessDenied(req.text)
                if req.status_code == 200:
                    return json.loads(req.text)
                raise ValueError(
                    f"error {req.status_code} in worker request:\n{req.text}")
            if method == METHOD_FILE:
                if files is None:
                    raise ValueError(f"file method must have files: {files}")
                req = requests.post(url, data=args, files={
                    key: (
                        getattr(value, "name", key),
                        value,
                        "application/octet-stream",
                    ) for (key, value) in files.items()
                })
                if req.status_code == 403:
                    raise AccessDenied(req.text)
                if req.status_code == 200:
                    return json.loads(req.text)
                raise ValueError(
                    f"error {req.status_code} in worker request:\n{req.text}")
            if method == METHOD_POST:
                req = requests.post(url, json=args)
                if req.status_code == 403:
                    raise AccessDenied(req.text)
                if req.status_code == 200:
                    return json.loads(req.text)
                raise ValueError(
                    f"error {req.status_code} in worker request:\n{req.text}")
            if method == METHOD_PUT:
                req = requests.put(url, json=args)
                if req.status_code == 403:
                    raise AccessDenied(req.text)
                if req.status_code == 200:
                    return json.loads(req.text)
                raise ValueError(
                    f"error {req.status_code} in worker request:\n{req.text}")
            if method == METHOD_DELETE:
                req = requests.delete(url, json=args)
                if req.status_code == 403:
                    raise AccessDenied(req.text)
                if req.status_code == 200:
                    return json.loads(req.text)
                raise ValueError(
                    f"error {req.status_code} in worker request:\n{req.text}")
            if method == METHOD_LONGPOST:
                try:
                    return quick_server.worker_request(url, args)
                except quick_server.WorkerError as e:
                    if e.get_status_code() == 403:
                        raise AccessDenied(e.args)
                    raise e
            raise ValueError(f"unknown method {method}")
        except json.decoder.JSONDecodeError:
            if req is None:
                raise
            raise ValueError(req.text)

    def _login(self) -> None:
        if self._user is None or self._password is None:
            raise ValueError("cannot login without user or password")
        # FIXME
        # res = cast(UserLogin, self._raw_request_json(METHOD_POST, "/login", {
        #     "user": self._user,
        #     "pw": self._password,
        # }))
        # if not res["success"]:
        #     raise AccessDenied("login was not successful")
        # self._token = res["token"]
        # self._permissions = res["permissions"]

    def logout(self) -> None:
        if self._token is None:
            return
        # FIXME
        # self._raw_request_json(METHOD_POST, "/logout", {
        #     "token": self._token,
        # })

    def request_bytes(
            self,
            method: str,
            path: str,
            args: Dict[str, Any],
            files: Optional[Dict[str, BytesIO]] = None,
            add_prefix: bool = True,
            api_version: Optional[int] = None) -> BytesIO:
        if self._token is None:
            self._login()

        def execute() -> BytesIO:
            args["token"] = self._token
            return self._raw_request_bytes(
                method, path, args, files, add_prefix, api_version)

        try:
            return execute()
        except AccessDenied:
            self._login()
            return execute()

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
        if self._token is None:
            self._login()

        def execute() -> Dict[str, Any]:
            args["token"] = self._token
            res = self._raw_request_json(
                method, path, args, add_prefix, files, api_version)
            if capture_err and "errMessage" in res and res["errMessage"]:
                raise ValueError(res["errMessage"])
            return res

        try:
            return execute()
        except AccessDenied:
            self._login()
            return execute()

    # FIXME
    # def get_user_info(self) -> UserInfo:
    #     return cast(UserInfo, self._request_json(
    #         METHOD_POST, "/username", {}, capture_err=False))

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

    def create_new_pipeline(self) -> str:
        return cast(PipelineInit, self._request_json(
            METHOD_POST, "/pipeline_init", {}, capture_err=False))["pipeline"]

    def set_pipeline(
            self, pipe_id: str, defs: PipelineDef) -> 'PipelineHandle':
        pipe_id = cast(PipelineCreate, self._request_json(
            METHOD_POST, "/pipeline_create", {
                "pipeline": pipe_id,
                "defs": defs,
            }, capture_err=True))["pipeline"]
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
        self._state_publisher: Optional[str] = None
        self._notify_publisher: Optional[str] = None
        self._state: Optional[str] = None
        self._is_high_priority: Optional[bool] = None
        self._nodes: Dict[str, NodeHandle] = {}
        self._settings: Optional[Dict[str, Any]] = None

    def refresh(self) -> None:
        self._name = None
        self._company = None
        self._state_publisher = None
        self._notify_publisher = None
        self._state = None
        self._is_high_priority = None
        # NOTE: we don't reset nodes

    def _maybe_refresh(self) -> None:
        if self._client.is_auto_refresh():
            self.refresh()

    def _maybe_fetch(self) -> None:
        if self._name is None:
            self._fetch_info()

    def _fetch_info(self) -> None:
        info = cast(PipelineInfo, self._client._request_json(
            METHOD_GET, "/pipeline_info", {
                "pipeline": self._pipe_id,
            }, capture_err=False))
        self._name = info["name"]
        self._company = info["company"]
        self._state_publisher = info["state_publisher"]
        self._notify_publisher = info["notify_publisher"]
        self._state = info["state"]
        self._is_high_priority = info["high_priority"]
        self._settings = info["settings"]
        old_nodes = {} if self._nodes is None else self._nodes
        self._nodes = {
            node["id"]: NodeHandle.from_node_info(
                self._client, self, node, old_nodes.get(node["id"]))
            for node in info["nodes"]
        }

    def get_nodes(self) -> List[str]:
        self._maybe_refresh()
        self._maybe_fetch()
        return list(self._nodes.keys())

    def get_node(self, node_id: str) -> 'NodeHandle':
        self._maybe_refresh()
        self._maybe_fetch()
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

    def is_high_priority(self) -> bool:
        self._maybe_refresh()
        self._maybe_fetch()
        assert self._is_high_priority is not None
        return self._is_high_priority

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

    def dynamic(self, input_data: BytesIO) -> BytesIO:
        cur_res = self._client.request_bytes(
            METHOD_FILE, "/dynamic", {
                "pipeline": self._pipe_id,
            }, files={
                "file": input_data,
            }).read()
        if not cur_res:
            raise ValueError("empty response")
        return BytesIO(cur_res)

    def dynamic_obj(self, input_obj: Any) -> Any:
        bio = BytesIO(json.dumps(
            input_obj,
            separators=(",", ":"),
            indent=None,
            sort_keys=True).encode("utf-8"))
        out = self.dynamic(bio)
        return json.load(out)

    def pretty(self, allow_unicode: bool) -> str:
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
            kind: str) -> None:
        self._client = client
        self._pipeline = pipeline
        self._node_id = node_id
        self._type = kind
        self._state_key: Optional[str] = None
        self._blobs: Dict[str, BlobHandle] = {}
        self._inputs: Dict[str, Tuple[str, str]] = {}
        self._state: Optional[int] = None
        self._config_error: Optional[bool] = None

    @staticmethod
    def from_node_info(
            client: XYMEClient,
            pipeline: PipelineHandle,
            node_info: NodeInfo,
            prev: Optional['NodeHandle']) -> 'NodeHandle':
        if prev is None:
            res = NodeHandle(
                client, pipeline, node_info["id"], node_info["type"])
        else:
            if prev.get_pipeline() != pipeline:
                raise ValueError(f"{prev.get_pipeline()} != {pipeline}")
            res = prev
        res.update_info(node_info)
        return res

    def update_info(self, node_info: NodeInfo) -> None:
        if self._node_id != node_info["id"]:
            raise ValueError(f"{self._node_id} != {node_info['id']}")
        self._state_key = node_info["state_key"]
        self._type = node_info["type"]
        self._blobs = {
            key: BlobHandle(self._client, value, is_full=False)
            for (key, value) in node_info["blobs"].items()
        }
        self._inputs = node_info["inputs"]
        self._state = node_info["state"]
        self._config_error = node_info["config_error"]

    def get_pipeline(self) -> PipelineHandle:
        return self._pipeline

    def get_id(self) -> str:
        return self._node_id

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
        assert self._config_error is not None
        return self._config_error

    def get_blobs(self) -> List[str]:
        return sorted(self._blobs.keys())

    def get_blob_handle(self, key: str) -> 'BlobHandle':
        return self._blobs[key]

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

    def get_short_status(self, allow_unicode: bool) -> str:
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

    def read_blob(self, key: str, chunk: int) -> 'BlobHandle':
        res = cast(ReadNode, self._client._request_json(
            METHOD_LONGPOST, "/read_node", {
                "pipeline": self.get_pipeline().get_id(),
                "node": self.get_id(),
                "key": key,
                "chunk": chunk,
                "is_blocking": True,
            }, capture_err=False))
        uri = res["result_uri"]
        if uri is None:
            raise ValueError(f"uri is None: {res}")
        return BlobHandle(self._client, uri, is_full=True)

    def read(self, key: str, chunk: int) -> Optional[pd.DataFrame]:
        pipeline_id = self.get_pipeline().get_id()
        return self.read_blob(key, chunk).get_content(pipeline_id)

    def reset(self) -> NodeState:
        return cast(NodeState, self._client._request_json(
            METHOD_PUT, "/node_state", {
                "pipeline": self.get_pipeline().get_id(),
                "node": self.get_id(),
                "action": "reset",
            }, capture_err=False))

    def notify(self) -> NodeState:
        return cast(NodeState, self._client._request_json(
            METHOD_PUT, "/node_state", {
                "pipeline": self.get_pipeline().get_id(),
                "node": self.get_id(),
                "action": "notify",
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

    def get_input_example(self) -> Dict[str, Optional[pd.DataFrame]]:
        if self.get_type() != "custom_data":
            raise ValueError(
                "can only load example input data for 'custom' node")
        res = {}
        for key in self.get_inputs():
            input_node, out_key = self.get_input(key)
            df = input_node.read(out_key, 0)
            if df is not None:
                user_columns = self.get_user_columns(out_key)["user_columns"]
                rmap = {col: col.replace("user_", "") for col in user_columns}
                df = df.loc[:, user_columns].rename(columns=rmap)
            res[key] = df
        return res

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


class CSVBlobHandle:
    def __init__(
            self,
            client: XYMEClient,
            pipe: PipelineHandle,
            uri: str,
            count: int,
            pos: int,
            has_tmp: bool) -> None:
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

    def get_pipeline_id(self) -> str:
        return self._pipe.get_id()

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
            "pipeline": self.get_pipeline_id(),
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
            progress_bar.write(f"Uploading file:\n")
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


class JSONBlobHandle:
    def __init__(
            self,
            client: XYMEClient,
            pipe: PipelineHandle,
            uri: str,
            count: int) -> None:
        self._client = client
        self._pipe = pipe
        self._uri = uri
        self._count = count

    def get_uri(self) -> str:
        return self._uri

    def get_count(self) -> int:
        return self._count

    def get_pipeline_id(self) -> str:
        return self._pipe.get_id()

    def append_jsons(self, jsons: List[Any]) -> 'JSONBlobHandle':
        res = self._client._request_json(
            METHOD_PUT, "/json_append", {
                "pipeline": self.get_pipeline_id(),
                "blob": self.get_uri(),
                "jsons": jsons,
            }, capture_err=True)
        self._count = res["count"]
        return self

# *** JSONBlobHandle ***


EMPTY_BLOB_PREFIX = "null://"


class BlobHandle:
    def __init__(self, client: XYMEClient, uri: str, is_full: bool) -> None:
        self._client = client
        self._uri = uri
        self._is_full = is_full

    def is_full(self) -> bool:
        return self._is_full

    def is_empty(self) -> bool:
        return self._uri.startswith(EMPTY_BLOB_PREFIX)

    def get_uri(self) -> str:
        return self._uri

    def get_content(self, pipe_id: str) -> Optional[pd.DataFrame]:
        if not self.is_full():
            raise ValueError(f"URI must be full: {self}")
        if self.is_empty():
            return None
        with self._client._raw_request_bytes(
                METHOD_POST, "/uri", {
                    "uri": self._uri,
                    "pipeline": pipe_id,
                }) as fin:
            return pd.read_parquet(fin)

    def as_str(self) -> str:
        return f"{self.get_uri()}"

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


def create_xyme_client(
        url: str,
        user: Optional[str] = None,
        password: Optional[str] = None,
        token: Optional[str] = None) -> XYMEClient:
    return XYMEClient(url, user, password, token)


@contextlib.contextmanager
def create_xyme_session(
        url: str,
        user: Optional[str] = None,
        password: Optional[str] = None,
        token: Optional[str] = None) -> Iterator[XYMEClient]:
    client = None
    try:
        client = XYMEClient(url, user, password, token)
        yield client
    finally:
        if client is not None:
            client.logout()
