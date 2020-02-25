from typing import (
    Any,
    cast,
    Dict,
    IO,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    TYPE_CHECKING,
)
import io
import os
import json
import time
import weakref
import contextlib
from io import BytesIO
import requests
import pandas as pd
import quick_server

from .util import (
    get_max_retry,
    get_retry_sleep,
)
from .types import (
    MaintenanceResponse,
    NodeDefInfo,
    NodeInfo,
    NodeTypes,
    PipelineInfo,
    PipelineList,
    ReadNode,
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
            add_prefix: bool = True,
            api_version: Optional[int] = None) -> BytesIO:
        retry = 0
        while True:
            try:
                return self._fallible_raw_request_bytes(
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

    def _request_bytes(
            self,
            method: str,
            path: str,
            args: Dict[str, Any],
            add_prefix: bool = True,
            api_version: Optional[int] = None) -> BytesIO:
        if self._token is None:
            self._login()

        def execute() -> BytesIO:
            args["token"] = self._token
            return self._raw_request_bytes(
                method, path, args, add_prefix, api_version)

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

    def get_node_types(self) -> Dict[str, NodeDefInfo]:
        res = cast(NodeTypes, self._request_json(
            METHOD_GET, "/node_types", {}, capture_err=False))
        return res["info"]

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
        self._nodes: Dict[str, NodeHandle] = {}

    def refresh(self) -> None:
        self._name = None
        self._company = None
        self._state_publisher = None
        self._notify_publisher = None
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
        old_nodes = {} if self._nodes is None else self._nodes
        self._nodes = {
            node["id"]: NodeHandle.from_node_info(
                self._client, self, node, old_nodes.get(node["id"]))
            for node in info["nodes"]
        }

    def get_nodes(self) -> List[str]:
        return list(self._nodes.keys())

    def get_node(self, node_id: str) -> 'NodeHandle':
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

    @contextlib.contextmanager
    def bulk_operation(self) -> Iterator[bool]:
        with self._client.bulk_operation() as do_refresh:
            if do_refresh:
                self.refresh()
            yield do_refresh

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

    def get_pipeline(self) -> PipelineHandle:
        return self._pipeline

    def get_id(self) -> str:
        return self._node_id

    def get_type(self) -> str:
        return self._type

    def get_inputs(self) -> Set[str]:
        return set(self._inputs.keys())

    def get_input(self, key: str) -> Tuple['NodeHandle', str]:
        node_id, out_key = self._inputs[key]
        return self.get_pipeline().get_node(node_id), out_key

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
        return self.read_blob(key, chunk).get_content()

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

    def get_content(self) -> Optional[pd.DataFrame]:
        if not self.is_full():
            raise ValueError(f"URI must be full: {self}")
        if self.is_empty():
            return None
        with self._client._raw_request_bytes(
                METHOD_POST, "/uri", {
                    "uri": self._uri,
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
    try:
        client = XYMEClient(url, user, password, token)
        yield client
    finally:
        client.logout()
