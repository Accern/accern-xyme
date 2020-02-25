from typing import (
    Any,
    cast,
    Dict,
    IO,
    Iterator,
    List,
    Optional,
)
import io
import os
import json
import time
import contextlib
from io import BytesIO
import requests
import quick_server

from .util import (
    get_max_retry,
    get_retry_sleep,
)
from .types import (
    VersionResponse,
    MaintenanceResponse,
)


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
            METHOD_GET, "/version", {}))

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

    # @log_api_call(server.json_worker, prefix + '/:version/read_node')
    # def _read_node(args: WorkerArgs) -> ReadNode:

    # @log_api_call(server.text_post, prefix + '/:version/uri')
    # def _get_uri_content(_req: QSRH, rargs: ReqArgs) -> Optional[Response]:

    # @log_api_call(server.json_get, prefix + '/:version/node_types')
    # def _get_node_types(_req: QSRH, _rargs: ReqArgs) -> NodeTypes:

    # @log_api_call(server.json_get, prefix + '/:version/pipeline_info')
    # def _get_pipeline_info(_req: QSRH, rargs: ReqArgs) -> PipelineInfo:


# *** XYMEClient ***


# class PipelineHandle:
#     def __init__(self,
#                  client: XYMEClient,
#                  job_id: str,
#                  path: Optional[str],
#                  name: Optional[str],
#                  schema_obj: Optional[Dict[str, Any]],
#                  kinds: Optional[List[str]],
#                  status: Optional[str],
#                  permalink: Optional[str],
#                  time_total: Optional[float],
#                  time_start: Optional[str],
#                  time_end: Optional[str],
#                  time_estimate: Optional[str]) -> None:
#         self._client = client
#         self._job_id = job_id
#         self._name = name
#         self._path = path
#         self._schema_obj = schema_obj
#         self._kinds = kinds
#         self._permalink = permalink
#         self._status = status
#         self._time_total = time_total
#         self._time_start = time_start
#         self._time_end = time_end
#         self._time_estimate = time_estimate
#         self._buttons: Optional[List[str]] = None
#         self._can_rename: Optional[bool] = None
#         self._is_symjob: Optional[bool] = None
#         self._is_user_job: Optional[bool] = None
#         self._plot_order_types: Optional[List[str]] = None
#         self._tabs: Optional[List[str]] = None
#         self._tickers: Optional[List[str]] = None
#         self._source: Optional[SourceHandle] = None
#         self._is_async_fetch = False
#         self._async_lock = threading.RLock()

#     def refresh(self) -> None:
#         self._name = None
#         self._path = None
#         self._schema_obj = None
#         self._kinds = None
#         self._permalink = None
#         self._time_total = None
#         self._time_start = None
#         self._time_end = None
#         self._time_estimate = None
#         self._buttons = None
#         self._can_rename = None
#         self._is_symjob = None
#         self._is_user_job = None
#         self._plot_order_types = None
#         self._tabs = None
#         self._tickers = None
#         self._source = None
#         if not self._is_async_fetch:
#             self._status = None

#     def _maybe_refresh(self) -> None:
#         if self._client.is_auto_refresh():
#             self.refresh()

#     def _fetch_info(self) -> None:
#         res = self._client._request_json(
#             METHOD_LONGPOST, "/status", {
#                 "job": self._job_id,
#             }, capture_err=False)
#         if res.get("empty", True) and "name" not in res:
#             raise ValueError("could not update status")
#         info = cast(JobStatusInfo, res)
#         self._name = info["name"]
#         self._path = info["path"]
#         self._schema_obj = json.loads(info["schema"])
#         self._buttons = info["buttons"]
#         self._can_rename = info["canRename"]
#         self._is_symjob = info["symjob"]
#         self._is_user_job = info["isUserJob"]
#         self._kinds = info["allKinds"]
#         self._permalink = info["permalink"]
#         self._plot_order_types = info["plotOrderTypes"]
#         self._status = info["status"]
#         self._tabs = info["allTabs"]
#         self._tickers = info["tickers"]
#         self._time_total = info["timeTotal"]
#         self._time_start = info["timeStart"]
#         self._time_end = info["timeEnd"]
#         self._time_estimate = info["timeEstimate"]

#     def get_job_id(self) -> str:
#         return self._job_id

#     def get_schema(self) -> Dict[str, Any]:
#         self._maybe_refresh()
#         if self._schema_obj is None:
#             self._fetch_info()
#         assert self._schema_obj is not None
#         return copy.deepcopy(self._schema_obj)

#     def set_schema(self, schema: Dict[str, Any]) -> None:
#         res = cast(SchemaResponse, self._client._request_json(
#             METHOD_PUT, "/update_job_schema", {
#                 "job": self._job_id,
#                 "schema": json.dumps(schema),
#             }, capture_err=True))
#         self._schema_obj = json.loads(res["schema"])

#     @contextlib.contextmanager
#     def update_schema(self) -> Iterator[Dict[str, Any]]:
#         self._maybe_refresh()
#         if self._schema_obj is None:
#             self._fetch_info()
#         assert self._schema_obj is not None
#         yield self._schema_obj
#         self.set_schema(self._schema_obj)

#     @contextlib.contextmanager
#     def bulk_operation(self) -> Iterator[bool]:
#         with self._client.bulk_operation() as do_refresh:
#             if do_refresh:
#                 self.refresh()
#             yield do_refresh

#     def get_notes(self, force: bool) -> Optional[NotesInfo]:
#         res = cast(PreviewNotesResponse, self._client._request_json(
#             METHOD_LONGPOST, "/preview", {
#                 "job": self._job_id,
#                 "view": "summary",
#                 "force": force,
#                 "schema": None,
#                 "batch": None,
#             }, capture_err=False))
#         notes = res["notes"]
#         if notes is None:
#             return None
#         return {
#             "usage": notes.get("usage", {}),
#             "roles": notes.get("roles", {}),
#             "roles_renamed": notes.get("rolesRenamed", {}),
#             "dummy_columns": notes.get("dummyColumns", []),
#             "stats": notes.get("stats", {}),
#             "is_runnable": notes.get("isRunnable", False),
#             "suggestions": notes.get("suggestions", {}),
#             "is_preview": notes.get("isPreview", True),
#             "error": notes.get("error", True),
#         }

# *** PipelineHandle ***


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
