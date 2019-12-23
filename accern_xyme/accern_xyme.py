from typing import (
    Any,
    cast,
    Dict,
    IO,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)
import os
import copy
import json
import time
import urllib
import requests
import contextlib
import collections
from io import BytesIO, TextIOWrapper
import pandas as pd
from typing_extensions import TypedDict
import quick_server


__version__ = "0.0.4"
# FIXME: async calls, documentation, auth, metric/plots/get accuracy,
# summary – time it took etc., basically make old-tykhe obsolete


API_VERSION = 0


METHOD_DELETE = "DELETE"
METHOD_FILE = "FILE"
METHOD_GET = "GET"
METHOD_LONGPOST = "LONGPOST"
METHOD_POST = "POST"
METHOD_PUT = "PUT"

PREFIX = "/xyme"

SOURCE_TYPE_MULTI = "multi"
SOURCE_TYPE_CSV = "csv"
SOURCE_TYPE_IO = "io"
SOURCE_TYPE_PRICES = "prices"
SOURCE_TYPE_USER = "user"
ALL_SOURCE_TYPES = [
    SOURCE_TYPE_MULTI,
    SOURCE_TYPE_CSV,
    SOURCE_TYPE_IO,
    SOURCE_TYPE_PRICES,
    SOURCE_TYPE_USER,
]


VersionInfo = TypedDict('VersionInfo', {
    "time": str,
    "version": str,
    "xymeVersion": str,
})
UserLogin = TypedDict('UserLogin', {
    "token": str,
    "success": bool,
    "permissions": List[str],
})
UserInfo = TypedDict('UserInfo', {
    "company": str,
    "hasLogout": bool,
    "path": str,
    "username": str,
})
MaintenanceInfo = TypedDict('MaintenanceInfo', {
    "isAfterupdate": bool,
    "isMaintenance": bool,
    "queuedMaintenance": bool,
})
HintedMaintenanceInfo = TypedDict('HintedMaintenanceInfo', {
    "isAfterupdate": bool,
    "isMaintenance": bool,
    "pollHint": float,
    "queuedMaintenance": bool,
})
JobCreateResponse = TypedDict('JobCreateResponse', {
    "jobId": str,
    "name": str,
    "path": str,
    "schema": str,
})
SchemaResponse = TypedDict('SchemaResponse', {
    "schema": str,
})
SourceResponse = TypedDict('SourceResponse', {
    "sourceId": str,
})
JobStartResponse = TypedDict('JobStartResponse', {
    "job": Optional[str],
    "exit": Optional[int],
    "stdout": Optional[List[str]],
})
JobRenameResponse = TypedDict('JobRenameResponse', {
    "name": str,
    "path": str,
})
JobPauseResponse = TypedDict('JobPauseResponse', {
    "success": bool,
})
JobStatusInfo = TypedDict('JobStatusInfo', {
    "allKinds": List[str],
    "allTabs": List[str],
    "buttons": List[str],
    "empty": bool,
    "isUserJob": bool,
    "name": str,
    "path": str,
    "plotOrderTypes": List[str],
    "pollHint": float,
    "schema": str,
    "status": str,
    "tickers": List[str],
    "timeTotal": float,
    "timeStart": Optional[str],
    "timeEnd": Optional[str],
    "timeEstimate": Optional[str],
    "canRename": bool,
    "permalink": str,
    "symjob": bool,
})
JobListItemInfo = TypedDict('JobListItemInfo', {
    "deployTime": str,
    "jobId": str,
    "name": str,
    "path": str,
    "kinds": List[str],
    "timeTotal": float,
    "timeStart": Optional[str],
    "timeEnd": Optional[str],
    "timeEstimate": Optional[str],
    "status": str,
    "canDelete": bool,
    "canPause": bool,
    "canUnpause": bool,
    "permalink": str,
})
JobListResponse = TypedDict('JobListResponse', {
    "jobs": Dict[str, List[JobListItemInfo]],
    "counts": Dict[str, int],
    "allKinds": List[str],
    "allStatus": List[str],
    "pollHint": float,
    "isReadAdmin": bool,
})
StdoutMarker = int
StdoutLine = Tuple[StdoutMarker, str]
StdoutResponse = TypedDict('StdoutResponse', {
    "lines": List[StdoutLine],
    "messages": Dict[str, List[Tuple[str, List[StdoutLine]]]],
    "exceptions": List[Tuple[str, List[StdoutLine]]],
})
PredictionsResponse = TypedDict('PredictionsResponse', {
    "columns": List[str],
    "dtypes": List[str],
    "values": List[List[Union[str, float, int]]],
})
Approximate = TypedDict('Approximate', {
    "mean": float,
    "stddev": float,
    "q_low": float,
    "q_high": float,
    "vmin": float,
    "vmax": float,
    "count": int,
})
ApproximateImportance = TypedDict('ApproximateImportance', {
    "name": str,
    "importance": Approximate,
})
ApproximateUserExplanation = TypedDict('ApproximateUserExplanation', {
    "label": Union[str, int],
    "weights": List[ApproximateImportance],
})
DynamicPredictionResponse = TypedDict('DynamicPredictionResponse', {
    "predictions": Optional[PredictionsResponse],
    "explanations": Optional[List[ApproximateUserExplanation]],
    "stdout": StdoutResponse,
})
ShareList = TypedDict('ShareList', {
    "shareable": List[Tuple[str, str]],
})
ShareablePath = TypedDict('ShareablePath', {
    "name": str,
    "path": str,
})
ShareResponse = TypedDict('ShareResponse', {
    "job": str,
})
CreateSourceResponse = TypedDict('CreateSourceResponse', {
    "multiSourceId": str,
    "sourceId": str,
    "jobSchema": Optional[str],
})
CreateSource = TypedDict('CreateSource', {
    "source": 'SourceHandle',
    "multi_source": 'SourceHandle',
})
LockSourceResponse = TypedDict('LockSourceResponse', {
    "newSourceId": str,
    "immutable": bool,
    "jobSchema": Optional[str],
    "multiSourceId": Optional[str],
    "sourceSchemaMap": Dict[str, Dict[str, Any]],
})
LockSource = TypedDict('LockSource', {
    "new_source": 'SourceHandle',
    "immutable": bool,
    "multi_source": Optional['SourceHandle'],
    "source_schema_map": Dict[str, Dict[str, Any]],
})
SourceInfoResponse = TypedDict('SourceInfoResponse', {
    "dirty": bool,
    "immutable": bool,
    "sourceName": Optional[str],
    "sourceType": Optional[str],
})
SourceSchemaResponse = TypedDict('SourceSchemaResponse', {
    "sourceSchema": Dict[str, Any],
    "sourceInfo": SourceInfoResponse,
    "pollHint": float,
})
SystemLogResponse = TypedDict('SystemLogResponse', {
    "logs": Optional[List[StdoutLine]],
})
SourceItem = TypedDict('SourceItem', {
    "id": str,
    "name": str,
    "type": Optional[str],
    "help": str,
    "immutable": bool,
})
SourcesResponse = TypedDict('SourcesResponse', {
    "systemSources": List[SourceItem],
    "userSources": List[SourceItem],
})
InspectQuery = Optional[Dict[str, Any]]
InspectItem = TypedDict('InspectItem', {
    "clazz": str,
    "leaf": bool,
    "value": Union[None, str, List[Any]],
    "extra": str,
    "collapsed": bool,
})
InspectResponse = TypedDict('InspectResponse', {
    "inspect": InspectItem,
    "pollHint": float,
})
RedisQueueSizes = TypedDict('RedisQueueSizes', {
    "wait": int,
    "regular": int,
    "busy": int,
    "result": int,
    "failed": int,
    "paused": int,
})
JobsOverview = TypedDict('JobsOverview', {
    "current": Dict[str, Dict[str, int]],
    "queues": RedisQueueSizes,
    "pausedJobs": Optional[List[Dict[str, str]]],
})
OverviewResponse = TypedDict('OverviewResponse', {
    "jobs": JobsOverview,
    "requests": Dict[str, int],
    "sessions": Optional[List[Dict[str, Any]]],
    "workers": Dict[str, List[str]],
})


def df_to_csv(df: pd.DataFrame) -> BytesIO:
    bio = BytesIO()
    wrap = TextIOWrapper(bio, encoding="utf-8", write_through=True)
    df.to_csv(wrap, index=False)
    wrap.detach()
    bio.seek(0)
    return bio


def predictions_to_df(preds: PredictionsResponse) -> pd.DataFrame:
    df = pd.DataFrame(preds["values"], columns=preds["columns"])
    if "date" in df.columns:  # pylint: disable=unsupported-membership-test
        df["date"] = pd.to_datetime(df["date"])
    return df


def maybe_predictions_to_df(
        preds: Optional[PredictionsResponse]) -> Optional[pd.DataFrame]:
    if preds is None:
        return None
    return predictions_to_df(preds)


class AccessDenied(Exception):
    pass


class StdoutWrapper:
    def __init__(self, stdout: StdoutResponse) -> None:
        self._lines = stdout["lines"]
        self._messages = stdout["messages"]
        self._exceptions = stdout["exceptions"]

    def full_output(self) -> str:
        return "\n".join((line for _, line in self._lines))

    def __str__(self) -> str:
        return self.full_output()

    def __repr__(self) -> str:
        return self.full_output()


class XYMEClient:
    def __init__(self,
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
        self._init()

    def _init(self) -> None:
        if self._token is None:
            self._login()
            return
        res = cast(UserLogin, self._request_json(
            METHOD_GET, "/init", {}, capture_err=False))
        if not res["success"]:
            raise AccessDenied()
        self._token = res["token"]
        self._permissions = res["permissions"]

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
    def bulk_operation(self) -> Iterator[None]:
        old_refresh = self.is_auto_refresh()
        try:
            self.set_auto_refresh(False)
            yield
        finally:
            self.set_auto_refresh(old_refresh)

    def _raw_request_json(self,
                          method: str,
                          path: str,
                          args: Dict[str, Any],
                          add_prefix: bool = True,
                          files: Optional[Dict[str, IO[bytes]]] = None,
                          api_version: Optional[int] = None,
                          ) -> Dict[str, Any]:
        prefix = ""
        if add_prefix:
            if api_version is None:
                api_version = API_VERSION
            prefix = f"{PREFIX}/v{api_version}"
        url = f"{self._url}{prefix}{path}"
        if method != METHOD_FILE and files is not None:
            raise ValueError(
                f"files are only allow for post (got {method}): {files}")
        if method == METHOD_GET:
            req = requests.get(url, params=args)
            if req.status_code == 403:
                raise AccessDenied()
            if req.status_code == 200:
                return json.loads(req.text)
            raise ValueError(
                f"error {req.status_code} in worker request:\n{req.text}")
        elif method == METHOD_FILE:
            if files is None:
                raise ValueError(f"file method must have files: {files}")
            for fbuff in files.values():
                if hasattr(fbuff, "seek"):
                    fbuff.seek(0)
            req = requests.post(url, data=args, files={
                key: (
                    getattr(value, "name", key),
                    value,
                    "application/octet-stream",
                ) for (key, value) in files.items()
            })
            if req.status_code == 403:
                raise AccessDenied()
            if req.status_code == 200:
                return json.loads(req.text)
            raise ValueError(
                f"error {req.status_code} in worker request:\n{req.text}")
        elif method == METHOD_POST:
            req = requests.post(url, json=args)
            if req.status_code == 403:
                raise AccessDenied()
            if req.status_code == 200:
                return json.loads(req.text)
            raise ValueError(
                f"error {req.status_code} in worker request:\n{req.text}")
        elif method == METHOD_PUT:
            req = requests.put(url, json=args)
            if req.status_code == 403:
                raise AccessDenied()
            if req.status_code == 200:
                return json.loads(req.text)
            raise ValueError(
                f"error {req.status_code} in worker request:\n{req.text}")
        elif method == METHOD_DELETE:
            req = requests.delete(url, json=args)
            if req.status_code == 403:
                raise AccessDenied()
            if req.status_code == 200:
                return json.loads(req.text)
            raise ValueError(
                f"error {req.status_code} in worker request:\n{req.text}")
        elif method == METHOD_LONGPOST:
            try:
                return quick_server.worker_request(url, args)
            except quick_server.WorkerError as e:
                if e.get_status_code() == 403:
                    raise AccessDenied()
                raise e
        else:
            raise ValueError(f"unknown method {method}")

    def _login(self) -> None:
        if self._user is None or self._password is None:
            raise ValueError("cannot login without user or password")
        res = cast(UserLogin, self._raw_request_json(METHOD_POST, "/login", {
            "user": self._user,
            "pw": self._password,
        }))
        if not res["success"]:
            raise AccessDenied()
        self._token = res["token"]
        self._permissions = res["permissions"]

    def logout(self) -> None:
        if self._token is None:
            return
        self._raw_request_json(METHOD_POST, "/logout", {
            "token": self._token,
        })

    def _request_json(self,
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

    def get_user_info(self) -> UserInfo:
        return cast(UserInfo, self._request_json(
            METHOD_POST, "/username", {}, capture_err=False))

    def get_server_version(self) -> VersionInfo:
        return cast(VersionInfo,
                    self._raw_request_json(METHOD_GET, "/version", {}))

    def afterupdate(self) -> MaintenanceInfo:
        return cast(MaintenanceInfo, self._request_json(
            METHOD_POST, "/afterupdate", {}, capture_err=False))

    def set_maintenance_mode(self, is_maintenance: bool) -> MaintenanceInfo:
        return cast(MaintenanceInfo, self._request_json(
            METHOD_PUT, "/maintenance", {
                "isMaintenance": is_maintenance,
            }, capture_err=False))

    def get_maintenance_mode(self) -> HintedMaintenanceInfo:
        return cast(HintedMaintenanceInfo, self._request_json(
            METHOD_GET, "/maintenance", {}, capture_err=False))

    def get_system_logs(self,
                        query: Optional[str] = None,
                        context: Optional[int] = None) -> StdoutWrapper:
        obj: Dict[str, Any] = {
            # FIXME: only regular for now -- other value is deploy
            "logsKind": "regular",
        }
        if query is not None:
            obj["query"] = query
        if context is not None:
            obj["context"] = context
        res = cast(SystemLogResponse, self._request_json(
            METHOD_GET, "/monitor_logs", obj, capture_err=False))
        return StdoutWrapper({
            "lines": res["logs"] if res["logs"] is not None else [],
            "messages": {},
            "exceptions": [],
        })

    def get_system_overview(self) -> OverviewResponse:
        return cast(OverviewResponse, self._request_json(
            METHOD_GET, "/overview", {}, capture_err=False))

    def create_job(self,
                   schema: Optional[Dict[str, Any]] = None,
                   from_job_id: Optional[str] = None,
                   name: Optional[str] = None,
                   is_system_preset: bool = False) -> 'JobHandle':
        schema_str = json.dumps(schema) if schema is not None else None
        obj: Dict[str, Any] = {
            "schema": schema_str,
        }
        if schema_str is not None and schema_str:
            obj["isSystemPreset"] = is_system_preset
        if from_job_id is not None:
            obj["fromId"] = from_job_id
        if name is not None:
            obj["name"] = name
        res = cast(JobCreateResponse, self._request_json(
            METHOD_LONGPOST, "/create_job_id", obj, capture_err=True))
        return JobHandle(client=self,
                         job_id=res["jobId"],
                         path=res["path"],
                         name=res["name"],
                         schema_obj=json.loads(res["schema"]),
                         kinds=None,
                         status=None,
                         permalink=None,
                         time_total=None,
                         time_start=None,
                         time_end=None,
                         time_estimate=None)

    def get_job(self, job_id: str) -> 'JobHandle':
        return JobHandle(client=self,
                         job_id=job_id,
                         path=None,
                         name=None,
                         schema_obj=None,
                         kinds=None,
                         status=None,
                         permalink=None,
                         time_total=None,
                         time_start=None,
                         time_end=None,
                         time_estimate=None)

    def start_job(self,
                  job_id: Optional[str],
                  job_name: Optional[str] = None,
                  schema: Optional[Dict[str, Any]] = None,
                  user: Optional[str] = None,
                  company: Optional[str] = None,
                  nowait: Optional[bool] = None) -> JobStartResponse:
        obj: Dict[str, Any] = {}
        if job_id is not None:
            if job_name is not None or schema is not None:
                raise ValueError(
                    "can only start by job_id or by job_name and schema "
                    f"job_id: {job_id} job_name: {job_name} schema: {schema}")
            obj["jobId"] = job_id
        else:
            obj["job_name"] = job_name
            obj["schema"] = json.dumps(schema)
        if user is not None:
            obj["user"] = user
        if company is not None:
            obj["company"] = company
        if nowait is not None:
            obj["nowait"] = nowait
        return cast(JobStartResponse, self._request_json(
            METHOD_LONGPOST, "/start", obj, capture_err=True))

    def _raw_job_list(self,
                      search: Optional[str] = None,
                      selected_status: Optional[List[str]] = None,
                      active_workspace: Optional[List[str]] = None,
                      ) -> JobListResponse:
        obj: Dict[str, Any] = {}
        if search is not None:
            obj["search"] = search
        if selected_status is not None:
            obj["selectedStatus"] = selected_status
        if active_workspace is not None:
            obj["activeWorkspace"] = active_workspace
        return cast(JobListResponse, self._request_json(
            METHOD_LONGPOST, "/jobs", obj, capture_err=False))

    def get_workspaces(self) -> Dict[str, int]:
        res = self._raw_job_list(None, None, None)
        return res["counts"]

    def get_jobs(self, workspace: str) -> List['JobHandle']:
        res = self._raw_job_list(None, None, [workspace])
        return [
            JobHandle(
                client=self,
                job_id=job["jobId"],
                path=job["path"],
                name=job["name"],
                schema_obj=None,
                kinds=job["kinds"],
                status=job["status"],
                permalink=job["permalink"],
                time_total=job["timeTotal"],
                time_start=job["timeStart"],
                time_end=job["timeEnd"],
                time_estimate=job["timeEstimate"],
            )
            for job in res["jobs"][workspace]
        ]

    def get_shareable(self) -> List[ShareablePath]:
        res = cast(ShareList, self._request_json(
            METHOD_LONGPOST, "/share", {}, capture_err=False))
        return [{
            "name": name,
            "path": path,
        } for (name, path) in res["shareable"]]

    def _raw_create_source(self,
                           source_type: str,
                           name: Optional[str],
                           job: Optional['JobHandle'],
                           multi_source: Optional['SourceHandle'],
                           from_id: Optional['SourceHandle'],
                           ) -> CreateSource:
        if source_type not in ALL_SOURCE_TYPES:
            raise ValueError(
                f"invalid source type: {source_type}\nmust be one of "
                f"{', '.join(ALL_SOURCE_TYPES)}")
        multi_source_id = None
        if multi_source is not None:
            if not multi_source.is_multi_source():
                raise ValueError(f"source {multi_source.get_source_id()} "
                                 "must be multi source")
            multi_source_id = multi_source.get_source_id()
        from_id_str = from_id.get_source_id() if from_id is not None else None
        res = cast(CreateSourceResponse, self._request_json(
            METHOD_LONGPOST, "/create_source_id", {
                "name": name,
                "type": source_type,
                "job": job.get_job_id() if job is not None else None,
                "multiSourceId": multi_source_id,
                "fromId": from_id_str,
            }, capture_err=True))
        job_schema_str = res["jobSchema"]
        if job is not None and job_schema_str:
            job._schema_obj = json.loads(job_schema_str)
        return {
            "multi_source": SourceHandle(
                self, res["multiSourceId"], SOURCE_TYPE_MULTI),
            "source": SourceHandle(self, res["sourceId"], source_type),
        }

    def create_source(self, name: Optional[str]) -> 'SourceHandle':
        res = self._raw_create_source(
            SOURCE_TYPE_MULTI, name, None, None, None)
        return res["multi_source"]

    def set_immutable_raw(self,
                          source: 'SourceHandle',
                          multi_source: Optional['SourceHandle'],
                          job: Optional['JobHandle'],
                          is_immutable: Optional[bool]) -> LockSource:
        multi_source_id = None
        if multi_source is not None:
            if not multi_source.is_multi_source():
                raise ValueError(f"source {multi_source.get_source_id()} "
                                 "must be multi source")
            multi_source_id = multi_source.get_source_id()
        res = cast(LockSourceResponse, self._request_json(
            METHOD_PUT, "/lock_source", {
                "sourceId": source.get_source_id(),
                "multiSourceId": multi_source_id,
                "job": job.get_job_id() if job is not None else None,
                "immutable": is_immutable,
            }, capture_err=False))
        if "multiSourceId" not in res:
            res["multiSourceId"] = None
        if "sourceSchemaMap" not in res:
            res["sourceSchemaMap"] = {}
        job_schema_str = res["jobSchema"]
        if job is not None and job_schema_str:
            job._schema_obj = json.loads(job_schema_str)
        if res["newSourceId"] == source.get_source_id():
            new_source = source
        else:
            new_source = SourceHandle(
                self, res["newSourceId"], source.get_source_type())
        if res["multiSourceId"] is None:
            new_multi_source: Optional[SourceHandle] = None
        elif res["multiSourceId"] == new_source.get_source_id():
            new_multi_source = new_source
        elif res["multiSourceId"] == source.get_source_id():
            new_multi_source = source
        elif multi_source is not None and \
                res["multiSourceId"] == multi_source.get_source_id():
            new_multi_source = multi_source
        else:
            new_multi_source = SourceHandle(
                self, res["multiSourceId"], SOURCE_TYPE_MULTI)
        return {
            "new_source": new_source,
            "immutable": res["immutable"],
            "multi_source": new_multi_source,
            "source_schema_map": res["sourceSchemaMap"],
        }

    def get_sources(self,
                    filter_by: Optional[str] = None,
                    ) -> Iterable['SourceHandle']:
        if filter_by is not None and filter_by not in ("system", "user"):
            raise ValueError(f"invalid value for filter_by: {filter_by}")
        res = cast(SourcesResponse, self._request_json(
            METHOD_GET, "/sources", {}, capture_err=False))

        def respond(arr: List[SourceItem]) -> Iterable['SourceHandle']:
            for source in arr:
                yield SourceHandle(
                    self, source["id"], source["type"], infer_type=True,
                    name=source["name"], immutable=source["immutable"],
                    help_message=source["help"])

        if filter_by is None or filter_by == "system":
            yield from respond(res["systemSources"])
        if filter_by is None or filter_by == "user":
            yield from respond(res["userSources"])


class JobHandle:
    def __init__(self,
                 client: XYMEClient,
                 job_id: str,
                 path: Optional[str],
                 name: Optional[str],
                 schema_obj: Optional[Dict[str, Any]],
                 kinds: Optional[List[str]],
                 status: Optional[str],
                 permalink: Optional[str],
                 time_total: Optional[float],
                 time_start: Optional[str],
                 time_end: Optional[str],
                 time_estimate: Optional[str]) -> None:
        self._client = client
        self._job_id = job_id
        self._name = name
        self._path = path
        self._schema_obj = schema_obj
        self._kinds = kinds
        self._permalink = permalink
        self._status = status
        self._time_total = time_total
        self._time_start = time_start
        self._time_end = time_end
        self._time_estimate = time_estimate
        self._buttons: Optional[List[str]] = None
        self._can_rename: Optional[bool] = None
        self._is_symjob: Optional[bool] = None
        self._is_user_job: Optional[bool] = None
        self._plot_order_types: Optional[List[str]] = None
        self._tabs: Optional[List[str]] = None
        self._tickers: Optional[List[str]] = None
        self._source: Optional[SourceHandle] = None

    def refresh(self) -> None:
        self._name = None
        self._path = None
        self._schema_obj = None
        self._kinds = None
        self._permalink = None
        self._status = None
        self._time_total = None
        self._time_start = None
        self._time_end = None
        self._time_estimate = None
        self._buttons = None
        self._can_rename = None
        self._is_symjob = None
        self._is_user_job = None
        self._plot_order_types = None
        self._tabs = None
        self._tickers = None
        self._source = None

    def _maybe_refresh(self) -> None:
        if self._client.is_auto_refresh():
            self.refresh()

    def _fetch_info(self) -> None:
        res = self._client._request_json(
            METHOD_LONGPOST, "/status", {
                "job": self._job_id,
            }, capture_err=False)
        if res.get("empty", True) and "name" not in res:
            raise ValueError("could not update status")
        info = cast(JobStatusInfo, res)
        self._name = info["name"]
        self._path = info["path"]
        self._schema_obj = json.loads(info["schema"])
        self._buttons = info["buttons"]
        self._can_rename = info["canRename"]
        self._is_symjob = info["symjob"]
        self._is_user_job = info["isUserJob"]
        self._kinds = info["allKinds"]
        self._permalink = info["permalink"]
        self._plot_order_types = info["plotOrderTypes"]
        self._status = info["status"]
        self._tabs = info["allTabs"]
        self._tickers = info["tickers"]
        self._time_total = info["timeTotal"]
        self._time_start = info["timeStart"]
        self._time_end = info["timeEnd"]
        self._time_estimate = info["timeEstimate"]

    def get_job_id(self) -> str:
        return self._job_id

    def get_schema(self) -> Dict[str, Any]:
        self._maybe_refresh()
        if self._schema_obj is None:
            self._fetch_info()
        assert self._schema_obj is not None
        return copy.deepcopy(self._schema_obj)

    def set_schema(self, schema: Dict[str, Any]) -> None:
        res = cast(SchemaResponse, self._client._request_json(
            METHOD_PUT, "/update_job_schema", {
                "job": self._job_id,
                "schema": json.dumps(schema),
            }, capture_err=True))
        self._schema_obj = json.loads(res["schema"])

    @contextlib.contextmanager
    def update_schema(self) -> Iterator[Dict[str, Any]]:
        self._maybe_refresh()
        if self._schema_obj is None:
            self._fetch_info()
        assert self._schema_obj is not None
        yield self._schema_obj
        self.set_schema(self._schema_obj)

    @contextlib.contextmanager
    def bulk_operation(self) -> Iterator[None]:
        with self._client.bulk_operation():
            self.refresh()
            yield

    def start(self,
              user: Optional[str] = None,
              company: Optional[str] = None,
              nowait: Optional[bool] = None) -> JobStartResponse:
        res = self._client.start_job(
            self._job_id, user=user, company=company, nowait=nowait)
        self.refresh()
        return res

    def delete(self) -> None:
        self._client._request_json(METHOD_DELETE, "/clean", {
            "job": self._job_id,
        }, capture_err=True)
        self.refresh()

    def get_name(self) -> str:
        self._maybe_refresh()
        if self._name is None:
            self._fetch_info()
        assert self._name is not None
        return self._name

    def get_path(self) -> str:
        self._maybe_refresh()
        if self._path is None:
            self._fetch_info()
        assert self._path is not None
        return self._path

    def get_status(self) -> str:
        self._maybe_refresh()
        if self._status is None:
            self._fetch_info()
        assert self._status is not None
        return self._status

    def can_rename(self) -> bool:
        self._maybe_refresh()
        if self._can_rename is None:
            self._fetch_info()
        assert self._can_rename is not None
        return self._can_rename

    def is_symjob(self) -> bool:
        self._maybe_refresh()
        if self._is_symjob is None:
            self._fetch_info()
        assert self._is_symjob is not None
        return self._is_symjob

    def is_user_job(self) -> bool:
        self._maybe_refresh()
        if self._is_user_job is None:
            self._fetch_info()
        assert self._is_user_job is not None
        return self._is_user_job

    def is_paused(self) -> bool:
        return self.get_status() == "paused"

    def is_draft(self) -> bool:
        return self.get_status() == "draft"

    def get_permalink(self) -> str:
        self._maybe_refresh()
        if self._permalink is None:
            self._fetch_info()
        assert self._permalink is not None
        return self._permalink

    def get_tickers(self) -> List[str]:
        self._maybe_refresh()
        if self._tickers is None:
            self._fetch_info()
        assert self._tickers is not None
        return list(self._tickers)

    def set_name(self, name: str) -> None:
        res = cast(JobRenameResponse, self._client._request_json(
            METHOD_PUT, "/rename", {
                "job": self._job_id,
                "name": name,
            }, capture_err=True))
        self._name = res["name"]
        self._path = res["path"]

    def set_pause(self, is_pause: bool) -> bool:
        path = "/pause" if is_pause else "/unpause"
        res = cast(JobPauseResponse, self._client._request_json(
            METHOD_POST, path, {
                "job": self._job_id,
            }, capture_err=True))
        return is_pause if res["success"] else not is_pause

    def dynamic_predict(self,
                        method: str,
                        fbuff: IO[bytes]) -> DynamicPredictionResponse:
        res = cast(DynamicPredictionResponse, self._client._request_json(
            METHOD_FILE, "/dynamic_predict", {
                "job": self._job_id,
                "method": method,
            }, capture_err=True, files={"file": fbuff}))
        return res

    def predict(self,
                df: pd.DataFrame,
                ) -> Tuple[Optional[pd.DataFrame], StdoutWrapper]:
        buff = df_to_csv(df)
        res = self.dynamic_predict("dyn_pred", buff)
        return (
            maybe_predictions_to_df(res["predictions"]),
            StdoutWrapper(res["stdout"]),
        )

    def predict_proba(self,
                      df: pd.DataFrame,
                      ) -> Tuple[Optional[pd.DataFrame], StdoutWrapper]:
        buff = df_to_csv(df)
        res = self.dynamic_predict("dyn_prob", buff)
        return (
            maybe_predictions_to_df(res["predictions"]),
            StdoutWrapper(res["stdout"]),
        )

    def predict_file(self,
                     csv: str,
                     ) -> Tuple[Optional[pd.DataFrame], StdoutWrapper]:
        with open(csv, "rb") as f_in:
            res = self.dynamic_predict("dyn_pred", f_in)
            return (
                maybe_predictions_to_df(res["predictions"]),
                StdoutWrapper(res["stdout"]),
            )

    def predict_proba_file(self,
                           csv: str,
                           ) -> Tuple[Optional[pd.DataFrame], StdoutWrapper]:
        with open(csv, "rb") as f_in:
            res = self.dynamic_predict("dyn_prob", f_in)
            return (
                maybe_predictions_to_df(res["predictions"]),
                StdoutWrapper(res["stdout"]),
            )

    def share(self, path: str) -> 'JobHandle':
        shared = cast(ShareResponse, self._client._request_json(
            METHOD_PUT, "/share", {
                "job": self._job_id,
                "with": path,
            }, capture_err=True))
        return JobHandle(client=self._client,
                         job_id=shared["job"],
                         path=None,
                         name=None,
                         schema_obj=None,
                         kinds=None,
                         status=None,
                         permalink=None,
                         time_total=None,
                         time_start=None,
                         time_end=None,
                         time_estimate=None)

    def create_source(self,
                      source_type: str,
                      name: Optional[str] = None) -> 'SourceHandle':
        res = self._client._raw_create_source(
            source_type, name, self, None, None)
        return res["source"]

    def get_source(self, ticker: Optional[str] = None) -> 'SourceHandle':
        self._maybe_refresh()
        if self._source is not None:
            return self._source
        res = cast(SourceResponse, self._client._request_json(
            METHOD_GET, "/job_source", {
                "job": self._job_id,
                "ticker": ticker,
            }, capture_err=True))
        self._source = SourceHandle(
            self._client, res["sourceId"], None, infer_type=True)
        return self._source

    def inspect(self, ticker: Optional[str]) -> 'InspectHandle':
        return InspectHandle(self._client, self, ticker)

    def __repr__(self) -> str:
        name = ""
        if self._name is not None:
            name = f" ({self._name})"
        return f"{self.__class__.__name__}: {self._job_id}{name}"

    def __str__(self) -> str:
        return repr(self)


InspectValue = Union['InspectPath', bool, int, float, str, None]


class InspectPath(collections.abc.Mapping):
    def __init__(self,
                 clazz: str,
                 inspect: 'InspectHandle',
                 path: List[str],
                 summary: str):
        self._clazz = clazz
        self._inspect = inspect
        self._path = path
        self._cache: Dict[str, InspectValue] = {}
        self._key_cache: Optional[Set[str]] = None
        self._summary = summary

    def refresh(self) -> None:
        self._cache = {}

    def get_clazz(self) -> str:
        return self._clazz

    def has_cached_value(self, key: str) -> bool:
        return key in self._cache

    def _set_cache(self, key: str, value: InspectValue) -> None:
        self._cache[key] = value

    def _set_key_cache(self, keys: Iterable[str]) -> None:
        self._key_cache = set(keys)

    def __getitem__(self, key: str) -> InspectValue:
        # pylint: disable=unsupported-membership-test
        if self._key_cache is not None and key not in self._key_cache:
            raise KeyError(key)
        if not self.has_cached_value(key):
            self._cache[key] = self._inspect.get_value_for_path(
                self._path + [key])
        return self._cache[key]

    def _ensure_key_cache(self) -> None:
        if self._key_cache is not None:
            return
        self._key_cache = set()

    def __iter__(self) -> Iterator[str]:
        self._ensure_key_cache()
        assert self._key_cache is not None
        return iter(self._key_cache)

    def __len__(self) -> int:
        self._ensure_key_cache()
        assert self._key_cache is not None
        return len(self._key_cache)

    def __repr__(self) -> str:
        res = f"{self._clazz}: {{"
        first = True
        for key in self:
            if first:
                first = False
            else:
                res += ", "
            res += repr(key)
            res += ": "
            if self.has_cached_value(key):
                res += repr(self[key])
            else:
                res += "..."
        return f"{res}}}"

    def __str__(self) -> str:
        return f"{self._clazz}: {self._summary}"


class InspectHandle(InspectPath):
    def __init__(self,
                 client: XYMEClient,
                 job: JobHandle,
                 ticker: Optional[str]) -> None:
        super().__init__("uninitialized", self, [], "uninitialized")
        self._client = client
        self._job_id = job._job_id
        self._ticker = ticker
        init = self._query([])
        if init is not None:
            self._clazz = init["clazz"]
            self._summary = init["extra"]
            values = init["value"]
            if isinstance(values, list):
                self._set_key_cache((name for (name, _) in values))

    def _query(self, path: List[str]) -> Optional[InspectItem]:
        query: InspectQuery = {"state": {}}
        assert query is not None
        subq = query["state"]
        for seg in path:
            assert subq is not None
            subq[seg] = {}
            subq = subq[seg]
        res = cast(InspectResponse, self._client._request_json(
            METHOD_LONGPOST, "/inspect", {
                "job": self._job_id,
                "query": query,
                "ticker": self._ticker,
            }, capture_err=False))
        return res["inspect"]

    def get_value_for_path(self, path: List[str]) -> InspectValue:
        res: InspectValue = self
        do_query = False
        for seg in path:
            if not isinstance(res, InspectPath):
                raise TypeError(f"cannot query {seg} on {res}")
            if res.has_cached_value(seg):
                res = res[seg]
                continue
            do_query = True
            break

        def maybe_set_cache(key: str,
                            ipath: InspectPath,
                            obj: InspectItem) -> Optional[InspectPath]:
            has_value = False
            res = None
            if obj["leaf"]:
                if obj["clazz"] == "None":
                    val: InspectValue = None
                elif obj["clazz"] == "bool":
                    val = bool(obj["value"])
                elif obj["clazz"] == "int":
                    if obj["value"] is None:
                        raise TypeError(f"did not expect None: {obj}")
                    if isinstance(obj["value"], list):
                        raise TypeError(f"did not expect list: {obj}")
                    val = int(obj["value"])
                elif obj["clazz"] == "float":
                    if obj["value"] is None:
                        raise TypeError(f"did not expect None: {obj}")
                    if isinstance(obj["value"], list):
                        raise TypeError(f"did not expect list: {obj}")
                    val = float(obj["value"])
                elif obj["clazz"] == "str":
                    val = str(obj["value"])
                else:
                    if isinstance(obj["value"], list):
                        raise TypeError(f"did not expect list: {obj}")
                    val = obj["value"]
                has_value = True
            elif not obj["collapsed"]:
                res = InspectPath(
                    obj["clazz"], self, list(upto), obj["extra"])
                val = res
                values = obj["value"]
                if not isinstance(values, list):
                    raise TypeError(f"expected list got: {values}")
                keys = []
                for (name, cur) in values:
                    keys.append(name)
                    maybe_set_cache(name, val, cur)
                val._set_key_cache(keys)
                has_value = True
            if has_value:
                ipath._set_cache(key, val)
            return res

        if do_query:
            obj = self._query(path)
            res = self
            upto = []
            for seg in path:
                upto.append(seg)
                if obj is None:
                    raise KeyError(seg)
                if obj["leaf"]:
                    raise ValueError(f"early leaf node at {seg}")
                next_obj: Optional[InspectItem] = None
                values = obj["value"]
                if not isinstance(values, list):
                    raise TypeError(f"expected list got: {values}")
                for (name, value) in values:
                    if name == seg:
                        next_obj = value
                        break
                if next_obj is None:
                    raise KeyError(seg)
                obj = next_obj
                if not isinstance(res, InspectPath):
                    raise TypeError(f"cannot query {seg} on {res}")
                if res.has_cached_value(seg):
                    res = res[seg]
                else:
                    ipath = maybe_set_cache(seg, res, obj)
                    assert ipath is not None
                    res = ipath
        return res


class SourceHandle:
    def __init__(self,
                 client: XYMEClient,
                 source_id: str,
                 source_type: Optional[str],
                 infer_type: bool = False,
                 name: Optional[str] = None,
                 immutable: Optional[bool] = None,
                 help_message: Optional[str] = None):
        if not source_id:
            raise ValueError("source id is not set!")
        if not infer_type and (
                not source_type or source_type not in ALL_SOURCE_TYPES):
            raise ValueError(f"invalid source type: {source_type}")
        self._client = client
        self._source_id = source_id
        self._source_type = source_type
        self._name = name
        self._immutable = immutable
        self._help = help_message
        self._schema_obj: Optional[Dict[str, Any]] = None
        self._dirty: Optional[bool] = None

    def refresh(self) -> None:
        self._schema_obj = None
        self._dirty = None
        self._immutable = None
        self._name = None
        self._help = None

    def _maybe_refresh(self) -> None:
        if self._client.is_auto_refresh():
            self.refresh()

    def _fetch_info(self) -> None:
        res = cast(SourceSchemaResponse, self._client._request_json(
            METHOD_GET, "/source_schema", {
                "sourceId": self._source_id,
            }, capture_err=True))
        self._schema_obj = res["sourceSchema"]
        info = res["sourceInfo"]
        self._dirty = info["dirty"]
        self._immutable = info["immutable"]
        self._name = info["sourceName"]
        self._source_type = info["sourceType"]

    def _ensure_multi_source(self) -> None:
        if not self.is_multi_source():
            raise ValueError("can only add source to multi source")

    def get_schema(self) -> Dict[str, Any]:
        self._maybe_refresh()
        if self._schema_obj is None:
            self._fetch_info()
        assert self._schema_obj is not None
        return copy.deepcopy(self._schema_obj)

    def set_schema(self, schema: Dict[str, Any]) -> None:
        res = cast(SchemaResponse, self._client._request_json(
            METHOD_PUT, "/source_schema", {
                "sourceId": self._source_id,
                "schema": json.dumps(schema),
            }, capture_err=True))
        schema_obj = json.loads(res["schema"])
        self._schema_obj = schema_obj
        # NOTE: we can infer information about
        # the source by being able to change it
        self._name = schema_obj.get("name", self._source_id)
        self._source_type = schema_obj.get("type")
        self._help = schema_obj.get("help", "")
        self._dirty = True
        self._immutable = False

    def get_name(self) -> str:
        self._maybe_refresh()
        if self._name is None:
            self._fetch_info()
        assert self._name is not None
        return self._name

    def get_help_message(self) -> str:
        self._maybe_refresh()
        if self._help is None:
            self._fetch_info()
        assert self._help is not None
        return self._help

    @contextlib.contextmanager
    def bulk_operation(self) -> Iterator[None]:
        with self._client.bulk_operation():
            self.refresh()
            yield

    @contextlib.contextmanager
    def update_schema(self) -> Iterator[Dict[str, Any]]:
        self._maybe_refresh()
        if self._schema_obj is None:
            self._fetch_info()
        assert self._schema_obj is not None
        yield self._schema_obj
        self.set_schema(self._schema_obj)

    def is_dirty(self) -> bool:
        self._maybe_refresh()
        if self._dirty is None:
            self._fetch_info()
        assert self._dirty is not None
        return self._dirty

    def is_immutable(self) -> bool:
        self._maybe_refresh()
        if self._immutable is None:
            self._fetch_info()
        assert self._immutable is not None
        return self._immutable

    def get_source_id(self) -> str:
        return self._source_id

    def get_source_type(self) -> str:
        # NOTE: we don't refresh source type frequently
        if self._source_type is None:
            self._fetch_info()
        assert self._source_type is not None
        return self._source_type

    def is_multi_source(self) -> bool:
        return self.get_source_type() == SOURCE_TYPE_MULTI

    def set_immutable(self, is_immutable: bool) -> 'SourceHandle':
        res = self._client.set_immutable_raw(self, None, None, is_immutable)
        return res["new_source"]

    def flip_immutable(self) -> 'SourceHandle':
        res = self._client.set_immutable_raw(self, None, None, None)
        return res["new_source"]

    def add_new_source(self,
                       source_type: str,
                       name: Optional[str] = None) -> 'SourceHandle':
        self._ensure_multi_source()
        res = self._client._raw_create_source(
            source_type, name, None, self, None)
        self.refresh()
        return res["source"]

    def add_source(self, source: 'SourceHandle') -> None:
        with self.bulk_operation():
            self._ensure_multi_source()
            with self.update_schema() as schema_obj:
                if "config" not in schema_obj:
                    schema_obj["config"] = {}
                config = schema_obj["config"]
                if "sources" not in config:
                    config["sources"] = []
                config["sources"].append(source.get_source_id())

    def get_sources(self) -> Iterable['SourceHandle']:
        with self.bulk_operation():
            self._ensure_multi_source()
            if self._schema_obj is None:
                self._fetch_info()
            assert self._schema_obj is not None
            sources = self._schema_obj.get("config", {}).get("sources", [])
            for source_id in sources:
                yield SourceHandle(
                    self._client, source_id, None, infer_type=True)

    def __repr__(self) -> str:
        name = ""
        if self._name is not None:
            name = f" ({self._name})"
        return f"{self.__class__.__name__}: {self._source_id}{name}"

    def __str__(self) -> str:
        return repr(self)


def create_xyme_client(url: str,
                       user: Optional[str] = None,
                       password: Optional[str] = None,
                       token: Optional[str] = None) -> XYMEClient:
    return XYMEClient(url, user, password, token)


@contextlib.contextmanager
def create_xyme_session(url: str,
                        user: Optional[str] = None,
                        password: Optional[str] = None,
                        token: Optional[str] = None) -> Iterator[XYMEClient]:
    try:
        client = XYMEClient(url, user, password, token)
        yield client
    finally:
        client.logout()
