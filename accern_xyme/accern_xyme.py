from typing import Any, cast, Dict, List, Optional, Iterator, IO, Tuple, Union
import os
import copy
import json
import time
import urllib
import requests
import contextlib
from io import BytesIO, TextIOWrapper
import pandas as pd
from typing_extensions import TypedDict
import quick_server


__version__ = "0.0.3"


METHOD_DELETE = "DELETE"
METHOD_FILE = "FILE"
METHOD_GET = "GET"
METHOD_LONGPOST = "LONGPOST"
METHOD_POST = "POST"
METHOD_PUT = "PUT"

PREFIX = "/xyme"


VersionInfo = TypedDict('VersionInfo', {
    "time": str,
    "version": str,
    "xymeVersion": str,
})
UserLogin = TypedDict('UserLogin', {
    "permissions": List[str],
    "success": bool,
    "token": str,
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


def df_to_csv(df: pd.DataFrame) -> BytesIO:
    bio = BytesIO()
    wrap = TextIOWrapper(bio, encoding="utf-8", write_through=True)
    df.to_csv(wrap, index=False)
    wrap.detach()
    bio.seek(0)
    return bio


def predictions_to_df(preds: PredictionsResponse) -> pd.DataFrame:
    return pd.DataFrame(preds["values"], columns=preds["columns"])


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

    def _raw_request_json(self,
                          method: str,
                          path: str,
                          args: Dict[str, Any],
                          add_prefix: bool = True,
                          files: Optional[Dict[str, IO[bytes]]] = None,
                          ) -> Dict[str, Any]:
        url = f"{self._url}{PREFIX if add_prefix else ''}{path}"
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
            return quick_server.worker_request(url, args)
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
                      ) -> Dict[str, Any]:
        if self._token is None:
            self._login()

        def execute() -> Dict[str, Any]:
            args["token"] = self._token
            res = self._raw_request_json(method, path, args, add_prefix, files)
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

    def create_job(self,
                   schema: Optional[Dict[str, Any]],
                   from_job_id: Optional[str],
                   name: Optional[str],
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

    def flush_caches(self) -> None:
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
        if self._schema_obj is None:
            self._fetch_info()
        assert self._schema_obj is not None
        yield self._schema_obj
        self.set_schema(self._schema_obj)

    def start(self,
              user: Optional[str] = None,
              company: Optional[str] = None,
              nowait: Optional[bool] = None) -> JobStartResponse:
        res = self._client.start_job(
            self._job_id, user=user, company=company, nowait=nowait)
        self.flush_caches()
        return res

    def delete(self) -> None:
        self._client._request_json(METHOD_DELETE, "/clean", {
            "job": self._job_id,
        }, capture_err=True)
        self.flush_caches()

    def get_name(self) -> str:
        if self._name is None:
            self._fetch_info()
        assert self._name is not None
        return self._name

    def get_path(self) -> str:
        if self._path is None:
            self._fetch_info()
        assert self._path is not None
        return self._path

    def get_status(self) -> str:
        if self._status is None:
            self._fetch_info()
        assert self._status is not None
        return self._status

    def can_rename(self) -> bool:
        if self._can_rename is None:
            self._fetch_info()
        assert self._can_rename is not None
        return self._can_rename

    def is_symjob(self) -> bool:
        if self._is_symjob is None:
            self._fetch_info()
        assert self._is_symjob is not None
        return self._is_symjob

    def is_user_job(self) -> bool:
        if self._is_user_job is None:
            self._fetch_info()
        assert self._is_user_job is not None
        return self._is_user_job

    def is_paused(self) -> bool:
        return self.get_status() == "paused"

    def is_draft(self) -> bool:
        return self.get_status() == "draft"

    def get_permalink(self) -> str:
        if self._permalink is None:
            self._fetch_info()
        assert self._permalink is not None
        return self._permalink

    def get_tickers(self) -> List[str]:
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


def create_xyme_client(url: str,
                       user: Optional[str] = None,
                       password: Optional[str] = None,
                       token: Optional[str] = None) -> XYMEClient:
    return XYMEClient(url, user, password, token)
