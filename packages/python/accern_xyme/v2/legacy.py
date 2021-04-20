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
import io
import os
import sys
import copy
import json
import time
import threading
import contextlib
import collections
from io import BytesIO
import requests
import pandas as pd
from typing_extensions import TypedDict, Literal, overload
import quick_server

from accern_xyme.v2.util import (
    get_file_upload_chunk_size,
    get_max_retry,
    get_retry_sleep,
    maybe_timestamp,
    df_to_csv,
    setup_matplotlib,
    get_progress_bar,
    get_file_hash,
)


API_VERSION = 2


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

INPUT_CSV_EXT = ".csv"
INPUT_TSV_EXT = ".tsv"
INPUT_ZIP_EXT = ".zip"
INPUT_EXT = [INPUT_ZIP_EXT, INPUT_CSV_EXT, INPUT_TSV_EXT]

UPLOAD_IN_PROGRESS = "in_progress"
UPLOAD_DONE = "done"
UPLOAD_START = "start"

FILTER_SYSTEM = "system"
FILTER_USER = "user"
FILTERS = [FILTER_SYSTEM, FILTER_USER]

PLOT_PKL = "pkl"
PLOT_CSV = "csv"
PLOT_OUTPUT = "output"
PLOT_META = "meta"

PRED_LATEST = "latest"  # using the current snapshot of the model
PRED_HISTORIC = "historic"  # using the historic data
PRED_PREDICTED = "predicted"  # using csv
PRED_PROBA = "proba_outcome"  # using historic probabilities
# using filtered historic probabilities
PRED_PROBA_FILTERED = "proba_outcome_filter"
PRED_PROBS = "probs"  # using historic probabilities
PRED_VALID = "validation"  # using validation data


VersionInfo = TypedDict('VersionInfo', {
    "apiVersion": Union[str, int],
    "callerApiVersion": str,
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
JobBackupResponse = TypedDict('JobBackupResponse', {
    "jobId": str,
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
CreateInputResponse = TypedDict('CreateInputResponse', {
    "inputId": str,
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
InputDetailsResponse = TypedDict('InputDetailsResponse', {
    "name": Optional[str],
    "path": str,
    "extension": Optional[str],
    "inputId": str,
    "lastByteOffset": Optional[int],
    "size": Optional[int],
    "progress": Optional[str],
})
UploadResponse = TypedDict('UploadResponse', {
    "name": Optional[str],
    "path": str,
    "extension": Optional[str],
    "inputId": str,
    "lastByteOffset": Optional[int],
    "size": Optional[int],
    "progress": Optional[str],
    "exists": bool,
})
InputItem = TypedDict('InputItem', {
    "name": Optional[str],
    "path": str,
    "file": str,
    "immutable": bool,
})
InputsResponse = TypedDict('InputsResponse', {
    "systemFiles": List[InputItem],
    "userFiles": List[InputItem],
})
EmptyDict = TypedDict('EmptyDict', {})
AggMetricPlot = TypedDict('AggMetricPlot', {
    "inner": List[List[Union[str, float]]],
    "plot": str,
    "color": str,
    "name": str,
    "ticker": Optional[str],
    "count": int,
    "cat": bool,
    "catValues": Optional[List[Union[str, float]]],
})
RangedPlot = TypedDict('RangedPlot', {
    "xrange": List[str],
    "yrange": List[float],
    "lines": List[AggMetricPlot],
    "kind": Literal["time"],
})
PlotCoords = List[Tuple[float, float]]
CoordPlot = TypedDict('CoordPlot', {
    "name": str,
    "color": str,
    "coords": PlotCoords,
})
RangedCoords = TypedDict('RangedCoords', {
    "xaxis": str,
    "yaxis": str,
    "xrange": List[float],
    "yrange": List[float],
    "coords": List[CoordPlot],
    "kind": Literal["coord"],
})
MetricResponse = TypedDict('MetricResponse', {
    "lines": Union[EmptyDict, RangedCoords, RangedPlot],
    "pollHint": float,
})
MetricListResponse = TypedDict('MetricListResponse', {
    "metrics": List[List[str]],
    "selectedPlots": List[List[str]],
    "hiddenPlots": List[List[str]],
    "pollHint": float,
})
MetricListInfo = TypedDict('MetricListInfo', {
    "metrics": List[List[str]],
    "selected_plots": List[List[str]],
    "hidden_plots": List[List[str]],
})
SummaryResponse = TypedDict('SummaryResponse', {
    "messages": Dict[str, List[Tuple[str, List[StdoutLine]]]],
    "exceptions": List[Tuple[str, List[StdoutLine]]],
    "lastEvent": Optional[Tuple[str, str, str]],
    "rows": Optional[int],
    "rowsTotal": Optional[int],
    "features": Optional[Dict[str, int]],
    "droppedFeatures": Optional[Dict[str, int]],
    "dataStart": Optional[str],
    "dataHigh": Optional[str],
    "dataEnd": Optional[str],
    "pollHint": float,
})
SummaryInfo = TypedDict('SummaryInfo', {
    "stdout": 'StdoutWrapper',
    "last_event": Optional[Tuple[str, str, str]],
    "rows": Optional[int],
    "rows_total": Optional[int],
    "features": Optional[Dict[str, int]],
    "dropped_features": Optional[Dict[str, int]],
    "data_start": Optional[pd.Timestamp],
    "data_high": Optional[pd.Timestamp],
    "data_end": Optional[pd.Timestamp],
})
JobStdoutResponse = TypedDict('JobStdoutResponse', {
    "lines": List[StdoutLine],
    "pollHint": float,
})
NotesResponse = TypedDict('NotesResponse', {
    "usage": Dict[str, bool],
    "roles": Dict[str, str],
    "rolesRenamed": Dict[str, str],
    "dummyColumns": List[str],
    "stats": Dict[str, Dict[str, Any]],
    "isRunnable": bool,
    "suggestions": Dict[str, List[Dict[str, str]]],
    "isPreview": bool,
    "error": bool,
})
NotesInfo = TypedDict('NotesInfo', {
    "usage": Dict[str, bool],
    "roles": Dict[str, str],
    "roles_renamed": Dict[str, str],
    "dummy_columns": List[str],
    "stats": Dict[str, Dict[str, Any]],
    "is_runnable": bool,
    "suggestions": Dict[str, List[Dict[str, str]]],
    "is_preview": bool,
    "error": bool,
})
PreviewNotesResponse = TypedDict('PreviewNotesResponse', {
    "notes": Optional[NotesResponse],
})
StrategyResponse = TypedDict('StrategyResponse', {
    "strategies": List[str],
    "pollHint": float,
})
BacktestResponse = TypedDict('BacktestResponse', {
    "pyfolio": Optional[Dict[str, Any]],
    "output": Optional[List[StdoutLine]],
    "errMessage": Optional[str],
    "pollHint": float,
})
SegmentResponse = TypedDict('SegmentResponse', {
    "segments": List[Tuple[int, Optional[str], Optional[str]]],
    "pollHint": float,
})
SimplePredictionsResponse = TypedDict('SimplePredictionsResponse', {
    "isClf": bool,
    "columns": List[str],
    "predsName": str,
    "predictions": List[List[Union[str, float, int]]],
    "allPredictions": List[List[Union[str, float, int]]],
    "pollHint": float,
})
ForceFlushResponse = TypedDict('ForceFlushResponse', {
    "success": bool,
})
JobColumnsResponse = TypedDict('JobColumnsResponse', {
    "columns": List[str],
})
JobRegisterResponse = TypedDict('JobRegisterResponse', {
    "success": bool,
})
HidePlotResponse = TypedDict('HidePlotResponse', {
    "success": bool,
})
SelectPlotsResponse = TypedDict('SelectPlotsResponse', {
    "selectedPlots": List[List[str]],
})
DataPlotListResponse = TypedDict('DataPlotListResponse', {
    "dataPlots": List[List[str]],
    "noOrdering": bool,
    "hiddenPlots": List[List[str]],
    "pollHint": float,
    "selectedPlots": List[List[str]],
})
DataPlotListInfo = TypedDict('DataPlotListInfo', {
    "data_plots": List[List[str]],
    "selected_plots": List[List[str]],
    "hidden_plots": List[List[str]],
})


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

# *** AccessDenied ***


class StdoutWrapper:
    def __init__(self, stdout: StdoutResponse) -> None:
        self._lines = stdout["lines"]
        self._messages = stdout["messages"]
        self._exceptions = stdout["exceptions"]

    def full_output(self) -> str:
        return "\n".join((line for _, line in self._lines))

    def get_messages(self) -> Dict[str, List[Tuple[str, List[StdoutLine]]]]:
        return self._messages

    def get_exceptions(self) -> List[Tuple[str, List[StdoutLine]]]:
        return self._exceptions

    def __str__(self) -> str:
        return self.full_output()

    def __repr__(self) -> str:
        return self.full_output()

# *** StdoutWrapper ***


class MetricWrapper(collections.abc.Sequence):
    def get_xaxis(self) -> str:
        raise NotImplementedError()

    def get_yaxis(self) -> str:
        raise NotImplementedError()

    def get_xrange(self) -> Tuple[Any, Any]:
        raise NotImplementedError()

    def get_yrange(self) -> Tuple[Any, Any]:
        raise NotImplementedError()

    def is_coordinates(self) -> bool:
        raise NotImplementedError()

    def plot(self,
             figsize: Optional[Tuple[int, int]] = None,
             use_ranges: bool = False) -> None:
        import matplotlib.pyplot as plt
        setup_matplotlib()

        draw = False
        if figsize is not None:
            plt.figure(figsize=figsize)

        if len(self) == 1:
            plt.title(self[0].get_name())

        plt.xlabel(self.get_xaxis())
        if self.is_coordinates():
            plt.ylabel(self.get_yaxis())

        if use_ranges:

            def is_proper_range(v_range: Tuple[Any, Any]) -> bool:
                return v_range[0] != v_range[1]

            x_range = self.get_xrange()
            if is_proper_range(x_range):
                plt.xlim(x_range)
            y_range = self.get_yrange()
            if is_proper_range(y_range):
                plt.ylim(y_range)

        for plot in self:
            plot.plot(show=False)
            draw = True

        if draw:
            plt.show()

# *** MetricWrapper ***


class AggregatePlot:
    def __init__(self, plot: AggMetricPlot) -> None:
        self._id = plot["plot"]
        self._name = plot["name"]
        self._color = plot["color"]
        self._ticker = plot["ticker"]
        self._is_cat = plot["cat"]
        self._cat_values = plot["catValues"]
        self._count = plot["count"]
        cols = ["date", "vmean", "vstddev", "q_low", "q_high", "vmin", "vmax"]
        df = pd.DataFrame(plot["inner"], columns=cols)
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
        self._df = df

    def get_id(self) -> str:
        return self._id

    def get_name(self) -> str:
        return self._name

    def get_color(self) -> str:
        return self._color

    def get_ticker(self) -> Optional[str]:
        return self._ticker

    def is_categorical(self) -> bool:
        return self._is_cat

    def get_categorical_values(self) -> Optional[List[Union[str, float]]]:
        return self._cat_values

    def get_aggregate_count(self) -> int:
        return self._count

    def is_single(self) -> bool:
        return self._count == 1

    def get_single_df(self) -> pd.DataFrame:
        if not self.is_single():
            raise ValueError("metric is aggregate")
        res = self._df[["date", "vmean"]].copy()
        if self._is_cat:
            assert self._cat_values is not None
            mapping = dict(enumerate(self._cat_values))
            res["vmean"] = res["vmean"].map(mapping)
        if self._ticker is None:
            res = res.set_index(["date"])
        else:
            res["ticker"] = self._ticker
            res = res.set_index(["ticker", "date"])
        return res.rename(columns={"vmean": self._id})

    def get_full_df(self) -> pd.DataFrame:
        res = self._df.copy()
        if self._ticker is None:
            res = res.set_index(["date"])
        else:
            res["ticker"] = self._ticker
            res = res.set_index(["ticker", "date"])
        return res.rename(columns={
            "vmean": f"{self._id}_mean",
            "vstddev": f"{self._id}_stddev",
            "q_low": f"{self._id}_q_low",
            "q_high": f"{self._id}_q_high",
            "vmin": f"{self._id}_min",
            "vmax": f"{self._id}_max",
        })

    def plot(self, show: bool = True) -> None:
        import matplotlib.pyplot as plt
        setup_matplotlib()

        df = self._df
        if self.is_single():
            plt.plot(
                df["date"], df["vmean"], color=self._color, label=self._name)
        else:
            plt.plot(
                df["date"], df["vmean"], color=self._color, label=self._name)
            plt.fill_between(
                df["date"], df["q_low"], df["q_high"], interpolate=True,
                color=self._color, alpha=0.25)
            plt.plot(df["date"], df["q_low"], color=self._color, alpha=0.25)
            plt.plot(df["date"], df["q_high"], color=self._color, alpha=0.25)
            plt.plot(
                df["date"], df["vmin"], linestyle="dashed", color=self._color)
            plt.plot(
                df["date"], df["vmax"], linestyle="dashed", color=self._color)

        if show:
            plt.show()


# *** AggregatePlot ***


class MetricPlot(MetricWrapper):
    def __init__(self, plot: RangedPlot) -> None:
        self._xrange = (
            pd.to_datetime(plot["xrange"][0]),
            pd.to_datetime(plot["xrange"][-1]),
        )
        self._yrange = (plot["yrange"][0], plot["yrange"][-1])
        self._plots = [AggregatePlot(plot) for plot in plot["lines"]]

    @overload
    def __getitem__(self, index: int) -> AggregatePlot:
        ...

    @overload
    def __getitem__(self, index: slice) -> List[AggregatePlot]:
        ...

    def __getitem__(self,
                    index: Union[int, slice],
                    ) -> Union[AggregatePlot, List[AggregatePlot]]:
        return self._plots[index]

    def __len__(self) -> int:
        return len(self._plots)

    def get_xaxis(self) -> str:
        return "date"

    def get_yaxis(self) -> str:
        return "value"

    def get_xrange(self) -> Tuple[Any, Any]:
        return self._xrange

    def get_yrange(self) -> Tuple[Any, Any]:
        return self._yrange

    def is_coordinates(self) -> bool:
        return False


# *** MetricPlot ***


class CoordinatePlot:
    def __init__(self, plot: CoordPlot) -> None:
        self._name = plot["name"]
        self._color = plot["color"]
        self._df = pd.DataFrame(plot["coords"], columns=["x", "y"])

    def get_name(self) -> str:
        return self._name

    def get_color(self) -> str:
        return self._color

    def get_df(self) -> pd.DataFrame:
        return self._df.copy()

    def plot(self, show: bool = True) -> None:
        import matplotlib.pyplot as plt
        setup_matplotlib()

        df = self._df
        plt.plot(df["x"], df["y"], color=self._color, label=self._name)

        if show:
            plt.show()

# *** CoordinatePlot ***


class MetricCoords(MetricWrapper):
    def __init__(self, plot: RangedCoords) -> None:
        self._xaxis = plot["xaxis"]
        self._yaxis = plot["yaxis"]
        self._xrange = (plot["xrange"][0], plot["xrange"][-1])
        self._yrange = (plot["yrange"][0], plot["yrange"][-1])
        self._plots = [CoordinatePlot(plot) for plot in plot["coords"]]

    @overload
    def __getitem__(self, index: int) -> CoordinatePlot:
        ...

    @overload
    def __getitem__(self, index: slice) -> List[CoordinatePlot]:
        ...

    def __getitem__(
            self,
            index: Union[int, slice],
                ) -> Union[CoordinatePlot, List[CoordinatePlot]]:
        return self._plots[index]

    def __len__(self) -> int:
        return len(self._plots)

    def get_xaxis(self) -> str:
        return self._xaxis

    def get_yaxis(self) -> str:
        return self._yaxis

    def get_xrange(self) -> Tuple[Any, Any]:
        return self._xrange

    def get_yrange(self) -> Tuple[Any, Any]:
        return self._yrange

    def is_coordinates(self) -> bool:
        return True

# *** MetricCoords ***


class XYMELegacyClient:
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

        def get_version(legacy: bool) -> int:
            server_version = self.get_server_version(legacy)
            version = server_version.get("apiVersion", "v0")
            if isinstance(version, str):
                version = int(version.lstrip("v"))
            return int(version)

        try:
            self._api_version = min(get_version(False), API_VERSION)
        except ValueError:
            self._api_version = min(get_version(True), API_VERSION)
        self._init()

    def get_api_version(self) -> int:
        return self._api_version

    def _init(self) -> None:
        if self._token is None:
            self._login()
            return
        res = cast(UserLogin, self._request_json(
            METHOD_GET, "/init", {}, capture_err=False))
        if not res["success"]:
            raise AccessDenied("init was not successful")
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
                        raise AccessDenied(e.args) from e
                    raise e
            raise ValueError(f"unknown method {method}")
        except json.decoder.JSONDecodeError as json_e:
            if req is None:
                raise
            raise ValueError(req.text) from json_e

    def _login(self) -> None:
        if self._user is None or self._password is None:
            raise ValueError("cannot login without user or password")
        res = cast(UserLogin, self._raw_request_json(METHOD_POST, "/login", {
            "user": self._user,
            "pw": self._password,
        }))
        if not res["success"]:
            raise AccessDenied("login was not successful")
        self._token = res["token"]
        self._permissions = res["permissions"]

    def logout(self) -> None:
        if self._token is None:
            return
        self._raw_request_json(METHOD_POST, "/logout", {
            "token": self._token,
        })

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
            api_version: Optional[int] = None) -> Dict[str, Any]:
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

    def get_server_version(self, legacy: bool = False) -> VersionInfo:
        if legacy:
            return cast(VersionInfo, self._raw_request_json(
                METHOD_GET, "/version", {}, api_version=0))
        return cast(VersionInfo, self._raw_request_json(
            METHOD_GET, "/xyme/version", {}, add_prefix=False))

    def afterupdate(self) -> MaintenanceInfo:
        return cast(MaintenanceInfo, self._request_json(
            METHOD_POST, "/afterupdate", {}, capture_err=False))

    def set_maintenance_mode(self, is_maintenance: bool) -> MaintenanceInfo:
        """Set the maintenance mode of the server

        Args:
            is_maintenance (bool): If the server should be in maintenance mode.

        Returns:
            MaintenanceInfo: MaintenanceInfo object.
        """
        return cast(MaintenanceInfo, self._request_json(
            METHOD_PUT, "/maintenance", {
                "isMaintenance": is_maintenance,
            }, capture_err=False))

    def get_maintenance_mode(self) -> HintedMaintenanceInfo:
        return cast(HintedMaintenanceInfo, self._request_json(
            METHOD_GET, "/maintenance", {}, capture_err=False))

    def get_system_logs(
            self,
            query: Optional[str] = None,
            context: Optional[int] = None) -> StdoutWrapper:
        obj: Dict[str, Any] = {
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

    def create_job(
            self,
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
        return JobHandle(
            client=self,
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
        return JobHandle(
            client=self,
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

    def start_job(
            self,
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

    def _raw_job_list(
            self,
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
                time_estimate=job["timeEstimate"])
            for job in res["jobs"][workspace]
        ]

    def get_shareable(self) -> List[ShareablePath]:
        res = cast(ShareList, self._request_json(
            METHOD_LONGPOST, "/share", {}, capture_err=False))
        return [{
            "name": name,
            "path": path,
        } for (name, path) in res["shareable"]]

    def _raw_create_source(
            self,
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
                raise ValueError(
                    f"source {multi_source.get_source_id()} "
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

    def create_multi_source(self, name: Optional[str]) -> 'SourceHandle':
        res = self._raw_create_source(
            SOURCE_TYPE_MULTI, name, None, None, None)
        return res["multi_source"]

    def create_multi_source_file(
            self,
            filename: str,
            ticker_column: str,
            date_column: Optional[str],
            name_multi: Optional[str] = None,
            progress_bar: Optional[IO[Any]] = sys.stdout) -> 'SourceHandle':
        with self.bulk_operation():
            multi = self.create_multi_source(name_multi)
            multi.add_new_source_file(
                filename, ticker_column, date_column, progress_bar)
            return multi

    def create_multi_source_df(
            self,
            df: pd.DataFrame,
            name_csv: str,
            ticker_column: str,
            date_column: Optional[str],
            name_multi: Optional[str] = None,
            progress_bar: Optional[IO[Any]] = sys.stdout) -> 'SourceHandle':
        with self.bulk_operation():
            multi = self.create_multi_source(name_multi)
            multi.add_new_source_df(
                df, name_csv, ticker_column, date_column, progress_bar)
            return multi

    def get_source(self, source_id: str) -> 'SourceHandle':
        return SourceHandle(self, source_id, None, infer_type=True)

    def set_immutable_raw(
            self,
            source: 'SourceHandle',
            multi_source: Optional['SourceHandle'],
            job: Optional['JobHandle'],
            is_immutable: Optional[bool]) -> LockSource:
        multi_source_id = None
        if multi_source is not None:
            if not multi_source.is_multi_source():
                raise ValueError(
                    f"source {multi_source.get_source_id()} "
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

    def get_sources(
            self,
            filter_by: Optional[str] = None) -> Iterable['SourceHandle']:
        if filter_by is not None and filter_by not in FILTERS:
            raise ValueError(f"invalid value for filter_by: {filter_by}")
        res = cast(SourcesResponse, self._request_json(
            METHOD_GET, "/sources", {}, capture_err=False))

        def respond(arr: List[SourceItem]) -> Iterable['SourceHandle']:
            for source in arr:
                yield SourceHandle(
                    self, source["id"], source["type"], infer_type=True,
                    name=source["name"], immutable=source["immutable"],
                    help_message=source["help"])

        if filter_by is None or filter_by == FILTER_SYSTEM:
            yield from respond(res["systemSources"])
        if filter_by is None or filter_by == FILTER_USER:
            yield from respond(res["userSources"])

    def create_input(
            self,
            name: str,
            ext: str,
            size: int,
            hash_str: Optional[str]) -> 'InputHandle':
        res = cast(CreateInputResponse, self._request_json(
            METHOD_LONGPOST, "/create_input_id", {
                "name": name,
                "extension": ext,
                "size": size,
                "hash": None if self.get_api_version() < 1 else hash_str,
            }, capture_err=False))
        return InputHandle(self, res["inputId"], name=name, ext=ext, size=size)

    def get_input(self, input_id: str) -> 'InputHandle':
        return InputHandle(self, input_id)

    def input_from_io(
            self,
            io_in: IO[bytes],
            name: str,
            ext: str,
            progress_bar: Optional[IO[Any]] = sys.stdout,
                ) -> 'InputHandle':
        from_pos = io_in.seek(0, io.SEEK_CUR)
        size = io_in.seek(0, io.SEEK_END) - from_pos
        io_in.seek(from_pos, io.SEEK_SET)
        hash_str = get_file_hash(io_in)
        with self.bulk_operation():
            res: InputHandle = self.create_input(name, ext, size, hash_str)
            if not res.is_complete():
                res.upload_full(io_in, name, progress_bar)
            return res

    def input_from_file(
            self,
            filename: str,
            progress_bar: Optional[IO[Any]] = sys.stdout) -> 'InputHandle':
        if filename.endswith(f"{INPUT_CSV_EXT}{INPUT_ZIP_EXT}") \
                or filename.endswith(f"{INPUT_TSV_EXT}{INPUT_ZIP_EXT}"):
            filename = filename[:-len(INPUT_ZIP_EXT)]
        ext_pos = filename.rfind(".")
        if ext_pos >= 0:
            ext = filename[ext_pos + 1:]
        else:
            ext = ""
        fname = os.path.basename(filename)
        with open(filename, "rb") as fbuff:
            return self.input_from_io(fbuff, fname, ext, progress_bar)

    def input_from_df(
            self,
            df: pd.DataFrame,
            name: str,
            progress_bar: Optional[IO[Any]] = sys.stdout) -> 'InputHandle':
        io_in = df_to_csv(df)
        return self.input_from_io(io_in, name, "csv", progress_bar)

    def get_inputs(
            self, filter_by: Optional[str] = None) -> Iterable['InputHandle']:
        if filter_by is not None and filter_by not in FILTERS:
            raise ValueError(f"invalid value for filter_by: {filter_by}")
        res = cast(InputsResponse, self._request_json(
            METHOD_GET, "/user_files", {}, capture_err=False))

        def respond(arr: List[InputItem]) -> Iterable['InputHandle']:
            for input_obj in arr:
                filename = input_obj["file"]
                ext_pos = filename.rfind(".")
                if ext_pos >= 0:
                    ext: Optional[str] = filename[ext_pos + 1:]
                    input_id = filename[:ext_pos]
                else:
                    ext = None
                    input_id = filename
                progress = UPLOAD_DONE if input_obj["immutable"] else None
                yield InputHandle(
                    self,
                    input_id,
                    name=input_obj["name"],
                    path=input_obj["path"],
                    ext=ext,
                    progress=progress)

        if filter_by is None or filter_by == FILTER_SYSTEM:
            yield from respond(res["systemFiles"])
        if filter_by is None or filter_by == FILTER_USER:
            yield from respond(res["userFiles"])

    def get_strategies(self) -> List[str]:
        res = cast(StrategyResponse, self._request_json(
            METHOD_GET, "/strategies", {}, capture_err=False))
        return res["strategies"]

    def register(self, user_folder: str) -> bool:
        method = METHOD_PUT if self._api_version < 2 else METHOD_LONGPOST
        return cast(JobRegisterResponse, self._request_json(
            method, "/register_job", {
                "userFolder": user_folder,
            }, capture_err=True)).get("success", False)

# *** XYMELegacyClient ***


class JobHandle:
    def __init__(
            self,
            client: XYMELegacyClient,
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
        self._is_async_fetch = False
        self._async_lock = threading.RLock()

    def refresh(self) -> None:
        self._name = None
        self._path = None
        self._schema_obj = None
        self._kinds = None
        self._permalink = None
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
        if not self._is_async_fetch:
            self._status = None

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
    def bulk_operation(self) -> Iterator[bool]:
        with self._client.bulk_operation() as do_refresh:
            if do_refresh:
                self.refresh()
            yield do_refresh

    def get_notes(self, force: bool) -> Optional[NotesInfo]:
        res = cast(PreviewNotesResponse, self._client._request_json(
            METHOD_LONGPOST, "/preview", {
                "job": self._job_id,
                "view": "summary",
                "force": force,
                "schema": None,
                "batch": None,
            }, capture_err=False))
        notes = res["notes"]
        if notes is None:
            return None
        return {
            "usage": notes.get("usage", {}),
            "roles": notes.get("roles", {}),
            "roles_renamed": notes.get("rolesRenamed", {}),
            "dummy_columns": notes.get("dummyColumns", []),
            "stats": notes.get("stats", {}),
            "is_runnable": notes.get("isRunnable", False),
            "suggestions": notes.get("suggestions", {}),
            "is_preview": notes.get("isPreview", True),
            "error": notes.get("error", True),
        }

    def can_start(self, force: bool) -> bool:
        notes = self.get_notes(force)
        # NOTE: notes is None if the job has been started
        # this is a bug right now
        if notes is None:
            return True
        return notes["is_runnable"] and not notes["error"]

    def start(
            self,
            user: Optional[str] = None,
            company: Optional[str] = None,
            nowait: Optional[bool] = None) -> JobStartResponse:
        if not self.can_start(force=False):
            raise ValueError("Cannot start job. Missing data or target?")
        res = self._client.start_job(
            self._job_id, user=user, company=company, nowait=nowait)
        self.refresh()
        self._status = "waiting"
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

    def get_status(self, fast_return: bool = True) -> str:
        """Returns the status of the job.

        Args:
            fast_return (bool): If set the function is non-blocking
                and will fetch the current status in the background. The
                function will return some previous status value and a future
                call will eventually return the status returned by the call.
                This guarantees that the function call does not block for a
                long time for freshly started jobs. Defaults to True.

        Returns:
            str: The status of the job. Most common statuses are:
                unknown, draft, waiting, running, killed, error, paused, done
        """
        status = self._status
        res: str = status if status is not None else "unknown"

        def retrieve() -> None:
            if self._client.is_auto_refresh() or self._status is None:
                self.refresh()
                self._status = res
                self._fetch_info()

        def async_retrieve() -> None:
            retrieve()
            self._is_async_fetch = False

        if not fast_return:
            retrieve()
            status = self._status
            assert status is not None
            return status

        if self._is_async_fetch:
            return res

        with self._async_lock:
            if self._is_async_fetch:
                return res
            self._is_async_fetch = True

            th = threading.Thread(target=async_retrieve)
            th.start()
        return res

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
                "newJobName": name,
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

    def input_data(
            self,
            column: str,
            ticker: Optional[str],
            slow: bool = False) -> Any:

        obj: Dict[str, Any] = {
            "job": self._job_id,
            "ticker": ticker,
            "plot": column,
            "allowCat": True,
            "slow": slow,
        }
        return self._client._request_json(
            METHOD_LONGPOST, "/input", obj, capture_err=False)

    def feature_importance(
            self,
            method: str,
            ticker: Optional[str],
            date: Optional[str],
            last_n: Optional[int],
            agg_mode: Optional[str] = None) -> Any:
        obj: Dict[str, Any] = {
            "job": self._job_id,
            "ticker": ticker,
            "method": method,
            "date": date,
            "filters": {},
        }
        if agg_mode is not None:
            obj["agg_mode"] = agg_mode
        if last_n is not None:
            obj["last_n"] = last_n
        res = self._client._request_json(
            METHOD_LONGPOST, "/explain", obj, capture_err=False)
        return res

    def dynamic_predict(
            self,
            method: str,
            fbuff: Optional[IO[bytes]],
            source: Optional['SourceHandle'],
                ) -> DynamicPredictionResponse:
        if fbuff is not None:
            res = cast(DynamicPredictionResponse, self._client._request_json(
                METHOD_FILE, "/dynamic_predict", {
                    "job": self._job_id,
                    "method": method,
                }, capture_err=True, files={"file": fbuff}))
        elif source is not None:
            if self._client.get_api_version() < 1:
                raise ValueError("the XYME version does not "
                                 "support dynamic predict sources")
            res = cast(DynamicPredictionResponse, self._client._request_json(
                METHOD_POST, "/dynamic_predict", {
                    "job": self._job_id,
                    "method": method,
                    "multiSourceId": source.get_source_id(),
                }, capture_err=True))
        else:
            raise ValueError("one of fbuff or source must not be None")
        return res

    def predict_source(
            self,
            source: 'SourceHandle',
                ) -> Tuple[Optional[pd.DataFrame], StdoutWrapper]:
        res = self.dynamic_predict("dyn_pred", fbuff=None, source=source)
        return (
            maybe_predictions_to_df(res["predictions"]),
            StdoutWrapper(res["stdout"]),
        )

    def predict_proba_source(
            self,
            source: 'SourceHandle',
                ) -> Tuple[Optional[pd.DataFrame], StdoutWrapper]:
        res = self.dynamic_predict("dyn_prob", fbuff=None, source=source)
        return (
            maybe_predictions_to_df(res["predictions"]),
            StdoutWrapper(res["stdout"]),
        )

    def predict(
            self,
            df: pd.DataFrame,
                ) -> Tuple[Optional[pd.DataFrame], StdoutWrapper]:
        buff = df_to_csv(df)
        res = self.dynamic_predict("dyn_pred", fbuff=buff, source=None)
        return (
            maybe_predictions_to_df(res["predictions"]),
            StdoutWrapper(res["stdout"]),
        )

    def predict_proba(
            self,
            df: pd.DataFrame,
                ) -> Tuple[Optional[pd.DataFrame], StdoutWrapper]:
        buff = df_to_csv(df)
        res = self.dynamic_predict("dyn_prob", fbuff=buff, source=None)
        return (
            maybe_predictions_to_df(res["predictions"]),
            StdoutWrapper(res["stdout"]),
        )

    def predict_file(
            self,
            csv: str,
                ) -> Tuple[Optional[pd.DataFrame], StdoutWrapper]:
        with open(csv, "rb") as f_in:
            res = self.dynamic_predict("dyn_pred", fbuff=f_in, source=None)
            return (
                maybe_predictions_to_df(res["predictions"]),
                StdoutWrapper(res["stdout"]),
            )

    def predict_proba_file(
            self,
            csv: str,
                ) -> Tuple[Optional[pd.DataFrame], StdoutWrapper]:
        with open(csv, "rb") as f_in:
            res = self.dynamic_predict("dyn_prob", fbuff=f_in, source=None)
            return (
                maybe_predictions_to_df(res["predictions"]),
                StdoutWrapper(res["stdout"]),
            )

    def get_table(
            self,
            offset: int,
            size: int,
            reverse_order: bool) -> pd.DataFrame:
        resp = self._client._request_bytes(
            METHOD_POST, "/job_data", {
                "job": self._job_id,
                "format": "csv",
                "offset": offset,
                "size": size,
                "reverse_order": reverse_order,
            })
        return pd.read_csv(resp)

    def get_predictions_table(
            self,
            method: Optional[str],
            offset: int,
            size: int,
            reverse_order: bool = False) -> Optional[pd.DataFrame]:
        resp = self._client._request_bytes(
            METHOD_GET, "/predictions", {
                "job": self._job_id,
                "format": "csv",
                "method": method,
                "last_n": size,
                "offset": offset,
                "reverse_order": reverse_order,
            })
        return pd.read_csv(resp)

    def get_predictions(
            self,
            method: Optional[str],
            ticker: Optional[str],
            date: Optional[str],
            last_n: int,
            filters: Optional[Dict[str, Any]]) -> Optional[pd.DataFrame]:
        if filters is None:
            filters = {}
        res = cast(SimplePredictionsResponse, self._client._request_json(
            METHOD_LONGPOST, "/predictions", {
                "job": self._job_id,
                "method": method,
                "ticker": ticker,
                "date": date,
                "last_n": last_n,
                "filters": filters,
            }, capture_err=False))
        columns = res.get("columns", None)
        predictions = res.get("predictions", None)
        if columns is not None and predictions is not None:
            return pd.DataFrame(predictions, columns=columns)
        return None

    def share(self, path: str) -> 'JobHandle':
        shared = cast(ShareResponse, self._client._request_json(
            METHOD_PUT, "/share", {
                "job": self._job_id,
                "with": path,
            }, capture_err=True))
        return JobHandle(
            client=self._client,
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

    def create_source(
            self,
            source_type: str,
            name: Optional[str] = None) -> 'SourceHandle':
        res = self._client._raw_create_source(
            source_type, name, self, None, None)
        return res["source"]

    def get_sources(self) -> List['SourceHandle']:
        schema_obj = self.get_schema()
        x_obj = schema_obj.get("X", {})
        res = []
        main_source_id = x_obj.get("source", None)
        if main_source_id:
            res.append(SourceHandle(
                self._client, main_source_id, None, infer_type=True))
        for progress in x_obj.get("source_progress", []):
            cur_source_ix = progress.get("source", None)
            if cur_source_ix:
                res.append(SourceHandle(
                    self._client, cur_source_ix, None, infer_type=True))
        return res

    def get_fast_segments(self) -> List[
            Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]]:
        schema_obj = self.get_schema()
        x_obj = schema_obj.get("X", {})
        res: List[Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]] = \
            [(None, None)]
        res.extend((
            maybe_timestamp(progress.get("start_time")),
            maybe_timestamp(progress.get("end_time")),
        ) for progress in x_obj.get("source_progress", []))
        return res

    def get_segments(
            self,
            ticker: Optional[str] = None) -> List[Tuple[
                int, Optional[pd.Timestamp], Optional[pd.Timestamp]]]:
        res = cast(SegmentResponse, self._client._request_json(
            METHOD_LONGPOST, "/segments", {
                "job": self._job_id,
                "ticker": ticker,
            }, capture_err=False))
        return [
            (rows, maybe_timestamp(start_time), maybe_timestamp(end_time))
            for (rows, start_time, end_time) in res.get("segments", [])
        ]

    def get_main_source(self, ticker: Optional[str] = None) -> 'SourceHandle':
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

    def set_main_source(self, source: 'SourceHandle') -> None:
        with self.update_schema() as obj:
            x_obj = obj.get("X", {})
            x_obj["source"] = source.get_source_id()
            obj["X"] = x_obj

    def append_source(
            self,
            source: 'SourceHandle',
            start_time: Optional[Union[str, pd.Timestamp]],
            end_time: Optional[Union[str, pd.Timestamp]],
            change_set: Optional[Dict[str, Any]] = None) -> None:
        with self.update_schema() as obj:
            x_obj = obj.get("X", {})
            progress = x_obj.get("source_progress", [])
            progress.append({
                "source": source.get_source_id(),
                "start_time": None if start_time is None else f"{start_time}",
                "end_time": None if end_time is None else f"{end_time}",
                "change_set": {} if change_set is None else change_set,
            })
            x_obj["source_progress"] = progress
            obj["X"] = x_obj

    def inspect(self, ticker: Optional[str]) -> 'InspectHandle':
        return InspectHandle(self._client, self, ticker)

    def get_metrics(self, kind: str = PLOT_PKL) -> MetricListInfo:
        res = cast(MetricListResponse, self._client._request_json(
            METHOD_LONGPOST, "/metric_plots", {
                "job": self._job_id,
                "kind": kind,
            }, capture_err=False))
        return {
            "metrics": res["metrics"],
            "selected_plots": res["selectedPlots"],
            "hidden_plots": res["hiddenPlots"],
        }

    def get_data_plots(self) -> DataPlotListInfo:
        res = cast(DataPlotListResponse, self._client._request_json(
            METHOD_LONGPOST, "/data_plots", {
                "job": self._job_id,
            }, capture_err=False))
        return {
            "data_plots": res["dataPlots"],
            "selected_plots": res["selectedPlots"],
            "hidden_plots": res["hiddenPlots"],
        }

    def select_plots(
            self,
            plots: List[List[str]],
            plot_type: str) -> List[List[str]]:
        res = cast(SelectPlotsResponse, self._client._request_json(
            METHOD_PUT, "/select_plots", {
                "job": self._job_id,
                "selectedPlots": plots,
                "type": plot_type,
            }, capture_err=False))
        return res["selectedPlots"]

    def hide_plot(self, plot: List[str], plot_type: str, hide: bool) -> bool:
        return cast(HidePlotResponse, self._client._request_json(
            METHOD_PUT, "/hide_plot", {
                "hide": hide,
                "job": self._job_id,
                "plot": plot,
                "type": plot_type,
            }, capture_err=True)).get("success", False)

    def get_metric(
            self,
            metric: str,
            ticker: Optional[str],
            kind: str = PLOT_PKL) -> Optional[MetricWrapper]:
        res = cast(MetricResponse, self._client._request_json(
            METHOD_LONGPOST, "/metric", {
                "job": self._job_id,
                "kind": kind,
                "metric": metric,
                "ticker": ticker,
            }, capture_err=False))
        plot = res["lines"]
        if not plot:
            return None
        if plot["kind"] == "time":
            return MetricPlot(plot)
        if plot["kind"] == "coord":
            return MetricCoords(plot)
        raise ValueError(f"invalid plot kind: {plot['kind']}")

    def _raw_stdout(
            self,
            ticker: Optional[str],
            do_filter: bool,
            query_str: Optional[str],
            pos: Optional[int],
            context: int,
            before: Optional[int],
            after: Optional[int]) -> List[StdoutLine]:
        res = cast(JobStdoutResponse, self._client._request_json(
            METHOD_POST, "/stdout", {
                "job": self._job_id,
                "ticker": ticker,
                "filter": do_filter,
                "query": query_str,
                "pos": pos,
                "context": context,
                "before": before,
                "after": after,
            }, capture_err=False))
        return res["lines"]

    def get_logs(self) -> StdoutWrapper:
        return StdoutWrapper({
            "lines": self._raw_stdout(None, False, None, None, 0, None, None),
            "messages": {},
            "exceptions": [],
        })

    def get_summary(self, ticker: Optional[str]) -> SummaryInfo:
        res = cast(SummaryResponse, self._client._request_json(
            METHOD_LONGPOST, "/summary", {
                "job": self._job_id,
                "ticker": ticker,
            }, capture_err=False))
        messages = res.get("messages")
        exceptions = res.get("exceptions")
        stdout = StdoutWrapper({
            "messages": {} if messages is None else messages,
            "exceptions": [] if exceptions is None else exceptions,
            "lines": [],
        })
        data_start = res.get("dataStart")
        data_high = res.get("dataHigh")
        data_end = res.get("dataEnd")
        return {
            "stdout": stdout,
            "last_event": res.get("lastEvent"),
            "rows": res.get("rows"),
            "rows_total": res.get("rowsTotal"),
            "features": res.get("features"),
            "dropped_features": res.get("droppedFeatures"),
            "data_start": maybe_timestamp(data_start),
            "data_high": maybe_timestamp(data_high),
            "data_end": maybe_timestamp(data_end),
        }

    def get_backtest(
            self,
            strategy: Optional[str],
            base_strategy: Optional[str],
            price_source: Optional['SourceHandle'],
            prediction_feature: Optional[str],
            ticker: Optional[str] = None,
                ) -> Tuple[Optional[Dict[str, Any]], StdoutWrapper]:
        res = cast(BacktestResponse, self._client._request_json(
            METHOD_LONGPOST, "/summary", {
                "job": self._job_id,
                "ticker": ticker,
                "predictionFeature": prediction_feature,
                "strategy": strategy,
                "baseStrategy": base_strategy,
                "priceSourceId": price_source,
            }, capture_err=False))
        output = res.get("output", [])
        stdout = StdoutWrapper({
            "lines": output if output is not None else [],
            "messages": {},
            "exceptions": [],
        })
        if res.get("errMessage", None):
            raise ValueError(res["errMessage"], stdout)
        return res.get("pyfolio", None), stdout

    def get_columns(self, ticker: Optional[str]) -> List[str]:
        res = cast(JobColumnsResponse, self._client._request_json(
            METHOD_POST, "/job_columns", {
                "job": self._job_id,
                "ticker": ticker,
            }, capture_err=True))
        return res["columns"]

    def get_data(
            self, ticker: Optional[str], columns: List[str]) -> pd.DataFrame:
        resp = self._client._request_bytes(
            METHOD_POST, "/job_data", {
                "job": self._job_id,
                "ticker": ticker,
                "columns": columns,
                "format": "csv",
            })
        return pd.read_csv(resp)

    def force_flush(self) -> None:
        res = cast(ForceFlushResponse, self._client._request_json(
            METHOD_PUT, "/force_flush", {
                "job": self._job_id,
            }, capture_err=False))
        if not res["success"]:
            raise AccessDenied(f"cannot access job {self._job_id}")
        self.refresh()

    def create_backup(self) -> 'JobHandle':
        res = cast(JobBackupResponse, self._client._request_json(
            METHOD_LONGPOST, "/backup_job", {
                "job": self._job_id,
            }, capture_err=True))
        return JobHandle(
            client=self._client,
            job_id=res["jobId"],
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

    def __repr__(self) -> str:
        name = ""
        if self._name is not None:
            name = f" ({self._name})"
        return f"{type(self).__name__}: {self._job_id}{name}"

    def __str__(self) -> str:
        return repr(self)

# *** JobHandle ***


InspectValue = Union['InspectPath', bool, int, float, str, None]


class InspectPath(collections.abc.Mapping):
    def __init__(
            self,
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

# *** InspectPath ***


class InspectHandle(InspectPath):
    def __init__(
            self,
            client: XYMELegacyClient,
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

        def maybe_set_cache(
                key: str,
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

# *** InspectHandle ***


class SourceHandle:
    def __init__(
            self,
            client: XYMELegacyClient,
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

    def _ensure_csv_source(self) -> None:
        if not self.is_csv_source():
            raise ValueError("can only set input for csv source")

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
    def bulk_operation(self) -> Iterator[bool]:
        with self._client.bulk_operation() as do_refresh:
            if do_refresh:
                self.refresh()
            yield do_refresh

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

    def is_csv_source(self) -> bool:
        return self.get_source_type() == SOURCE_TYPE_CSV

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

    def add_new_source_file(
            self,
            filename: str,
            ticker_column: str,
            date_column: Optional[str],
            progress_bar: Optional[IO[Any]] = sys.stdout,
                ) -> 'SourceHandle':
        with self.bulk_operation():
            source = self.add_new_source(
                SOURCE_TYPE_CSV, os.path.basename(filename))
            source.set_input_file(
                filename, ticker_column, date_column, progress_bar)
            return source

    def add_new_source_df(
            self,
            df: pd.DataFrame,
            name: str,
            ticker_column: str,
            date_column: Optional[str],
            progress_bar: Optional[IO[Any]] = sys.stdout,
                ) -> 'SourceHandle':
        with self.bulk_operation():
            source = self.add_new_source(SOURCE_TYPE_CSV, name)
            source.set_input_df(
                df, name, ticker_column, date_column, progress_bar)
            return source

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

    def set_input(
            self,
            input_obj: 'InputHandle',
            ticker_column: Optional[str],
            date_column: Optional[str]) -> None:
        with self.bulk_operation():
            self._ensure_csv_source()
            with self.update_schema() as schema_obj:
                with input_obj.bulk_operation():
                    if "config" not in schema_obj:
                        schema_obj["config"] = {}
                    config = schema_obj["config"]
                    ext = input_obj.get_extension()
                    if ext is None:
                        ext = "csv"  # should be correct in 99% of the cases
                    config["filename"] = f"{input_obj.get_input_id()}.{ext}"
                    if ticker_column is not None:
                        config["ticker"] = ticker_column
                    if date_column is not None:
                        config["date"] = date_column

    def set_input_file(
            self,
            filename: str,
            ticker_column: str,
            date_column: Optional[str],
            progress_bar: Optional[IO[Any]] = sys.stdout) -> None:
        with self.bulk_operation():
            self._ensure_csv_source()
            input_obj = self._client.input_from_file(filename, progress_bar)
            self.set_input(input_obj, ticker_column, date_column)

    def set_input_df(
            self,
            df: pd.DataFrame,
            name: str,
            ticker_column: str,
            date_column: Optional[str],
            progress_bar: Optional[IO[Any]] = sys.stdout) -> None:
        with self.bulk_operation():
            self._ensure_csv_source()
            input_obj = self._client.input_from_df(df, name, progress_bar)
            self.set_input(input_obj, ticker_column, date_column)

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

    def get_input(self) -> Optional['InputHandle']:
        with self.bulk_operation():
            self._ensure_csv_source()
            if self._schema_obj is None:
                self._fetch_info()
            assert self._schema_obj is not None
            fname = self._schema_obj.get("config", {}).get("filename", None)
            if fname is None:
                return None
            dot = fname.find(".")
            input_id = fname[:dot] if dot >= 0 else fname
            return InputHandle(self._client, input_id)

    def __repr__(self) -> str:
        name = ""
        if self._name is not None:
            name = f" ({self._name})"
        return f"{type(self).__name__}: {self._source_id}{name}"

    def __str__(self) -> str:
        return repr(self)

# *** SourceHandle ***


class InputHandle:
    def __init__(
            self,
            client: XYMELegacyClient,
            input_id: str,
            name: Optional[str] = None,
            path: Optional[str] = None,
            ext: Optional[str] = None,
            size: Optional[int] = None,
            progress: Optional[str] = None) -> None:
        if not input_id:
            raise ValueError("input id is not set!")
        self._client = client
        self._input_id = input_id
        self._name = name
        self._ext = ext
        self._size = size
        self._path = path
        self._progress = progress
        self._last_byte_offset: Optional[int] = None

    def refresh(self) -> None:
        self._name = None
        self._path = None
        self._ext = None
        self._progress = None
        self._last_byte_offset = None
        self._size = None

    def _maybe_refresh(self) -> None:
        if self._client.is_auto_refresh():
            self.refresh()

    def _fetch_info(self) -> None:
        res = cast(InputDetailsResponse, self._client._request_json(
            METHOD_GET, "/input_details", {
                "inputId": self._input_id,
            }, capture_err=False))
        self._input_id = res["inputId"]
        self._name = res["name"]
        self._path = res["path"]
        self._ext = res["extension"]
        self._last_byte_offset = res["lastByteOffset"]
        self._size = res["size"]
        self._progress = res["progress"]
        if self._progress is None:
            self._progress = "unknown"

    def get_input_id(self) -> str:
        return self._input_id

    def get_name(self) -> Optional[str]:
        self._maybe_refresh()
        if self._name is None:
            self._fetch_info()
        return self._name

    def get_path(self) -> str:
        self._maybe_refresh()
        if self._path is None:
            self._fetch_info()
        assert self._path is not None
        return self._path

    def get_extension(self) -> Optional[str]:
        self._maybe_refresh()
        if self._ext is None:
            self._fetch_info()
        return self._ext

    def get_last_byte_offset(self) -> Optional[int]:
        self._maybe_refresh()
        if self._last_byte_offset is None:
            self._fetch_info()
        return self._last_byte_offset

    def get_size(self) -> Optional[int]:
        self._maybe_refresh()
        if self._size is None:
            self._fetch_info()
        return self._size

    def get_progress(self) -> str:
        self._maybe_refresh()
        if self._progress is None:
            self._fetch_info()
        assert self._progress is not None
        return self._progress

    def is_complete(self) -> bool:
        return self.get_progress() == UPLOAD_DONE

    @contextlib.contextmanager
    def bulk_operation(self) -> Iterator[bool]:
        with self._client.bulk_operation() as do_refresh:
            if do_refresh:
                self.refresh()
            yield do_refresh

    def upload_partial(self, chunk_content: IO[bytes]) -> bool:
        if self._last_byte_offset is None or self._name is None:
            self._fetch_info()
            if self._last_byte_offset is None:
                self._last_byte_offset = 0
            if self._name is None:
                self._name = "unnamed file"
        res = cast(UploadResponse, self._client._request_json(
            METHOD_FILE, "/upload", {
                "inputId": self._input_id,
                "chunkByteOffset": self._last_byte_offset,
                "type": "input",
                "name": self._name,
            }, capture_err=True, files={
                "file": chunk_content,
            }))
        self._input_id = res["inputId"]
        self._name = res["name"]
        self._path = res["path"]
        self._ext = res["extension"]
        self._last_byte_offset = res["lastByteOffset"]
        self._size = res["size"]
        self._progress = res["progress"]
        return res["exists"]

    def upload_full(
            self,
            file_content: IO[bytes],
            file_name: Optional[str],
            progress_bar: Optional[IO[Any]] = sys.stdout) -> int:
        if file_name is not None:
            self._name = file_name
        total_size: Optional[int] = None
        if progress_bar is not None:
            if hasattr(file_content, "seek"):
                init_pos = file_content.seek(0, io.SEEK_CUR)
                total_size = file_content.seek(0, io.SEEK_END) - init_pos
                file_content.seek(init_pos, io.SEEK_SET)
                end = ":"
            else:
                end = "."
            if file_name is None:
                msg = f"Uploading unnamed file{end}\n"
            else:
                msg = f"Uploading file {file_name}{end}\n"
            progress_bar.write(msg)
        print_progress = get_progress_bar(out=progress_bar)
        cur_size = 0
        while True:
            if total_size is not None:
                print_progress(cur_size / total_size, False)
            buff = file_content.read(get_file_upload_chunk_size())
            if not buff:
                break
            cur_size += len(buff)
            if self.upload_partial(BytesIO(buff)):
                break
        if total_size is not None:
            print_progress(cur_size / total_size, True)
        return cur_size

    def __repr__(self) -> str:
        name = ""
        if self._name is not None:
            name = f" ({self._name})"
        return f"{type(self).__name__}: {self._input_id}{name}"

    def __str__(self) -> str:
        return repr(self)

# *** InputHandle ***


def create_legacy_client(
        url: str,
        user: Optional[str] = None,
        password: Optional[str] = None,
        token: Optional[str] = None) -> XYMELegacyClient:
    return XYMELegacyClient(url, user, password, token)


@contextlib.contextmanager
def create_legacy_session(
        url: str,
        user: Optional[str] = None,
        password: Optional[str] = None,
        token: Optional[str] = None) -> Iterator[XYMELegacyClient]:
    client = None
    try:
        client = XYMELegacyClient(url, user, password, token)
        yield client
    finally:
        if client is not None:
            client.logout()
