from typing import Any, cast, Dict, Optional
import json
import time
import urllib
import requests
from typing_extensions import TypedDict
from quick_server.worker_request import worker_request


__version__ = "0.0.1"


METHOD_GET = "GET"
METHOD_POST = "POST"
METHOD_LONGPOST = "LONGPOST"

PREFIX = "/xyme"


VersionInfo = TypedDict('VersionInfo', {
    "version": str,
    "xymeVersion": str,
    "time": str,
})


class XYMEClient:
    def __init__(self, url: str, user: str, password: str) -> None:
        self._url = url.rstrip("/")
        self._user = user
        self._password = password
        self._token: Optional[str] = None
        self._last_action = time.monotonic()

    def _raw_request_json(self,
                          method: str,
                          path: str,
                          args: Dict[str, Any],
                          add_prefix: bool = True) -> Dict[str, Any]:
        url = f"{self._url}{PREFIX if add_prefix else ''}{path}"
        if method == METHOD_GET:
            args_str = ""
            for (key, value) in args.items():
                if args_str:
                    args_str += "&"
                else:
                    args_str += "?"
                args_str += (f"{urllib.parse.quote(key)}="
                             f"{urllib.parse.quote(value)}")
            req = requests.get(f"{url}{args_str}")
            if req.status_code == 200:
                return json.loads(req.text)
            raise ValueError(
                f"error {req.status_code} in worker request:\n{req.text}")
        elif method == METHOD_POST:
            req = requests.post(url, headers={
                "Content-Type": "application/json",
            }, data=json.dumps(args))
            if req.status_code == 200:
                return json.loads(req.text)
            raise ValueError(
                f"error {req.status_code} in worker request:\n{req.text}")
        elif method == METHOD_LONGPOST:
            return worker_request(url, args)
        else:
            raise ValueError(f"unknown method {method}")

    def get_server_version(self) -> VersionInfo:
        return cast(VersionInfo,
                    self._raw_request_json(METHOD_GET, "/version", {}))


def create_xyme_client(url: str, user: str, password: str) -> XYMEClient:
    return XYMEClient(url, user, password)
