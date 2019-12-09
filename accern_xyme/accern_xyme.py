from typing import Any, cast, Dict, List, Optional
import os
import json
import time
import urllib
import requests
from typing_extensions import TypedDict
import quick_server


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
UserLogin = TypedDict('UserLogin', {
    "token": str,
    "success": bool,
    "permissions": List[str],
})
UserInfo = TypedDict('UserInfo', {
    "username": str,
    "company": str,
    "hasLogout": bool,
    "path": str,
})


class AccessDenied(Exception):
    pass


class XYMEClient:
    def __init__(self,
                 url: str,
                 user: Optional[str],
                 password: Optional[str]) -> None:
        self._url = url.rstrip("/")
        if user is None:
            user = os.environ["ACCERN_USER"]
        self._user: str = user
        if password is None:
            password = os.environ["ACCERN_PASSWORD"]
        self._password: str = password
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
            if req.status_code == 403:
                raise AccessDenied()
            if req.status_code == 200:
                return json.loads(req.text)
            raise ValueError(
                f"error {req.status_code} in worker request:\n{req.text}")
        elif method == METHOD_POST:
            req = requests.post(url, headers={
                "Content-Type": "application/json",
            }, data=json.dumps(args))
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
                      add_prefix: bool = True) -> Dict[str, Any]:
        if self._token is None:
            self._login()
        try:
            args["token"] = self._token
            return self._raw_request_json(method, path, args, add_prefix)
        except AccessDenied:
            self._login()
            args["token"] = self._token
            return self._raw_request_json(method, path, args, add_prefix)

    def get_user_info(self) -> UserInfo:
        return cast(UserInfo,
                    self._request_json(METHOD_POST, "/username", {}))

    def get_server_version(self) -> VersionInfo:
        return cast(VersionInfo,
                    self._raw_request_json(METHOD_GET, "/version", {}))


def create_xyme_client(url: str, user: str, password: str) -> XYMEClient:
    return XYMEClient(url, user, password)
