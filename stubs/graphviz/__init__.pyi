# pylint: disable=unused-argument,no-self-use,redefined-builtin
from typing import Any, Optional


class Source:
    def __init__(
            self,
            filename: Optional[Any] = None,
            format: Optional[str] = None,
            directory: Optional[str] = None,
            engine: Optional[str] = None,
            encoding: Optional[str] = "utf-8") -> None:
        ...

    def pipe(
            self,
            format: Optional[str] = None,
            renderer: Optional[Any] = None,
            formatter: Optional[Any] = None,
            quiet: bool = False) -> Any:
        ...
