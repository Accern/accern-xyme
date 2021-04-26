# pylint: disable=unused-argument,no-self-use
from typing import Any, Optional


class Source:
    def __init__(
            self,
            filename: Optional[Any] = None,
            directory: Optional[str] = None,
            engine: Optional[str] = None,
            encoding: Optional[str] = "utf-8") -> None:
        ...
