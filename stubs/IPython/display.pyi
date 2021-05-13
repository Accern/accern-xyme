# pylint: disable=unused-argument,no-self-use
from typing import Any, Optional, List, Dict


class ProgressBar:
    def __init__(self, total: int) -> None:
        ...

    def display(self) -> None:
        ...

    def update(self) -> None:
        ...

    def _set_progress(self, progress: int) -> None:
        ...

    def _get_progress(self) -> int:
        ...

    progress = property(_get_progress, _set_progress)


class SVG:
    def __init__(
            self,
            data: Optional[Any] = None,
            url: Optional[str] = None,
            filename: Optional[str] = None,
            metadata: Optional[Dict[str, Any]] = None) -> None:
        ...


class Image:
    def __init__(
            self,
            data: Optional[Any] = None,
            url: Optional[str] = None,
            filename: Optional[str] = None,
            format: Optional[str] = None,
            embed: Optional[bool] = None,
            width: Optional[int] = None,
            height: Optional[int] = None,
            retina: bool = False,
            unconfined: bool = False,
            metadata: Optional[Dict[str, Any]] = None) -> None:
        ...


def display(
        *objs: Any,
        include: Optional[List[Any]] = None,
        exclude: Optional[List[Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        transient: Optional[Dict[str, Any]] = None,
        display_id: Optional[List[Any]] = None,
        **kwargs: Any) -> None:
    ...
