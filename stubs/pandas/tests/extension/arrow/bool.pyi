# Stubs for pandas.tests.extension.arrow.bool (Python 3)
#
# NOTE: This dynamically typed stub was automatically generated by stubgen.
# pylint: disable=unused-argument,redefined-outer-name,no-self-use,invalid-name
# pylint: disable=relative-beyond-top-level,abstract-method

from typing import Any, Optional
from pandas.api.extensions import ExtensionArray, ExtensionDtype


class ArrowBoolDtype(ExtensionDtype):
    type: Any = ...
    kind: str = ...
    name: str = ...
    na_value: Any = ...

    @classmethod
    def construct_from_string(cls, string: Any) -> Any:
        ...

    @classmethod
    def construct_array_type(cls) -> Any:
        ...


class ArrowBoolArray(ExtensionArray):
    def __init__(self, values: Any) -> None:
        ...

    @classmethod
    def from_scalars(cls, values: Any) -> Any:
        ...

    @classmethod
    def from_array(cls, arr: Any) -> Any:
        ...

    def __getitem__(self, item: Any) -> Any:
        ...

    def __len__(self) -> int:
        ...

    def astype(self, dtype: Any, copy: bool = ...) -> Any:
        ...

    @property
    def dtype(self) -> Any:
        ...

    @property
    def nbytes(self) -> Any:
        ...

    def isna(self) -> Any:
        ...

    def take(
            self, indices: Any, allow_fill: bool = ...,
            fill_value: Optional[Any] = ...) -> Any:
        ...

    def copy(self) -> Any:
        ...

    def __invert__(self) -> Any:
        ...

    def any(self, axis: int = ..., out: Optional[Any] = ...) -> Any:
        ...

    def all(self, axis: int = ..., out: Optional[Any] = ...) -> Any:
        ...
