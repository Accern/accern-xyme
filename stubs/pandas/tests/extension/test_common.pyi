# Stubs for pandas.tests.extension.test_common (Python 3)
#
# NOTE: This dynamically typed stub was automatically generated by stubgen.
# pylint: disable=unused-argument,redefined-outer-name,no-self-use,invalid-name
# pylint: disable=relative-beyond-top-level,line-too-long,arguments-differ
# pylint: disable=no-member,too-few-public-methods,keyword-arg-before-vararg
# pylint: disable=super-init-not-called,abstract-method,redefined-builtin
# pylint: disable=unused-import,useless-import-alias,signature-differs
# pylint: disable=blacklisted-name,c-extension-no-member,too-many-ancestors

from typing import Any
from pandas.core.arrays import ExtensionArray
from pandas.core.dtypes import dtypes

class DummyDtype(dtypes.ExtensionDtype):
    ...


class DummyArray(ExtensionArray):
    data: Any = ...
    def __init__(self, data: Any) -> None:
        ...

    def __array__(self, dtype: Any) -> Any:
        ...

    @property
    def dtype(self):
        ...

    def astype(self, dtype: Any, copy: bool = ...) -> Any:
        ...


class TestExtensionArrayDtype:
    def test_is_extension_array_dtype(self, values: Any) -> None:
        ...

    def test_is_not_extension_array_dtype(self, values: Any) -> None:
        ...


def test_astype() -> None:
    ...

def test_astype_no_copy() -> None:
    ...

def test_is_extension_array_dtype(dtype: Any) -> None:
    ...
