# Stubs for pandas.core.sparse.series (Python 3)
#
# NOTE: This dynamically typed stub was automatically generated by stubgen.
# pylint: disable=unused-argument

from pandas.core.series import Series
from typing import Any, Optional

depr_msg: str


class SparseSeries(Series):
    def __init__(self, data: Optional[Any] = ..., index: Optional[Any] = ...,
                 sparse_index: Optional[Any] = ..., kind: str = ...,
                 fill_value: Optional[Any] = ..., name: Optional[Any] = ...,
                 dtype: Optional[Any] = ..., copy: bool = ...,
                 fastpath: bool = ...) -> None:
        ...

    def __array_ufunc__(self, ufunc: Any, method: Any, *inputs: Any,
                        **kwargs: Any) -> Any:
        ...

    def __pos__(self) -> Any:
        ...

    def __neg__(self) -> Any:
        ...

    def __invert__(self) -> Any:
        ...

    @property
    def block(self) -> Any:
        ...

    @property
    def fill_value(self) -> Any:
        ...

    @fill_value.setter
    def fill_value(self, v: Any) -> None:
        ...

    @property
    def sp_index(self) -> Any:
        ...

    @property
    def sp_values(self) -> Any:
        ...

    @property
    def npoints(self) -> Any:
        ...

    @classmethod
    def from_array(cls, arr: Any, index: Optional[Any] = ...,
                   name: Optional[Any] = ..., copy: bool = ...,
                   fill_value: Optional[Any] = ...,
                   fastpath: bool = ...) -> Any:
        ...

    @property
    def kind(self) -> Any:
        ...

    def as_sparse_array(self, kind: Optional[Any] = ...,
                        fill_value: Optional[Any] = ...,
                        copy: bool = ...) -> Any:
        ...

    def __getitem__(self, key: Any) -> Any:
        ...

    def abs(self) -> Any:
        ...

    def get(self, label: Any, default: Optional[Any] = ...) -> Any:
        ...

    def get_value(self, label: Any, takeable: bool = ...) -> Any:
        ...

    def set_value(self, label: Any, value: Any, takeable: bool = ...) -> Any:
        ...

    def to_dense(self) -> Any:
        ...

    @property
    def density(self) -> Any:
        ...

    def copy(self, deep: bool = ...) -> Any:
        ...

    def reindex(self, index: Optional[Any] = ..., method: Optional[Any] = ...,
                copy: bool = ..., limit: Optional[Any] = ...,
                **kwargs: Any) -> Any:
        ...

    def sparse_reindex(self, new_index: Any) -> Any:
        ...

    def cumsum(self, axis: int = ..., *args: Any, **kwargs: Any) -> Any:
        ...

    def isna(self) -> Any:
        ...

    isnull: Any = ...

    def notna(self) -> Any:
        ...

    notnull: Any = ...

    def dropna(self, axis: int = ..., inplace: bool = ...,
               **kwargs: Any) -> Any:
        ...


    def combine_first(self, other: Any) -> Any:
        ...


    def to_coo(self, row_levels: Any = ..., column_levels: Any = ...,
               sort_labels: bool = ...) -> Any:
        ...


    @classmethod
    def from_coo(cls, A: Any, dense_index: bool = ...) -> Any:
        ...
