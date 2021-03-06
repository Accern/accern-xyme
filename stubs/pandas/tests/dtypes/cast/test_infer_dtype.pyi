# Stubs for pandas.tests.dtypes.cast.test_infer_dtype (Python 3)
#
# NOTE: This dynamically typed stub was automatically generated by stubgen.
# pylint: disable=unused-argument,redefined-outer-name

from typing import Any


def pandas_dtype(request: Any) -> Any:
    ...


def test_infer_dtype_from_int_scalar(any_int_dtype: Any) -> None:
    ...


def test_infer_dtype_from_float_scalar(float_dtype: Any) -> None:
    ...


def test_infer_dtype_from_python_scalar(data: Any, exp_dtype: Any) -> None:
    ...


def test_infer_dtype_from_boolean(bool_val: Any) -> None:
    ...


def test_infer_dtype_from_complex(complex_dtype: Any) -> None:
    ...


def test_infer_dtype_from_datetime(data: Any) -> None:
    ...


def test_infer_dtype_from_timedelta(data: Any) -> None:
    ...


def test_infer_dtype_from_period(freq: Any, pandas_dtype: Any) -> None:
    ...


def test_infer_dtype_misc(data: Any) -> None:
    ...


def test_infer_from_scalar_tz(tz: Any, pandas_dtype: Any) -> None:
    ...


def test_infer_dtype_from_scalar_errors() -> None:
    ...


def test_infer_dtype_from_array(
        arr: Any, expected: Any, pandas_dtype: Any) -> None:
    ...


def test_cast_scalar_to_array(obj: Any, dtype: Any) -> None:
    ...
