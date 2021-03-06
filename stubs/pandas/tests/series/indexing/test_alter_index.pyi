# Stubs for pandas.tests.series.indexing.test_alter_index (Python 3)
#
# NOTE: This dynamically typed stub was automatically generated by stubgen.
# pylint: disable=unused-argument,redefined-outer-name,no-self-use,invalid-name
# pylint: disable=relative-beyond-top-level,line-too-long,arguments-differ
# pylint: disable=no-member,too-few-public-methods,keyword-arg-before-vararg
# pylint: disable=super-init-not-called,abstract-method,redefined-builtin

from typing import Any


def test_align(test_data: Any, first_slice: Any, second_slice: Any, join_type: Any, fill: Any) -> None:
    ...


def test_align_fill_method(test_data: Any, first_slice: Any, second_slice: Any, join_type: Any, method: Any, limit: Any) -> None:
    ...


def test_align_nocopy(test_data: Any) -> None:
    ...


def test_align_same_index(test_data: Any) -> None:
    ...


def test_align_multiindex() -> None:
    ...


def test_reindex(test_data: Any) -> None:
    ...


def test_reindex_nan() -> None:
    ...


def test_reindex_series_add_nat() -> None:
    ...


def test_reindex_with_datetimes() -> None:
    ...


def test_reindex_corner(test_data: Any) -> None:
    ...


def test_reindex_pad() -> None:
    ...


def test_reindex_nearest() -> None:
    ...


def test_reindex_backfill() -> None:
    ...


def test_reindex_int(test_data: Any) -> None:
    ...


def test_reindex_bool(test_data: Any) -> None:
    ...


def test_reindex_bool_pad(test_data: Any) -> None:
    ...


def test_reindex_categorical() -> None:
    ...


def test_reindex_like(test_data: Any) -> None:
    ...


def test_reindex_fill_value() -> None:
    ...


def test_reindex_datetimeindexes_tz_naive_and_aware() -> None:
    ...


def test_reindex_empty_series_tz_dtype() -> None:
    ...


def test_rename():
    ...


def test_drop_unique_and_non_unique_index(data: Any, index: Any, axis: Any, drop_labels: Any, expected_data: Any, expected_index: Any) -> None:
    ...


def test_drop_exception_raised(data: Any, index: Any, drop_labels: Any, axis: Any, error_type: Any, error_desc: Any) -> None:
    ...


def test_drop_with_ignore_errors() -> None:
    ...


def test_drop_empty_list(index: Any, drop_labels: Any) -> None:
    ...


def test_drop_non_empty_list(data: Any, index: Any, drop_labels: Any) -> None:
    ...
