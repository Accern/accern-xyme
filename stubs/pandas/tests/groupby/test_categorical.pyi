# Stubs for pandas.tests.groupby.test_categorical (Python 3)
#
# NOTE: This dynamically typed stub was automatically generated by stubgen.
# pylint: disable=unused-argument,redefined-outer-name,no-self-use,invalid-name
# pylint: disable=relative-beyond-top-level

from typing import Any

def cartesian_product_for_groupers(result: Any, args: Any, names: Any) -> Any:
    ...


def test_apply_use_categorical_name(df: Any) -> None:
    ...


def test_basic() -> None:
    ...


def test_level_get_group(observed: Any) -> None:
    ...


def test_apply(ordered: Any) -> None:
    ...


def test_observed(observed: Any) -> None:
    ...


def test_observed_codes_remap(observed: Any) -> None:
    ...


def test_observed_perf() -> None:
    ...


def test_observed_groups(observed: Any) -> None:
    ...


def test_observed_groups_with_nan(observed: Any) -> None:
    ...


def test_dataframe_categorical_with_nan(observed: Any) -> None:
    ...


def test_dataframe_categorical_ordered_observed_sort(
        ordered: Any, observed: Any, sort: Any) -> None:
    ...


def test_datetime() -> None:
    ...


def test_categorical_index() -> None:
    ...


def test_describe_categorical_columns() -> None:
    ...


def test_unstack_categorical() -> None:
    ...


def test_bins_unequal_len() -> None:
    ...


def test_as_index():
    ...


def test_preserve_categories() -> None:
    ...


def test_preserve_categorical_dtype() -> None:
    ...


def test_preserve_on_ordered_ops(func: Any, values: Any) -> None:
    ...


def test_categorical_no_compress() -> None:
    ...


def test_sort():
    ...


def test_sort2() -> None:
    ...


def test_sort_datetimelike() -> None:
    ...


def test_empty_sum() -> None:
    ...


def test_empty_prod() -> None:
    ...


def test_groupby_multiindex_categorical_datetime() -> None:
    ...


def test_groupby_agg_observed_true_single_column(
        as_index: Any, expected: Any) -> None:
    ...


def test_shift(fill_value: Any) -> None:
    ...


def df_cat(df: Any) -> Any:
    ...


def test_seriesgroupby_observed_true(
        df_cat: Any, operation: Any, kwargs: Any) -> None:
    ...


def test_seriesgroupby_observed_false_or_none(
        df_cat: Any, observed: Any, operation: Any) -> None:
    ...


def test_seriesgroupby_observed_apply_dict(
        df_cat: Any, observed: Any, index: Any, data: Any) -> None:
    ...
