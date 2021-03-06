# Stubs for pandas.tests.reshape.merge.test_merge (Python 3)
#
# NOTE: This dynamically typed stub was automatically generated by stubgen.
# pylint: disable=unused-argument,redefined-outer-name,no-self-use,invalid-name
# pylint: disable=relative-beyond-top-level,line-too-long,arguments-differ
# pylint: disable=no-member,too-few-public-methods,keyword-arg-before-vararg
# pylint: disable=super-init-not-called,abstract-method,redefined-builtin

from typing import Any

N: int
NGROUPS: int

def get_test_data(ngroups: Any = ..., n: Any = ...) -> Any:
    ...

def get_series() -> Any:
    ...

def get_series_na() -> Any:
    ...

def series_of_dtype(request: Any) -> Any:
    ...

def series_of_dtype2(request: Any) -> Any:
    ...

def series_of_dtype_all_na(request: Any) -> Any:
    ...


class TestMerge:
    df: Any = ...
    df2: Any = ...
    left: Any = ...
    right: Any = ...
    def setup_method(self, method: Any) -> None:
        ...

    def test_merge_inner_join_empty(self) -> None:
        ...

    def test_merge_common(self) -> None:
        ...

    def test_merge_index_as_on_arg(self) -> None:
        ...

    def test_merge_index_singlekey_right_vs_left(self) -> None:
        ...

    def test_merge_index_singlekey_inner(self) -> None:
        ...

    def test_merge_misspecified(self) -> None:
        ...

    def test_index_and_on_parameters_confusion(self) -> None:
        ...

    def test_merge_overlap(self) -> None:
        ...

    def test_merge_different_column_key_names(self) -> None:
        ...

    def test_merge_copy(self) -> None:
        ...

    def test_merge_nocopy(self) -> None:
        ...

    def test_intelligently_handle_join_key(self) -> None:
        ...

    def test_merge_join_key_dtype_cast(self) -> None:
        ...

    def test_handle_join_key_pass_array(self) -> None:
        ...

    def test_no_overlap_more_informative_error(self) -> None:
        ...

    def test_merge_non_unique_indexes(self) -> None:
        ...

    def test_merge_non_unique_index_many_to_many(self) -> None:
        ...

    def test_left_merge_empty_dataframe(self) -> None:
        ...

    def test_merge_left_empty_right_empty(self, join_type: Any, kwarg: Any) -> None:
        ...

    def test_merge_left_empty_right_notempty(self) -> None:
        ...

    def test_merge_left_notempty_right_empty(self) -> None:
        ...

    def test_merge_empty_frame(self, series_of_dtype: Any, series_of_dtype2: Any) -> None:
        ...

    def test_merge_all_na_column(self, series_of_dtype: Any, series_of_dtype_all_na: Any) -> None:
        ...

    def test_merge_nosort(self) -> None:
        ...

    def test_merge_nan_right(self) -> None:
        ...

    def test_merge_type(self):
        ...

    def test_join_append_timedeltas(self) -> None:
        ...

    def test_other_datetime_unit(self) -> None:
        ...

    def test_other_timedelta_unit(self, unit: Any) -> None:
        ...

    def test_overlapping_columns_error_message(self) -> None:
        ...

    def test_merge_on_datetime64tz(self) -> None:
        ...

    def test_merge_on_datetime64tz_empty(self) -> None:
        ...

    def test_merge_datetime64tz_with_dst_transition(self) -> None:
        ...

    def test_merge_non_unique_period_index(self) -> None:
        ...

    def test_merge_on_periods(self) -> None:
        ...

    def test_indicator(self) -> None:
        ...

    def test_validation(self) -> None:
        ...

    def test_merge_two_empty_df_no_division_error(self) -> None:
        ...

    def test_merge_on_index_with_more_values(self, how: Any, index: Any, expected_index: Any) -> None:
        ...

    def test_merge_right_index_right(self) -> None:
        ...

    def test_merge_take_missing_values_from_index_of_other_dtype(self) -> None:
        ...


class TestMergeDtypes:
    def test_different(self, right_vals: Any) -> None:
        ...

    def test_join_multi_dtypes(self, d1: Any, d2: Any) -> None:
        ...

    def test_merge_on_ints_floats(self, int_vals: Any, float_vals: Any, exp_vals: Any) -> None:
        ...

    def test_merge_on_ints_floats_warning(self) -> None:
        ...

    def test_merge_incompat_infer_boolean_object(self) -> None:
        ...

    def test_merge_incompat_dtypes_are_ok(self, df1_vals: Any, df2_vals: Any) -> None:
        ...

    def test_merge_incompat_dtypes_error(self, df1_vals: Any, df2_vals: Any) -> None:
        ...


def left() -> Any:
    ...

def right() -> Any:
    ...


class TestMergeCategorical:
    def test_identical(self, left: Any) -> None:
        ...

    def test_basic(self, left: Any, right: Any) -> None:
        ...

    def test_merge_categorical(self) -> None:
        ...

    def tests_merge_categorical_unordered_equal(self) -> None:
        ...

    def test_other_columns(self, left: Any, right: Any) -> None:
        ...

    def test_dtype_on_merged_different(self, change: Any, join_type: Any, left: Any, right: Any) -> None:
        ...

    def test_self_join_multiple_categories(self):
        ...

    def test_dtype_on_categorical_dates(self) -> None:
        ...

    def test_merging_with_bool_or_int_cateorical_column(self, category_column: Any, categories: Any, expected_categories: Any, ordered: Any) -> None:
        ...

    def test_merge_on_int_array(self) -> None:
        ...


def left_df() -> Any:
    ...

def right_df() -> Any:
    ...


class TestMergeOnIndexes:
    def test_merge_on_indexes(self, left_df: Any, right_df: Any, how: Any, sort: Any, expected: Any) -> None:
        ...


def test_merge_index_types(index: Any) -> None:
    ...

def test_merge_series(on: Any, left_on: Any, right_on: Any, left_index: Any, right_index: Any, nm: Any) -> None:
    ...

def test_merge_suffix(col1: Any, col2: Any, kwargs: Any, expected_cols: Any) -> None:
    ...

def test_merge_suffix_error(col1: Any, col2: Any, suffixes: Any) -> None:
    ...

def test_merge_suffix_none_error(col1: Any, col2: Any, suffixes: Any) -> None:
    ...

def test_merge_equal_cat_dtypes(cat_dtype: Any, reverse: Any) -> None:
    ...

def test_merge_equal_cat_dtypes2() -> None:
    ...
