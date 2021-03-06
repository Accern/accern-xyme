# Stubs for pandas.tests.indexes.test_category (Python 3)
#
# NOTE: This dynamically typed stub was automatically generated by stubgen.
# pylint: disable=unused-argument,redefined-outer-name,no-self-use,invalid-name
# pylint: disable=relative-beyond-top-level,line-too-long

from typing import Any, Optional
from .common import Base


class TestCategoricalIndex(Base):
    indices: Any = ...

    def setup_method(self, method: Any) -> None:
        ...

    def create_index(
            self, categories: Optional[Any] = ..., ordered: bool = ...) -> Any:
        ...

    def test_can_hold_identifiers(self) -> None:
        ...

    def test_construction(self) -> None:
        ...

    def test_construction_with_dtype(self) -> None:
        ...

    def test_construction_empty_with_bool_categories(self) -> None:
        ...

    def test_construction_with_categorical_dtype(self) -> None:
        ...

    def test_create_categorical(self) -> None:
        ...

    def test_disallow_set_ops(self, func: Any, op_name: Any) -> None:
        ...

    def test_method_delegation(self):
        ...

    def test_contains(self) -> None:
        ...

    def test_contains_interval(self, item: Any, expected: Any) -> None:
        ...

    def test_contains_list(self) -> None:
        ...

    def test_map(self):
        ...

    def test_map_with_categorical_series(self) -> None:
        ...

    def test_map_with_nan(self, data: Any, f: Any) -> None:
        ...

    def test_where(self, klass: Any) -> None:
        ...

    def test_append(self) -> None:
        ...

    def test_append_to_another(self) -> None:
        ...

    def test_insert(self) -> None:
        ...

    def test_delete(self) -> None:
        ...

    def test_astype(self) -> None:
        ...

    def test_astype_category(self, name: Any, dtype_ordered: Any, index_ordered: Any) -> None:  # type: ignore
        ...

    def test_astype_category_ordered_none_deprecated(
            self, none: Any, warning: Any) -> None:
        ...

    def test_reindex_base(self) -> None:
        ...

    def test_reindexing(self) -> None:
        ...

    def test_reindex_dtype(self) -> None:
        ...

    def test_reindex_duplicate_target(self) -> None:
        ...

    def test_reindex_empty_index(self) -> None:
        ...

    def test_is_monotonic(self, data: Any, non_lexsorted_data: Any) -> None:
        ...

    def test_has_duplicates(self) -> None:
        ...

    def test_drop_duplicates(self) -> None:
        ...

    def test_get_indexer(self) -> None:
        ...

    def test_get_loc(self) -> None:
        ...

    def test_repr_roundtrip(self) -> None:
        ...

    def test_isin(self) -> None:
        ...

    def test_identical(self) -> None:
        ...

    def test_ensure_copied_data(self):
        ...

    def test_equals_categorical(self) -> None:
        ...

    def test_equals_categoridcal_unordered(self) -> None:
        ...

    def test_frame_repr(self) -> None:
        ...

    def test_string_categorical_index_repr(self) -> None:
        ...

    def test_fillna_categorical(self) -> None:
        ...

    def test_take_fill_value(self) -> None:
        ...

    def test_take_fill_value_datetime(self) -> None:
        ...

    def test_take_invalid_kwargs(self) -> None:
        ...

    def test_engine_type(self, dtype: Any, engine_type: Any) -> None:
        ...
