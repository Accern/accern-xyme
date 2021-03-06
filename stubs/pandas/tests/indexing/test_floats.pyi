# Stubs for pandas.tests.indexing.test_floats (Python 3)
#
# NOTE: This dynamically typed stub was automatically generated by stubgen.
# pylint: disable=unused-argument,redefined-outer-name,no-self-use,invalid-name
# pylint: disable=relative-beyond-top-level,line-too-long,arguments-differ
# pylint: disable=no-member,too-few-public-methods,keyword-arg-before-vararg
# pylint: disable=super-init-not-called,abstract-method,redefined-builtin
# pylint: disable=blacklisted-name

from typing import Any

ignore_ix: Any

class TestFloatIndexers:
    def check(self, result: Any, original: Any, indexer: Any, getitem: Any) -> None:
        ...

    def test_scalar_error(self) -> None:
        ...

    def test_scalar_non_numeric(self):
        ...

    def test_scalar_with_mixed(self):
        ...

    def test_scalar_integer(self):
        ...

    def test_scalar_float(self):
        ...

    def test_slice_non_numeric(self):
        ...

    def test_slice_integer(self):
        ...

    def test_integer_positional_indexing(self):
        ...

    def test_slice_integer_frame_getitem(self):
        ...

    def test_slice_float(self):
        ...

    def test_floating_index_doc_example(self) -> None:
        ...

    def test_floating_misc(self) -> None:
        ...

    def test_floating_tuples(self) -> None:
        ...

    def test_float64index_slicing_bug(self) -> None:
        ...
