# Stubs for pandas.tests.indexes.interval.test_interval_range (Python 3)
#
# NOTE: This dynamically typed stub was automatically generated by stubgen.
# pylint: disable=unused-argument,redefined-outer-name,no-self-use,invalid-name
# pylint: disable=relative-beyond-top-level,line-too-long

from typing import Any

def name(request: Any) -> Any:
    ...


class TestIntervalRange:
    def test_constructor_numeric(self, closed: Any, name: Any, freq: Any, periods: Any) -> None:
        ...

    def test_constructor_timestamp(self, closed: Any, name: Any, freq: Any, periods: Any, tz: Any) -> None:
        ...

    def test_constructor_timedelta(self, closed: Any, name: Any, freq: Any, periods: Any) -> None:
        ...

    def test_early_truncation(self, start: Any, end: Any, freq: Any, expected_endpoint: Any) -> None:
        ...

    def test_no_invalid_float_truncation(self, start: Any, end: Any, freq: Any) -> None:
        ...

    def test_linspace_dst_transition(self, start: Any, mid: Any, end: Any) -> None:
        ...

    def test_float_subtype(self, start: Any, end: Any, freq: Any) -> None:
        ...

    def test_constructor_coverage(self) -> None:
        ...

    def test_errors(self) -> None:
        ...
