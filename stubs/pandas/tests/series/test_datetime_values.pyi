# Stubs for pandas.tests.series.test_datetime_values (Python 3)
#
# NOTE: This dynamically typed stub was automatically generated by stubgen.
# pylint: disable=unused-argument,redefined-outer-name,no-self-use,invalid-name
# pylint: disable=relative-beyond-top-level,line-too-long,arguments-differ
# pylint: disable=no-member,too-few-public-methods,keyword-arg-before-vararg
# pylint: disable=super-init-not-called,abstract-method,redefined-builtin

from typing import Any

class TestSeriesDatetimeValues:
    def test_dt_namespace_accessor(self):
        ...

    def test_dt_round(self, method: Any, dates: Any) -> None:
        ...

    def test_dt_round_tz(self) -> None:
        ...

    def test_dt_round_tz_ambiguous(self, method: Any) -> None:
        ...

    def test_dt_round_tz_nonexistent(self, method: Any, ts_str: Any, freq: Any) -> None:
        ...

    def test_dt_namespace_accessor_categorical(self) -> None:
        ...

    def test_dt_accessor_no_new_attributes(self) -> None:
        ...

    def test_dt_accessor_datetime_name_accessors(self, time_locale: Any) -> None:
        ...

    def test_strftime(self) -> None:
        ...

    def test_valid_dt_with_missing_values(self) -> None:
        ...

    def test_dt_accessor_api(self) -> None:
        ...

    def test_dt_accessor_invalid(self, ser: Any) -> None:
        ...

    def test_dt_accessor_updates_on_inplace(self) -> None:
        ...

    def test_between(self) -> None:
        ...

    def test_date_tz(self):
        ...

    def test_datetime_understood(self) -> None:
        ...

    def test_dt_timetz_accessor(self, tz_naive_fixture: Any) -> None:
        ...

    def test_setitem_with_string_index(self) -> None:
        ...

    def test_setitem_with_different_tz(self) -> None:
        ...
