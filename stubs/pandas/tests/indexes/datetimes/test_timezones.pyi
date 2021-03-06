# Stubs for pandas.tests.indexes.datetimes.test_timezones (Python 3)
#
# NOTE: This dynamically typed stub was automatically generated by stubgen.
# pylint: disable=unused-argument,redefined-outer-name,no-self-use,invalid-name
# pylint: disable=relative-beyond-top-level,line-too-long

from datetime import tzinfo
from typing import Any


class FixedOffset(tzinfo):
    def __init__(self, offset: Any, name: Any) -> None:
        ...

    def utcoffset(self, dt: Any) -> Any:
        ...

    def tzname(self, dt: Any) -> Any:
        ...

    def dst(self, dt: Any) -> Any:
        ...


fixed_off: Any
fixed_off_no_name: Any

class TestDatetimeIndexTimezones:
    def test_tz_convert_nat(self) -> None:
        ...

    def test_dti_tz_convert_compat_timestamp(self, prefix: Any) -> None:
        ...

    def test_dti_tz_convert_hour_overflow_dst(self) -> None:
        ...

    def test_dti_tz_convert_hour_overflow_dst_timestamps(self, tz: Any) -> None:
        ...

    def test_dti_tz_convert_trans_pos_plus_1__bug(self, freq: Any, n: Any) -> None:
        ...

    def test_dti_tz_convert_dst(self) -> None:
        ...

    def test_tz_convert_roundtrip(self, tz_aware_fixture: Any) -> None:
        ...

    def test_dti_tz_convert_tzlocal(self) -> None:
        ...

    def test_dti_tz_convert_utc_to_local_no_modify(self, tz: Any) -> None:
        ...

    def test_tz_convert_unsorted(self, tzstr: Any) -> None:
        ...

    def test_dti_tz_localize_nonexistent_raise_coerce(self) -> None:
        ...

    def test_dti_tz_localize_ambiguous_infer(self, tz: Any) -> None:
        ...

    def test_dti_tz_localize_ambiguous_times(self, tz: Any) -> None:
        ...

    def test_dti_tz_localize_pass_dates_to_utc(self, tzstr: Any) -> None:
        ...

    def test_dti_tz_localize(self, prefix: Any) -> None:
        ...

    def test_dti_tz_localize_utc_conversion(self, tz: Any) -> None:
        ...

    def test_dti_tz_localize_roundtrip(self, tz_aware_fixture: Any) -> None:
        ...

    def test_dti_tz_localize_naive(self) -> None:
        ...

    def test_dti_tz_localize_tzlocal(self) -> None:
        ...

    def test_dti_tz_localize_ambiguous_nat(self, tz: Any) -> None:
        ...

    def test_dti_tz_localize_ambiguous_flags(self, tz: Any) -> None:
        ...

    def test_dti_construction_ambiguous_endpoint(self, tz: Any) -> None:
        ...

    def test_dti_construction_nonexistent_endpoint(self, tz: Any, option: Any, expected: Any) -> None:
        ...

    def test_dti_tz_localize_bdate_range(self) -> None:
        ...

    def test_dti_tz_localize_nonexistent(self, tz: Any, method: Any, exp: Any) -> None:
        ...

    def test_dti_tz_localize_nonexistent_shift(self, start_ts: Any, tz: Any, end_ts: Any, shift: Any, tz_type: Any) -> None:
        ...

    def test_dti_tz_localize_nonexistent_shift_invalid(self, offset: Any, tz_type: Any) -> None:
        ...

    def test_dti_tz_localize_errors_deprecation(self) -> None:
        ...

    def test_normalize_tz(self) -> None:
        ...

    def test_normalize_tz_local(self, timezone: Any) -> None:
        ...

    def test_dti_constructor_static_tzinfo(self, prefix: Any) -> None:
        ...

    def test_dti_constructor_with_fixed_tz(self) -> None:
        ...

    def test_dti_convert_datetime_list(self, tzstr: Any) -> None:
        ...

    def test_dti_construction_univalent(self) -> None:
        ...

    def test_dti_from_tzaware_datetime(self, tz: Any) -> None:
        ...

    def test_dti_tz_constructors(self, tzstr: Any) -> None:
        ...

    def test_join_utc_convert(self, join_type: Any) -> None:
        ...

    def test_date_accessor(self, dtype: Any) -> None:
        ...

    def test_time_accessor(self, dtype: Any) -> None:
        ...

    def test_timetz_accessor(self, tz_naive_fixture: Any) -> None:
        ...

    def test_dti_drop_dont_lose_tz(self) -> None:
        ...

    def test_dti_tz_conversion_freq(self, tz_naive_fixture: Any) -> None:
        ...

    def test_drop_dst_boundary(self) -> None:
        ...

    def test_date_range_localize(self) -> None:
        ...

    def test_timestamp_equality_different_timezones(self) -> None:
        ...

    def test_dti_intersection(self) -> None:
        ...

    def test_dti_equals_with_tz(self) -> None:
        ...

    def test_dti_tz_nat(self, tzstr: Any) -> None:
        ...

    def test_dti_astype_asobject_tzinfos(self, tzstr: Any) -> None:
        ...

    def test_dti_with_timezone_repr(self, tzstr: Any) -> None:
        ...

    def test_dti_take_dont_lose_meta(self, tzstr: Any) -> None:
        ...

    def test_utc_box_timestamp_and_localize(self, tzstr: Any) -> None:
        ...

    def test_dti_to_pydatetime(self) -> None:
        ...

    def test_dti_to_pydatetime_fizedtz(self) -> None:
        ...

    def test_with_tz(self, tz: Any) -> None:
        ...

    def test_field_access_localize(self, prefix: Any) -> None:
        ...

    def test_dti_convert_tz_aware_datetime_datetime(self, tz: Any) -> None:
        ...

    def test_dti_union_aware(self) -> None:
        ...

    def test_dti_union_mixed(self) -> None:
        ...

    def test_iteration_preserves_nanoseconds(self, tz: Any) -> None:
        ...


class TestDateRange:
    def test_hongkong_tz_convert(self) -> None:
        ...

    def test_date_range_span_dst_transition(self, tzstr: Any) -> None:
        ...

    def test_date_range_timezone_str_argument(self, tzstr: Any) -> None:
        ...

    def test_date_range_with_fixedoffset_noname(self) -> None:
        ...

    def test_date_range_with_tz(self, tzstr: Any) -> None:
        ...


class TestToDatetime:
    def test_to_datetime_utc(self) -> None:
        ...

    def test_to_datetime_fixed_offset(self) -> None:
        ...
