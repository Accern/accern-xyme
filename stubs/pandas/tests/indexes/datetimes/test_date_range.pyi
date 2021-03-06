# Stubs for pandas.tests.indexes.datetimes.test_date_range (Python 3)
#
# NOTE: This dynamically typed stub was automatically generated by stubgen.
# pylint: disable=unused-argument,redefined-outer-name,no-self-use,invalid-name
# pylint: disable=relative-beyond-top-level

from typing import Any
from pandas.tests.series.common import TestData

START: Any
END: Any

class TestTimestampEquivDateRange:
    def test_date_range_timestamp_equiv(self) -> None:
        ...

    def test_date_range_timestamp_equiv_dateutil(self) -> None:
        ...

    def test_date_range_timestamp_equiv_explicit_pytz(self) -> None:
        ...

    def test_date_range_timestamp_equiv_explicit_dateutil(self) -> None:
        ...

    def test_date_range_timestamp_equiv_from_datetime_instance(self) -> None:
        ...

    def test_date_range_timestamp_equiv_preserve_frequency(self) -> None:
        ...


class TestDateRanges(TestData):
    def test_date_range_nat(self) -> None:
        ...

    def test_date_range_multiplication_overflow(self) -> None:
        ...

    def test_date_range_unsigned_overflow_handling(self) -> None:
        ...

    def test_date_range_int64_overflow_non_recoverable(self) -> None:
        ...

    def test_date_range_int64_overflow_stride_endpoint_different_signs(
            self) -> None:
        ...

    def test_date_range_out_of_bounds(self) -> None:
        ...

    def test_date_range_gen_error(self) -> None:
        ...

    def test_begin_year_alias(self, freq: Any) -> None:
        ...

    def test_end_year_alias(self, freq: Any) -> None:
        ...

    def test_business_end_year_alias(self, freq: Any) -> None:
        ...

    def test_date_range_negative_freq(self) -> None:
        ...

    def test_date_range_bms_bug(self) -> None:
        ...

    def test_date_range_normalize(self) -> None:
        ...

    def test_date_range_fy5252(self) -> None:
        ...

    def test_date_range_ambiguous_arguments(self) -> None:
        ...

    def test_date_range_convenience_periods(self) -> None:
        ...

    def test_date_range_linspacing_tz(
            self, start: Any, end: Any, result_tz: Any) -> None:
        ...

    def test_date_range_businesshour(self) -> None:
        ...

    def test_range_misspecified(self) -> None:
        ...

    def test_compat_replace(self) -> None:
        ...

    def test_catch_infinite_loop(self) -> None:
        ...

    def test_wom_len(self, periods: Any) -> None:
        ...

    def test_construct_over_dst(self) -> None:
        ...

    def test_construct_with_different_start_end_string_format(self) -> None:
        ...

    def test_error_with_zero_monthends(self) -> None:
        ...

    def test_range_bug(self) -> None:
        ...

    def test_range_tz_pytz(self) -> None:
        ...

    def test_range_tz_dst_straddle_pytz(self, start: Any, end: Any) -> None:
        ...

    def test_range_tz_dateutil(self):
        ...

    def test_range_closed(self, freq: Any) -> None:
        ...

    def test_range_closed_with_tz_aware_start_end(self) -> None:
        ...

    def test_range_closed_boundary(self, closed: Any) -> None:
        ...

    def test_years_only(self) -> None:
        ...

    def test_freq_divides_end_in_nanos(self) -> None:
        ...

    def test_cached_range_bug(self) -> None:
        ...

    def test_timezone_comparaison_bug(self) -> None:
        ...

    def test_timezone_comparaison_assert(self) -> None:
        ...

    def test_negative_non_tick_frequency_descending_dates(
            self, tz_aware_fixture: Any) -> None:
        ...


class TestGenRangeGeneration:
    def test_generate(self) -> None:
        ...

    def test_generate_cday(self) -> None:
        ...

    def test_1(self) -> None:
        ...

    def test_2(self) -> None:
        ...

    def test_3(self) -> None:
        ...

    def test_precision_finer_than_offset(self) -> None:
        ...

    dt1: Any = ...
    dt2: Any = ...
    tz1: Any = ...
    tz2: Any = ...
    def test_mismatching_tz_raises_err(self, start: Any, end: Any) -> None:
        ...


class TestBusinessDateRange:
    def test_constructor(self) -> None:
        ...

    def test_naive_aware_conflicts(self) -> None:
        ...

    def test_misc(self) -> None:
        ...

    def test_date_parse_failure(self) -> None:
        ...

    def test_daterange_bug_456(self) -> None:
        ...

    def test_bdays_and_open_boundaries(self, closed: Any) -> None:
        ...

    def test_bday_near_overflow(self) -> None:
        ...

    def test_bday_overflow_error(self) -> None:
        ...


class TestCustomDateRange:
    def test_constructor(self) -> None:
        ...

    def test_misc(self) -> None:
        ...

    def test_daterange_bug_456(self) -> None:
        ...

    def test_cdaterange(self) -> None:
        ...

    def test_cdaterange_weekmask(self) -> None:
        ...

    def test_cdaterange_holidays(self) -> None:
        ...

    def test_cdaterange_weekmask_and_holidays(self) -> None:
        ...

    def test_all_custom_freq(self, freq: Any) -> None:
        ...

    def test_range_with_millisecond_resolution(self, start_end: Any) -> None:
        ...
