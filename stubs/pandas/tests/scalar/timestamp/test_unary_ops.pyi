# Stubs for pandas.tests.scalar.timestamp.test_unary_ops (Python 3)
#
# NOTE: This dynamically typed stub was automatically generated by stubgen.
# pylint: disable=unused-argument,redefined-outer-name,no-self-use,invalid-name
# pylint: disable=relative-beyond-top-level,line-too-long,arguments-differ
# pylint: disable=no-member,too-few-public-methods,keyword-arg-before-vararg
# pylint: disable=super-init-not-called,abstract-method,redefined-builtin

from typing import Any

class TestTimestampUnaryOps:
    def test_round_frequencies(self, timestamp: Any, freq: Any, expected: Any) -> None:
        ...

    def test_round_tzaware(self) -> None:
        ...

    def test_round_30min(self) -> None:
        ...

    def test_round_subsecond(self) -> None:
        ...

    def test_round_nonstandard_freq(self) -> None:
        ...

    def test_round_invalid_arg(self) -> None:
        ...

    def test_ceil_floor_edge(self, test_input: Any, rounder: Any, freq: Any, expected: Any) -> None:
        ...

    def test_round_minute_freq(self, test_input: Any, freq: Any, expected: Any, rounder: Any) -> None:
        ...

    def test_ceil(self) -> None:
        ...

    def test_floor(self) -> None:
        ...

    def test_round_dst_border_ambiguous(self, method: Any) -> None:
        ...

    def test_round_dst_border_nonexistent(self, method: Any, ts_str: Any, freq: Any) -> None:
        ...

    def test_round_int64(self, timestamp: Any, freq: Any) -> None:
        ...

    def test_replace_naive(self) -> None:
        ...

    def test_replace_aware(self, tz_aware_fixture: Any) -> None:
        ...

    def test_replace_preserves_nanos(self, tz_aware_fixture: Any) -> None:
        ...

    def test_replace_multiple(self, tz_aware_fixture: Any) -> None:
        ...

    def test_replace_invalid_kwarg(self, tz_aware_fixture: Any) -> None:
        ...

    def test_replace_integer_args(self, tz_aware_fixture: Any) -> None:
        ...

    def test_replace_tzinfo_equiv_tz_localize_none(self) -> None:
        ...

    def test_replace_tzinfo(self) -> None:
        ...

    def test_replace_across_dst(self, tz: Any, normalize: Any) -> None:
        ...

    def test_replace_dst_border(self) -> None:
        ...

    def test_replace_dst_fold(self, fold: Any, tz: Any) -> None:
        ...

    def test_normalize(self, tz_naive_fixture: Any, arg: Any) -> None:
        ...

    def test_timestamp(self) -> None:
        ...
