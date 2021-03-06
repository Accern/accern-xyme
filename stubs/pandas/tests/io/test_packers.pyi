# Stubs for pandas.tests.io.test_packers (Python 3)
#
# NOTE: This dynamically typed stub was automatically generated by stubgen.
# pylint: disable=unused-argument,redefined-outer-name,no-self-use,invalid-name
# pylint: disable=relative-beyond-top-level,line-too-long,arguments-differ
# pylint: disable=no-member,too-few-public-methods,keyword-arg-before-vararg
# pylint: disable=super-init-not-called,abstract-method,redefined-builtin

from typing import Any, Optional

nan: Any

def current_packers_data():
    ...

def all_packers_data():
    ...

def check_arbitrary(a: Any, b: Any) -> None:
    ...


class TestPackers:
    path: Any = ...
    def setup_method(self, method: Any) -> None:
        ...

    def teardown_method(self, method: Any) -> None:
        ...

    def encode_decode(self, x: Any, compress: Optional[Any] = ..., **kwargs: Any) -> Any:
        ...


class TestAPI(TestPackers):
    def test_string_io(self) -> None:
        ...

    def test_path_pathlib(self) -> None:
        ...

    def test_path_localpath(self) -> None:
        ...

    def test_iterator_with_string_io(self) -> None:
        ...

    read: int = ...
    def test_invalid_arg(self) -> None:
        ...


class TestNumpy(TestPackers):
    def test_numpy_scalar_float(self) -> None:
        ...

    def test_numpy_scalar_complex(self) -> None:
        ...

    def test_scalar_float(self) -> None:
        ...

    def test_scalar_bool(self) -> None:
        ...

    def test_scalar_complex(self) -> None:
        ...

    def test_list_numpy_float(self) -> None:
        ...

    def test_list_numpy_float_complex(self) -> None:
        ...

    def test_list_float(self) -> None:
        ...

    def test_list_float_complex(self) -> None:
        ...

    def test_dict_float(self) -> None:
        ...

    def test_dict_complex(self) -> None:
        ...

    def test_dict_numpy_float(self) -> None:
        ...

    def test_dict_numpy_complex(self) -> None:
        ...

    def test_numpy_array_float(self) -> None:
        ...

    def test_numpy_array_complex(self):
        ...

    def test_list_mixed(self) -> None:
        ...


class TestBasic(TestPackers):
    def test_timestamp(self) -> None:
        ...

    def test_nat(self) -> None:
        ...

    def test_datetimes(self) -> None:
        ...

    def test_timedeltas(self) -> None:
        ...

    def test_periods(self) -> None:
        ...

    def test_intervals(self) -> None:
        ...


class TestIndex(TestPackers):
    d: Any = ...
    mi: Any = ...
    def setup_method(self, method: Any) -> None:
        ...

    def test_basic_index(self) -> None:
        ...

    def test_multi_index(self) -> None:
        ...

    def test_unicode(self) -> None:
        ...

    def categorical_index(self) -> None:
        ...


class TestSeries(TestPackers):
    d: Any = ...
    def setup_method(self, method: Any) -> None:
        ...

    def test_basic(self) -> None:
        ...


class TestCategorical(TestPackers):
    d: Any = ...
    def setup_method(self, method: Any) -> None:
        ...

    def test_basic(self) -> None:
        ...


class TestNDFrame(TestPackers):
    frame: Any = ...
    def setup_method(self, method: Any) -> None:
        ...

    def test_basic_frame(self) -> None:
        ...

    def test_multi(self) -> None:
        ...

    def test_iterator(self) -> None:
        ...

    def tests_datetimeindex_freq_issue(self) -> None:
        ...

    def test_dataframe_duplicate_column_names(self) -> None:
        ...


class TestSparse(TestPackers):
    def test_sparse_series(self) -> None:
        ...

    def test_sparse_frame(self) -> None:
        ...


class TestCompression(TestPackers):
    frame: Any = ...
    def setup_method(self, method: Any) -> None:
        ...

    def test_plain(self) -> None:
        ...

    def test_compression_zlib(self) -> None:
        ...

    def test_compression_blosc(self) -> None:
        ...

    def test_compression_warns_when_decompress_caches_zlib(self, monkeypatch: Any) -> None:
        ...

    def test_compression_warns_when_decompress_caches_blosc(self, monkeypatch: Any) -> None:
        ...

    def test_small_strings_no_warn_zlib(self) -> None:
        ...

    def test_small_strings_no_warn_blosc(self) -> None:
        ...

    def test_readonly_axis_blosc(self) -> None:
        ...

    def test_readonly_axis_zlib(self) -> None:
        ...

    def test_readonly_axis_blosc_to_sql(self) -> None:
        ...

    def test_readonly_axis_zlib_to_sql(self) -> None:
        ...


class TestEncoding(TestPackers):
    frame: Any = ...
    utf_encodings: Any = ...
    def setup_method(self, method: Any) -> None:
        ...

    def test_utf(self) -> None:
        ...

    def test_default_encoding(self) -> None:
        ...


files: Any

def legacy_packer(request: Any, datapath: Any) -> Any:
    ...


class TestMsgpack:
    minimum_structure: Any = ...
    def check_min_structure(self, data: Any, version: Any) -> None:
        ...

    def compare(self, current_data: Any, all_data: Any, vf: Any, version: Any) -> Any:
        ...

    def compare_series_dt_tz(self, result: Any, expected: Any, typ: Any, version: Any) -> None:
        ...

    def compare_frame_dt_mixed_tzs(self, result: Any, expected: Any, typ: Any, version: Any) -> None:
        ...

    def test_msgpacks_legacy(self, current_packers_data: Any, all_packers_data: Any, legacy_packer: Any, datapath: Any) -> None:
        ...

    def test_msgpack_period_freq(self) -> None:
        ...
