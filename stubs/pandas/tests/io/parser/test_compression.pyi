# Stubs for pandas.tests.io.parser.test_compression (Python 3)
#
# NOTE: This dynamically typed stub was automatically generated by stubgen.
# pylint: disable=unused-argument,redefined-outer-name,no-self-use,invalid-name
# pylint: disable=relative-beyond-top-level,line-too-long,arguments-differ
# pylint: disable=no-member,too-few-public-methods,keyword-arg-before-vararg
# pylint: disable=super-init-not-called,abstract-method,redefined-builtin


from typing import Any

def buffer(request: Any) -> Any:
    ...


def parser_and_data(all_parsers: Any, csv1: Any) -> Any:
    ...


def test_zip(parser_and_data: Any, compression: Any) -> None:
    ...


def test_zip_error_multiple_files(parser_and_data: Any, compression: Any) -> None:
    ...


def test_zip_error_no_files(parser_and_data: Any) -> None:
    ...


def test_zip_error_invalid_zip(parser_and_data: Any) -> None:
    ...


def test_compression(parser_and_data: Any, compression_only: Any, buffer: Any, filename: Any) -> None:
    ...


def test_infer_compression(all_parsers: Any, csv1: Any, buffer: Any, ext: Any) -> None:
    ...


def test_compression_utf16_encoding(all_parsers: Any, csv_dir_path: Any) -> None:
    ...


def test_invalid_compression(all_parsers: Any, invalid_compression: Any) -> None:
    ...
