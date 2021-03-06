# Stubs for pandas.tests.io.json.test_readlines (Python 3)
#
# NOTE: This dynamically typed stub was automatically generated by stubgen.
# pylint: disable=unused-argument,redefined-outer-name,no-self-use,invalid-name
# pylint: disable=relative-beyond-top-level,line-too-long,arguments-differ
# pylint: disable=no-member,too-few-public-methods,keyword-arg-before-vararg
# pylint: disable=super-init-not-called,abstract-method,redefined-builtin

from typing import Any

def lines_json_df():
    ...


def test_read_jsonl() -> None:
    ...


def test_read_jsonl_unicode_chars() -> None:
    ...


def test_to_jsonl() -> None:
    ...


def test_readjson_chunks(lines_json_df: Any, chunksize: Any) -> None:
    ...


def test_readjson_chunksize_requires_lines(lines_json_df: Any) -> None:
    ...


def test_readjson_chunks_series() -> None:
    ...


def test_readjson_each_chunk(lines_json_df: Any) -> None:
    ...


def test_readjson_chunks_from_file() -> None:
    ...


def test_readjson_chunks_closes(chunksize: Any) -> None:
    ...


def test_readjson_invalid_chunksize(lines_json_df: Any, chunksize: Any) -> None:
    ...


def test_readjson_chunks_multiple_empty_lines(chunksize: Any) -> None:
    ...
