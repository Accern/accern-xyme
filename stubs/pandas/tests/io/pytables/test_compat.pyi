# Stubs for pandas.tests.io.pytables.test_compat (Python 3)
#
# NOTE: This dynamically typed stub was automatically generated by stubgen.
# pylint: disable=unused-argument,redefined-outer-name,no-self-use,invalid-name
# pylint: disable=relative-beyond-top-level,line-too-long,arguments-differ
# pylint: disable=no-member,too-few-public-methods,keyword-arg-before-vararg
# pylint: disable=super-init-not-called,abstract-method,redefined-builtin

from typing import Any

tables: Any

def pytables_hdf5_file() -> None:
    ...


class TestReadPyTablesHDF5:
    def test_read_complete(self, pytables_hdf5_file: Any) -> None:
        ...

    def test_read_with_start(self, pytables_hdf5_file: Any) -> None:
        ...

    def test_read_with_stop(self, pytables_hdf5_file: Any) -> None:
        ...

    def test_read_with_startstop(self, pytables_hdf5_file: Any) -> None:
        ...
