from typing import Any, Callable, IO, Optional
import io
import shutil
from io import BytesIO, TextIOWrapper
import pandas as pd

FILE_UPLOAD_CHUNK_SIZE = 100 * 1024  # 100kb
FILE_HASH_CHUNK_SIZE = FILE_UPLOAD_CHUNK_SIZE
MAX_RETRY = 20
RETRY_SLEEP = 5.0


def set_file_upload_chunk_size(size: int) -> None:
    global FILE_UPLOAD_CHUNK_SIZE

    FILE_UPLOAD_CHUNK_SIZE = size


def get_file_upload_chunk_size() -> int:
    return FILE_UPLOAD_CHUNK_SIZE


def set_file_hash_chunk_size(size: int) -> None:
    global FILE_HASH_CHUNK_SIZE

    FILE_HASH_CHUNK_SIZE = size


def get_file_hash_chunk_size() -> int:
    return FILE_HASH_CHUNK_SIZE


def get_max_retry() -> int:
    """Returns the maximum number of retries on connection errors.

    Returns:
        int -- The number of times a connection tries to be established.
    """
    return MAX_RETRY


def get_retry_sleep() -> float:
    return RETRY_SLEEP


def maybe_timestamp(timestamp: Optional[str]) -> Optional[pd.Timestamp]:
    return None if timestamp is None else pd.Timestamp(timestamp)


def df_to_csv(df: pd.DataFrame) -> BytesIO:
    bio = BytesIO()
    wrap = TextIOWrapper(bio, encoding="utf-8", write_through=True)
    df.to_csv(wrap, index=False)
    wrap.detach()
    bio.seek(0)
    return bio


MPL_SETUP = False


def setup_matplotlib() -> None:
    global MPL_SETUP

    if MPL_SETUP:
        return
    from pandas.plotting import register_matplotlib_converters

    register_matplotlib_converters()
    MPL_SETUP = True


IS_JUPYTER: Optional[bool] = None


def is_jupyter() -> bool:
    global IS_JUPYTER

    if IS_JUPYTER is not None:
        return IS_JUPYTER

    try:
        from IPython import get_ipython

        IS_JUPYTER = get_ipython() is not None
    except (NameError, ModuleNotFoundError) as _:
        IS_JUPYTER = False
    return IS_JUPYTER


def get_progress_bar(out: Optional[IO[Any]]) -> Callable[[float, bool], None]:
    # pylint: disable=unused-argument

    def no_bar(progress: float, final: bool) -> None:
        return

    if out is None:
        return no_bar

    io_out: IO[Any] = out

    if is_jupyter():
        from IPython.display import ProgressBar

        mul = 1000
        bar = ProgressBar(mul)
        bar.display()

        def jupyter_bar(progress: float, final: bool) -> None:
            bar.progress = int(progress * mul)
            end = "\n" if final else "\r"
            io_out.write(f"{progress * 100.0:.2f}%{end}")

        return jupyter_bar

    cols, _ = shutil.get_terminal_size((80, 20))
    max_len = len(" 100.00%")
    border = "|"

    def stdout_bar(progress: float, final: bool) -> None:
        pstr = f" {progress * 100.0:.2f}%"
        cur_len = len(pstr)
        if cur_len < max_len:
            pstr = f"{' ' * (max_len - cur_len)}{pstr}"
        end = "\n" if final else "\r"
        full_len = len(border) * 2 + len(pstr) + len(end)
        bar = "█" * int(progress * (cols - full_len))
        mid = ' ' * max(0, cols - full_len - len(bar))
        io_out.write(f"{border}{bar}{mid}{border}{pstr}{end}")

    return stdout_bar


def get_file_hash(buff: IO[bytes]) -> str:
    """Return sha224 hash of data files

    Args:
        buff (IO[bytes]): Data used to generate the hash.

    Returns:
        str: A sha224 hashed string.
    """
    import hashlib

    sha = hashlib.sha224()
    chunk_size = FILE_HASH_CHUNK_SIZE
    init_pos = buff.seek(0, io.SEEK_CUR)
    while True:
        chunk = buff.read(chunk_size)
        if not chunk:
            break
        sha.update(chunk)
    buff.seek(init_pos, io.SEEK_SET)
    return sha.hexdigest()
