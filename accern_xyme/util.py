from typing import (
    Any,
    Callable,
    Dict,
    IO,
    Iterable,
    List,
    Optional,
    TypeVar,
    Union,
)
import io
import json
import shutil
import time
import threading
from io import BytesIO, TextIOWrapper
import pandas as pd
from scipy import sparse
import torch
from .types import QueueStatsResponse, QueueStatus

FILE_UPLOAD_CHUNK_SIZE = 100 * 1024  # 100kb
FILE_HASH_CHUNK_SIZE = FILE_UPLOAD_CHUNK_SIZE
MAX_RETRY = 5
RETRY_SLEEP = 5.0


RT = TypeVar('RT')


ByteResponse = Union[pd.DataFrame, dict, IO[bytes], List[dict]]


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


def interpret_ctype(data: IO[bytes], ctype: str) -> ByteResponse:
    if ctype == "application/json":
        return json.load(data)
    if ctype == "application/json-error":
        res = json.load(data)
        if res["errMessage"] is None:
            raise NotFoundError()
        raise ServerSideError(res["errMessage"])
    if ctype == "application/parquet":
        return pd.read_parquet(data)
    if ctype == "application/torch":
        return torch.load(data)
    if ctype == "application/npz":
        return sparse.load_npz(data)
    if ctype == "application/jsonl":
        return [
            json.load(BytesIO(line))
            for line in data
        ]
    # NOTE: try best guess...
    content = BytesIO(data.read())
    try:
        return pd.read_parquet(content)
    except OSError:
        pass
    content.seek(0)
    try:
        return json.load(content)
    except json.decoder.JSONDecodeError:
        pass
    except UnicodeDecodeError:
        pass
    content.seek(0)
    try:
        return [
            json.load(BytesIO(line))
            for line in content
        ]
    except json.decoder.JSONDecodeError:
        pass
    except UnicodeDecodeError:
        pass
    content.seek(0)
    return content


def async_compute(
        arr: List[Any],
        start: Callable[[List[Any]], List[RT]],
        get: Callable[[RT], ByteResponse],
        check_queue: Callable[[], QueueStatsResponse],
        get_status: Callable[[List[RT]], Dict[RT, QueueStatus]],
        max_buff: int,
        block_size: int) -> Iterable[ByteResponse]:
    assert max_buff > 0
    assert block_size > 0
    done: List[bool] = [False]
    end_produce: List[bool] = [False]
    exc: List[Optional[Exception]] = [None]
    cond = threading.Condition()
    ids: Dict[RT, int] = {}
    res: Dict[int, ByteResponse] = {}

    def can_push_more() -> bool:
        if exc[0] is not None:
            return True
        if len(ids) < max_buff:
            return True
        remote_queue = check_queue()
        return remote_queue["data"] - remote_queue["active"] < max_buff

    def produce() -> None:
        try:
            pos = 0
            while pos < len(arr):
                with cond:
                    cond.wait_for(can_push_more)
                if exc[0] is not None:
                    break
                start_pos = pos
                cur = arr[pos:pos + block_size]
                pos += len(cur)
                ids.update({
                    cur_id: cur_ix + start_pos
                    for (cur_ix, cur_id) in enumerate(start(cur))
                })
                with cond:
                    cond.notify_all()
        finally:
            end_produce[0] = True
            with cond:
                cond.notify_all()

    def consume() -> None:
        while not done[0]:
            with cond:
                cond.wait_for(
                    lambda: exc[0] is not None or done[0] or len(ids) > 0)
            do_wait = False
            while ids:
                if do_wait:
                    time.sleep(1)
                do_wait = True
                status = get_status([
                    hnd.get_id() for hnd, _ in ids.items()
                ])
                for (t_id, t_status) in status.items():
                    if t_status in ("waiting", "running"):
                        continue
                    do_wait = False
                    try:
                        t_ix = ids.pop(t_id)
                        res[t_ix] = get(t_id)
                    except NotFoundError:
                        pass
                    except ServerSideError as e:
                        if exc[0] is None:
                            exc[0] = e
                    except KeyError as e:
                        pass
            with cond:
                cond.notify_all()


    prod_th = threading.Thread(target=produce)
    prod_th.start()
    consume_th = threading.Thread(target=consume)
    consume_th.start()
    yield_ix = 0
    while yield_ix < len(arr):
        with cond:
            cond.wait_for(lambda: exc[0] is not None or bool(res))
        if exc[0] is not None:
            break
        try:
            while res:
                yield res.pop(yield_ix)
                yield_ix += 1
        except KeyError:
            pass
    if exc[0] is not None:
        with cond:
            cond.wait_for(lambda: end_produce[0])
    done[0] = True
    with cond:
        cond.notify_all()
    prod_th.join()
    consume_th.join()
    raise_e = exc[0]
    if isinstance(raise_e, BaseException):
        raise raise_e  # pylint: disable=raising-bad-type


class ServerSideError(Exception):
    def __init__(self, message: str) -> None:
        self._message = message
        super().__init__(self._message)

    def __str__(self) -> str:
        return f"Error from xyme backend: \n{self._message}"


class NotFoundError(Exception):
    def __str__(self) -> str:
        return "404 Not Found"
