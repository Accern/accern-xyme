from typing import (
    Any,
    Callable,
    cast,
    Dict,
    IO,
    Iterable,
    List,
    Optional,
    Tuple,
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
from accern_xyme.types import MinimalQueueStatsResponse, QueueStatus

VERBOSE = False
FILE_UPLOAD_CHUNK_SIZE = 100 * 1024  # 100kb
FILE_HASH_CHUNK_SIZE = FILE_UPLOAD_CHUNK_SIZE
MAX_RETRY = 20
RETRY_SLEEP = 5.0


RT = TypeVar('RT')


ByteResponse = Union[pd.DataFrame, dict, IO[bytes], List[dict]]


def set_verbose() -> None:
    global VERBOSE

    import logging
    import http.client as http_client

    http_client.HTTPConnection.debuglevel = 1  # type: ignore
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    requests_log = logging.getLogger("requests.packages.urllib3")
    requests_log.setLevel(logging.DEBUG)
    requests_log.propagate = True
    VERBOSE = True


def is_verbose() -> bool:
    return VERBOSE


MINUTE = 60.0
HOUR = 60.0 * MINUTE
DAY = 24.0 * HOUR
WEEK = 7.0 * DAY
YEAR = 365.0 * DAY


def get_age(cur_time: float, other_time: Optional[float]) -> str:
    if other_time is None:
        return "never"
    diff = cur_time - other_time
    if diff < 0.0:
        return "soon"
    if diff < 0.1:
        return "now"
    if diff < 1.0:
        return "<1s"
    if diff < MINUTE:
        return "<1m"
    if diff < HOUR:
        return f"{diff // MINUTE:.0f}m"
    if diff < DAY:
        return f"{diff // HOUR:.0f}h"
    if diff < WEEK:
        return f"{diff // DAY:.0f}d"
    if diff < YEAR:
        return f"{diff // WEEK:.0f}w"
    return f"{diff // YEAR:.0f}y"


def safe_opt_num(num: Optional[float]) -> Tuple[bool, float]:
    if num is None:
        return (False, 0.0)
    return (True, num)


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
    if ctype == "application/problem+json":
        res = json.load(data)
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
    content = BytesIO(data.read())
    if ctype == "application/octet-stream":
        return content
    # NOTE: try best guess...
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


def merge_ctype(datas: List[ByteResponse], ctype: str) -> ByteResponse:
    if ctype == "application/json":
        return cast(ByteResponse, datas)
    if ctype == "application/parquet":
        return pd.concat(datas)
    if ctype == "application/torch":
        return torch.cat(datas, dim=0)  # pylint: disable=no-member
    if ctype == "application/npz":
        return sparse.vstack(datas)
    if ctype == "application/jsonl":
        return [
            cast(Any, obj)
            for arr in datas
            for obj in arr
        ]
    return cast(ByteResponse, datas)


def async_compute(
        arr: List[Any],
        start: Callable[[List[Any]], List[RT]],
        get: Callable[[RT], ByteResponse],
        check_queue: Callable[[], MinimalQueueStatsResponse],
        get_status: Callable[[List[RT]], Dict[RT, QueueStatus]],
        max_buff: int,
        block_size: int,
        num_threads: int) -> Iterable[ByteResponse]:
    assert max_buff > 0
    assert block_size > 0
    assert num_threads > 0
    arr = list(arr)
    done: List[bool] = [False]
    end_produce: List[bool] = [False]
    exc: List[Optional[BaseException]] = [None]
    cond = threading.Condition()
    ids: Dict[RT, int] = {}
    res: Dict[int, ByteResponse] = {}
    min_size_th = 20
    main_threads = 3

    def get_waiting_count(remote_queue: MinimalQueueStatsResponse) -> int:
        return remote_queue["total"] - remote_queue["active"]

    def can_push_more() -> bool:
        if exc[0] is not None:
            return True
        if len(ids) < max_buff:
            return True
        try:
            waiting_count = get_waiting_count(check_queue())
        except BaseException as e:  # pylint: disable=broad-except
            if exc[0] is None:
                exc[0] = e
            return True
        return waiting_count < max_buff

    def push(cur: List[Any], start_pos: int) -> None:
        if len(cur) <= min_size_th * block_size:
            try:
                for block_ix in range(0, len(cur), block_size):
                    ids.update({
                        cur_id: cur_ix + start_pos + block_ix
                        for (cur_ix, cur_id) in enumerate(
                            start(cur[block_ix:block_ix + block_size]))
                    })
            except BaseException as e:  # pylint: disable=broad-except
                if exc[0] is None:
                    exc[0] = e
        else:
            half_ix: int = len(cur) // 2
            args = (cur[half_ix:], start_pos + half_ix)
            push_th = threading.Thread(target=push, args=args)
            push_th.start()
            push(cur[:half_ix], start_pos)
            push_th.join()

    def produce() -> None:
        try:
            pos = 0
            while pos < len(arr):
                with cond:
                    while not cond.wait_for(can_push_more, timeout=0.1):
                        pass
                if exc[0] is not None:
                    break
                start_pos = pos
                try:
                    remote_queue = check_queue()
                except BaseException as e:  # pylint: disable=broad-except
                    if exc[0] is None:
                        exc[0] = e
                    continue
                waiting_count = get_waiting_count(remote_queue)
                add_more = max(
                    max_buff - len(ids),
                    max_buff - waiting_count)
                if add_more > 0:
                    cur = arr[pos:pos + add_more]
                    pos += len(cur)
                    push(cur, start_pos)
                with cond:
                    cond.notify_all()
        finally:
            end_produce[0] = True
            with cond:
                cond.notify_all()

    def get_one(t_ix: int, t_id: RT) -> None:
        try:
            res[t_ix] = get(t_id)
        except KeyError:
            pass
        except BaseException as e:  # pylint: disable=broad-except
            if exc[0] is None:
                exc[0] = e

    def consume() -> None:
        while not done[0]:
            with cond:
                while not cond.wait_for(
                        lambda: exc[0] is not None or done[0] or len(ids) > 0,
                        timeout=0.1):
                    pass
            do_wait = False
            while ids:
                do_wait = True
                sorted_ids = sorted(ids.items(), key=lambda v: v[1])
                lookahead = main_threads * num_threads
                check_ids = [v[0] for v in sorted_ids[0:lookahead]]
                if not check_ids:
                    continue
                status = get_status(check_ids)
                ths: List[threading.Thread] = []
                for (t_id, t_status) in status.items():
                    if t_status in ("waiting", "running"):
                        continue
                    do_wait = False
                    try:
                        t_ix = ids.pop(t_id)
                        args = (t_ix, t_id)
                        r_th = threading.Thread(target=get_one, args=args)
                        r_th.start()
                        ths.append(r_th)
                    except KeyError:
                        pass
                for r_th in ths:
                    r_th.join()
                if do_wait:
                    time.sleep(0.1)
                else:
                    with cond:
                        cond.notify_all()

    try:
        prod_th = threading.Thread(target=produce)
        prod_th.start()
        consume_ths = [
            threading.Thread(target=consume)
            for _ in range(main_threads)
        ]
        for th in consume_ths:
            th.start()
        with cond:
            cond.notify_all()
        yield_ix = 0
        while yield_ix < len(arr):
            with cond:
                while not cond.wait_for(
                        lambda: exc[0] is not None or bool(res), timeout=0.1):
                    pass
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
    finally:
        done[0] = True
    with cond:
        cond.notify_all()
    prod_th.join()
    for th in consume_ths:
        th.join()
    raise_e = exc[0]
    if isinstance(raise_e, BaseException):
        raise raise_e  # pylint: disable=raising-bad-type


class ServerSideError(Exception):
    def __init__(self, message: str) -> None:
        self._message = message
        super().__init__(self._message)

    def __str__(self) -> str:
        return f"Error from xyme backend: \n{self._message}"
