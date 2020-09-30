# pylint: disable=redefined-builtin,unused-argument
from typing import Any, Tuple


Size: Any
Tensor: Any

load: Any
save: Any
stack: Any

cat: Any


def sum(
        input: Tensor,
        dim: Tuple[int, ...],
        keepdim: bool = False,
        dtype: Any = None) -> Tensor:
    ...
