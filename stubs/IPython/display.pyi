# pylint: disable=unused-argument,no-self-use


class ProgressBar:
    def __init__(self, total: int) -> None:
        ...

    def display(self) -> None:
        ...

    def update(self) -> None:
        ...

    def _set_progress(self, progress: int) -> None:
        ...

    def _get_progress(self) -> int:
        ...

    progress = property(_get_progress, _set_progress)
