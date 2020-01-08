# pylint: disable=unused-argument, redefined-outer-name
from typing import Any, Dict, Optional, Union, Tuple

from matplotlib.figure import Figure

def figure(
        num: Optional[Union[int, str]] = None,
        figsize: Optional[Tuple[float, float]] = None,
        dpi: Optional[int] = None,
        facecolor: Optional[Any] = None,  # defaults to rc figure.facecolor
        edgecolor: Optional[Any] = None,  # defaults to rc figure.edgecolor
        frameon: bool = True,
        FigureClass: Any = Figure,
        clear: bool = False,
        **kwargs: Any,
        ) -> Figure:
    """
    Create a new figure.
    Parameters
    ----------
    num : int or str, optional
        If not provided, a new figure will be created, and the figure number
        will be incremented. The figure objects holds this number in a `number`
        attribute.
        If num is provided, and a figure with this id already exists, make
        it active, and returns a reference to it. If this figure does not
        exists, create it and returns it.
        If num is a string, the window title will be set to this figure's
        *num*.
    figsize : (float, float), default: :rc:`figure.figsize`
        Width, height in inches.
    dpi : int, default: :rc:`figure.dpi`
        The resolution of the figure in dots-per-inch.
    facecolor : color, default: :rc:`figure.facecolor`
        The background color.
    edgecolor : color, default: :rc:`figure.edgecolor`
        The border color.
    frameon : bool, default: True
        If False, suppress drawing the figure frame.
    FigureClass : subclass of `~matplotlib.figure.Figure`
        Optionally use a custom `.Figure` instance.
    clear : bool, default: False
        If True and the figure already exists, then it is cleared.
    Returns
    -------
    figure : `~matplotlib.figure.Figure`
        The `.Figure` instance returned will also be passed to
        new_figure_manager in the backends, which allows to hook custom
        `.Figure` classes into the pyplot interface. Additional kwargs will be
        passed to the `.Figure` init function.
    Notes
    -----
    If you are creating many figures, make sure you explicitly call
    `.pyplot.close` on the figures you are not using, because this will
    enable pyplot to properly clean up the memory.
    `~matplotlib.rcParams` defines the default values, which can be modified
    in the matplotlibrc file.
    """
    ...


def fill_between(
        x: Any,
        y1: Any,
        y2: Any,
        where: Optional[Any] = None,
        interpolate: bool = False,
        step: Optional[Any] = None,
        *,
        data: Optional[Any] = None,
        **kwargs: Any) -> Any:
    ...


def plot(
        *args: Any,
        scalex: bool = True,
        scaley: bool = True,
        data: Optional[Any]=None,
        **kwargs: Any) -> Any:
    ...


def show(*args: Any, **kw: Any) -> Any:
    ...


def title(
        label: str,
        fontdict: Optional[Dict[str, Any]] = None,
        loc: Optional[Any] = None,
        pad: Optional[Any] = None,
        **kwargs: Any) -> Any:
    ...


def xlabel(
        xlabel: str,
        fontdict: Optional[Dict[str, Any]] = None,
        labelpad: Optional[Any] = None,
        **kwargs: Any) -> Any:
    ...


def ylabel(
        ylabel: str,
        fontdict: Optional[Dict[str, Any]] = None,
        labelpad: Optional[Any] = None,
        **kwargs: Any) -> Any:
    ...


def xlim(*args: Any, **kwargs: Any) -> Any:
    ...


def ylim(*args: Any, **kwargs: Any) -> Any:
    ...
