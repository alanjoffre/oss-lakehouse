"""Medição de tempo: decorator `timed` e context manager `Timer`.

O decorator preserva a assinatura da função decorada (ParamSpec): o editor e o type checker
continuam sabendo quais argumentos ela aceita — sem ParamSpec, tudo viraria `*args: Any`.
"""

from __future__ import annotations

import functools
import logging
import time
from collections.abc import Callable
from types import TracebackType
from typing import Self, overload

log = logging.getLogger(__name__)


@overload
def timed[**P, R](func: Callable[P, R], /) -> Callable[P, R]:
    """Forma `@timed`, sem parênteses: recebe a função e a devolve decorada, com a mesma assinatura."""


@overload
def timed[**P, R](
    *, logger: logging.Logger | None = None, level: int = logging.INFO
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Forma `@timed(logger=..., level=...)`: devolve o decorator que será aplicado à função."""


def timed[**P, R](
    func: Callable[P, R] | None = None,
    /,
    *,
    logger: logging.Logger | None = None,
    level: int = logging.INFO,
) -> Callable[P, R] | Callable[[Callable[P, R]], Callable[P, R]]:
    """Loga a duração de cada chamada, com sucesso ou erro. Uso: `@timed` ou `@timed(level=...)`.

    O log é estruturado (campos em `extra`), para virar coluna num agregador de logs.
    """

    def decorator(fn: Callable[P, R]) -> Callable[P, R]:
        lg = logger or logging.getLogger(fn.__module__)

        @functools.wraps(fn)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            start = time.perf_counter()
            status = "error"
            try:
                result = fn(*args, **kwargs)
                status = "ok"
                return result
            finally:
                secs = time.perf_counter() - start
                lg.log(
                    level,
                    "%s %s em %.3fs",
                    fn.__qualname__,
                    status,
                    secs,
                    extra={"func": fn.__qualname__, "status": status, "duration_s": round(secs, 4)},
                )

        return wrapper

    if func is not None:  # usado como @timed, sem parênteses
        return decorator(func)
    return decorator


class Timer:
    """`with Timer() as t: ...` → `t.seconds`. Também funciona se o bloco levantar exceção."""

    def __init__(self) -> None:
        self.seconds: float = 0.0
        self._start = 0.0

    def __enter__(self) -> Self:
        self._start = time.perf_counter()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.seconds = time.perf_counter() - self._start
