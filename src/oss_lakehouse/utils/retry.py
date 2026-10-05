"""Retry com backoff exponencial e jitter — usado por toda chamada de rede do projeto.

Por que jitter: sem ele, N clientes que falharam juntos tentam de novo juntos
(thundering herd) e derrubam o servidor outra vez.
"""

from __future__ import annotations

import functools
import logging
import random
import time
from collections.abc import Callable
from typing import ParamSpec, TypeVar

P = ParamSpec("P")
R = TypeVar("R")

log = logging.getLogger(__name__)


def retry(
    exceptions: tuple[type[BaseException], ...] = (Exception,),
    attempts: int = 4,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    sleep: Callable[[float], None] = time.sleep,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorator: tenta de novo em `exceptions`, esperando base*2^n (+ jitter), até `attempts`.

    `sleep` é injetável para o teste não esperar de verdade.
    """

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            for attempt in range(1, attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    if attempt == attempts:
                        raise
                    delay = min(max_delay, base_delay * 2 ** (attempt - 1))
                    delay = random.uniform(0, delay)  # "full jitter" (AWS Architecture Blog)
                    log.warning("%s falhou (%s), tentativa %d/%d; nova em %.1fs",
                                func.__name__, exc, attempt, attempts, delay)
                    sleep(delay)
            raise AssertionError("inalcançável")

        return wrapper

    return decorator
