from collections import deque
from typing import Iterable


def consume(iterator: Iterable) -> None:
    deque(iterator, maxlen=0)
