from collections import deque


def consume(iterator):
    deque(iterator, maxlen=0)
