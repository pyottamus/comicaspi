import asyncio
from datetime import date
from typing import Any
from urllib.parse import urlparse
import itertools as it
from collections.abc import AsyncIterable, Callable


def THIS_YEAR():
    return date.today().year

def get_path_suffix(path: str):
    i = path.rfind('.')
    if i == -1:
        return ''
    return path[i:]

def get_url_path_suffix(url: str):
    return get_path_suffix(urlparse(url).path)

def reverse_year_range_maker(START_YEAR, STOP_YEAR):
    if START_YEAR is None:
        START_YEAR, STOP_YEAR = STOP_YEAR, START_YEAR

    range_is_inf = False
    if STOP_YEAR is None:
        if START_YEAR is None:
            raise RuntimeError("Both START_YEAR and STOP_YEAR are None")

        if START_YEAR == THIS_YEAR():
            it_range = it.count(START_YEAR, -1)
            it_str = f"(-∞, {START_YEAR}]"
            range_is_inf = True
        elif START_YEAR > THIS_YEAR():
            raise RuntimeError(f"Cannot download comics from the future {START_YEAR}")
        else:
            it_range = range(THIS_YEAR(), START_YEAR - 1, -1)
            it_str = f"[{START_YEAR}, {THIS_YEAR()}]"
    else:
        if STOP_YEAR > START_YEAR:
            START_YEAR, STOP_YEAR = STOP_YEAR, START_YEAR

        if START_YEAR > THIS_YEAR():
            raise RuntimeError(f"Cannot download comics from the future {START_YEAR}")

        it_range = range(START_YEAR, STOP_YEAR - 1, -1)
        it_str = f"[{STOP_YEAR}, {START_YEAR}]"

    return range_is_inf, it_range, it_str

class Semaphore(asyncio.Semaphore):
    def take_all(self):
        if not self.locked():
            prev_value = self._value
            self._value = 0
            return prev_value

        return 0
    def try_acquire(self):
        """Acquire a semaphore.

        If the internal counter is larger than zero on entry,
        decrement it by one and return True. Otherwise, return
        False
        """
        
        if not self.locked():
            # Maintain FIFO, wait for others to start even if _value > 0.
            self._value -= 1
            return True
        return False


def done_deducer(it, limit=10, cmp=bool, discard_callback=None):
    stack = []
    for elem in it:
        if cmp(elem):
            if stack:
                yield from stack
                stack.clear()
            yield elem
                
        else:
            stack.append(elem)
            if len(stack) == limit:
                if discard_callback is not None:
                    discard_callback(stack)
                break
    else:
        yield from stack

async def async_done_deducer(ait, limit=10, cmp=Callable[[Any], bool], discard_callback=None):
    stack = []
    async for elem in ait:
        if cmp(elem):
            for val in stack:
                yield val
            stack.clear()
            yield elem
        else:
            stack.append(elem)
            if len(stack) == limit:
                if discard_callback is not None:
                    discard_callback(stack)
                await ait.aclose()
                break
    else:
        for val in stack:
            yield val

async def async_map_done_deducer(gen, fn, limit=10, cmp=bool):
    stack = []
    for elem in gen:
        res = await fn(elem)
        if cmp(res):
            for prev in stack:
                yield prev

            stack.clear()
            yield elem, res
        else:
            stack.append((elem, res))
            if len(stack) == limit:
                break
            

    


def async_iter_wrapper(it):
    if isinstance(it, AsyncIterable):
        return it
    else:
        async def wrapper():
            nonlocal it
            for elem in it:
                yield elem
        return wrapper()
