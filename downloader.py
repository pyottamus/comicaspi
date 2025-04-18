from helpers import *

from asyncio import Event
from collections import deque

from pathlib import Path
from datetime import date



def mk_name_picker(name_date: bool):
    if name_date:
        return lambda comic: comic.release_date.isoformat()
    else:
        return lambda comic: comic.title
class Downloader:
    """
    Downloads Comics
    data is passed as a parameter to resolvers
    resolve_count specifies how many times .resolve(data) gets called
        before a comic is produced
    """
    __slots__ = 'data', 'resolve_count', 'resolver', 'name_date', 'orginize_by_year', 'output_dir'
    def __init__(self, data, resolve_count,
                 name_date: bool, orginize_by_year: bool,
                 output_dir: Path):
        self.data = data
        self.comic = comic
        self.name_date = name_date
        self.orginize_by_year = orginize_by_year
        self.output_dir = output_dir
    def name_picker(self):
        def _pick_title(comic):
            return comic.title
        def _pick_date(comic):
            return comic.release_date.isoformat()

        if self.name_date:
            return _pick_date
        else:
            return _pick_title

    def mk_parent_dir(self):
        root = self.output_dir
        def _mk_fake(year):
            nonlocal root
            return root

        def _mk_organize_by_year(year):
            nonlocal root
            path = root / str(year)
            path.mkdir(exist_ok=True)
            return path

        if self.orginize_by_year:
            return _mk_organize_by_year
        else:
            return _mk_fake
    async def mk_resolver(self):

        data = self.data
        resolve_count = self.resolve_count

        async def _resolver(fn):
            nonlocal data, resolve_count
            for _ in range(self.resolve_count):
                fn = await fn.resolve(data)
            return fn

        return _resolver
    def mk_downloader(self):
        name_fn = self.name_picker()
        mk_parent = mk_parent_dir()


    def download_year_chunks(self, it):
        it = iter(it)
        resolver = self.mk_resolver()
        name_fn = self.name_picker()
        data = self.data
        def wrap_it(it, parent_dir):
            nonlocal resolver, name_fn, data
            async def download():
                nonlocal parent_dir, resolver, name_fn, data
                comic = await resolver(fn)
                return await comic.download(parent_dir / name_fn(comic), data)
                
            return (download(fn) for fn in it)

        parent_dir_fn = mk_parent_dir()

        def next_it_callback():
            nonlocal it, parent_dir_fn, wrap_it
            try:
                val = next(it)
            except StopIteration:
                return None
            root = parent_dir_fn(val[0].year)
            return wrap_it(val, root)
        try:
            ccl = ConcurentChainLimiter(self.data.io_man.loop, (), next_it_callback=next_it_callback)
        except StopIteration:
            return
        
        
        for elem in it:
            elem = iter(elem)
            
