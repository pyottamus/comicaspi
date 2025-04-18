import asyncio
from aiohttp import ClientSession, TCPConnector
from contextlib import contextmanager, closing
from typing import Any, ContextManager, Iterator
import ssl
import certifi
from partfile import PartFile 

@contextmanager
def _aiohttp_session_manager(loop) -> Iterator[ClientSession]:
    session : ClientSession = loop.run_until_complete(ClientSession(loop = loop, connector=TCPConnector(loop=loop, ssl=ssl.create_default_context(cafile=certifi.where()))).__aenter__())

    try:
        yield session
    finally:
        loop.run_until_complete(session.__aexit__(None, None, None))


@contextmanager
def _IoManager_context_manager() -> Iterator[tuple[asyncio.EventLoop, ClientSession]]:
    with closing(asyncio.new_event_loop()) as loop, _aiohttp_session_manager(loop) as session:
        yield loop, session


class IoManager:
    session: ClientSession | None
    _manager: ContextManager | None
    loop: asyncio.EventLoop
    closed: bool
    open_files: set[PartFile]
    def __init__(self):
        self._manager = _IoManager_context_manager()
        self.loop, self.session = self._manager.__enter__()
        self.get = self.session.get
        self.closed = False
        self.open_files = set()
    def __enter__(self):
        return self

    def __exit__(self,  exc_type, exc_val, exc_tb):
        assert self._manager is not None
        ret = self._manager.__exit__(exc_type, exc_val, exc_tb)
        self._manager = None
        self.session = None
        
        return ret

    def close(self):
        if self._manager is not None:
            self.__exit__(None, None, None)
    def __del__(self):
        self.close()
    
    async def get_json(self, url) -> Any:
        async with self.session.get(url) as response:
            response.raise_for_status()
            return await response.json()
    async def download(self, url, file_name, part_suffix='.part'):
        part_file = PartFile(file_name, 'wb+', part_suffix)
        self.open_files.add(part_file)
        with part_file as f:

            async with self.get(url) as r:
                r.raise_for_status()
                async for chunk in r.content.iter_any(): 
                    f.write(chunk)
        self.open_files.remove(part_file)

