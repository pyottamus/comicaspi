from pathlib import Path
from abc import ABC, abstractmethod
from datetime import date
from typing import Any

from iomanager import IoManager
from urllib.parse import urlparse
from helpers import *

type Url = str
type DownloadUrl = Url

class DownloadOptions:
    __slots__ = 'name_date', 'organize_by_year', 'out_path'
    name_date: bool
    organize_by_year: bool
    out_path: Path
    def __init__(self, name_date: bool, organize_by_year: bool, out_path: Path):
        self.name_date = name_date
        self.organize_by_year = organize_by_year
        self.out_path = out_path
        
class BoundDataClass:
    __slots__ = 'dopts', 'io_man', 'data'
    dopts: DownloadOptions
    io_man: IoManager
    data: Any
    def __init__(self, dopts: DownloadOptions,
                 io_man: IoManager, *args, **kwargs):
        self.dopts = dopts
        self.io_man = io_man
        self.__post_init__(*args, **kwargs)
    def __post_init__(self, *args, **kwargs):
        pass

class Resolvable(ABC):
    __slots__ = ()

    def __post_init__(self, *args, **kwargs):
        pass

    @abstractmethod
    async def resolve(self, data: BoundDataClass) -> Any:
        ...

class BaseComic:
    __slots__ = 'secondary', 'release_date', 'title', 'dup_suffix', 'url'
    release_date: date
    title: str
    secondary: int
    dup_suffix: int | None
    url: DownloadUrl
    
    def __init__(self, release_date, title, secondary, dup_suffix, url):
        
        self.release_date = release_date
        
        self.title = title
        self.secondary = secondary
        self.dup_suffix = dup_suffix
        self.url = url
        self.__post_init__()

    @property
    def dname(self):
        name = f'{self.release_date}'
        if self.dup_suffix is not None:
            name += f'({self.dup_suffix})'
        return name
    @classmethod
    def fromrelease(cls, release, url):
        return cls(release.release_date,  release.title, release.secondary, release.dup_suffix, url)
    def __repr__(self):
        return f'{type(self).__name__}({self.release_date}, {self.title}, {self.url})'
    
    def __post_init__(self):
        pass
    def get_url_path_suffix(self):
        return get_url_path_suffix(self.url)
    def get_file_name(self, path, name_date):
        if name_date:
            name = self.release_date.isoformat()
        else:
            name = self.title
        ext = self.get_url_path_suffix()

        if self.dup_suffix is not None:
            name +=  f'({self.dup_suffix})'
        name += ext

        return path / name
    

class BasePage(Resolvable, ABC):
    __slots__ = 'url', 'secondary', 'dup_suffix'
    secondary: int
    dup_suffix : int | None
    url : Url
    
    def __init__(self, secondary, url, dup_suffix = None):
        self.secondary = secondary
        self.url = url
        self.dup_suffix = dup_suffix
        self.__post_init__()

    @abstractmethod
    async def resolve(self, data: BoundDataClass) -> BaseComic:
        ...

class BaseTitlePage(BasePage, ABC):
    __slots__ = 'title',
    title : str

    def __init__(self, title, secondary, url,  dup_suffix = None):
        self.title = title
        super().__init__(secondary, url, dup_suffix)

class BaseReleasePage(BaseTitlePage, ABC):
    __slots__ = 'release_date',
    release_date : date

    @property
    def dname(self):
        name = f'{self.release_date}'
        if self.dup_suffix is not None:
            name += f'({self.dup_suffix})'
        return name
    
    def __repr__(self):
        return f'{type(self).__name__}({self.release_date}, {self.title}, {self.url})'
    
    def __init__(self, release_date, title, secondary, url, dup_suffix=None):
        self.release_date = release_date
        super().__init__(title, secondary, url, dup_suffix=None)

class BaseYearChunk(Resolvable, ABC):
    __slots__ = 'year', 'url'
    year: int
    url : Url

    def __init__(self, year, url):
        self.year = year
        self.url = url
        self.__post_init__()
    def __repr__(self):
        return f'{self.__class__.__name__}({self.year}, {self.url})'
    @abstractmethod
    async def resolve(self, data: BoundDataClass) -> list[BasePage]:
        ...

