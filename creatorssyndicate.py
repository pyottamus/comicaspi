from asyncio import CancelledError

from baseComicClasses import *
from helpers import *
from typing import ClassVar
import lxml.etree
from comicaspidb import ComicaspiDB

#### CHANGE THESE ####
START_YEAR = 2025
STOP_YEAR = 2025
name_date=True
organize_by_year=True
out_path=Path('wizard_of_id2')
title = "Wizard of Id"
feature_id=149
######################

class ComicDownloadError(Exception):
    pass

class CreatorsSyndicateData(BoundDataClass):
    __slots__ = 'feature_name', 'feature_id', '_get_release_dates_uri_base' 
    _get_release_dates_api_uri : ClassVar[str] = "https://www.creators.com/api/features/get_release_dates"

    @classmethod
    def _generate_get_release_dates_uri_base(cls, feature_id: int):
            return f'{cls._get_release_dates_api_uri}?feature_id={feature_id}&year='

    feature_name: str
    feature_id: int
    _get_release_dates_uri_base : str

    def __post_init__(self, feature_name, feature_id):
        self.feature_name = feature_name
        self.feature_id = feature_id        
        self._get_release_dates_uri_base = self._generate_get_release_dates_uri_base(self.feature_id)

    def get_release_dates_uri(self, year: int):
        return f'{self._get_release_dates_uri_base}{year}'


class _PullComicMetaExtractor:
    test = ("html", "head")
    def _init_reset(self):
        self.done = False
        self.ret = None
        self.anti = None
        self.anti_count = 0
        self.state = 0
        
    def __init__(self):
        self._init_reset()
        
    def start(self, tag, attrib):
        if self.done:
            return
        
        if self.state < 2:
            if tag == self.test[self.state]:
                self.state += 1
            else:
                self.done = True
                self.state = -1
        elif self.state == 2:
            if tag != 'meta' \
               or (prop := attrib.get('property')) is None \
               or prop != 'og:image':
                self.anti = tag
                self.anti_count = 1
                self.state = 3
                return

            self.state = 4
            self.done = True
            self.ret = attrib['content']
        else: # self.state == 3
            if tag == self.anti:
                self.anti_count += 1
        

    def end(self, tag):
        if self.done:
            return

        if self.state != 3:
            raise RuntimeError("hit end tag when state was not 3", tag)

        if tag == self.anti:
            self.anti_count -= 1
            if self.anti_count == 0:
                self.anti = None
                self.state = 2
        


    def close(self):
        retval = self.ret if self.state == 4 else None
        self._init_reset()
        return retval

class ComicMetaImageExtractor:
    def cleanup(self):
        return self.parser.close()
    def __init__(self):
        self._pull = _PullComicMetaExtractor()
        self.parser = lxml.etree.HTMLPullParser(target=self._pull)
    def pump(self, text):
        self.parser.feed(text)
        if self._pull.done:
            ret = self.cleanup()
            if ret is None:
                raise ValueError(ret)
            return ret
        else:
            return None

    def pump_from(self, it):
        pump = self.pump
        for chunk in it():
            if (ret := pump(chunk)) is not None:
                return ret

    async def async_pump_from(self, it):
        pump = self.pump
        async for chunk in it():
            if (ret := pump(chunk)) is not None:
                return ret

class CreatorsSyndicateComic(BaseComic):
    __slots__ = ()
    pass

class CreatorsSyndicateReleasePage(BaseReleasePage):
    __slots__ = ()
    async def resolve(self, data: CreatorsSyndicateData) -> CreatorsSyndicateComic:
        extractor = ComicMetaImageExtractor()
        async with data.io_man.get(self.url) as response:
            response.raise_for_status()
            download_url = await extractor.async_pump_from(response.content.iter_any)
            return CreatorsSyndicateComic.fromrelease(self, download_url)

def map_creators_args(release: str, title: str, url: str):
    i = url.rfind('/')
    if i == -1:
        raise RuntimeError("Could not find secondary key")
    secondary = int(url[i+1:])
    if secondary > (1 << 32):
        raise RuntimeError(f"Secondary key ({secondary}) is larger than 32 bits")
    
    

    return date.fromisoformat(release), title, secondary, url
    
class CreatorsSyndicateYearChunk(BaseYearChunk):
    __slots__ = ()
    async def resolve(self, data: CreatorsSyndicateData) -> list[CreatorsSyndicateReleasePage]:
        return [CreatorsSyndicateReleasePage(*map_creators_args(**x)) for x in await data.io_man.get_json(self.url)]

class CreatorsSyndicateChunkYearGenerator:
    __slots__ = 'data',
    def __init__(self, data):
        self.data = data
    @property
    def publication_name(self):
        return self.data.feature_name

    def get_year(self, year) -> CreatorsSyndicateYearChunk:
        return CreatorsSyndicateYearChunk(year, self.data.get_release_dates_uri(year))

import traceback
class Downloader:
    __slots__ = 'ycgen', 'data', 'name_date', 'organize_by_year', 'out_path', 'range_is_inf', 'climit', 'concurrent_limit', 'db', 'running', 'fail'
    
    @classmethod
    def Downloader(cls, feature_name, feature_id, start_year, stop_year, name_date, organize_by_year, out_path):
        range_is_inf, it_range, it_str = reverse_year_range_maker(start_year, stop_year)
        print(f"Downloading comics in range {it_str}")

        with IoManager() as io_man:
            download_opts = DownloadOptions(name_date, organize_by_year, out_path)
            data = CreatorsSyndicateData(download_opts, io_man, feature_name, feature_id)

            ycgen = CreatorsSyndicateChunkYearGenerator(data).get_year
            CreatorsSyndicateChunkYearGen = (ycgen(year) for year in it_range)

            data.dopts.out_path.mkdir(exist_ok=True)

            with ComicaspiDB(out_path / 'comics.db') as db:
                if db.already_initialized:
                    assert db.core_info == (name_date, organize_by_year)
                else:
                    db.init_db(name_date, organize_by_year)

                downloader = cls(CreatorsSyndicateChunkYearGen, data, range_is_inf, db)
                dl_task = io_man.loop.create_task(downloader.download())
                try:
                    io_man.loop.run_until_complete(dl_task)
                except KeyboardInterrupt:
                    dl_task.cancel()
                    io_man.loop.run_until_complete(dl_task)
    def __init__(self, ycgen, data, range_is_inf, db, concurrent_limit=16):
        self.data = data
        self.organize_by_year = data.dopts.organize_by_year
        self.name_date = data.dopts.name_date
        self.out_path = data.dopts.out_path
        self.range_is_inf = range_is_inf
        self.concurrent_limit = concurrent_limit
        self.climit = Semaphore(concurrent_limit)
        self.running = set()
        self.db = db
        self.fail = []
        self.ycgen = ycgen
    async def _yc_resolver(self, ycgen):
        data = self.data
        for elem in ycgen:
            async with self.climit:
                yield elem, await elem.resolve(data)
    def yc_resolver(self, ycgen):
        gen = self._yc_resolver(ycgen)
        if self.range_is_inf:
            gen = async_done_deducer(gen, cmp = lambda x: bool(x[1]))
        return gen
    
                
    async def download_comic_from_page(self, out_dir: Path, page: CreatorsSyndicateReleasePage):
        data = self.data
        try:
            comic = await page.resolve(data)
            name = comic.get_file_name(out_dir, data.dopts.name_date)

            await data.io_man.download(comic.url, name)
            return comic, name
        except Exception as e:
            raise ComicDownloadError(page) from e

    def download_comic_from_page_callback(self, future):
        
        self.climit.release()
        try:
            comic, name = future.result()
            self.db.complete_comic(comic, name.suffix)
            print(f"Completed {comic.dname}")
        except ComicDownloadError as e:
            print(f"Failed to download comic {e.args[0].dname}")
            traceback.print_exception(e)
            self.fail.append(e)

    async def finish(self):
    
        for task in list(self.running):
            try:
                await task
            except:
                pass
    async def cancel(self):
        for task in self.running:
            task.remove_done_callback(self.download_comic_from_page_callback)
            task.cancel()
            try:
                await task
            except:
                pass
    async def download(self):
        try:
            await self._download()
        except (CancelledError, KeyboardInterrupt):
            print("Canceling")
            await self.cancel()

    async def _download(self):
        acquire = self.climit.acquire
        root = self.data.dopts.out_path
        organize_by_year = self.organize_by_year
        create_task = self.data.io_man.loop.create_task
        download_comic_from_page = self.download_comic_from_page
        download_comic_from_page_callback = self.download_comic_from_page_callback
        
        async for yc, pages in self.yc_resolver(self.ycgen):
            try:
                pages = self.db.start_yc_pages_if_not_complete(yc.year, pages)
            except Exception as e:
                print(f"FAILED TO DOWNLOAD YEAR {yc.year}\n\n\n")
                traceback.print_exc()
                print('\n\n\n')
                print(pages)
                print('\n\n\n')
                continue
            if not pages:
                print(f"Year {yc.year} already downloaded")
                continue
            print(f"\n####Starting year {yc.year}####\n")

            out_dir = root
            if organize_by_year:
                out_dir = out_dir / str(yc.year)
                out_dir.mkdir(exist_ok=True)

            pages.sort(reverse=True, key=lambda x: x.release_date)
            for page in pages:
                
                await acquire()
                #print(f"Stating {page.release_date}")
                task = create_task(download_comic_from_page(out_dir, page))
                
                self.running.add(task)
                task.add_done_callback(download_comic_from_page_callback)
                
        await self.finish()

        print("Finished")

if __name__ == '__main__':
    Downloader.Downloader(title, feature_id, START_YEAR, STOP_YEAR, name_date, organize_by_year, out_path)
