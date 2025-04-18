from datetime import date
from pathlib import Path
import struct
from dateutil import *

from baseComicClasses import BaseComic
from transactions import *

app_id = 1348027458  # big endian b'PYDB'
db_version = 0

""" CREATE TABLE year_chunks (
        year INTEGER PRIMARY KEY,
        url  TEXT NOT NULL,
        loaded INTEGER NOT NULL,
        complete INTEGER NOT NULL,
        count INTEGER NOT NULL
    );


    CREATE TABLE status_enum (
        status INTEGER PRIMARY KEY,
        desc TEXT NOT NULL
    );


    INSERT INTO status_enum VALUES
      (0,  'NOT_STARTED'),
      (1,  'PARTIAL'),
      (2,  'PARTIAL_DONE'),
      (3,  'CONV_PARTIAL'),
      (4,  'CONV_DONE'),
      (5,  'EXIF_PARTIAL'),
      (6,  'EXIF_DONE'),
      (7,  'RENAME_DONE'),
      (8,  'COMPLETE');

    
    CREATE TABLE release_progress (
        release_date INTEGER PRIMARY KEY,
        status INTEGER NOT NULL,        
        download_or_page_url TEXT NOT NULL,
        
        FOREIGN KEY(release_date) REFERENCES comics(release_date)
        FOREIGN KEY(status) REFERENCES status_enum(status)
    );
"""


def _create_db(db):
    db.executescript(f"""
    PRAGMA application_id = {app_id};
          
    CREATE TABLE comics (
        id INTEGER PRIMARY KEY,
        release_date INTEGER NOT NULL,
        title TEXT NOT NULL,
        suffix TEXT,
        dup_suffix INTEGER NOT NULL,
        complete INTEGER NOT NULL
    );

    CREATE TABLE core_info (
        id INTEGER PRIMARY KEY,
        name_date INTEGER NOT NULL,
        organize_by_year INTEGER NOT NULL
    );
    
    """)


class ComicaspiDB:
    __slots__ = 'db', 'already_created', 'already_initialized', 'initialized'

    @property
    def version(self):
        return self.db.getone('SELECT version FROM core_info WHERE id = 0')

    @property
    def application_id(self):
        return self.db.getone('PRAGMA application_id')

    @property
    def active(self):
        return self.db.getone("SELECT active FROM core_info WHERE id = 0")

    @property
    def mode(self):
        return self.db.getone("SELECT mode from active_session where id = 0")

    @property
    def core_info(self):
        res = self.db.execute(f"SELECT name_date, organize_by_year FROM core_info WHERE id=0").fetchone()

        if res is None:
            return None
        return bool(res[0]), bool(res[1])

    def init_db(self, name_date: bool, organize_by_year: bool):
        self.db.execute(f"INSERT INTO core_info VALUES (0, ?, ?)", (name_date, organize_by_year))
        self.initialized = True

    def check_init(self):
        self.initialized = bool(self.db.execute("SELECT count(id) FROM core_info WHERE id = 0").fetchone()[0])
        return self.initialized

    def start_comic_if_not_complete(self, comic):
        with self.db.begin():
            res = self.db.execute("SELECT complete FROM comics WHERE release_date = ?",
                                  (serialize_date(comic.release_date),)).fetchone()
            if res is None:
                self.db.execute("INSERT INTO comics VALUES (?, ?, ?, NULL, 0)",
                                (*gen_key_serialize(comic.release_date, comic.secondary), comic.title))
                return True
            res = res[0]
            if res == 0:
                return True
            else:
                return False

    def complete_comic(self, comic, suffix):
        self.db.execute("UPDATE comics SET complete = 1, suffix = ? WHERE release_date = ?",
                        (suffix, serialize_date(comic.release_date)))

    def start_yc_pages_if_not_complete(self, year, pages):
        start_date = serialize_date(date(year, 1, 1))
        end_date = serialize_date(date(year, 12, 31))

        db = self.db
        page_data = [(*gen_key_serialize(page.release_date, page.secondary), page) for page in pages]
        page_data.sort()  # this sort is important. It ensures dup_suffix (column 'dup_suffix') is consistent
        hits = dict()
        for key, release_date, page in page_data:
            if (val := hits.get(release_date)) is None:
                hits[release_date] = page
                continue
            elif val.__class__ is int:
                page.dup_suffix = val
                hits[release_date] = val + 1
            else:
                val.dup_suffix = 1
                page.dup_suffix = 2
                hits[release_date] = 3
        del hits

        with db.begin():
            prev_releases = db.execute(
                "SELECT id, complete FROM comics WHERE release_date BETWEEN ? AND ? ORDER BY id ASC",
                (start_date, end_date)).fetchall()
            hits = {key for key, _ in prev_releases}
            complete = {key for key, complete in prev_releases if complete == 1}
            to_insert = [(key, sdate, page.title, page.dup_suffix or 0) for key, sdate, page, in page_data if
                         key not in hits]
            out = [page for key, _, page in page_data if key not in complete]
            db.executemany("INSERT INTO comics VALUES (?, ?, ?, NULL, ?, 0)", to_insert)

            return out

    def comic_completed(self, comic: BaseComic):
        res = self.db.execute("SELECT complete from comics where id = ?", (gen_key(comic.release_date, comic.secondary),)).fetchone()
        if res is None:
            return None
        return bool(res[0])

    def __init__(self, path: Path):
        db = sqlite3.connect(path, autocommit=True)
        self.db = Transactor(db)

        with self.db.begin_exclusive() as db:
            cur_app_id = self.application_id
            if cur_app_id == 0:
                self.already_created = False
                self.already_initialized = False
                self.initialized = False
                _create_db(db)
            elif cur_app_id == app_id:
                # print(db.connection.in_transaction)                
                self.already_initialized = self.check_init()
            else:
                self.db.close()
                val = struct.pack('>i', cur_app_id)
                raise RuntimeError(f"Database already initialized with application_id {val}({cur_app_id})")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.db.close()
        except:
            pass


if __name__ == '__main__':
    path = Path("../test")
    mode = 'download_years'
    db = ComicaspiDB(path / 'comic.db')
    # CoreInfo(True, True)
    # db, existed = open_init_db(path / 'cominc.db', mode)
