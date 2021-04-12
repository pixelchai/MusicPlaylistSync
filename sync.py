import sqlite3
import os
import argparse

DEBUG = False
DEBUG_PLAYLIST_ID = "PL-VqEBG5SiA2oBlTXeCzi4bPSi3GivOoq"

parser = argparse.ArgumentParser()

def row_factory(*args, **kwargs):
    """
    allows for the database cursor to yield dictionary objects, which are easier to use
    """
    return dict(sqlite3.Row(*args, **kwargs))

# Songs: pk, accoustic hash, youtube id, file path, (md5 [or similar] hash)
class Database:
    PATH = "mps.db"
    CURRENT_VERSION = "0.2.0"

    def __init__(self, overwrite, trace):
        existed = os.path.isfile(Database.PATH)

        if overwrite and existed:
            os.remove(Database.PATH)
            print("Overwrite flag so deleted existing db")
            existed = False

        self._conn = sqlite3.connect(Database.PATH)
        self._c = self._conn.cursor()

        if trace:
            self._conn.set_trace_callback(print)

        self._c.execute("PRAGMA foreign_keys = ON")
        self._c.row_factory = row_factory

        if not existed:
            self._init_schema()

    def __enter__(self):
        return self

    def close(self):
        self._conn.close()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _init_schema(self):
        self._c.executescript("""
        CREATE TABLE IF NOT EXISTS Meta (
            id	INTEGER PRIMARY KEY NOT NULL UNIQUE DEFAULT 1,
            version TEXT,
            playlist_id TEXT
        );
        
        CREATE TABLE IF NOT EXISTS Songs (
            id INTEGER PRIMARY KEY NOT NULL UNIQUE,
            fingerprint TEXT UNIQUE,
            hash TEXT UNIQUE,
            youtube_id TEXT UNIQUE,
            filepath TEXT UNIQUE,
            duration REAL
        );
        """)
        self._c.execute("INSERT OR REPLACE INTO Meta(version) "
                        "VALUES (?)", (Database.CURRENT_VERSION,))
        self._conn.commit()
        print("Schema initialised")

    def set_playlist_id(self, playlist_id):
        self._c.execute("UPDATE Meta SET playlist_id=?", (playlist_id,))
        self._conn.commit()

    def get_playlist_id(self):
        for row in self._c.execute("SELECT playlist_id FROM Meta WHERE id=1"):
            return row.get('playlist_id', None)

class Downloader:
    def __init__(self, playlist_id, **kwargs):
        self.db = Database(**kwargs)

        if playlist_id is not None:
            self.db.set_playlist_id(playlist_id)
            print("Set playlist id from command line!")

        if self.db.get_playlist_id() is None:
            if DEBUG:
                self.db.set_playlist_id(DEBUG_PLAYLIST_ID)
                print("Set to debug playlist id")
            else:
                print("The playlist id has not been set! "
                      "Please provide a youtube playlist id")
                print(parser.format_help())
                exit(-1)


    def __enter__(self):
        return self

    def close(self):
        self.db.close()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def main():
    parser.add_argument('playlist_id', default=None, nargs='?',
                        help="youtube playlist id")
    parser.add_argument('-x', '--overwrite', default=False, action='store_true',
                        help="whether to overwrite the database, if it exists")
    parser.add_argument('-t', '--trace', default=False, action='store_true',
                        help="trace SQL commands")
    args = vars(parser.parse_args())
    with Downloader(**args) as d:
        pass


if __name__ == '__main__':
    main()