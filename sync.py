import sqlite3
import os
import argparse
import logging
import subprocess
import json
import shlex
import hashlib
import itertools
from mutagen.id3 import ID3
from pathlib import Path

DEBUG = True
DEBUG_PLAYLIST_ID = "PL-VqEBG5SiA2oBlTXeCzi4bPSi3GivOoq"
AUDIO_EXTENSIONS = ["mp3", "wav", "flac", "aac", "ogg", "wma"]

logger = logging.getLogger('MusicPlaylistSync')
parser = argparse.ArgumentParser()

def row_factory(*args, **kwargs):
    """
    allows for the database cursor to yield dictionary objects, which are easier to work with
    """
    return dict(sqlite3.Row(*args, **kwargs))

def afcache(func):
    def wrapper(self, *args, **kwargs):
        key = func.__name__

        if key not in self._cache:
            # logger.debug(f"AudioFile: Running \"{key}\"")
            self._cache[key] = func(self, *args, **kwargs)

        # logger.debug(f"AudioFile: Got \"{key}\"")
        return self._cache[key]
    return wrapper

class AudioFile:
    def __init__(self, path):
        self.path = path
        self._cache = {}


    @staticmethod
    def _run(cmd, stdin=None):
        logger.debug("COMMAND: " + cmd)
        try:
            result = subprocess.run(cmd, input=stdin, shell=True, stdout=subprocess.PIPE)
            return result.stdout
        except Exception as ex:
            logger.exception("Error running the following command: " + cmd, exc_info=ex)

    @property
    @afcache
    def fingerprint(self):
        result = json.loads(self._run("fpcalc -json " + shlex.quote(self.path)))
        self._cache.update(result)
        return result["fingerprint"]

    @property
    @afcache
    def duration(self):
        result = self._run("ffprobe -v error "
                           "-show_entries format=duration "
                           "-of default=noprint_wrappers=1:nokey=1 "
                           + shlex.quote(self.path))
        return round(float(result) * 100) / 100

    @property
    @afcache
    def rating(self):
        song_file = ID3(self.path)
        for popm in song_file.getall('POPM'):
            return round(popm.rating / 255 * 10)/10
        return None

    @property
    @afcache
    def hash(self):
        # https://stackoverflow.com/a/44873382
        h = hashlib.sha256()
        b = bytearray(128 * 1024)
        mv = memoryview(b)
        with open(self.path, 'rb', buffering=0) as f:
            for n in iter(lambda: f.readinto(mv), 0):
                h.update(mv[:n])
        return h.hexdigest()

class Database:
    PATH = "mps.db"
    CURRENT_VERSION = "0.3.0"

    def __init__(self, overwrite, trace):
        existed = os.path.isfile(Database.PATH)

        if overwrite and existed:
            os.remove(Database.PATH)
            logger.warning("Overwrite flag so deleted existing db")
            existed = False

        self._conn = sqlite3.connect(Database.PATH)
        self._c = self._conn.cursor()

        if trace:
            self._conn.set_trace_callback(logger.debug)

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
            duration REAL,
            rating REAL
        );
        """)
        self._c.execute("INSERT OR REPLACE INTO Meta(version) "
                        "VALUES (?)", (Database.CURRENT_VERSION,))
        self._conn.commit()
        logger.info("Schema initialised")

    def set_playlist_id(self, playlist_id):
        self._c.execute("UPDATE Meta SET playlist_id=?", (playlist_id,))
        self._conn.commit()

    def get_playlist_id(self):
        for row in self._c.execute("SELECT playlist_id FROM Meta WHERE id=1"):
            return row.get('playlist_id', None)

    def _single_row(self, sql, params):
        row_generator = self._c.execute(sql, params)
        for row in self._c.execute(sql, params):
            return row

    def get_song_id(self, key_name, value):
        for row in self._c.execute(f"SELECT id FROM Songs WHERE \"{key_name}\"=?", (value,)):
            return row.get('id', None)

    def add_song(self, data):
        column_names = ",".join((f"\"{x}\"" for x in data.keys()))
        placeholders = ",".join(("?" for _ in range(len(data))))
        self._c.execute(f"INSERT INTO Songs({column_names}) VALUES({placeholders})", tuple(data.values()))

    def delete_song(self, id_to_delete):
        self._c.execute("DELETE FROM Songs WHERE id=?", (id_to_delete,))

    def get_songs(self):
        return self._c.execute("SELECT * FROM Songs")

    def commit(self):
        self._conn.commit()

class Downloader:
    def __init__(self, playlist_id, **kwargs):
        self.db = Database(**kwargs)

        if playlist_id is not None:
            self.db.set_playlist_id(playlist_id)
            logger.info("Set playlist id from command line!")

        if self.db.get_playlist_id() is None:
            if DEBUG:
                self.db.set_playlist_id(DEBUG_PLAYLIST_ID)
                logger.warning("Set to debug playlist id")
            else:
                logger.warning("The playlist id has not been set! "
                               "Please provide a youtube playlist id.")
                print(parser.format_help())
                exit(-1)

    def __enter__(self):
        return self

    def close(self):
        self.db.close()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def verify_filepaths(self):
        logger.info("Verifying database filepaths...")

        ids_to_delete = []
        for song_row in self.db.get_songs():
            path = Path(song_row["filepath"])

            if not path.is_file():
                ids_to_delete.append(song_row["id"])
                logger.info("Deleting row for nonexistent filepath: " + str(path))

        for id_to_delete in ids_to_delete:
            self.db.delete_song(id_to_delete)

        self.db.commit()
        logger.info("Verifying database filepaths done!")

    def index_filesystem(self):
        logger.info("Indexing filesystem...")

        for file in itertools.chain(*(Path().rglob("*." + audio_ext) for audio_ext in AUDIO_EXTENSIONS)):

            filepath = file.relative_to(".").as_posix()
            existing_id = self.db.get_song_id("filepath", filepath)

            if existing_id is None:
                af = AudioFile(str(file))
                try:
                    self.db.add_song({
                        "fingerprint": af.fingerprint,
                        "hash": af.hash,
                        "filepath": filepath,
                        "duration": af.duration,
                        "rating": af.rating
                    })
                    logger.debug("Inserted: " + str(file))
                except sqlite3.IntegrityError as ex:
                    logger.warning("Skipped inserting exceptional file: " + str(file) + ". Reason: " + str(ex))

        self.db.commit()
        logger.info("Indexing filesystem done!")

    def pull(self):
        logger.info("pulling...")
        logger.info("pulling done!")


def main_setup():
    # logging
    logger.setLevel(logging.DEBUG if DEBUG else logging.INFO)

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)s: \t%(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')
    ch.setFormatter(formatter)

    if not logger.hasHandlers():
        logger.addHandler(ch)

    # argument parsing
    parser.add_argument('playlist_id', default=None, nargs='?',
                        help="youtube playlist id")
    parser.add_argument('-x', '--overwrite', default=False, action='store_true',
                        help="whether to overwrite the database, if it exists")
    parser.add_argument('-t', '--trace', default=DEBUG, action='store_true',
                        help="trace SQL commands")

def main():
    main_setup()
    args = vars(parser.parse_args())

    with Downloader(**args) as d:
        d.verify_filepaths()
        d.index_filesystem()


if __name__ == '__main__':
    main()
