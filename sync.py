import sqlite3
import os
import argparse
import logging
import subprocess
import json
import shlex
import hashlib
import itertools
import tempfile
import unicodedata
import shutil
import re
from pathlib import Path
from youtube_dl import YoutubeDL
from mutagen.id3 import ID3
from Levenshtein import distance

DEBUG = False
AUDIO_EXTENSIONS = ["mp3", "wav", "flac", "aac", "ogg", "wma"]
DIR_OUTPUT = "mps"
FINGERPRINT_SIMILARITY_THRESH = 10

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

class Utils:
    @staticmethod
    def run(cmd, stdin=None):
        logger.debug("COMMAND: " + cmd)
        try:
            result = subprocess.run(cmd, input=stdin, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            return result.stdout
        except Exception as ex:
            logger.exception("Error running the following command: " + cmd, exc_info=ex)

    @staticmethod
    def sanitise_filename(filename):
        # from: https://gitlab.com/jplusplus/sanitize-filename
        """Return a fairly safe version of the filename.

        We don't limit ourselves to ascii, because we want to keep municipality
        names, etc, but we do want to get rid of anything potentially harmful,
        and make sure we do not exceed Windows filename length limits.
        Hence a less safe blacklist, rather than a whitelist.
        """
        blacklist = ["\\", "/", ":", "*", "?", "\"", "<", ">", "|", "\0"]
        reserved = [
            "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5",
            "COM6", "COM7", "COM8", "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5",
            "LPT6", "LPT7", "LPT8", "LPT9",
        ]  # Reserved words on Windows
        filename = "".join(c for c in filename if c not in blacklist)
        # Remove all charcters below code point 32
        filename = "".join(c for c in filename if 31 < ord(c))
        filename = unicodedata.normalize("NFKD", filename)
        filename = filename.rstrip(". ")  # Windows does not allow these at end
        filename = filename.strip()
        if all([x == "." for x in filename]):
            filename = "__" + filename
        if filename in reserved:
            filename = "__" + filename
        if len(filename) == 0:
            filename = "__"
        if len(filename) > 255:
            parts = re.split(r"/|\\", filename)[-1].split(".")
            if len(parts) > 1:
                ext = "." + parts.pop()
                filename = filename[:-len(ext)]
            else:
                ext = ""
            if filename == "":
                filename = "__"
            if len(ext) > 254:
                ext = ext[254:]
            maxl = 255 - len(ext)
            filename = filename[:maxl]
            filename = filename + ext
            # Re-check last character (if there was no extension)
            filename = filename.rstrip(". ")
            if len(filename) == 0:
                filename = "__"
        return filename


class AudioFile:
    def __init__(self, path):
        self.path = path
        self._cache = {}

    @property
    @afcache
    def fingerprint(self):
        result = json.loads(Utils.run("fpcalc -json " + shlex.quote(self.path)))
        self._cache.update(result)
        return result["fingerprint"]

    @property
    @afcache
    def duration(self):
        result = Utils.run("ffprobe -v error "
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

    def get_songs(self, constraints=None):
        if constraints is None:
            return self._c.execute("SELECT * FROM Songs")
        else:
            where_clause = " AND ".join((f"{x}=?" for x in constraints.keys()))
            return self._c.execute(f"SELECT * FROM Songs WHERE {where_clause}", tuple(constraints.values()))

    def update_song(self, id_, payload):
        set_clause = ",".join((f"{x}=?" for x in payload.keys()))
        return self._c.execute(f"UPDATE Songs SET {set_clause} WHERE id=?", (*payload.values(), id_, ))

    def commit(self):
        self._conn.commit()

class Downloader:
    def __init__(self, playlist_id, **kwargs):
        self.db = Database(**kwargs)

        if playlist_id is not None:
            self.db.set_playlist_id(playlist_id)
            logger.info("Set playlist id from command line!")

        if self.db.get_playlist_id() is None:
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

    def _insert_audio_file(self, af: AudioFile, filepath=None, **extra):
        if filepath is None:
            filepath = Path(af.path).relative_to(".").as_posix()

        try:
            payload = {
                "fingerprint": af.fingerprint,
                "hash": af.hash,
                "filepath": filepath,
                "duration": af.duration,
                "rating": af.rating
            }
            payload.update(extra)
            self.db.add_song(payload)
            logger.info("Inserted: " + str(af.path))
        except sqlite3.IntegrityError as ex:
            logger.warning("Skipped inserting exceptional file: " + str(af.path) + ". Reason: " + str(ex))
            raise

    def index_filesystem(self):
        logger.info("Indexing filesystem...")

        for file in itertools.chain(*(Path().rglob("*." + audio_ext) for audio_ext in AUDIO_EXTENSIONS)):

            filepath = file.relative_to(".").as_posix()
            existing_id = self.db.get_song_id("filepath", filepath)

            if existing_id is None:
                af = AudioFile(str(file))
                try:
                    self._insert_audio_file(af, filepath)
                except sqlite3.IntegrityError:
                    pass

        self.db.commit()
        logger.info("Indexing filesystem done!")

    def _get_playlist_info(self):
        with YoutubeDL({
            "extract_flat": True,
            'quiet': True,
            'no_warnings': True,
        }) as ytdl:
            playlist_info = ytdl.extract_info(
                "https://www.youtube.com/playlist?list=" + self.db.get_playlist_id()
            )

        logger.info("Loaded playlist info: {}".format(playlist_info.get("title", "??")))
        return playlist_info

    @staticmethod
    def _download_from_url(url, name):
        temp_dir = tempfile.TemporaryDirectory()
        with YoutubeDL({
            'format': 'bestaudio',
            "outtmpl": os.path.join(temp_dir.name, "out"),
            'quiet': True,
            'no_warnings': True,
        }) as ytdl:
            logger.info(f"Downloading: {url}: {name}")
            ytdl.download([url])

            file_names = os.listdir(temp_dir.name)
            if len(file_names) > 1:
                logger.warning("YouTubeDL unexpectedly produced more than one output file!")

            safe_name = Utils.sanitise_filename(name)
            out_path = os.path.join(temp_dir.name, safe_name + ".mp3")
            Utils.run("ffmpeg -y -i {} {}".format(
                shlex.quote(os.path.join(temp_dir.name, file_names[0])),
                shlex.quote(out_path)
            ))

            if not os.path.isfile(out_path):
                raise RuntimeError("FFMpeg did not produce file!")

            return Path(out_path).as_posix(), temp_dir

    def _check_audio_file_in_db(self, af: AudioFile):
        for song_row in self.db.get_songs({"duration": af.duration}):
            fingerprint_distance = distance(song_row["fingerprint"], af.fingerprint)
            if fingerprint_distance < FINGERPRINT_SIMILARITY_THRESH:
                af_name = os.path.split(af.path)[1]
                logger.warning(f"Identified AudioFile already in db w/finger print distance {fingerprint_distance}: "
                               f"{af_name}")
                return song_row
        return None

    def pull(self):
        logger.info("Pulling...")
        os.makedirs(DIR_OUTPUT, exist_ok=True)

        playlist_info = self._get_playlist_info()
        for entry in playlist_info["entries"]:
            youtube_id = entry["id"]
            existing_id = self.db.get_song_id("youtube_id", youtube_id)
            if existing_id is None:
                out_path, temp_dir = self._download_from_url(entry["url"], entry["title"])

                af = AudioFile(out_path)
                song_row = self._check_audio_file_in_db(af)
                if song_row is None:
                    out_name = os.path.split(out_path)[1]
                    final_path = os.path.join(DIR_OUTPUT, out_name)

                    i = 0
                    while os.path.isfile(final_path):
                        final_path = os.path.join(DIR_OUTPUT, str(i) + out_name)
                        i += 1

                    shutil.move(af.path, final_path)
                    af.path = final_path
                    self._insert_audio_file(af, youtube_id=youtube_id)
                    self.db.commit()
                else:
                    self.db.update_song(song_row["id"], {
                        "youtube_id": youtube_id
                    })
                    self.db.commit()
                temp_dir.cleanup()

        logger.info("Pulling done!")


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
        d.pull()


if __name__ == '__main__':
    main()
