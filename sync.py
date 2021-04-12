import sqlite3
import os

def row_factory(*args, **kwargs):
    """
    allows for the database cursor to yield dictionary objects, which are easier to use
    """
    return dict(sqlite3.Row(*args, **kwargs))

# Songs: pk, accoustic hash, youtube id, file path, (md5 [or similar] hash)
class Database:
    PATH = "mps.db"
    CURRENT_VERSION = "0.1.0"

    def __init__(self, overwrite=False, trace=True):
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
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._conn.close()

    def _init_schema(self):
        self._c.executescript("""
        CREATE TABLE IF NOT EXISTS DatabaseMeta (
            id	INTEGER PRIMARY KEY NOT NULL UNIQUE DEFAULT 1,
            version TEXT
        );
        """)
        self._c.execute("INSERT OR REPLACE INTO DatabaseMeta(version) "
                        "VALUES (?)", (Database.CURRENT_VERSION,))
        self._conn.commit()
        print("Schema initialised")

if __name__ == '__main__':
    with Database(True) as d:
        pass
