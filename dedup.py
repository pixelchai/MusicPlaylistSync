import argparse
import logging
import collections
from Levenshtein import distance
from tqdm import tqdm
from sync import Database, AudioFile, parser, parser_setup, logging_setup

DEBUG = True


logger = logging.getLogger('MusicPlaylistSync__Dedup')

# def dedup():
#     pairs = {}   # frozenset(id1, id2): distance
#
#     with Database(False, )

class Deduper:
    def __init__(self, **kwargs):
        self.db = Database(**kwargs)


    def build_list(self):
        pairs = {}  # frozenset(song_id, other_song_id): distance

        remaining_song_ids = collections.deque()
        for row in self.db.get_songs():
            remaining_song_ids.append(row["id"])

        total_count = len(remaining_song_ids)

        with tqdm(total=total_count) as pbar:
            while len(remaining_song_ids) > 0:
                song_id = remaining_song_ids.pop()
                song_row = tuple(self.db.get_songs({"id": song_id}))[0]

                for other_song_id in remaining_song_ids:
                    other_song_row = tuple(self.db.get_songs({"id": other_song_id}))[0]
                    pairs[frozenset((song_id, other_song_id))] = distance(
                        song_row['fingerprint'],
                        other_song_row['fingerprint']
                    )
                pbar.update(total_count - len(remaining_song_ids))

        print(pairs)

    def __enter__(self):
        return self

    def close(self):
        self.db.close()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def main():
    parser_setup()
    args = vars(parser.parse_args())

    global DEBUG
    DEBUG = args.get("debug", DEBUG)
    logging_setup()

    with Deduper(**args) as d:
        d.build_list()

if __name__ == '__main__':
    main()