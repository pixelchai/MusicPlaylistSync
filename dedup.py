import logging
import collections
from Levenshtein import distance
from tqdm import tqdm
import pickle
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

        remaining_song_rows = []
        for row in self.db.get_songs():
            remaining_song_rows.append(row)

        remaining_song_rows = list(sorted(remaining_song_rows, key=lambda x: x['duration']))

        while len(remaining_song_rows) > 0:
            print("Remaining: {:04d}".format(len(remaining_song_rows)))

            song_row = remaining_song_rows.pop(0)

            for other_song_row in remaining_song_rows:
                if other_song_row["duration"] - song_row["duration"] > 5:
                    break

                lev_dist = distance(song_row['fingerprint'], other_song_row['fingerprint'])
                pairs[frozenset((song_row['id'], other_song_row['id']))] = lev_dist

                if lev_dist < 2300:
                    print(f"Possible pair w/dist={lev_dist}: {song_row['filepath']} \t{other_song_row['filepath']}")

            with open("pairs.pk", "wb") as f:
                pickle.dump(pairs, f)


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