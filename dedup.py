import argparse
import logging
from sync import Database, AudioFile, parser_setup, logging_setup

DEBUG = True


logger = logging.getLogger('MusicPlaylistSync__Dedup')
parser = argparse.ArgumentParser()

def dedup():
    pairs = {}   # frozenset(id1, id2): distance


def main():
    parser_setup()
    args = vars(parser.parse_args())

    global DEBUG
    DEBUG = args.get("debug", DEBUG)
    logging_setup()

    dedup()

if __name__ == '__main__':
    main()