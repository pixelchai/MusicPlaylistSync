# MusicPlaylistSync
A script for downloading music from a YouTube playlist, which caches and skips downloading duplicate/already downloaded files using audio fingerprinting.

# Usage
This script has been designed so that it should be easy to work with an already existing music collection.
Once the dependencies are installed, you can simply place `sync.py` in the root folder of your music collection folder and run it.
The available command line arguments are described in the help message below, which is also shown when running `python3 sync.py -h`:

```
usage: sync.py [-h] [-x] [-t] [playlist_id]

positional arguments:
  playlist_id      youtube playlist id

optional arguments:
  -h, --help       show this help message and exit
  -x, --overwrite  whether to overwrite the database, if it exists
  -t, --trace      trace SQL commands
```

The playlist_id only needs to be provided on the first usage. 
On subsequent usages, this data will be stored in the `mps.db` sqlite database created by the script, and you should be able to simply run `python3 sync.py`.

# What it Does
The script has three main stages, outlined below:

### 1: File Paths Verification
The script goes through all the songs stored in the `mps.db` database (if it exists) and checks whether they still are present in the filesystem (they might have been deleted/renamed externally).
Records which are not reflected in the filesystem are deleted (if they were renamed rather than being deleted from the filesystem, they will be recreated in the next step).

### 2: Filesystem Indexing
The containing directory is recursively scanned for audio files. 
Any songs which are in the filesystem but not in the database will be stored in the database, alongside its computed audio fingerprint.

### 3: Pulling (Downloading)
A list of ids for each song in the provided playlist is retrieved. Songs with ids that are not already present in the database are downloaded into `mps/`, have their fingerprints computed and are stored in the database.
If a song has a fingerprint that matches one already in the database, the record in the database is updated with its song id, and will therefore not be downloaded again on subsequent invocations of the script.

# Installation
Install `chromaprint` and `ffmpeg`, making sure they are accessible from your PATH. Then install the pip dependencies as shown below:

```
git clone https://github.com/pixelzery/MusicPlaylistSync.git
cd MusicPlaylistSync
python3 -m pip install -r requirements.txt
```
