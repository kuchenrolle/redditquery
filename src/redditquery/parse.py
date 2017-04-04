#!/usr/bin/python3
import os
import argparse


description = """
redditquery's behaviour can be changed with various parameters. Specifying mode is obligatory:

mode:  1 Build Inverted Index (requires specifying -f and -l)
       2 Query existing Inverted Index
       3 Build Inverted Index and Query (requires specifying -f and -l)

If the index is build, you will be required to specify the range of months to build the index on, by specifying the first and last month to be processed:

-f --first: first month to be downloaded as YYYY/MM
-l --last:  last month to be downloaded as YYYY/MM

All other parameters are optional, here is what they do and their defaults:

-d or --dir:      directory path to store data in (defaults to working dir)
-c or --cores:    number of cores to use for downloading/decompressing monthly data (defaults to single-core)
-m or --minfreq:  minimum frequeny to keep terms in index (defaults to 5)
-n or --num:      number of results to show for each query (defaults to 10)
-p or --progress: output progress information for download/processing (only single core, defaults to no progress shown)
- h or --help:    show help file
"""

def parser():
    """Parses arguments from comman line using argparse.
    Parameters"""
    # default directory for reddit files
    default_directory = os.path.join(os.cwd(), "data")

    parser = argparse.ArgumentParser(description = description)
    # obligatory
    parser.add_argument("mode", type = int, help = "execution mode: 1 build index, 2: query using existing index, 3 build and query")
    # conditionally obligatory
    parser.add_argument("-f", "--first", type = str, help = "first year/month")
    parser.add_argument("-l", "--last", type = str, help = "last year/month")
    # optional with defaults
    parser.add_argument("--dir", "-d", type = str, nargs = "?", default = default_directory, help = "directory for data storage")
    parser.add_argument("--num", "-n", type = int, nargs = "?", default = 10, help = "number of results per query")
    parser.add_argument("--cores", "-c", type = int, nargs = "?", default = 1, help = "number of cores to use")
    parser.add_argument("--minfreq", "-m", type = int, nargs = "?", default = 5, help = "minimum term frequency")
    parser.add_argument("--progress", "-p", action = "store_true", help = "report progress")
    return parser