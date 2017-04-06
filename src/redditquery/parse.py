#!/usr/bin/python3
import os
import argparse


def parser():
    """Parses arguments from command line using argparse.
    Parameters"""
    # default directory for reddit files
    default_directory = os.path.join(os.getcwd(), "data")

    parser = argparse.ArgumentParser()
    # obligatory
    parser.add_argument("mode", type = int, help = "execution mode: 1 build index, 2: query using existing index, 3 build and query")
    # conditionally obligatory
    parser.add_argument("--start", "-s", type = str, help = "first year/month")
    parser.add_argument("--end", "-e", type = str, help = "last year/month")
    # optional with defaults
    parser.add_argument("--dir", "-d", type = str, nargs = "?", default = default_directory, help = "directory for data storage")
    parser.add_argument("--num", "-n", type = int, nargs = "?", default = 10, help = "number of results per query")
    parser.add_argument("--fulltext", "-f", action = "store_true", help = "store fulltext and/or return in queries")
    parser.add_argument("--minfreq", "-m", type = int, nargs = "?", default = 5, help = "minimum term frequency")
    parser.add_argument("--lemma", "-l", action = "store_true", help = "lemmatize comments/queries")
    parser.add_argument("--cores", "-c", type = int, nargs = "?", default = 1, help = "number of cores to use")
    parser.add_argument("--progress", "-p", action = "store_true", help = "report progress")
    return parser