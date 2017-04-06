#!/usr/bin/python3
import json
import os
import sys
import spacy
from pandas import period_range
from multiprocessing import Pool
from urllib.request import FancyURLopener
from redditquery.utils import recursive_walk, check_directory

urlretrieve = FancyURLopener().retrieve


# data will be downloaded from http://files.pushshift.io/reddit/comments/
# data structure is document in https://github.com/reddit/reddit/wiki/JSON


class RedditDownloader():
    """ Downloads and Decompresses reddit comment archive files.
    Parameters
    ----------
    start :             str
                        First month to be processed as YYYY/MM
    end :               str
                        Last month to be processed as YYYY/MM
    directory :         str
                        Directory to store data in
    report_progress :   Boolean
                        Display progress report to stderr
    keep_compressed :   Boolean
                        Keep compressed archive files
    """

    def __init__(self, start, end, directory, report_progress, keep_compressed):
        self.directory = check_directory(directory)
        self.report_progress = report_progress
        self.keep_compressed = keep_compressed
        self.months = period_range(start, end, freq = "M")


    def decompress(self, compressed_path, decompressed_path):
        """Decompress bz2 file (compressed_path) incrementally.
        Parameters
        ----------
        compressed_path :   str or path object
                            file to be decompressed
        decompressed_path : str or path object
                            file to be decompressed into
        """
        from bz2 import BZ2Decompressor
        with open(decompressed_path, 'wb') as decompressed, open(compressed_path, 'rb') as compressed:
            decompressor = BZ2Decompressor()
            total_size = os.path.getsize(compressed_path)
            size_so_far = 0
            if self.report_progress:
                sys.stderr.write("\n")
            for data in iter(lambda : compressed.read(100 * 1024), b''):
                decompressed.write(decompressor.decompress(data))
                if self.report_progress:
                    size_so_far += 102400
                    percentage = min(int(size_so_far * 100 / total_size), 100)
                    sys.stderr.write("\rDecompression: {}%".format(percentage))
                    sys.stderr.flush()
        if not self.keep_compressed:
            os.remove(compressed_path)

    @staticmethod
    def download_progress(count, block_size, total_size):
        """Hook to update download progress."""
        percentage = min(int(100 * count * block_size / total_size),100)
        sys.stderr.write("\rDownload: {}%".format(percentage))
        sys.stderr.flush()

    def download_month(self, month):
        """Download data for given month.
        Parameters
        ----------
        month : str or date object
                Month to be downloaded, str(month) must result in YYYY/MM
        """
        file_url = "http://files.pushshift.io/reddit/comments/RC_{}.bz2".format(month)
        file_path = os.path.join(self.directory, "RC_{}.bz2".format(month))
        if self.report_progress:
            urlretrieve(file_url, file_path, reporthook = RedditDownloader.download_progress)
        else:
            urlretrieve(file_url, file_path)

    def download_all(self):
        """Downloads data for all months in self.months"""
        for month in self.months:
            if self.report_progress:
                sys.stderr.write("\n")
            self.download_month(month)

    def decompress_month(self, month):
        """Decompress archive file for given month.
        Parameters
        ----------
        month : str or date object
                Month to be decompressed, str(month) must result in YYYY/MM
        """
        compressed_path = os.path.join(self.directory, "RC_{}.bz2".format(month))
        decompressed_path = os.path.join(self.directory, "RC_{}.json".format(month))
        self.decompress(compressed_path = compressed_path, decompressed_path = decompressed_path)

    def decompress_all(self):
        """Decompress files for all months."""
        for month in self.months:
            self.decompress_month(month)

    def process_month(self, month):
        """Download file for specific month and decompress.
        Parameters
        ----------
        month : str or date object
                Month to be processed, str(month) must result in YYYY/MM
        """
        self.download_month(month)
        self.decompress_month(month)

    def process_all(self):
        """Download and decompress files for all months."""
        for month in self.months:
            self.process_month(month)

    def process_all_parallel(self, num_cores):
        """Download and decompress files for all months in parallel
        Parameters
        ----------
        num_cores : int
                    Number of cores to use
        """
        if num_cores == 1:
            self.process_all()
        else:
            self.report_progress = False
            with Pool(num_cores) as pool:
                for _ in pool.imap_unordered(self.process_month, self.months):
                    pass


def DocumentGenerator(directory, fulltext, lemmatize):
    """
    Takes a directory with reddit comment archive files (JSON)
    and returns tuples of the comment id and a list of tokens for each comment.
    Parameters
    ----------
    directory : str or path object
                Directory with comment files
    fulltext :  Boolean
                return full comment as well
    lemmatize : Boolean
                lemmatize tokens in comments
    """
    files = recursive_walk(directory)
    nlp = spacy.load("en")
    for month in files:
        if not month.endswith("json"):
            continue
        month = open(month, "r")
        for comment in month:
            comment = json.loads(comment)
            text = comment["body"]
            comment_id = comment["id"]
            tokens = nlp(text)
            if lemmatize:
                tokens = [token.lemma_.strip().lower() for token in tokens if not token.pos_.startswith(u"PU")] # filter punctuation
            else:
                tokens = [token.string.strip().lower() for token in tokens if not token.pos_.startswith(u"PU")]
            if not fulltext:
                text = ""
            yield comment_id, tokens, text