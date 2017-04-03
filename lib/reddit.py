#!/usr/bin/python3
import json
import os
import sys
import spacy
from pandas import period_range
from multiprocessing import Pool
from urllib.request import FancyURLopener
from lib.utils import recursive_walk, check_directory

urlretrieve = FancyURLopener().retrieve


# data will be downloaded from http://files.pushshift.io/reddit/comments/
# data structure is document in https://github.com/reddit/reddit/wiki/JSON


class RedditDownloader():

    def __init__(self, start, end, directory, report_progress, keep_compressed):
        self.directory = check_directory(directory)
        self.report_progress = report_progress
        self.keep_compressed = keep_compressed
        self.months = period_range(start, end, freq = "M")


    # decompress bz2 file (compressed_path) incrementally
    def decompress(self, compressed_path, decompressed_path):
        from bz2 import BZ2Decompressor
        with open(decompressed_path, 'wb') as decompressed, open(compressed_path, 'rb') as compressed:
            decompressor = BZ2Decompressor()
            total_size = os.path.getsize(compressed_path)
            size_so_far = 0
            if self.report_progress:
                sys.stdout.write("\n")
            for data in iter(lambda : compressed.read(100 * 1024), b''):
                decompressed.write(decompressor.decompress(data))
                if self.report_progress:
                    size_so_far += 102400
                    percentage = min(int(size_so_far * 100 / total_size), 100)
                    sys.stdout.write("\rDecompression: {}%".format(percentage))
                    sys.stdout.flush()
        if not self.keep_compressed:
            os.remove(compressed_path)

    @staticmethod
    # hook to update download progress
    def download_progress(count, block_size, total_size):
        percentage = min(int(100 * count * block_size / total_size),100)
        sys.stdout.write("\rDownload: {}%".format(percentage))
        sys.stdout.flush()

    # download data for given month and year
    def download_month(self, month):
        file_url = "http://files.pushshift.io/reddit/comments/RC_{}.bz2".format(month)
        file_path = os.path.join(self.directory, "RC_{}.bz2".format(month))
        if self.report_progress:
            urlretrieve(file_url, file_path, reporthook = RedditDownloader.download_progress)
        else:
            urlretrieve(file_url, file_path)

    # download all files
    def download_all(self):
        for month in self.months:
            if self.report_progress:
                sys.stdout.write("\n")
            self.download_month(month)

    # decompress file associated with specific month
    def decompress_month(self, month):
            compressed_path = os.path.join(self.directory, "RC_{}.bz2".format(month))
            decompressed_path = os.path.join(self.directory, "RC_{}.json".format(month))
            self.decompress(compressed_path = compressed_path, decompressed_path = decompressed_path)

    # decompress files for all months
    def decompress_all(self):
        for month in self.months:
            self.decompress_month(month)

    # download file for specific month and decompress
    def process_month(self, month):
        self.download_month(month)
        self.decompress_month(month)

    # download and decompress files for all momths
    def process_all(self):
        for month in self.months:
            self.process_month(month)

    # download and decompress files for all months in parallel
    def process_all_parallel(self, num_cores):
        if num_cores == 1:
            self.process_all()
        else:
            self.report_progress = False
            with Pool(num_cores) as pool:
                for _ in pool.imap_unordered(self.process_month, self.months):
                    pass


# takes a directory with reddit comment archive files (JSON)
# and returns the comment id and a list of tokens for each comment
def DocumentGenerator(irectory, lemmatize = False):
    files = recursive_walk(directory)
    nlp = spacy.load("en")
    for month in files:
        month = open(month, "r")
        for comment in month:
            comment = json.loads(comment)
            text = comment["body"]
            comment_id = comment["id"]
            tokens = nlp(text)
            if lemmatize:
                tokens = [token.lemma_.lower() for token in tokens if not token.pos_.startswith(u"PU")] # filter punctuation
            else:
                tokens = [token.string.lower() for token in tokens if not token.pos_.startswith(u"PU")]
            yield comment_id, tokens