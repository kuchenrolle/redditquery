#!/usr/bin/python3
import os
import sys
import pickle
from redditquery.database import DataBase
from redditquery.parse import parser
from redditquery.index import InvertedIndex, QueryProcessor
from redditquery.reddit import RedditDownloader, DocumentGenerator


def main():
    """Build and/or Query Inverted Index from reddit comments."""
    parse = parser()
    args = parse.parse_args()

    if args.mode in (1,3):

        # check that conditionally obligatory arguments have been put in:
        if not args.start or not args.end:
            raise ValueError("first and last month must be specified when building index.")

        # download data and decompress
        downloader = RedditDownloader(start = args.start, end = args.end, directory = os.path.join(args.dir, "monthly_data"), report_progress = args.progress, keep_compressed = False)
        downloader.process_all_parallel(num_cores = args.cores)

        # make document generator
        documents = DocumentGenerator(directory = os.path.join(args.dir, "monthly_data"), fulltext = args.fulltext, lemmatize = args.lemma)

        # make inverted index
        database = DataBase(database_file = os.path.join(args.dir,"database.sql"))
        inverted_index = InvertedIndex(documents = documents, database = database, frequency_threshold = args.minfreq)

        # pickle inverted index for later use
        # temporarily remove database connection
        # (cannot be pickled)
        with open(os.path.join(args.dir,"inverted_index.pickle"), "wb") as pickled:
            db_tmp = inverted_index.database
            inverted_index.database = 0
            pickle.dump(inverted_index, pickled)
            inverted_index.database = db_tmp


    if args.mode == 2:
        # load inverted index and add database connection back in
        with open(os.path.join(args.dir,"inverted_index.pickle"), "rb") as pickled:
            inverted_index = pickle.load(pickled)
            inverted_index.database = DataBase(database_file = os.path.join(args.dir,"database.sql"), existing = True)


    if args.mode in (2,3):
        # set-up query processor
        processor = QueryProcessor(inverted_index = inverted_index, lemmatize = args.lemma)

        # process queries
        for query in sys.stdin:
            processor.query_index(query, num_results = args.num, fulltext = args.fulltext)


if __name__ == "__main__":
    main()