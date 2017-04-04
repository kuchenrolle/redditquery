#!/usr/bin/python3
import os
import sys
import pickle
from redditQuery.database import DataBase
from redditQuery.parse import Parser
from redditQuery.index import InvertedIndex, QueryProcessor
from redditQuery.reddit import RedditDownloader, DocumentGenerator


# obligatory parameters:
# mode:     1 build index
#           2 query existing index
#           3 build and run

# conditionally obligatory parameters:
# if mode is 1 or 3:
# -f or --first YYYY/MM: first month to be processed
# -l or --last YYYY/MM:  last month to be processed

# optional parameters with defaults:
# -d or --dir:      directory path to store data in (defaults to ./data)
# -c or --cores:    number of cores to use for downloading/decompressing monthly data (defaults to single-core)
# -m or --minfreq:  minimum frequeny to keep terms in index (defaults to 5)
# -n or --num:      number of results to show for each query (defaults to 10)
# -p or --progress: output progress information for download/processing (only single core, defaults to no progress shown)


def main():
    # parse arguments from shell
    parser = Parser(base_directory = os.path.dirname(__file__))
    args = parser.parse_args()

    if args.mode in (1,3):

        # check that conditionally obligatory arguments have been put in:
        if not args.first or not args.last:
            raise ValueError("first and last month must be specified when building index.")

        # download data and decompress
        downloader = RedditDownloader(start = args.first, end = args.last, directory = os.path.join(args.dir, "monthly_data"), report_progress = args.progress, keep_compressed = False)
        downloader.process_all_parallel(num_cores = args.cores)

        # make document generator
        documents = DocumentGenerator(directory = os.path.join(args.dir, "monthly_data"))

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
        processor = QueryProcessor(inverted_index = inverted_index)
    
        # process queries
        for query in sys.stdin:
            processor.query_index(query, num_results = args.num)


if __name__ == "__main__":
    main()