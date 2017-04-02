import os
import sys
import spacy
import pickle
from lib.database import DataBase
from lib.parse import Parser
from lib.index import InvertedIndex, QueryProcessor
from lib.reddit import RedditDownloader, DocumentGenerator


# obligatory parameters:
# mode - 1 build index; 2 query existing index; 3 build and run

# conditionally obligatory parameters:
# if mode is 1 or 3:
# -f or --first YYYY/MM - first month to be processed
# -l or --last YYYY/MM - last month to be processed

# optional parameters with defaults:
# -d or --dir - directory path to store data in (defaults to ./data)
# -c or --cores - number of cores to use for downloading/decompressing monthly data (defaults to 1)
# -m or --minfreq - minimum frequeny to keep terms in index (defaults to 5)
# -n or --num - number of results to to show for each query (default to 10)
# -p or --progress - output progress information for download/processing (only single core, defaults to no progress shown)


def main():
    # parse arguments from shell
    parser = Parser(base_directory = os.path.dirname(__file__))
    args = parser.parse_args()

    # download data and decompress
    if args.mode in (1,3):

        # check that conditionally obligatory arguments have been put in:
        if not args.first or not args.last:
            raise ValueError("first and last month must be specified when building index.")

        downloader = RedditDownloader(start = args.first, end = args.last, directory = os.path.join(args.dir, "monthly_data"), report_progress = args.progress, keep_compressed = False)
        downloader.process_all_parallel(num_cores = args.cores)

        # make document generator
        nlp = spacy.load("en")
        documents = DocumentGenerator(directory = os.path.join(args.dir, "monthly_data"), nlp = nlp)

        # set-up query processor
        database = DataBase(database_file = os.path.join(args.dir,"database.sql"))
        inverted_index = InvertedIndex(documents = documents, database = database, frequency_threshold = args.minfreq)

        # pickle inverted_index for later use
        with open(os.path.join(args.dir,"inverted_index.pickle"), "wb") as pickled:
            tmp = inverted_index.database
            inverted_index.database = 0 # cannot pickle database connections
            pickle.dump(inverted_index, pickled)
            inverted_index.database = tmp # put database back in


    if args.mode == 2:
        with open(os.path.join(args.dir,"inverted_index.pickle"), "rb") as pickled:
            inverted_index = pickle.load(pickled)
            inverted_index.database = DataBase(database_file = os.path.join(args.dir,"database.sql"), existing = True)


    if args.mode in (2,3):
        processor = QueryProcessor(inverted_index = inverted_index)
    
        # process queries
        for query in sys.stdin:
            processor.query_index(query, num_results = args.num)


if __name__ == "__main__":
    main()