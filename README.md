# redditquery

An offline information retrieval system for full-text search on reddit comments.


## Getting Started

Once redditquery is set-up on your system (see Installation and Prerequisites), you can call the package from the command line like so (see Parameters):

```shell
user@host:~ redditquery mode [-h] [-s [START]] [-e [END]] [-d [DIR]] [-n [NUM]]
                   [-c [CORES]] [-m [MINFREQ]] [-p [PROGRESS]] [-f [FULLTEXT]]
                   [-l [LEMMA]] [-a [ALL]]
                   
```

Alterantively, you can use it from inside python to interact with it dynamically (see Examples).


## Parameters

redditquery's behaviour can be changed with various parameters. Specifying mode is obligatory:

```
mode:  1 Build Inverted Index (requires specifying -f and -l)
       2 Query existing Inverted Index
       3 Build Inverted Index and Query (requires specifying -f and -l)
```

If the index is build, you will be required to specify the range of months to build the index on, by specifying the first and last month to be processed:

```
-s --start: first month to be downloaded as YYYY/MM
-e --end:   last month to be downloaded as YYYY/MM
```

All other parameters are optional, here is what they do and their defaults:

```
-d or --dir:      directory path to store data in (defaults to working dir)
-c or --cores:    number of cores to use for downloading/decompressing monthly data (defaults to single-core)
-m or --minfreq:  minimum frequeny to keep terms in index (defaults to 5)
-n or --num:      number of results to show for each query (defaults to 10)
-f or --fulltext: store/retrieve full text of reddit comments (defaults to only storing/retrieving comment ids)
-a or --all:      return documents containing all query terms (defaults to documents containing any of the query terms)
-l or --lemma:    lemmatize documents/queries
-p or --progress: output progress information for download/processing (only single core, defaults to no progress shown)
- h or --help:    show help file
```


## Examples

Build inverted index from reddit comments between december 2005 and march 2006 from the command line:

```shell
user@host:~ redditquery 1 -s 2005/12 -e 2006/03
```

Query inverted index that already exists in myDirectory with queries from myQueries.txt in the same directory:

```shell
user@host:~ redditquery 2 -d path/to/myDirectory path/to/myDirectory/myQueries.txt
```

Build and query the same index as above in one go from inside python:

```python
>>> import os
>>> import sys
>>> import pickle
>>> from redditquery.database import DataBase
>>> from redditquery.parse import Parser
>>> from redditquery.index import InvertedIndex, QueryProcessor
>>> from redditquery.reddit import RedditDownloader, DocumentGenerator

>>> directory = "myDirectory"
>>> queries = "myDirectory/myQueries.txt"
>>> start = "2005/12"
>>> end = "2006/03"
>>> minimum_freq = 5
>>> num_results = 10
>>> 
>>> downloader = RedditDownloader(start = start, end = end, directory = directory, keep_compressed = False)
>>> downloader.process_all_parallel()
>>> 
>>> documents = DocumentGenerator(directory = os.path.join(directory, "monthly_data"), fulltext = False, lemmatize = False)
>>> database = DataBase(database_file = os.path.join(directory,"database.sql"))
>>> inverted_index = InvertedIndex(documents = documents, database = database, frequency_threshold = minimum_freq)
>>> 
>>> processor = QueryProcessor(inverted_index = inverted_index, lemmatize = False)
>>> with open(queries, "r") as queries:
>>>     for query in queries:
>>>     processor.query_index(query.strip(), num_results = num_results, fulltext = False, conjunctive = False)
```

### Prerequisites

redditquery has two dependencies that are not part of the standard distribution, Pandas and Spacy. If you install this package using pip, the dependencies should be installed automatically. On Unix systems, you should also be able to install them separately using pip:

```shell
user@host:~ pip install pandas
user@host:~ pip install spacy
```

An alternative, especially for Windows users, is to use a conda distribution that should come shipped with pandas and add spacy like so and then install, still using pip:

```shell
user@host:~ [source] activate <environment>
(environment)user@host:~ conda install spacy
(environment)user@host:~ pip install redditquery
```

Lastly, you can clone the repository and use the setup.py to install the package manually:

```shell
user@host:~ git clone https://github.com/kuchenrolle/redditquery.git
user@host:~ python setup.py install
```


If you encounter any problems installing the dependencies, please consult the installation instructions for [Pandas](http://pandas.pydata.org/pandas-docs/stable/install.html) and [Spacy](https://spacy.io/docs/usage/).


### Installation

This package is pip-installable:

```shell
user@host:~ pip install redditquery
```

If you're using conda, then first activate the target environment and then install. Alternatively, clone this repository to your local directory and install manually:

```shell
user@host:~ git clone git@github.com:kuchenrolle/redditquery.git <path_to_destination_folder>
user@host:~ [source] activate <environment>
(environment)user@host:~ python setup.py install
```


## Author

**Christian Adam**


## License

This project is licensed under the MIT License - see the [LICENSE.txt](LICENSE.txt) file for details