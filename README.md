# RedditQuery

An offline information retrieval system for full-text search on reddit comments.


## Getting Started

To get started, install the dependencies (see Prerequisites) and then clone the repository to your local machine. Once set-up, you can use the package as an executable like so:

```
python3 RedditQuery mode <parameters>
```


### Prerequisites

RedditQuery has two dependencies that are not part of the standard distribution, Pandas and Spacy. If you install this package using pip, the dependencies should be installed automatically. On Unix systems, you should also be able to install them separately using pip3:

```
pip3 install pandas
pip3 install spacy
```

An alternative, especially for Windows users, is to use a conda distribution that should come shipped with pandas and add spacy like so:

```
source activate <your_virtual_environment_name>
conda install spacy
```

If you encounter any problems, please consult the installation instructions for [Pandas](http://pandas.pydata.org/pandas-docs/stable/install.html) and [Spacy](https://spacy.io/docs/usage/).


### Installing

This package is pip-installable:

```
pip3 install redditQuery
```

If you're using conda, then first activate the target environment as above and then install. Alternatively, clone this repository to your local directory and install manually:

```
git clone git@github.com:kuchenrolle/RedditQuery.git <path_to_destination_folder>
python setup.py install
```


## Author

**Christian Adam**


## License

This project is licensed under the MIT License - see the [LICENSE.txt](LICENSE.txt) file for details