#!/usr/bin/python3
import sys
from math import log2
from heapq import nlargest
from collections import Counter
from redditquery.utils import Numberer, l2_norm
from redditquery.database import DataBase


class InvertedIndex:
    """
    Creates an InvertedIndex stored in a database.
    Parameters
    ----------
    database :              database object
                            Database to use
    documents :             iterable of tuples of int, list of strings
                            Documents to insert into index
    frequency_threshold :   int
                            Minimum frequency for terms to be in index
    """
    def __init__(self, database, documents, frequency_threshold):
        self.num_documents = 0
        self.database = database
        self.vocabulary_indices = Numberer()
        self.document_frequencies = Counter()
        self.document_ids = dict()

        infrequent = self.make_indices(documents = documents, frequency_threshold = frequency_threshold)
        self.remove_infrequent(infrequent)
        self.transform_to_tfidf()

    def make_indices(self, documents, frequency_threshold):
        """Insert documents into index.
        Parameters
        ----------
        documents :             iterable of tuples of int, list of strings
                                Documents to insert into index
        frequency_threshold :   int
                                Minimum frequency for terms to be in index

        """
        vocabulary = Counter()
        self.prepare_inserts()
        for document in documents:
            self.process_document(document, vocabulary)
        infrequent = [term_id for term_id, freq in vocabulary.items() if freq < frequency_threshold]
        return infrequent


    def process_document(self, document, vocabulary):
        """Add document to index.
        Parameters
        ----------
        document :      tuple of int, list of strings
                        Document to be processed.
        vocabulary :    Counter
                        Vocabulary counter to filter out words at the end
        """
        doc_id = document[0]
        document = [self.vocabulary_indices.get(term) for term in document[1]]
        term_counts = Counter(document)
        vocabulary += term_counts
        for term in term_counts:
            self.document_frequencies[term] += 1
        self.insert_document(self.num_documents, list(term_counts.items()))
        self.document_ids[self.num_documents] = doc_id
        self.num_documents += 1

    def remove_infrequent(self, infrequent):
        """Remove infrequent terms from index.
        Parameters
        ----------
        infrequent :    list of int
                        ids of terms to be removed
        """
        self.prepare_deletes()
        self.remove_terms([(term,) for term in infrequent])
        self.vocabulary_indices.remove_values(set(infrequent))


    def transform_to_tfidf(self):
        """Turn frequency counts in index into pmi values."""
        self.prepare_updates()
        updates = list()
        for i, document_id in enumerate(self.document_ids):
            frequencies = self.get_document(document_id)
            tfidfs = [(term_id, self.tfidf(term_id, frequency)) for term_id, frequency in frequencies]
            norm = l2_norm([tfidf for _, tfidf in tfidfs])
            normed = [(tfidf/norm, document_id, term_id) for term_id, tfidf in tfidfs]
            updates += normed
            if i%10000 == 0:
                self.update_documents(updates)
                updates = list()
        if updates:
            self.update_documents(updates)

    def tfidf(self, term_id, frequency):
        """Calculate tf-idf.
        Parameters
        ----------
        term_id :   int
                    id of term
        frequency : int
                    Frequency of term"""
        return frequency * self.idf(term_id)

    def idf(self, term_id):
        """Calculate idf.
        Parameters
        ----------
        term_id :   int
                    id of term
        """
        idf = log2(self.num_documents / max(self.document_frequencies[term_id],1))
        return idf

    def get_document_name(self, doc_id):
        """get document name associated with doc id.
        Parameters
        ----------
        doc_id :    int
                    id of document
        """
        return self.document_ids[doc_id]

    def get_term_id(self, term):
        """Return term_id for given term.
        Parameters
        ----------
        term :  str
                term to get id for
        """
        return self.vocabulary_indices.get(term)


    # interfaces for database
    def get_postings_list(self, term_id):
        """Retrieve ids of documents that a given term_id appeared in.
        Parameters
        ----------
        term_id :   int
                    id of term
        """
        return self.database.retrieve_term(term_id)

    def get_document(self, document_id):
        """Retrieve document from database by id.
        Parameters
        ----------
        doc_id :    int
                    id of document
        """
        return self.database.retrieve_document(document_id)

    def insert_document(self, doc_id, scores):
        """Insert document with its corresponding terms/scores
            into database.
        Parameters
        ----------
        document_id :   int
                        id of the document
        term_scores :   iterable of tuples of int, float
                        term ids and term scores
        """
        self.database.insert_document(doc_id, scores)

    def remove_terms(self, infrequent):
        """Remove list of terms from database
        Parameters
        ----------
        term_ids :  iterable of singleton tuples of int
                    term ids to be removed
        """
        self.database.remove_terms(infrequent)

    def update_documents(self, updates):
        """Change term scores of a given document
        Parameters
        ----------
        score_tuples :  iterable of tuples of float, int, int
                        scores for document ids and term ids to be updated
        """
        self.database.update_documents(updates)

    def prepare_inserts(self):
        """Prepare database for insertions"""
        self.database.prepare_inserts()

    def prepare_deletes(self):
        """Prepare database for deletions"""
        self.database.prepare_deletes()

    def prepare_updates(self):
        """Prepare database for updates"""
        self.database.prepare_updates()


class QueryProcessor():
    """Queries an inverted index.
    Parameters
    ----------
    inverted_index :    InvertedIndex
                        Index to be queried
    """

    def __init__(self, inverted_index):
        self.inverted_index = inverted_index

    def query_index(self, query, num_results):
        """Query the index.
        Parameters
        ----------
        query :         str
                        Query to be processed
        num_results :   int
                        Number of most similar results to return
        """
        # ignore multiple occurrences of terms in query
        query = list(set(query.strip().split(" ")))
        term_ids = [self.get_term_id(term) for term in query]
        # get all documents containing any of the query terms
        candidates = set()
        for term_id in term_ids:
            doc_ids = self.get_postings_list(term_id)
            candidates.update(doc_ids)
        # get similarity between documents and query
        similarities = list()
        for candidate in candidates:
            similarities.append((self.get_similarity(candidate, term_ids), candidate))
        for i, term in enumerate(query):
            sys.stdout.write("idf({0}): {1:2f}".format(term, self.get_idf(term_ids[i])))
        for similarity, doc_id in nlargest(num_results, similarities):
            doc_name = self.get_document_name(doc_id)
            sys.stdout.write("{0} ({1:3f}): {2}".format(doc_id, similarity, doc_name))

    def get_similarity(self, candidate, query):
        """Return cosine similarity between candidate and query
        Parameters
        ----------
        candidate : int
                    id of candidate document
        query :     iterable of int
                    ids of terms in the query
        """
        query = self.query_to_tfidf(query)
        candidate = dict(self.get_document(candidate))
        cosine = 0
        for term_id, tf_idf in query:
            cosine += tf_idf * candidate.setdefault(term_id, 0)
        return cosine

    def query_to_tfidf(self, query):
        """Turn query into vector of normed tf-ids scores
        Parameters
        ----------
        query : iterable of int
                ids of terms in the query
        """
        query = [(term_id, self.tfidf(term_id, 1)) for term_id in query]
        norm = l2_norm([tfidf for _, tfidf in query])
        query = [(term_id, tf_idf/norm) for term_id, tf_idf in query]
        return query


    # interfaces to communicate with inverted_index
    def get_idf(self, term):
        """Get idf for a term
        Parameters
        ----------
        term_id :   int
                    id of term to get idf for
        """
        return self.inverted_index.idf(term)

    def get_term_id(self, term):
        """Get id of a term
        Parameters
        ----------
        term :  str
                Term to get id for"""
        return self.inverted_index.get_term_id(term)

    def get_postings_list(self, term_id):
        """Get document ids term_id appears in.
        Parameters
        ----------
        term_id :   id of term to get postings list for"""
        return self.inverted_index.get_postings_list(term_id)

    def get_document_name(self, doc_id):
        """Get name associated with document id.
        Parameters
        ----------
        doc_id :    int
                    id of document to get name for"""
        return self.inverted_index.get_document_name(doc_id)

    def get_document(self, doc_id):
        """Get term ids and tf-idf scores of terms in a document
        Parameters
        ----------
        doc_id :    int
                    id of document to get doc ids and associated scores for
        """
        return self.inverted_index.get_document(doc_id)

    def tfidf(self, term_id, frequency):
        """Calculate tf-idf.
        Parameters
        ----------
        term_id :   int
                    id of term
        frequency : int
                    Frequency of term"""
        return self.inverted_index.tfidf(term_id, frequency)
