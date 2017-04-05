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
        self.document_ids = dict()

        self.make_indices(documents = documents)
        self.remove_infrequent(frequency_threshold)
        self.transform_to_tfidf()


    def make_indices(self, documents):
        """Insert documents into index.
        Parameters
        ----------
        documents : iterable of tuples of int, list of strings
                    Documents to insert into index
        """
        self.prepare_inserts()
        for document in documents:
            self.process_document(document)


    def process_document(self, document):
        """Add document to index.
        Parameters
        ----------
        document :      tuple of int, list of strings
                        Document to be processed
        """
        doc_id = document[0]
        terms = [self.vocabulary_indices.get(term) for term in document[1]]
        term_counts = Counter(terms)
        self.insert_document(self.num_documents, list(term_counts.items()))
        self.document_ids[self.num_documents] = doc_id
        self.num_documents += 1


    def remove_infrequent(self, frequency_threshold):
        """Remove infrequent terms from index.
        Parameters
        ----------
        frequency_threshold :   int
                                frequency below which ids will be selected
        """
        infrequent = self.get_infrequent(frequency_threshold)
        self.prepare_deletes()
        self.remove_terms(infrequent)
        self.vocabulary_indices.remove_values(set([term[0] for term in infrequent]))


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
            #update database every 10000 documents
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
        idf = log2(self.num_documents / max(self.get_document_frequency(term_id),1))
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


    def get_infrequent(self, frequency_threshold):
        """Get ids for term with a total frequency lower than threshold.
        Parameters
        ----------
        frequency_threshold :   int
                                frequency below which ids will be selected
        """
        return self.database.get_infrequent(frequency_threshold)


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

    def get_document_frequency(self, term_id):
        """Get number of documents term id appears in.
        Parameters
        ----------
        term_id :   int
                    id of term to get number of containing documents for
        """
        return self.database.get_document_frequency(term_id)

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
