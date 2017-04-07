#!/usr/bin/python3
import sys
import spacy
from math import log2
from heapq import nlargest
from functools import partial
from collections import Counter
from redditquery.utils import Numberer, l2_norm


class InvertedIndex:
    """
    Creates an InvertedIndex stored in a database.
    Parameters
    ----------
    database :              database object
                            Database to use
    documents :             iterable of tuples of int, list of strings, json
                            Documents to insert into index
    frequency_threshold :   int
                            Minimum frequency for terms to be in index
    """
    def __init__(self, database, documents, frequency_threshold):
        self.num_documents = 0
        self.database = database
        self.vocabulary_indices = Numberer()

        self.make_indices(documents = documents)
        self.remove_infrequent(frequency_threshold)
        self.transform_to_tfidf()
        self.prepare_searches()


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
        document :      tuple of int, list of strings, json
                        Document to be processed
        """
        document_name = document[0]
        fulltext = document[2]
        terms = [self.vocabulary_indices.get(term) for term in document[1]]
        term_counts = Counter(terms)
        self.insert_document(self.num_documents, document_name, term_counts.items(), fulltext)
        self.num_documents += 1


    def remove_infrequent(self, frequency_threshold):
        """Remove infrequent terms from index.
        Parameters
        ----------
        frequency_threshold :   int
                                frequency below which terms will be removed
        """
        infrequent = self.get_infrequent(frequency_threshold)
        self.prepare_deletes()
        self.remove_terms(infrequent)
        self.vocabulary_indices.remove_values([term[0] for term in infrequent])


    def transform_to_tfidf(self):
        """Turn frequency counts in index into pmi values."""
        self.prepare_updates()
        updates = list()
        for doc_id in range(self.num_documents):
            frequencies = self.get_document(doc_id)
            tfidfs = [(term_id, self.tfidf(term_id, frequency)) for term_id, frequency in frequencies]
            norm = l2_norm([tfidf for _, tfidf in tfidfs])
            normed = [(tfidf/norm, doc_id, term_id) for term_id, tfidf in tfidfs]
            updates += normed
            #update database every 10000 documents
            if doc_id%10000 == 0:
                self.update_documents(updates)
                updates = list()
        # update remaining documents
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
        return frequency * self.get_idf(term_id)


    def get_idf(self, term_id):
        """Calculate idf.
        Parameters
        ----------
        term_id :   int
                    id of term
        """
        idf = log2(self.num_documents / max(self.get_document_frequency(term_id),1))
        return idf



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

    def get_document_name(self, doc_id):
        """get document name associated with doc id.
        Parameters
        ----------
        doc_id :    int
                    id of document
        """
        return self.database.get_document_name(doc_id)


    def get_infrequent(self, frequency_threshold):
        """Get ids for term with a total frequency lower than threshold.
        Parameters
        ----------
        frequency_threshold :   int
                                frequency below which ids will be selected
        """
        return self.database.get_infrequent(frequency_threshold)


    def get_document(self, doc_id):
        """Retrieve document from database by id.
        Parameters
        ----------
        doc_id :    int
                    id of document
        """
        return self.database.retrieve_document(doc_id)

    def insert_document(self, document_id, document_name, term_scores, fulltext):
        """Insert document into database table(s).
        Parameters
        ----------
        document_id :   int
                        id of the document
        document_name : str
                        Name of the document
        term_scores :   iterable of tuples of int, float
                        Term ids and term scores
        fulltext :      str
                        string of document's text
        """
        self.database.insert_document(document_id, document_name, term_scores, fulltext)

    def remove_terms(self, infrequent):
        """Remove list of terms from database.
        Parameters
        ----------
        term_ids :  iterable of singleton tuples of int
                    term ids to be removed
        """
        self.database.remove_terms(infrequent)

    def update_documents(self, updates):
        """Change term scores of a given document.
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

    def get_fulltext(self, doc_id):
        """Retrieve text of a document by its id.
        Parameters
        ----------
        document_id :   int
                        id of document
        """
        return self.database.get_fulltext(doc_id)

    def prepare_inserts(self):
        """Prepare database for insertions."""
        self.database.prepare_inserts()

    def prepare_deletes(self):
        """Prepare database for deletions."""
        self.database.prepare_deletes()

    def prepare_updates(self):
        """Prepare database for updates."""
        self.database.prepare_updates()

    def prepare_searches(self):
        """Pepare database for searches."""
        self.database.prepare_searches()


class QueryProcessor():
    """Queries an inverted index.
    Parameters
    ----------
    inverted_index :    InvertedIndex
                        Index to be queried
    lemmatize :         Boolean
                        Lemmatize queries
    """

    def __init__(self, inverted_index, lemmatize):
        self.inverted_index = inverted_index
        self.lemmatize = lemmatize
        self.nlp = spacy.load("en")


    def query_index(self, query, num_results, fulltext, conjunctive):
        """Query the index.
        Parameters
        ----------
        query :         str
                        Query to be processed
        num_results :   int
                        Number of most similar results to return
        fulltext :      Boolean
                        Return documents' full text as results
        conjunctive :   Boolean
                        Return documents containing all rather than any of the query terms
        """
        tokens = self.nlp(query)
        if self.lemmatize:
            query = [token.lemma_.strip().lower() for token in tokens if not token.pos_.startswith(u"PU")] # filter punctuation
        else:
            query = [token.string.strip().lower() for token in tokens if not token.pos_.startswith(u"PU")]
        # ignore multiple occurrences, keep as list to guarantee
        # query[i] belongs to term_ids[i]
        query = list(set(query))
        term_ids = [self.get_term_id(term) for term in query]
        # get all documents containing any of the query terms
        candidates = set()
        for term_id in term_ids:
            doc_ids = self.get_postings_list(term_id)
            if 'candidates' not in locals():
                candidates = set(doc_ids)
            elif conjunctive:
                candidates.intersection_update(doc_ids)
            else:
                candidates.update(doc_ids)
        # get similarity between documents and query
        query_tfidfs = self.query_to_tfidf(term_ids)
        get_similarity = partial(self.get_similarity, query_tfidfs = query_tfidfs)
        similarities = map(get_similarity, candidates)
        for i, term in enumerate(query):
            term_idf = self.get_idf(term_ids[i])
            sys.stdout.write("idf({0}): {1:2f}\n".format(term, term_idf))
        for similarity, doc_id in nlargest(num_results, similarities):
            doc_name = self.get_document_name(doc_id)
            sys.stdout.write("{0} ({1:3f}): {2}\n".format(doc_id, similarity, doc_name))
            if fulltext:
                text = self.get_fulltext(doc_id)
                sys.stdout.write(text.strip()+"\n\n")
        sys.stdout.write("\n")


    def get_similarity(self, candidate, query_tfidfs):
        """Calculate cosine similarity between candidate and query.
        Parameters
        ----------
        candidate :     int
                        id of candidate document
        query_tfidfs :  iterable of tuples of int, float
                        ids and tfidfs of terms in the query
        """
        candidate_tfidfs = dict(self.get_document(candidate))
        cosine = 0
        for term_id, tf_idf in query_tfidfs:
            if term_id in candidate_tfidfs:
                cosine += tf_idf * candidate_tfidfs[term_id]
        return cosine, candidate


    def query_to_tfidf(self, query):
        """Turn query into vector of normed tf-ids scores
        Parameters
        ----------
        query : iterable of int
                ids of terms in the query
        """
        query_tfidfs = [(term_id, self.tfidf(term_id, 1)) for term_id in query]
        norm = l2_norm([tfidf for _, tfidf in query_tfidfs])
        query_normed = [(term_id, tf_idf/norm) for term_id, tf_idf in query_tfidfs]
        return query_normed


    # interfaces to communicate with inverted_index
    def get_idf(self, term_id):
        """Get idf for a term
        Parameters
        ----------
        term_id :   int
                    id of term to get idf for
        """
        return self.inverted_index.get_idf(term_id)

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

    def get_fulltext(self, doc_id):
        """Retrieve text of a document by its id.
        Parameters
        ----------
        doc_id :    int
                    id of document
        """
        return self.inverted_index.get_fulltext(doc_id)

    def tfidf(self, term_id, frequency):
        """Calculate tf-idf.
        Parameters
        ----------
        term_id :   int
                    id of term
        frequency : int
                    Frequency of term"""
        return self.inverted_index.tfidf(term_id, frequency)
