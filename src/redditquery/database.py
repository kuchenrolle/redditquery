#!/usr/bin/python3
import os
import sqlite3 as lite
from itertools import repeat

class DataBase:
    """Relationalizes a sparse term-document matrix
    as (row_id, doc_id, value)-tuples.
    Parameters
    ----------
    database_file : String or path object
                    File to use to store database in
    existing :      Boolean, optional
                    Connect to existing database instead of creating one
    """

    def __init__(self, database_file, existing = False):
        db_exists = os.path.isfile(database_file)
        if db_exists and not existing:
            raise FileExistsError("Database file already exists!")
        self.connection = lite.connect(database = database_file, isolation_level = None)
        self.cursor = self.connection.cursor()
        self.cursor.execute("PRAGMA journal_mode=OFF")
        self.cursor.execute("PRAGMA synchronous=OFF")
        self.cursor.execute("PRAGMA secure_delete=FALSE")
        self.cursor.execute("PRAGMA page_size=4096")
        if not existing:
            self.cursor.execute(
                '''
                CREATE TABLE doc_term_table(
                document_id INTEGER,
                term_id INTEGER,
                score REAL
                )
                ''')
            self.connection.commit()

    def insert_document(self, document_id, term_scores):
        """Insert document with its corresponding terms/scores
                    into index table.
        Parameters
        ----------
        document_id :   int
                        id of the document
        term_scores :   iterable of tuples of int, float
                        term ids and term scores
        """
        self.cursor.executemany(
            '''
            INSERT INTO doc_term_table
            VALUES({},?,?)
            '''.format(document_id),list(term_scores))

    def retrieve_term(self, term_id):
        """Retrieve postings list for term
        Parameters
        ----------
        term_id :   int
                    id of term whose postings list is to be retrieved
        """
        document_ids = self.cursor.execute(
            '''
            SELECT document_id FROM doc_term_table
            WHERE term_id == ?
            ''',(term_id,)).fetchall()
        return [document_id[0] for document_id in document_ids]

    def retrieve_document(self, document_id):
        """Retrieve terms and scores for a document from index table.
        Parameters
        ----------
        document_id :   int
                        id of document to be retrieved
        """
        terms_scores = self.cursor.execute(
            '''
            SELECT term_id, score FROM doc_term_table
            WHERE document_id == ?
            ''',(document_id,)).fetchall()
        return terms_scores

    def remove_terms(self, term_ids):
        """Remove list of terms from index table
        Parameters
        ----------
        term_ids :  iterable of singleton tuples of int
                    term ids to be removed
        """
        self.cursor.executemany(
            '''
            DELETE FROM doc_term_table
            WHERE term_id == ?
            ''', term_ids)
        self.connection.commit()

    def update_documents(self, score_tuples):
        """Change term scores of a given document
        Parameters
        ----------
        score_tuples :  iterable of tuples of float, int, int
                        scores for document ids and term ids to be updated
        """
        self.cursor.executemany(
            '''
            UPDATE doc_term_table SET score = ?
            WHERE document_id == ?
            AND term_id == ?
            ''', score_tuples)
        self.connection.commit()

    def create_index(self, column_name):
        """Create index over column.
        Parameters
        ----------
        column_name :   str
                        Name of the column to create index over
        """
        self.cursor.execute("""
            CREATE INDEX {0}_index ON doc_term_table ({0})
            """.format(column_name))
        self.connection.commit()

    def create_covering_index(self):
        """Create composite index on document_id and term_id"""
        self.cursor.execute("""
            CREATE INDEX covering_index ON doc_term_table (document_id, term_id)
            """)
        self.connection.commit()

    def prepare_inserts(self):
        """start transaction for fast inserts"""
        self.cursor.execute("BEGIN")

    def prepare_deletes(self):
        """commit inserts, create index on term_id for fast deletions"""
        self.connection.commit()
        self.create_index("term_id")

    def prepare_updates(self):
        """create index on document_id and covering index for fast updates"""
        self.create_index("document_id")
        self.create_covering_index()