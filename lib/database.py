#!/usr/bin/python3
import os
import sqlite3 as lite
from itertools import repeat

# relationalizes a sparse term-document matrix
# as (row_id, doc_id, value)-tuples
class DataBase:

    # create databse with table for the term-document index
    # or connect to existing one
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

    # insert document with its corresponding terms/scores
    # into index table
    def insert_document(self, document_id, term_scores):
        self.cursor.executemany(
            '''
            INSERT INTO doc_term_table
            VALUES({},?,?)
            '''.format(document_id),list(term_scores))

    # retrieve postings list for term
    def retrieve_term(self, term_id):
        document_ids = self.cursor.execute(
            '''
            SELECT document_id FROM doc_term_table
            WHERE term_id == ?
            ''',(term_id,)).fetchall()
        return [document_id[0] for document_id in document_ids]

    # retrieve term with scores for document
    def retrieve_document(self, document_id):
        terms_scores = self.cursor.execute(
            '''
            SELECT term_id, score FROM doc_term_table
            WHERE document_id == ?
            ''',(document_id,)).fetchall()
        return terms_scores

    # remove list of terms from table
    def remove_terms(self, term_ids):
        self.cursor.executemany(
            '''
            DELETE FROM doc_term_table
            WHERE term_id == ?
            ''', term_ids)
        self.connection.commit()

    # change term scores of a given document
    def update_documents(self, score_tuples):
        self.cursor.executemany(
            '''
            UPDATE doc_term_table SET score = ?
            WHERE document_id == ?
            AND term_id == ?
            ''', score_tuples)
        self.connection.commit()

    # create index on column
    def create_index(self, column_name):
        self.cursor.execute("""
            CREATE INDEX {0}_index ON doc_term_table ({0})
            """.format(column_name))
        self.connection.commit()

    # create composite index on document_id and term_id
    def create_covering_index(self):
        self.cursor.execute("""
            CREATE INDEX covering_index ON doc_term_table (document_id, term_id)
            """)
        self.connection.commit()

    # start transaction for fast inserts
    def prepare_inserts(self):
        self.cursor.execute("BEGIN")

    # commit inserts, create index on term_id for fast deletions
    def prepare_deletes(self):
        self.connection.commit()
        self.create_index("term_id")

    # create index on document_id and covering index for fast updates
    def prepare_updates(self):
        self.create_index("document_id")
        self.create_covering_index()