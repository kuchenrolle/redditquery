#!/usr/bin/python3
import os
import sqlite3
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
        self.connection = sqlite3.connect(database = database_file, isolation_level = None)
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
                score REAL,
                PRIMARY KEY (document_id, term_id)
                )
                ''')
            self.cursor.execute(
                '''
                CREATE TABLE document_table(
                document_id INTEGER PRIMARY KEY,
                document_name NVARCHAR,
                fulltext NVARCHAR
                )
                ''')
            # auto-delete documents that cannot be queried after term deletion
            self.cursor.execute(
                '''
                CREATE TRIGGER auto_delete AFTER DELETE ON doc_term_table 
                BEGIN
                DELETE FROM document_table
                WHERE document_table.document_id = old.document_id
                AND NOT EXISTS
                (SELECT 1 FROM doc_term_table AS s
                WHERE s.document_id = old.document_id);
                END;
                ''')
            self.connection.commit()
        else:
            assert(self.table_exists("doc_term_table"))
            assert(self.table_exists("document_table"))


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
        self.cursor.execute(
            '''
            INSERT INTO document_table
            VALUES(?,?,?)
            ''',(document_id, document_name, fulltext))
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


    def get_fulltext(self, document_id):
        """Retrieve text of a document by its id.
        Parameters
        ----------
        document_id :   int
                        id of document
        """
        fulltext = self.cursor.execute(
            '''
            SELECT fulltext FROM document_table
            WHERE document_id == ?
            ''',(document_id,)).fetchone()
        return fulltext[0]


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


    def get_document_frequency(self, term_id):
        """Get number of documents term id appears in.
        Parameters
        ----------
        term_id :   int
                    id of term to get number of containing documents for
        """
        documents = self.cursor.execute(
            '''
            SELECT document_id FROM doc_term_table
            WHERE term_id == ?
            ''',(term_id,)).fetchall()
        return len(documents)


    def get_document_name(self, document_id):
        """Get name associated with document id.
        Parameters
        ----------
        doc_id :    int
                    id of document to get name for
        """
        document_name = self.cursor.execute(
            '''
            SELECT document_name FROM document_table
            WHERE document_id == ?
            ''', (document_id,)).fetchone()
        return document_name[0]


    def get_infrequent(self, frequency_threshold):
        """Get ids for term with a total frequency lower than threshold.
        Parameters
        ----------
        frequency_threshold :   int
                                frequency below which ids will be selected
        """
        infrequent = self.cursor.execute(
            '''
            SELECT term_id FROM
            (SELECT term_id, sum(score) AS total
            FROM doc_term_table
            GROUP BY term_id)
            WHERE total <= ?
            ''',(frequency_threshold,)).fetchall()
        return infrequent


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


    def table_exists(self, table):
        """Check whether table exists.
        Parameters
        ----------
        table : str
                Name of the table
        """
        exists = self.cursor.execute("""
            SELECT name FROM sqlite_master WHERE type='table' AND name='{}'
            """.format(table)).fetchone()
        return exists is not None


    def prepare_inserts(self):
        """start transaction for fast inserts"""
        self.cursor.execute("BEGIN")

    def prepare_deletes(self):
        """commit inserts, create index on term_id for fast deletions"""
        self.connection.commit()
        self.create_index("term_id")

    def prepare_updates(self):
        """create index on document id for fast updates"""
        self.create_index("document_id")

    def prepare_searches(self):
        """Vacuum."""
        self.connection.execute('VACUUM')
        self.connection.commit()