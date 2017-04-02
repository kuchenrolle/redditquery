import os
import time
import spacy
import sqlite3 as lite
from random import choice
from collections import Counter
from string import ascii_lowercase
from lib.index import Numberer
from lib.reddit import DocumentGenerator

class DataBase:

    # create table for the index
    # as (doc_id, term_id, score)-tuples
    def __init__(self, db_file):
        self.connection = lite.connect(database = db_file, isolation_level = None)
        self.cursor = self.connection.cursor()
        self.cursor.execute("PRAGMA journal_mode=OFF")
        self.cursor.execute("PRAGMA synchronous=OFF")
        self.cursor.execute("PRAGMA secure_delete=FALSE")
        self.cursor.execute("PRAGMA page_size=4096")
        self.cursor.execute(
            '''
            CREATE TABLE doc_term_index(
            document_id INTEGER,
            term_id INTEGER,
            score REAL
            )
            ''')
        self.connection.commit()

    # insert document with its corresponding terms/scores
    def insert_document(self, document_id, term_scores):
        self.cursor.executemany(
            '''
            INSERT INTO doc_term_index
            VALUES({},?,?)
            '''.format(document_id),list(term_scores))

    # remove list of terms from both indices
    def remove_terms(self, term_ids):
        self.cursor.executemany(
            '''
            DELETE FROM doc_term_index
            WHERE term_id == ?
            ''', term_ids)
        self.connection.commit()

    # retrieve bag-of-terms scores for document
    # from forward index
    def retrieve_document(self, document_id):
        terms_scores = self.cursor.execute(
            '''
            SELECT term_id, score FROM doc_term_index
            WHERE document_id == ?
            ''',(document_id,)).fetchall()
        return terms_scores

    # change term scores for a given document
    # input is a list of
    # (score, document_id, term_id)-tuples
    def update_document(self, score_tuples):
        self.cursor.executemany(
            '''
            UPDATE doc_term_index SET score = ?
            WHERE document_id == ?
            AND term_id == ?
            ''', score_tuples)
        self.connection.commit()

    def make_index(self, column_name):
        self.cursor.execute("""
            CREATE INDEX {0}_index ON doc_term_index ({0})
            """.format(column_name))
        self.connection.commit()



def DocumentGenerator(num_docs, num_words, word_len):
    for doc_id in range(num_docs):
        document = list()
        for i in range(num_words):
            word = ''.join(choice(ascii_lowercase) for _ in range(word_len))
            document.append(word)
        yield doc_id, document


def main(num_docs = 5000, num_terms = 20, len_term = 3, indices = False):
    database = DataBase("test.db")
    num = Numberer()
    documents = DocumentGenerator(num_docs, num_terms, len_term)
    vocabulary = Counter()

    num_inserts = 0
    if indices == 1:
        database.make_index("term_id")
        database.make_index("document_id")
    insert_start = time.time()
    database.cursor.execute("BEGIN")
    for doc_id, document in documents:
        document = Counter([num.get(term) for term in document])
        vocabulary += document
        num_inserts += len(document)
        database.insert_document(doc_id, list(document.items()))
    database.cursor.execute("END")
    database.connection.commit()
    insert_end = time.time()

    if indices == 2:
        database.make_index("term_id")
        database.make_index("document_id")
    delete_start = time.time()
    infrequent = [(term,) for term, frequency in vocabulary.items() if frequency < 5]
    database.remove_terms(infrequent)
    delete_end = time.time()
    num_deletes = num_inserts - len(database.cursor.execute("SELECT * FROM doc_term_index").fetchall())

    if indices == 3:
        database.make_index("term_id")
        database.make_index("document_id")
    update_start = time.time()
    for doc_id in range(num_docs):
        previous = database.retrieve_document(doc_id)
        new = [(score+1, doc_id, term) for term, score in previous]
        database.update_document(new)
    update_end = time.time()

    print("{} inserts took {} seconds".format(num_inserts, int(insert_end-insert_start)), end = "\n")
    print("{} deletions took {} seconds".format(num_deletes, int(delete_end-delete_start)))
    print("{} updates took {} seconds".format(num_inserts - num_deletes, int(update_end-update_start)))

    os.remove("test.db")



if __name__ == "__main__":
    print("No Indices:\n")
    main()
    print("Indices before Insert:\n")
    main(indices = 1)
    print("Indices before Delete\n")
    main(indices = 2)
    print("Indices before Update\n")
    main(indices = 3)


# no index
# 99941 inserts took 6 seconds
# 18344 deletions took 37 seconds
# 81597 updates took 511 seconds

# index first
# 99953 inserts took 6 seconds
# 18040 deletions took 0 seconds
# 81913 updates took 2 seconds

# index before delete
# 99950 inserts took 6 seconds
# 18073 deletions took 0 seconds
# 81877 updates took 2 seconds

# index before update
# 99944 inserts took 6 seconds
# 17932 deletions took 41 seconds
# 82012 updates took 2 seconds
