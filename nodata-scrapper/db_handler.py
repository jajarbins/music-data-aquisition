import sqlite3
from sqlite3 import Error
import os
import sys


def get_db_path():
    """
    get the directory from where the script is launched and add the db to this path

    Note: will certainly have to change the db path to regroup all the db in same place, not sure of
    the architecture for th moment

    :return (str): the path for the db
    """
    return os.path.join(sys.path[0], "my_db.db")


def create_connection(db_file_path):
    """ create a SQLite database if it doesn't exists """
    connection = None
    try:
        connection = sqlite3.connect(db_file_path)
    except Error as e:
        print(e)
    finally:
        if connection:
            connection.close()


def create_table(db_file_path):
    """create a table if it doesn't exists """
    connection = None
    try:
        connection = sqlite3.connect(db_file_path)  # to access the db
        cursor = connection.cursor()  # to be able to manipulate the db

        cursor.execute('''CREATE TABLE IF NOT EXISTS records (artist text, record_name text, 
        record_type text, tag text, label text)''')  # creation_date date,

        connection.commit()  # commit all the changes
    except Error as e:
        print(e)
    finally:
        if connection:
            connection.close()


def init_nodata_db(db_file_path):
    if os.path.isfile(db_file_path):
        create_connection(db_file_path)
    create_table(db_file_path)


def insert_rows(db_file_path, ):
    conn = None
    try:
        conn = sqlite3.connect(db_file_path)
        c = conn.cursor()
        purchases = [('daft punk', 'Homework', 'LP', 'good stuff', 'Virgin')]  # '2006-03-28',
        c.executemany('INSERT INTO records VALUES (?,?,?,?,?)', purchases)
        conn.commit()
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()


def get_posts(db_file_path):
    conn = None
    try:
        conn = sqlite3.connect(db_file_path)
        cur = conn.cursor()
        with conn:
            cur.execute("SELECT * FROM records")
            print(cur.fetchall())
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    db_path = get_db_path()
    init_nodata_db(db_path)
    insert_rows(db_path)
    get_posts(db_path)


