import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Description: This function is responsible to load data in staging tables 

    Arguments:
        cur: the cursor object.
        conn: Redshit DB connection.

    Returns:
        None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Description: This function is responsible to load data in final dimensional star scheme tables for analytical team

    Arguments:
        cur: the cursor object.
        conn: Redshit DB connection.

    Returns:
        None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Description: This function is responsible to process all the user event and Songs data 
    which are stored in S3 Storage. firstly load them in staging tables to process them 
    and then load them in to final analytics tables.

    Arguments:
        None

    Returns:
        None
    """

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()