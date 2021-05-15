import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Description: This function is responsible to drop all staging and final tables which are define in sql_queries.py 

    Arguments:
        cur: the cursor object.
        conn: Redshit DB connection.

    Returns:
        None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Description: This function is responsible to create all staging and final tables which are define in sql_queries.py 

    Arguments:
        cur: the cursor object.
        conn: Redshit DB connection.

    Returns:
        None
    """

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Description: This function is responsible to drop and create new tables.

    Arguments:
        None

    Returns:
        None
    """

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()