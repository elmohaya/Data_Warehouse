import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    
    # the objective of this function is to
    # drop existing tables in DB by executing
    # drop_table_queries that are in sql_queries file
    
    #Input: cur, conn
    #Output: Dropping the tables
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    
    # the objective of this function is to
    # create tables in DB by executing
    # create_table_queries that are in sql_queries file
    
    #Input: cur, conn
    #Output: creating the tables
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    # here in main we establish the connection
    # and create the tables by first dropping 
    # old tables
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()