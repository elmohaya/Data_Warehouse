import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    
    # this function copy tha data from S3
    # to the staging tables created in sql_queries file
    
    #Input: cur, conn
    #Output: the data is transfered to staging tables
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    
    # this function insert the data from staging tables
    # into the schema we created (songplays, users, artists...)
    
    #Input: cur, conn
    #Output: data are modeled in a star schema
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    # here we call the above two functions to insert and create 
    # our schema after we establish the connection to the DB
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()