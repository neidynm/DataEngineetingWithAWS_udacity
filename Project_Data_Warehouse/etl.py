import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def queries_analytics(cur, conn):
    print("What is the most played song?")
    cur.execute(most_played_song)

    print("Which artist has the most streams?")
    cur.execute(most_streams_artist)

    print("when we have the most traffic for the top 10 most played artists?")
    cur.execute(most_traffic_top10_artists)

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    queries_analytics(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()