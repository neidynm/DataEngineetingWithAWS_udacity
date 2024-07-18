#README

##Project: Data Warehouse
###Introduction
The music streaming startup, Sparkify, allows their users to stream music from their S3 servers and now they want to analyze the user trends in order to inform their product. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This project aims to enable Sparkify to better understand their customer base by transforming the data from their current system into a set of dimensional tables for their analytics teams. They are looking into understanding:
* What is the most played song?
* Which artist has the most streams?
* When do we have the most traffic?
* Which platform users connect from?

###Prerequisites
Python 3.x
AWS account with IAM role and permissions
Configured S3 buckets with the source data
Configured AWS Redshift cluster

##Configuration
The script uses the configparser library to read configurations from a dwh.cfg file. Ensure that the dwh.cfg file is correctly set up with the necessary parameters:

```
[S3]
LOG_JSONPATH = 's3://path/to/log_jsonpath'
SONG_DATA = 's3://path/to/song_data'

[IAM_ROLE]
ARN = 'arn:aws:iam::your-account-id:role/your-role-name'
ROLE_NAME = 'your-role-name'

[CLUSTER]
HOST='your-cluster-endpoint'
DB_NAME='your-db-name'
DB_USER='your-db-user'
DB_PASSWORD='your-db-password'
DB_PORT='your-db-port'
```



##Script Details
###create_tables.py
The create_tables.py script is responsible for setting up the database schema by dropping any existing tables and creating the necessary tables for the ETL process.

####Import Libraries
```python
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
```

####Drop Tables
The script includes functions to drop existing tables if they exist:

```python
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
```

####Create Tables
The script includes functions to create necessary staging and final tables:

```python
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

```

####Main Function
The main function establishes a connection to the Redshift cluster and executes the drop and create table functions:

```python
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()

```

###etl.py
The etl.py script is responsible for extracting data from S3, transforming it, and loading it into the staging and final tables in Redshift.

####Import Libraries
```python
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
```

####Load Staging Tables
The script includes functions to load data into the staging tables:

```python
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


```

####Insert Data into Final Tables
The script includes functions to insert data into the final fact and dimension tables:

```python
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
```

####Main Function
The main function establishes a connection to the Redshift cluster and executes the load and insert table functions:


```python
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()
```

##How to Run the Project
First, start with the creation of the different tables supporting the ETL process. The script will create the staging tables and the data analytics tables.

To run the create_tables.py script, open a terminal window and run the following command:

```bash
python create_tables.py
```

Once the tables are created, the next step is to start the ETL pipeline. For that, run the etl.py script in your terminal window by following the command:
```bash
python etl.py
```

