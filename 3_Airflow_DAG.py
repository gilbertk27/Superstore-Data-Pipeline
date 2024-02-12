'''
=================================================
Milestone 3

Name  : Gilbert Kurniawan Hariyanto
Batch : FTDS-026-FTDS

This program is a Directed Acyclic Graph (DAG) that is used to automate the process of 
transforming and loading data from PostgreSQL to ElasticSearch. 
The dataset used in this project is 'Giant' Superstore dataset from Kaggle containing sales data of various products.
=================================================
'''

import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch

def getDataPostgresql():
    '''
    This function is used to get data from PostgreSQL to be cleaned later.

    Parameters:
    -----------
    None

    Return:
    -------
    None
        
    Usage:
    ------------
    >>> getData = PythonOperator(task_id = 'GetDataPostgresql',
                                    python_callable = getDataPostgresql)        
    '''
    conn_string = "dbname='airflow' host='postgres' user='airflow' password='airflow' port='5432'"
    conn = db.connect(conn_string)
    df = pd.read_sql("select * from table_m3",conn)
    df.to_csv('P2M3_gilbert_kurniawan_data_raw.csv')
    
def cleanDataPostgresql(): 
    '''
    This function is used to clean data from PostgreSQL and save it to csv.

    Parameters:
    -----------
    None
    
    Return:
    -------
    None
        
    Usage:
    ------------
    >>> cleanData = PythonOperator(task_id = 'CleanDataPostgresql',
                                    python_callable = cleanDataPostgresql)      
    '''
    conn_string = "dbname='airflow' host='postgres' user='airflow' password='airflow' port='5432'"
    conn = db.connect(conn_string)
     
    df = pd.read_csv('P2M3_gilbert_kurniawan_data_raw.csv', encoding='ISO-8859-1', index_col=0)
     
    # handling duplicate values
    df.drop_duplicates(inplace=True)
     
    # lowercase column names
    for i in df.columns:
        df.rename(columns={i:i.lower()}, inplace=True)
        print(i)
    
    # replace space with underscore
    for i in df.columns:
        df.rename(columns={i:i.replace(' ', '_')}, inplace=True) 
        print(i)
    
    # (for Sub-Category) replace - with _
    df.rename(columns={'sub-category':'sub_category'}, inplace=True)
         
    # check for spaces or special characters
    special_characters = "!@#$%^&*()-+?_=,<>/"

    # Replace special characters in column names
    new_columns = [col.replace(special_characters, '') for col in df.columns]
    df.columns = new_columns    
     
    # Print column names and data types
    for column, dtype in df.dtypes.items():
        print(f"Column: {column}, Data Type: {dtype}")
     
    # Replace missing values in the entire DataFrame
    replacement_values = {
        'object': 'None',    # Replace missing values in object columns with 'None'
        'float64': 0,         # Replace missing values in float columns with 0
        'int64': 0            # Replace missing values in integer columns with 0
    }

    df.fillna(replacement_values, inplace=True)
              
    # save cleaned data to csv
    df.to_csv('/opt/airflow/dags/P2M3_gilbert_kurniawan_data_clean.csv')

def insertElasticsearch():
    '''
    This function is used to insert data from csv to Elasticsearch.

    Parameters:
    -----------
    None
    
    Return:
    -------
    None
        
    Usage:
    ------------
    >>> insertData = PythonOperator(task_id = 'InsertElasticsearch',
                                    python_callable = insertElasticsearch)
    '''
    
    es = Elasticsearch('http://elasticsearch:9200') 
    df = pd.read_csv('/opt/airflow/dags/P2M3_gilbert_kurniawan_data_clean.csv')
    
    for i,r in df.iterrows():
        doc = r.to_json()
        try:
            res = es.index(index="frompostgresql", doc_type="doc", body=doc)
            print(res)	
        except:
            print('error inserting data')
            pass

default_args = {
    'owner': 'GilbertK',
    'start_date': dt.datetime(2024, 1, 23, 22, 50, 0) - dt.timedelta(hours=7),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
} 

with DAG('P2M3_gilbert_kurniawan_DAG',
         default_args = default_args,
         schedule_interval = "30 6 * * *",
         catchup = False
         ) as dag:

    getData = PythonOperator(task_id = 'GetDataPostgresql',
                              python_callable = getDataPostgresql)
    
    cleanData = PythonOperator(task_id = 'CleanDataPostgresql',
                                python_callable = cleanDataPostgresql)

    insertData = PythonOperator(task_id = 'InsertElasticsearch',
                                 python_callable = insertElasticsearch)

getData >> cleanData >> insertData

 
# curl -XPOST "http://localhost:9200/frompostgresql/_delete_by_query" -H "Content-Type: application/json" -d '
# {
#    "query": {
#       "match_all": {}
#    }
# }'