'''
=================================================
Milestone 3

Name  : Gilbert Kurniawan Hariyanto
Batch : FTDS-026-FTDS

This program is a script to load data from CSV to PostgreSQL.
=================================================
'''

import pandas as pd
import psycopg2 as db

# Connect to PostgreSQL
conn_string = "dbname='airflow' host='localhost' user='airflow' password='airflow' port='5434'"
conn = db.connect(conn_string)
cur = conn.cursor()

# Drop existing table
# drop_table_query = 'DROP TABLE IF EXISTS table_m3;'
# cur.execute(drop_table_query)
# conn.commit()

# Create the table
sql = '''
    CREATE TABLE IF NOT EXISTS table_m3 (
        "Row ID"            INT NOT NULL PRIMARY KEY,
        "Order ID"          VARCHAR(255),
        "Order Date"        Date,
        "Ship Date"         Date,
        "Ship Mode"         VARCHAR(255),
        "Customer ID"       VARCHAR(255),
        "Customer Name"     VARCHAR(255),
        "Segment"           VARCHAR(255),
        "Country"           VARCHAR(255),
        "City"              VARCHAR(255),
        "State"             VARCHAR(255),
        "Postal Code"       INT,
        "Region"            VARCHAR(255),
        "Product ID"        VARCHAR(255),
        "Category"          VARCHAR(255),
        "Sub-Category"      VARCHAR(255),
        "Product Name"      VARCHAR(255),
        "Sales"             FLOAT,
        "Quantity"          INT,
        "Discount"          FLOAT,
        "Profit"            FLOAT   
    );
'''

cur.execute(sql)
conn.commit()

# Insert data
df = pd.read_csv('P2M3_gilbert_kurniawan_data_raw.csv', encoding='ISO-8859-1')
print('Start data insertion...')
for index, row in df.iterrows():
    insert_query = 'INSERT INTO table_m3 ("Row ID", "Order ID", "Order Date", "Ship Date", "Ship Mode", "Customer ID", "Customer Name", "Segment", "Country", "City", "State", "Postal Code", "Region", "Product ID", "Category", "Sub-Category", "Product Name", "Sales", "Quantity", "Discount", "Profit") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
    values = list(row)
    cur.execute(insert_query, values)

print('Data inserted successfully.')
conn.commit()
conn.close()
