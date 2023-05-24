#!/usr/bin/env python
# coding: utf-8

# In[1]:


# import necessary modules
from datetime import datetime
import pandas as pd
import psycopg2


# In[2]:


# print start of the job
start_time = datetime.now()
print('Data Aggregation job started at at {}'.format(start_time))


# In[3]:


# Establish a connection to the PostgreSQL server
host = "localhost"
user = "postgres"
password = "mm4easy1T"
conn = psycopg2.connect(host=host,  user=user, password=password)


# In[4]:


# create a table
try:
    # disable autocommit
    conn.autocommit = False
    # Create a cursor object
    cursor = conn.cursor()
    
    table_name = "pcard_expenditures.agg_division_transations"   
    
    # transaction 1: create the table
    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        "Division" VARCHAR(250), 
        "TransactionAmount" float,
        "SavedOn" date DEFAULT CURRENT_DATE
    )
    """
    cursor.execute(sql) 
    
    # transaction 2: truncate the table
    sql = f'TRUNCATE TABLE {table_name}'
    cursor.execute(sql)
    
    # transaction 3: save the aggregated data
    sql = f"""
        insert into {table_name} ("Division", "TransactionAmount")
        SELECT "Division", SUM(CAST("TransactionAmount"AS float))
        FROM pcard_expenditures.raw_data_transactions
        group by "Division"
    """
    
    cursor.execute(sql)
    # Commit the changes
    conn.commit()
    
    # Close the cursor and the connection
    cursor.close()
    
    print('Completed data aggregation')
    
except Exception as e:
    print(f'An error occurred: {e}')


# In[5]:


end_time = datetime.now()
print(f"""
Data Aggregation job started at {start_time}.
Data Aggregation job finished execution at {end_time}.
Total time taken is for Data Aggregation is {end_time-start_time}
""")

