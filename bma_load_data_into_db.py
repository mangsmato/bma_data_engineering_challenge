#!/usr/bin/env python
# coding: utf-8

# In[1]:


# import necessary modules
from datetime import datetime
import pandas as pd
import psycopg2


# In[2]:


# Establish a connection to the PostgreSQL server
host = "localhost"
user = "postgres"
password = "mm4easy1T"
conn = psycopg2.connect(host=host,  user=user, password=password)


# In[3]:


# print start of the job
start_time = datetime.now()
print('Data Load job started at at {}'.format(start_time))


# In[4]:


# import data from the reports folder (staging environment)
df_raw = pd.read_csv('Reports/Combined_pcard_expenditures.csv')
print(df_raw.shape)
df_raw.head()


# In[5]:


# drop any duplicates
df_raw.drop_duplicates(subset=['BATCHTRANSACTIONID'],inplace=True)
print(df_raw.shape)


# In[6]:


df_raw.columns


# In[7]:


# we now want to save our data in the database
# we get all the records into individual rows and save them at once
raw_rows = []
for index, row in df_raw.iterrows():
    record = (row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],
              row[9],row[10],row[11],row[12],row[13],row[14],row[15],row[16])
    raw_rows.append(record)
raw_rows[:2]


# In[8]:


# save the raw data
try:
    # disable autocommit
    conn.autocommit = True
    
    # Create a cursor object
    cursor = conn.cursor()
    
    # transaction 1: truncate the raw data table
    sql = f'TRUNCATE TABLE pcard_expenditures.raw_data_transactions'
    cursor.execute(sql)
    
    # transaction 2: save the new data
    sql = """
        INSERT INTO pcard_expenditures.raw_data_transactions ("Division", "BatchTransactionID", 
        "TransactionDate", "CardPostingDate", "MerchantName", "TransactionAmount", "TransactionCurrency", 
        "OriginalAmount", "OriginalCurrency", "GLAccount", "GLAccountDescription", "CostCenterWBSElementOrder", 
        "CostCenterWBSElementOrderDescription", "MerchantType", "MerchantTypeDescription", "Purpose", "SourceFileName")
     VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    cursor.executemany(sql,raw_rows)    
    conn.commit()
    
    # Close the cursor
    cursor.close()
    
    print(f'{len(df_raw)} rows successfully saved in pcard_expenditures.raw_data_transactions')
    
except Exception as e:
    print(f'An error occurred while inserting records: {e}')


# In[9]:


end_time = datetime.now()
print(f"""
Data Load job started at {start_time}.
Data Load job finished execution at {end_time}.
Total time taken is for Data Load is {end_time-start_time}
""")


# In[ ]:




