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
print('Data Transformation job started at at {}'.format(start_time))


# In[3]:


# Establish a connection to the PostgreSQL server
host = "localhost"
user = "postgres"
password = "mm4easy1T"
conn = psycopg2.connect(host=host,  user=user, password=password)


# In[4]:


df_raw = pd.read_sql(f"SELECT * FROM pcard_expenditures.raw_data_transactions",conn)
print(df_raw.shape)
df_raw.head()


# ### Infer Tables

# ### TRANSACTIONS

# In[5]:


df_raw.columns


# In[6]:


# transactions
transactions = df_raw.groupby(['BatchTransactionID','TransactionDate', 'CardPostingDate',
                               'TransactionAmount', 'TransactionCurrency', 'OriginalAmount','OriginalCurrency',
                               'GLAccount','CostCenterWBSElementOrder','MerchantName',
                               'Division','Purpose'
                              ]).size().reset_index(name='Count').drop(columns='Count')
print(transactions.shape)
transactions.head()


# In[7]:


transactions.columns


# In[8]:


# we now want to save our data in the database
# we get all the records into individual rows and save them at once
trxn_rows = []
for index, row in transactions.iterrows():
    record = (row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],
              row[9],row[10],row[11])
    trxn_rows.append(record)
trxn_rows[:2]


# In[9]:


# create a table
try:
    # disable autocommit
    conn.autocommit = False
    # Create a cursor object
    cursor = conn.cursor()
    
    table_name = "pcard_expenditures.pcard_transactions"    
    
    # transaction 1: create the table
    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        "BatchTransactionID" VARCHAR(250), 
        "TransactionDate" DATE, 
        "CardPostingDate" DATE, 
        "TransactionAmount" VARCHAR(250), 
        "TransactionCurrency" VARCHAR(250), 
        "OriginalAmount" VARCHAR(250), 
        "OriginalCurrency" VARCHAR(250), 
        "GLAccount" VARCHAR(250), 
        "CostCenterWBSElementOrder" VARCHAR(250), 
        "MerchantName" VARCHAR(250), 
        "Division" VARCHAR(250), 
        "Purpose" VARCHAR(250)
    )
    """
    cursor.execute(sql) 
    
    print(f'{table_name} successfully created')
    
    # transaction 2: truncate the table
    sql = f'TRUNCATE TABLE {table_name}'
    cursor.execute(sql)
    
    # transaction 3: save the new data
    sql = f"""
        INSERT INTO {table_name} ("BatchTransactionID",  "TransactionDate", "CardPostingDate",
         "TransactionAmount", "TransactionCurrency","OriginalAmount", "OriginalCurrency","GLAccount",
         "CostCenterWBSElementOrder", "MerchantName","Division", "Purpose")
     VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    
    
    cursor.executemany(sql,trxn_rows)
    # Commit the changes
    conn.commit()
    
    # Close the cursor and the connection
    cursor.close()

    print(f'{len(transactions)} rows successfully saved in {table_name}')
    
except Exception as e:
    print(f'An error occurred: {e}')


# ### MERCHANTS

# In[10]:


# merchants
merchants = df_raw.groupby(['MerchantName','MerchantType']).size().reset_index(name='Count').drop(columns='Count')
print(merchants.shape)
merchants.head()


# In[11]:


# remove the digits after decimal point
merchants['MerchantType'] = merchants['MerchantType'].apply(lambda x: x.replace('.0',''))
merchants.head()


# In[12]:


# we now want to save our data in the database
# we get all the records into individual rows and save them at once
merc_rows = []
for index, row in merchants.iterrows():
    record = (row[0],row[1])
    merc_rows.append(record)
merc_rows[:2]


# In[13]:


# create a table
try:
    # disable autocommit
    conn.autocommit = False
    # Create a cursor object
    cursor = conn.cursor()
    
    table_name = "pcard_expenditures.pcard_merchants"    
    
    # transaction 1: create the table
    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        "MerchantName" VARCHAR(250), 
        "MerchantType" VARCHAR(10), 
    )
    """
    cursor.execute(sql) 
    # Close the cursor and the connection
    cursor.close()
    print(f'{table_name} successfully created')
    
    # transaction 2: truncate the table
    sql = f'TRUNCATE TABLE {table_name}'
    cursor.execute(sql)
    
    # transaction 3: save the new data
    sql = f"""
        INSERT INTO {table_name} ("MerchantName",  "MerchantType") VALUES (%s,%s)
    """
    
    
    cursor.executemany(sql,merc_rows)
    # Commit the changes
    conn.commit()
    
    # Close the cursor and the connection
    cursor.close()

    print(f'{len(merchants)} rows successfully saved in {table_name}')
    
except Exception as e:
    print(f'An error occurred: {e}')


# ### MERCHANT TYPES

# In[14]:


# merchants
merchant_types = df_raw.groupby(['MerchantType','MerchantTypeDescription']).size().reset_index(name='Count').drop(columns='Count')
print(merchant_types.shape)
merchant_types.head()


# In[15]:


# remove the digits after decimal point
merchant_types['MerchantType'] = merchant_types['MerchantType'].apply(lambda x: x.replace('.0',''))
merchant_types.head()


# In[16]:


# we now want to save our data in the database
# we get all the records into individual rows and save them at once
merc_type_rows = []
for index, row in merchant_types.iterrows():
    record = (row[0],row[1])
    merc_type_rows.append(record)
merc_type_rows[:2]


# In[17]:


# create a table
try:
    # disable autocommit
    conn.autocommit = False
    # Create a cursor object
    cursor = conn.cursor()
    
    table_name = "pcard_expenditures.pcard_merchant_types"    
    
    # transaction 1: create the table
    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        "MerchantType" VARCHAR(10), 
        "MerchantTypeDescription" VARCHAR(250), 
    )
    """
    cursor.execute(sql) 
    # Close the cursor and the connection
    cursor.close()
    print(f'{table_name} successfully created')
    
    # transaction 2: truncate the table
    sql = f'TRUNCATE TABLE {table_name}'
    cursor.execute(sql)
    
    # transaction 3: save the new data
    sql = f"""
        INSERT INTO {table_name} ("MerchantType",  "MerchantTypeDescription") VALUES (%s,%s)
    """
    
    
    cursor.executemany(sql,merc_type_rows)
    # Commit the changes
    conn.commit()
    
    # Close the cursor and the connection
    cursor.close()

    print(f'{len(merchant_types)} rows successfully saved in {table_name}')
    
except Exception as e:
    print(f'An error occurred: {e}')


# ### GL ACCOUNTS

# In[18]:


df_raw.columns


# In[19]:


# merchants
gl_accounts = df_raw.groupby(['GLAccount','GLAccountDescription']).size().reset_index(name='Count').drop(columns='Count')
print(gl_accounts.shape)
gl_accounts.head()


# In[20]:


# we now want to save our data in the database
# we get all the records into individual rows and save them at once
gl_rows = []
for index, row in gl_accounts.iterrows():
    record = (row[0],row[1])
    gl_rows.append(record)
gl_rows[:2]


# In[21]:


# create a table
try:
    # disable autocommit
    conn.autocommit = False
    # Create a cursor object
    cursor = conn.cursor()
    
    table_name = "pcard_expenditures.pcard_gl_accounts"    
    
    # transaction 1: create the table
    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        "GLAccount" VARCHAR(250), 
        "GLAccountDescription" VARCHAR(250), 
    )
    """
    cursor.execute(sql) 
    # Close the cursor and the connection
    cursor.close()
    print(f'{table_name} successfully created')
    
    # transaction 2: truncate the table
    sql = f'TRUNCATE TABLE {table_name}'
    cursor.execute(sql)
    
    # transaction 3: save the new data
    sql = f"""
        INSERT INTO {table_name} ("GLAccount",  "GLAccountDescription") VALUES (%s,%s)
    """
    
    
    cursor.executemany(sql,gl_rows)
    # Commit the changes
    conn.commit()
    
    # Close the cursor and the connection
    cursor.close()

    print(f'{len(gl_accounts)} rows successfully saved in {table_name}')
    
except Exception as e:
    print(f'An error occurred: {e}')


# ### WBS Elements

# In[22]:


# merchants
wbs = df_raw.groupby(['CostCenterWBSElementOrder','CostCenterWBSElementOrderDescription']).size().reset_index(name='Count').drop(columns='Count')
print(wbs.shape)
wbs.head()


# In[23]:


# we now want to save our data in the database
# we get all the records into individual rows and save them at once
wbs_rows = []
for index, row in wbs.iterrows():
    record = (row[0],row[1])
    wbs_rows.append(record)
wbs_rows[:2]


# In[24]:


# create a table
try:
    # disable autocommit
    conn.autocommit = False
    # Create a cursor object
    cursor = conn.cursor()
    
    table_name = "pcard_expenditures.pcard_wbs_elements"    
    
    # transaction 1: create the table
    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        "CostCenterWBSElementOrder" VARCHAR(250), 
        "CostCenterWBSElementOrderDescription" VARCHAR(250), 
    )
    """
    cursor.execute(sql) 
    # Close the cursor and the connection
    cursor.close()
    print(f'{table_name} successfully created')
    
    # transaction 2: truncate the table
    sql = f'TRUNCATE TABLE {table_name}'
    cursor.execute(sql)
    
    # transaction 3: save the new data
    sql = f"""
        INSERT INTO {table_name} ("CostCenterWBSElementOrder",  "CostCenterWBSElementOrderDescription") VALUES (%s,%s)
    """
    
    
    cursor.executemany(sql,wbs_rows)
    # Commit the changes
    conn.commit()
    
    # Close the cursor and the connection
    cursor.close()

    print(f'{len(wbs)} rows successfully saved in {table_name}')
    
except Exception as e:
    print(f'An error occurred: {e}')


# In[25]:


end_time = datetime.now()
print(f"""
Data Transformation job started at {start_time}.
Data Transformation job finished execution at {end_time}.
Total time taken is for Data Transformation is {end_time-start_time}
""")


# In[ ]:




