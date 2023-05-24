#!/usr/bin/env python
# coding: utf-8

# In[1]:


try:
    # import the library to connect to db
    import psycopg2
    
    # Establish a connection to the PostgreSQL server
    host = "localhost"
    user = "postgres"
    password = "mm4easy1T"
    conn = psycopg2.connect(host=host, user=user, password=password)

    # Create a cursor object
    cursor = conn.cursor()

    # Specify the database name
    database_name = "pcard_expenditures"

    # If the database doesn't exist, create it    
    cursor.execute("COMMIT")  # End any active transaction
    cursor.execute("CREATE SCHEMA IF NOT EXISTS {}".format(database_name))
    
    # Specify the table name
    database_name = "pcard_expenditures"
    table_name = "raw_data_transactions"
    # create the table that holds raw data
    table_sql = f"""
    CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (
        "Division" TEXT, 
        "BatchTransactionID" TEXT, 
        "TransactionDate" TEXT, 
        "CardPostingDate" TEXT, 
        "MerchantName" TEXT, 
        "TransactionAmount" TEXT, 
        "TransactionCurrency" TEXT, 
        "OriginalAmount" TEXT, 
        "OriginalCurrency" TEXT, 
        "GLAccount" TEXT, 
        "GLAccountDescription" TEXT, 
        "CostCenterWBSElementOrder" TEXT, 
        "CostCenterWBSElementOrderDescription" TEXT, 
        "MerchantType" TEXT, 
        "MerchantTypeDescription" TEXT, 
        "Purpose" TEXT, 
        "SourceFileName" TEXT
    )
    """
    cursor.execute(table_sql)     
   
    # Commit the changes
    conn.commit()
    
    # save a new connection for later use
    db_conn = psycopg2.connect(host=host, database=database_name, user=user, password=password)
    
    print('Database connected successfully')

    # Close the cursor and the connection
    cursor.close()
    conn.close()
    
except Exception as e:
        print(f"An error occurred {e}")

