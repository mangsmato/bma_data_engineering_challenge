#!/usr/bin/env python
# coding: utf-8

# In[3]:


try:
    # import the library to connect to db
    import psycopg2
    import sqlalchemy
    
    # Establish a connection to the PostgreSQL server
    host = "localhost"
    user = "postgres"
    password = "mm4@syIT"
    conn = psycopg2.connect(host=host, user=user, password=password)

    # Create a cursor object
    cursor = conn.cursor()

    # Specify the database name
    database_name = "pcard_expenditures"

    # Check if the database already exists
    cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", (database_name,))
    exists = cursor.fetchone()

    # If the database doesn't exist, create it
    if not exists:
        cursor.execute("COMMIT")  # End any active transaction
        cursor.execute("CREATE DATABASE {}".format(database_name))
        print(f"{database_name} Database created successfully.")
    else:
        print(f"{database_name} Database already exists")

    # Commit the changes
    conn.commit()
    
    # create an sqlachemy object
    # connection_uri = f"postgresql://{user}:{password}@{host}:5432/{database_name}"    
    connection_uri = "postgresql://postgres:mm4easy!T@localhost:5432/pcard_expenditures"
    db_conn = sqlalchemy.create_engine(connection_uri)

    # Close the cursor and the connection
#     cursor.close()
#     conn.close()
    
except Exception as e:
        print(f"An error occurred {e}")


# In[ ]:




