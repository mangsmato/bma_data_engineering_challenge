#!/usr/bin/env python
# coding: utf-8

# In[1]:


# import necessary modules
from datetime import datetime


# In[2]:


# print start of the job
start_time = datetime.now()
print('Entire ELT job started at at {}'.format(start_time))


# In[3]:


def main():
    # import the script to extract the data
    import bma_extract_data_from_api
    
    # import the script to load the raw data
    import bma_load_data_into_db
    
    # import the script to transform the data
    import bma_transform_data
    
    # import the script to aggregate the data
    import bma_data_aggregation
    


# In[4]:


if __name__ == "__main__":
    main()


# In[5]:


end_time = datetime.now()
print(f"""
Entire ELT job started at {start_time}.
Entire ELT job finished execution at {end_time}.
Total time taken is for Entire ELT is {end_time-start_time}
""")


# In[ ]:




