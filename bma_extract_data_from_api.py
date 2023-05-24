#!/usr/bin/env python
# coding: utf-8

# In[1]:


# import necessary modules
import pandas as pd
import requests
import zipfile
import os
from datetime import datetime


# In[2]:


# print start of the job
start_time = datetime.now()
print('Data Extraction job started at at {}'.format(start_time))


# In[3]:


# Set display option to show all columns
pd.set_option('display.max_columns', None)


# In[4]:


# create a folder to hold data from the zipped file
data_folder = 'Data'
# Check if the folder exists
if not os.path.exists(data_folder):
    # Create the folder
    os.makedirs(data_folder)
    print(f'{data_folder} folder created successfully')
else:
    print(f'{data_folder} folder already exists')


# In[5]:


# create a folder to hold the imported data
reports_folder = 'Reports'
# Check if the folder exists
if not os.path.exists(reports_folder):
    # Create the folder
    os.makedirs(reports_folder)
    print(f'{reports_folder} folder created successfully')
else:
    print(f'{reports_folder} folder already exists')


# In[6]:


# url of the API
base_url = "https://ckan0.cf.opendata.inter.prod-toronto.ca"

# retrieve the metadata for pcard-expenditures package and its resources
url = base_url + "/api/3/action/package_show"
params = { "id": "pcard-expenditures"}
package = requests.get(url, params = params, verify=False).json()


# In[7]:


package


# In[8]:


package.keys()


# In[9]:


# get the urls with data
data_url = ''
for r in package['result']['resources']:
    print(r['format'],r['url'])
    if r['format'] == 'ZIP':
        data_url = r['url']

print(f'Link for data {data_url}')


# In[10]:


# Send a GET request to the URL
data_response = requests.get(data_url, verify=False)


# In[11]:


# Save the content of the response to a file
downloaded_zip = 'expenditures.zip'
with open(downloaded_zip, "wb") as file:
    file.write(data_response.content)


# In[12]:


# we want to extract the contents of the downloaded zip folder
# Open the ZIP file
with zipfile.ZipFile(downloaded_zip,'r') as zip_obj:
    # extract the data into the data folder we created
    zip_obj.extractall(data_folder)


# In[13]:


# create a function to clean column names
def clean_column_names(columns):    
    # replace / with nothing
    cleaned_columns = columns.str.replace('/','')
    # remove double spaces
    cleaned_columns = cleaned_columns.str.replace(' ','')
    # remove full stops
    cleaned_columns = cleaned_columns.str.replace('.','',regex=False)
    # remove hyphens
    cleaned_columns = cleaned_columns.str.replace('-','')
    # remove new lines
    cleaned_columns = cleaned_columns.str.replace('\n','')
    # remove #
    cleaned_columns = cleaned_columns.str.replace('#','')
    # make them upper case
    cleaned_columns = cleaned_columns.str.upper()
    # remove Order
    cleaned_columns = cleaned_columns.str.replace('ORDER','')
    # remove No
    cleaned_columns = cleaned_columns.str.replace('NO','')
    # remove Number
    cleaned_columns = cleaned_columns.str.replace('NUMBER','')
    # remove Work
    cleaned_columns = cleaned_columns.str.replace('WORK','')
    # remove Work
    cleaned_columns = cleaned_columns.str.replace('(MCC)','',regex=False)
    # standardize (WBLS and WBS)
    cleaned_columns = cleaned_columns.str.replace('ORIGINALCURRENCY1','TRANSACTIONCURRENCY') 
    # remove 1
    cleaned_columns = cleaned_columns.str.replace('1','')
    
    # standardize the column names (especially centre and center)
    cleaned_columns = cleaned_columns.str.replace('CENTRE','CENTER')
    # standardize (GL Account and Expense Type Descriptions)
    cleaned_columns = cleaned_columns.str.replace('EXPTYPEDESC','GLACCOUNTDESCRIPTION')
    # standardize (Elelment and Element)
    cleaned_columns = cleaned_columns.str.replace('ELELMENT','ELEMENT')
    # standardize (Descrption and Description)
    cleaned_columns = cleaned_columns.str.replace('DESCRPTION','DESCRIPTION')
    # standardize (Discription and Description)
    cleaned_columns = cleaned_columns.str.replace('DISCRIPTION','DESCRIPTION')
    # standardize (Discriprion and Description)
    cleaned_columns = cleaned_columns.str.replace('DECRIPTION','DESCRIPTION')
    # standardize (Discriprion and Description)
    cleaned_columns = cleaned_columns.str.replace('DISCRIPRION','DESCRIPTION')
    # standardize (WBLS and WBS)
    cleaned_columns = cleaned_columns.str.replace('WBLS','WBS')    
    # standardize (Dt and Date)
    cleaned_columns = cleaned_columns.str.replace('DT','DATE')
    # standardize (Divison and Division)
    cleaned_columns = cleaned_columns.str.replace('DIVISON','DIVISION')
    # standardize (Divison and Division)
    cleaned_columns = cleaned_columns.str.replace('EXPENSE','ACCOUNT')
    # standardize (Amt and Amount)
    cleaned_columns = cleaned_columns.str.replace('AMT','AMOUNT')
    # standardize (Trx and Amount)
    cleaned_columns = cleaned_columns.str.replace('TRX','TRANSACTION')
    # standardize (TrCurrency and Amount)
    cleaned_columns = cleaned_columns.str.replace('TRCURRENCY','TRANSACTIONCURRENCY')
    # standardize (LongText and GLAccountDescription)
    cleaned_columns = cleaned_columns.str.replace('LONGTEXT','GLACCOUNTDESCRIPTION')
    
    # strip spaces from column names
    cleaned_columns = cleaned_columns.str.strip()
    
    
    return cleaned_columns


# In[14]:


# import data from the extracted files above

# an empty list that will hold our data
li_df = []
# list of files we extracted
li_files = os.listdir(data_folder)
print(f'we shall import data from {len(li_files)} files')

# initialize a counter
c = 0

for file in li_files:
    # import the data from the files
    df = pd.read_excel(data_folder + '\\' + file)
    
    # clean column names     
    df.columns = clean_column_names(df.columns)
    
    # drop null records
    df.dropna(how='any',inplace=True)
    
    # drop null columns
    df.dropna(how='all',axis=1,inplace=True)
    
    # reset the index
    df.reset_index(drop=True,inplace=True)
    
    # add a column to idenfity which file we extracted the data from
    df['SOURCE_FILE_NAME'] = file
    
    # add the data to our list of dataframes
    li_df.append(df)
    print(f'{c+1}. data from {file} imported successfully. Rows: {df.shape[0]}. Columns: {df.shape[1]}')    
    c += 1


# In[16]:


# combine the imported data frames into one
combined_df = pd.concat(li_df,ignore_index=True)
print(combined_df.shape)
combined_df


# In[17]:


# export the data to a csv file
combined_df.to_csv(f'{reports_folder}\\Combined_pcard_expenditures.csv',index=False)


# In[18]:


end_time = datetime.now()
print(f"""
Data Extraction job started at {start_time}.
Data Extraction job finished execution at {end_time}.
Total time taken is for Data Extraction is {end_time-start_time}
""")


# In[ ]:




