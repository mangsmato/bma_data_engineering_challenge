# ELT Job for City of Toronto monthly reports of its employee’s expense card transactions

This repository contains an ELT (Extract, Load, Transform) job that facilitates data processing and transformation.

## Objectives

The ELT job is designed to perform the following tasks:

- Extract data from various sources.
- Load the extracted data into a target destination, such as a data warehouse or database.
- Perform necessary transformations on the data to meet specific requirements.

## Dependencies

The ELT job has the following dependencies:

- Programming Language: [Python](https://www.python.org)
- Database Engine: [PostgreSQL](https://www.postgresql.org/)
- Data Warehouse: [PostgreSQL](https://www.postgresql.org/)
- Additional Libraries: (numpy, pandas, psycopg2, python-dateutil, pytz, six, tzdata)

## Configuration

Before running the ELT job, ensure that you have set up the necessary configuration:

1. Install the PostgreSQL on your environment

2. Set up a user with admin privileges

## Usage

To run the ELT job, follow these steps:

1. Install the required dependencies by running the following command:

   ```bash
   pip install -r requirements.txt

2. Run the ***bma_main_elt_file.py*** file that will execute all the other scripts in the predefined order

## Note
- The python files and jupyter notebooks are well documented for proper understanding
