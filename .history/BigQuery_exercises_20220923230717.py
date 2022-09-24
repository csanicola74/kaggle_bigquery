# Set up feedack system
from google.cloud import bigquery
from learntools.sql.ex1 import *
from learntools.core import binder


binder.bind(globals())
print("Setup Complete")


# Create a "Client" object
client = bigquery.Client()

# Construct a reference to the "chicago_crime" dataset
dataset_ref = client.dataset("chicago_crime", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

####  Exercises  ####
# 1) Count tables in the dataset
# How many tables are in the Chicago Crime dataset?

# Write the code you need here to figure out the answer
# List all the tables in the "chicago_crime" dataset
tables = list(client.list_tables(dataset))
# Print number of tables in the dataset
print(len(tables))

# List all the tables in the "chicago_crime" dataset
tables = list(client.list_tables(dataset))

# Print names of all tables in the dataset (there are four!)
for table in tables:
    print(table.table_id)
