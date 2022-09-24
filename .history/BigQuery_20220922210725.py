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
