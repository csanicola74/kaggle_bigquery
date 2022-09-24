# Set up feedack system
from learntools.sql.ex2 import *
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

num_tables = 1  # Store the answer as num_tables and then run this cell

# 2) Explore the table schema
# How many columns in the crime table have TIMESTAMP data?

# Write the code to figure out the answer
# Construct a reference to the "crime" table
table_ref = dataset_ref.table("crime")

# API request - fetch the table
table = client.get_table(table_ref)

# Print information on all the columns in the "crime" table in the "chicago_crime" dataset
print(table.schema)

num_timestamp_fields = 2  # Put your answer here

# 3) Create a crime map
# If you wanted to create a map with a dot at the location of each crime, what are the names of the two fields you likely need to pull out of the crime table to plot the crimes on a map?

# Write the code here to explore the data so you can find the answer
client.list_rows(table, max_results=5).to_dataframe()

fields_for_plotting = ['x_coordinate', 'y_coordinate']  # Put your answers here

# Thinking about the question above, there are a few columns that appear to have geographic data. Look at a few values (with the list_rows() command) to see if you can determine their relationship. Two columns will still be hard to interpret. But it should be obvious how the location column relates to latitude and longitude.

# Try writing some SELECT statements of your own to explore a large dataset of air pollution measurements.
# Set up feedback system
binder.bind(globals())
print("Setup Complete")

# The code cell below fetches the global_air_quality table from the openaq dataset. We also preview the first five rows of the table.

# Create a "Client" object
client = bigquery.Client()

# Construct a reference to the "openaq" dataset
dataset_ref = client.dataset("openaq", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

# Construct a reference to the "global_air_quality" table
table_ref = dataset_ref.table("global_air_quality")

# API request - fetch the table
table = client.get_table(table_ref)

# Preview the first five lines of the "global_air_quality" table
client.list_rows(table, max_results=5).to_dataframe()
