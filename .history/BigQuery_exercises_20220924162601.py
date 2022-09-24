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

# Exercises
# 1) Units of measurement
# Which countries have reported pollution levels in units of "ppm"? In the code cell below, set first_query to an SQL query that pulls the appropriate entries from the country column.

# In case it's useful to see an example query, here's some code from the tutorial:

query = """
        SELECT city
        FROM `bigquery-public-data.openaq.global_air_quality`
        WHERE country = 'US'
        """

# Query to select countries with units of "ppm"
first_query = """
                SELECT country
                FROM `bigquery-public-data.openaq.global_air_quality`
                WHERE unit = 'ppm'
                """

# Set up the query (cancel the query if it would use too much of
# your quota, with the limit set to 10 GB)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
first_query_job = client.query(first_query, job_config=safe_config)

# API request - run the query, and return a pandas DataFrame
first_results = first_query_job.to_dataframe()

# View top few rows of results
print(first_results.head())

# 2) High air quality
# Which pollution levels were reported to be exactly 0?

# Set zero_pollution_query to select all columns of the rows where the value column is 0.
# Set zero_pollution_results to a pandas DataFrame containing the query results.

# Query to select all columns where pollution levels are exactly 0
zero_pollution_query = """
        SELECT *
        FROM `bigquery-public-data.openaq.global_air_quality`
        WHERE value = 0
        """

# Set up the query
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
query_job = client.query(zero_pollution_query, job_config=safe_config)

# API request - run the query and return a pandas DataFrame
zero_pollution_results = query_job.to_dataframe()  # Your code goes here

print(zero_pollution_results.head())
"""
That query wasn't too complicated, and it got the data you want. But these SELECT queries don't organizing data in a way that answers the most interesting questions. For that, we'll need the GROUP BY command.

If you know how to use groupby() in pandas, this is similar. But BigQuery works quickly with far larger datasets.
"""
