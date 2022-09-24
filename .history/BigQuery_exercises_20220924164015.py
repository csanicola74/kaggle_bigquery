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

############################
##  Select, From & Where  ##
############################

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

################################
##  Group By, Having & Count  ##
################################

# The code cell below fetches the comments table from the hacker_news dataset. We also preview the first five rows of the table.

# Create a "Client" object
client = bigquery.Client()

# Construct a reference to the "hacker_news" dataset
dataset_ref = client.dataset("hacker_news", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

# Construct a reference to the "comments" table
table_ref = dataset_ref.table("comments")

# API request - fetch the table
table = client.get_table(table_ref)

# Preview the first five lines of the "comments" table
client.list_rows(table, max_results=5).to_dataframe()

"""
Exercises
1) Prolific commenters
Hacker News would like to send awards to everyone who has written more than 10,000 posts. Write a query that returns all authors with more than 10,000 posts as well as their post counts. Call the column with post counts NumPosts.

In case sample query is helpful, here is a query you saw in the tutorial to answer a similar question:

query = \"""
        SELECT parent, COUNT(1) AS NumPosts
        FROM `bigquery-public-data.hacker_news.comments`
        GROUP BY parent
        HAVING COUNT(1) > 10
        \"""
"""

# Query to select prolific commenters and post counts
prolific_commenters_query = """
    SELECT author, COUNT(1) AS NumPosts
    FROM `bigquery-public-data.hacker_news.comments`
    GROUP BY author
    HAVING COUNT(1) >10000
    """

# Set up the query (cancel the query if it would use too much of
# your quota, with the limit set to 1 GB)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
query_job = client.query(prolific_commenters_query, job_config=safe_config)

# API request - run the query, and return a pandas DataFrame
prolific_commenters = query_job.to_dataframe()

# View top few rows of results
print(prolific_commenters.head())

"""2) Deleted comments
How many comments have been deleted? (If a comment was deleted, the deleted column in the comments table will have the value True.)

add Codeadd Markdown
# Query to select prolific commenters and post counts
deleted_comments_query = 
    \"""
    SELECT deleted, COUNT(1) AS NumPosts
    FROM `bigquery-public-data.hacker_news.comments`
    GROUP BY deleted
    \"""
"""

# Set up the query (cancel the query if it would use too much of
# your quota, with the limit set to 1 GB)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
query_job = client.query(deleted_comments_query, job_config=safe_config)

# API request - run the query, and return a pandas DataFrame
deleted_comments = query_job.to_dataframe()

# View top few rows of results
print(deleted_comments.head())

num_deleted_posts = ____  # Put your answer here

################
##  Order By  ##
################

# The World Bank has made tons of interesting education data available through BigQuery. Run the following cell to see the first few rows of the international_education table from the world_bank_intl_education dataset.


# Create a "Client" object
client = bigquery.Client()

# Construct a reference to the "world_bank_intl_education" dataset
dataset_ref = client.dataset(
    "world_bank_intl_education", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

# Construct a reference to the "international_education" table
table_ref = dataset_ref.table("international_education")

# API request - fetch the table
table = client.get_table(table_ref)

# Preview the first five lines of the "international_education" table
client.list_rows(table, max_results=5).to_dataframe()

"""
Exercises
The value in the indicator_code column describes what type of data is shown in a given row.

One interesting indicator code is SE.XPD.TOTL.GD.ZS, which corresponds to "Government expenditure on education as % of GDP (%)".

1) Government expenditure on education
Which countries spend the largest fraction of GDP on education?

To answer this question, consider only the rows in the dataset corresponding to indicator code SE.XPD.TOTL.GD.ZS, and write a query that returns the average value in the value column for each country in the dataset between the years 2010-2017 (including 2010 and 2017 in the average).

Requirements:

Your results should have the country name rather than the country code. You will have one row for each country.
The aggregate function for average is AVG(). Use the name avg_ed_spending_pct for the column created by this aggregation.
Order the results so the countries that spend the largest fraction of GDP on education show up first.
In case it's useful to see a sample query, here's a query you saw in the tutorial (using a different dataset):

# Query to find out the number of accidents for each day of the week
query = \"""
        SELECT COUNT(consecutive_number) AS num_accidents, 
               EXTRACT(DAYOFWEEK FROM timestamp_of_crash) AS day_of_week
        FROM `bigquery-public-data.nhtsa_traffic_fatalities.accident_2015`
        GROUP BY day_of_week
        ORDER BY num_accidents DESC
        \"""
"""

country_spend_pct_query = """
                          SELECT country_name, AVG(value) AS avg_ed_spending_pct
                          FROM `bigquery-public-data.world_bank_intl_education.international_education`
                          WHERE indicator_code = 'SE.XPD.TOTL.GD.ZS' and year >= 2010 and year <= 2017
                          GROUP BY country_name
                          ORDER BY avg_ed_spending_pct DESC
                          """
# Set up the query (cancel the query if it would use too much of
# your quota, with the limit set to 1 GB)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
country_spend_pct_query_job = client.query(
    country_spend_pct_query, job_config=safe_config)

# API request - run the query, and return a pandas DataFrame
country_spending_results = country_spend_pct_query_job.to_dataframe()

# View top few rows of results
print(country_spending_results.head())

"""
2) Identify interesting codes to explore
The last question started by telling you to focus on rows with the code SE.XPD.TOTL.GD.ZS. But how would you find more interesting indicator codes to explore?

There are 1000s of codes in the dataset, so it would be time consuming to review them all. But many codes are available for only a few countries. When browsing the options for different codes, you might restrict yourself to codes that are reported by many countries.

Write a query below that selects the indicator code and indicator name for all codes with at least 175 rows in the year 2016.

Requirements:

You should have one row for each indicator code.
The columns in your results should be called indicator_code, indicator_name, and num_rows.
Only select codes with 175 or more rows in the raw database (exactly 175 rows would be included).
To get both the indicator_code and indicator_name in your resulting DataFrame, you need to include both in your SELECT statement (in addition to a COUNT() aggregation). This requires you to include both in your GROUP BY clause.
Order from results most frequent to least frequent.
"""

# Your code goes here
code_count_query = """
                          SELECT indicator_code, indicator_name, COUNT(1) AS num_rows
                          FROM `bigquery-public-data.world_bank_intl_education.international_education`
                          WHERE year = 2016
                          GROUP BY indicator_code, indicator_name
                          HAVING COUNT(1) >= 175
                          ORDER BY COUNT(1) DESC
                          """


# Set up the query
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
code_count_query_job = client.query(code_count_query, job_config=safe_config)

# API request - run the query, and return a pandas DataFrame
code_count_results = code_count_query_job.to_dataframe()

# View top few rows of results
print(code_count_results.head())


#################
##  As & With  ##
#################

####################
##  Joining Data  ##
####################
