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

# You'll work with a dataset about taxi trips in the city of Chicago. Run the cell below to fetch the chicago_taxi_trips dataset.


# Create a "Client" object
client = bigquery.Client()

# Construct a reference to the "chicago_taxi_trips" dataset
dataset_ref = client.dataset(
    "chicago_taxi_trips", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

"""
Exercises
You are curious how much slower traffic moves when traffic volume is high. This involves a few steps.

1) Find the data
Before you can access the data, you need to find the table name with the data.

Hint: Tab completion is helpful whenever you can't remember a command. Type client. and then hit the tab key. Don't forget the period before hitting tab.
"""

tables = list(client.list_tables(dataset))
for table in tables:
    print(table.table_id)

# Write the table name as a string below
table_name = 'taxi_trips'

# 2) Peek at the data
# Use the next code cell to peek at the top few rows of the data. Inspect the data and see if any issues with data quality are immediately obvious.

table_ref = dataset_ref.table("taxi_trips")
table = client.get_table(table_ref)

# Construct a reference to the "taxi_trips" table
table_ref = dataset_ref.table("taxi_trips")

# API request - fetch the table
table = client.get_table(table_ref)

# Preview the first five lines of the "taxi_trips" table
client.list_rows(table, max_results=5).to_dataframe()

# Some trips in the top few rows have trip_seconds or trip_miles values of 0. Other location fields have values of None. That is a problem if we want to use those fields.

"""
3) Determine when this data is from
If the data is sufficiently old, we might be careful before assuming the data is still relevant to traffic patterns today. Write a query that counts the number of trips in each year.

Your results should have two columns:

    * year - the year of the trips
    * num_trips - the number of trips in that year

Hints:
    * When using GROUP BY and ORDER BY, you should refer to the columns by the alias year that you set at the top of the SELECT query.
    * The SQL code to SELECT the year from trip_start_timestamp is SELECT EXTRACT(YEAR FROM trip_start_timestamp)
    * The FROM field can be a little tricky until you are used to it. The format is :
        1. A backick(the symbol `).
        2. The project name. In this case it is bigquery-public-data.
        3. A period.
        4. The dataset name. In this case, it is chicago_taxi_trips.
        5. A period.
        6. The table name. You used this as your answer in 1) Find the data.
        7. A backtick(the symbol `).
"""

rides_per_year_query = """
                       SELECT EXTRACT(YEAR FROM trip_start_timestamp) AS year,
                              COUNT(1) AS num_trips
                       FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
                       GROUP BY year
                       ORDER BY year
                       """

# Set up the query (cancel the query if it would use too much of
# your quota)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
rides_per_year_query_job = client.query(
    rides_per_year_query, job_config=safe_config)

# API request - run the query, and return a pandas DataFrame
rides_per_year_result = rides_per_year_query_job.to_dataframe()

# View results
print(rides_per_year_result)

"""
4) Dive slightly deeper
You'd like to take a closer look at rides from 2017. Copy the query you used above in rides_per_year_query into the cell below for rides_per_month_query. Then modify it in two ways:

    1. Use a WHERE clause to limit the query to data from 2017.
    2. Modify the query to extract the month rather than the year.
"""
rides_per_month_query = """
                        SELECT EXTRACT(MONTH FROM trip_start_timestamp) AS month,
                               COUNT(1) AS num_trips
                        FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
                        WHERE EXTRACT(YEAR FROM trip_start_timestamp) = 2017
                        GROUP BY month
                        ORDER BY month
                        """

# Set up the query (cancel the query if it would use too much of
# your quota)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
rides_per_month_query_job = client.query(
    rides_per_month_query, job_config=safe_config)

# API request - run the query, and return a pandas DataFrame
rides_per_month_result = rides_per_month_query_job.to_dataframe()

# View results
print(rides_per_month_result)

"""
5) Write the query
It's time to step up the sophistication of your queries. Write a query that shows, for each hour of the day in the dataset, the corresponding number of trips and average speed.

Your results should have three columns:
    * hour_of_day - sort by this column, which holds the result of extracting the hour from trip_start_timestamp.
    * num_trips - the count of the total number of trips in each hour of the day(e.g. how many trips were started between 6AM and 7AM, independent of which day it occurred on).
    * avg_mph - the average speed, measured in miles per hour, for trips that started in that hour of the day. Average speed in miles per hour is calculated as 3600 * SUM(trip_miles) / SUM(trip_seconds). (The value 3600 is used to convert from seconds to hours.)

Restrict your query to data meeting the following criteria:
    * a trip_start_timestamp between 2017-01-01 and 2017-07-01
    * trip_seconds > 0 and trip_miles > 0
You will use a common table expression(CTE) to select just the relevant rides. Because this dataset is very big, this CTE should select only the columns you'll need to create the final output (though you won't actually create those in the CTE - - instead you'll create those in the later SELECT statement below the CTE).

This is a much harder query than anything you've written so far. Good luck!
"""

speeds_query = """
               WITH RelevantRides AS
               (
                   SELECT EXTRACT(HOUR FROM trip_start_timestamp) AS hour_of_day, 
                          trip_miles, 
                          trip_seconds
                   FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
                   WHERE trip_start_timestamp > '2017-01-01' AND 
                         trip_start_timestamp < '2017-07-01' AND 
                         trip_seconds > 0 AND 
                         trip_miles > 0
               )
               SELECT hour_of_day, 
                      COUNT(1) AS num_trips, 
                      3600 * SUM(trip_miles) / SUM(trip_seconds) AS avg_mph
               FROM RelevantRides
               GROUP BY hour_of_day
               ORDER BY hour_of_day
               """

# Set up the query (cancel the query if it would use too much of
# your quota)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
speeds_query_job = client.query(speeds_query, job_config=safe_config)

# API request - run the query, and return a pandas DataFrame
speeds_result = speeds_query_job.to_dataframe()

####################
##  Joining Data  ##
####################

"""
Stack Overflow is a widely beloved question and answer site for technical questions. You'll probably use it yourself as you keep using SQL (or any programming language).

Their data is publicly available. What cool things do you think it would be useful for ?

Here's one idea: You could set up a service that identifies the Stack Overflow users who have demonstrated expertise with a specific technology by answering related questions about it, so someone could hire those experts for in -depth help.

In this exercise, you'll write the SQL queries that might serve as the foundation for this type of service.

Run the next cell to fetch the stackoverflow dataset.
"""

# Create a "Client" object
client = bigquery.Client()

# Construct a reference to the "stackoverflow" dataset
dataset_ref = client.dataset("stackoverflow", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)


# Exercises
# ) Explore the data
# Before writing queries or JOIN clauses, you'll want to see what tables are available.

# Get a list of available tables
tables = list(client.list_tables(dataset))
list_of_tables = [table.table_id for table in tables]

# Print your answer
print(list_of_tables)

# Get a list of available tables
tables = list(client.list_tables(dataset))
list_of_tables = [table.table_id for table in tables]

# 2) Review relevant tables
# If you are interested in people who answer questions on a given topic, the posts_answers table is a natural place to look. Run the following cell, and look at the output.

# Construct a reference to the "posts_answers" table
answers_table_ref = dataset_ref.table("posts_answers")

# API request - fetch the table
answers_table = client.get_table(answers_table_ref)

# Preview the first five lines of the "posts_answers" table
client.list_rows(answers_table, max_results=5).to_dataframe()

# Construct a reference to the "posts_questions" table
questions_table_ref = dataset_ref.table("posts_questions")

# API request - fetch the table
questions_table = client.get_table(questions_table_ref)

# Preview the first five lines of the "posts_questions" table
client.list_rows(questions_table, max_results=5).to_dataframe()

"""
Solution: posts_questions has a column called tags which lists the topics/technologies each question is about.

posts_answers has a column called parent_id which identifies the ID of the question each answer is responding to. posts_answers also has an owner_user_id column which specifies the ID of the user who answered the question.

You can join these two tables to:

determine the tags for each answer, and then
select the owner_user_id of the answers on the desired tag.
This is exactly what you will do over the next few questions.

3) Selecting the right questions
A lot of this data is text.

We'll explore one last technique in this course which you can apply to this text.

A WHERE clause can limit your results to rows with certain text using the LIKE feature. For example, to select just the third row of the pets table from the tutorial, we could use the query in the picture below.

You can also use % as a "wildcard" for any number of characters. So you can also get the third row with:

query = \"""
        SELECT * 
        FROM `bigquery-public-data.pet_records.pets` 
        WHERE Name LIKE '%ipl%'
        \"""
Try this yourself. Write a query that selects the id, title and owner_user_id columns from the posts_questions table.

Restrict the results to rows that contain the word "bigquery" in the tags column.
Include rows where there is other text in addition to the word "bigquery" (e.g., if a row has a tag "bigquery-sql", your results should include that too).
"""

questions_query = """
                  SELECT id, title, owner_user_id
                  FROM `bigquery-public-data.stackoverflow.posts_questions`
                  WHERE tags LIKE '%bigquery%'
                  """

# Set up the query (cancel the query if it would use too much of
# your quota, with the limit set to 1 GB)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
questions_query_job = client.query(questions_query, job_config=safe_config)

# API request - run the query, and return a pandas DataFrame
questions_results = questions_query_job.to_dataframe()
# Preview results
print(questions_results.head())

"""
4) Your first join
Now that you have a query to select questions on any given topic (in this case, you chose "bigquery"), you can find the answers to those questions with a JOIN.

Write a query that returns the id, body and owner_user_id columns from the posts_answers table for answers to "bigquery"-related questions.

You should have one row in your results for each answer to a question that has "bigquery" in the tags.
Remember you can get the tags for a question from the tags column in the posts_questions table.
Here's a reminder of what a JOIN looked like in the tutorial:

query = \"""
        SELECT p.Name AS Pet_Name, o.Name AS Owner_Name
        FROM `bigquery-public-data.pet_records.pets` as p
        INNER JOIN `bigquery-public-data.pet_records.owners` as o 
            ON p.ID = o.Pet_ID
        \"""
It may be useful to scroll up and review the first several rows of the posts_answers and posts_questions tables.
"""

answers_query = """
                SELECT a.id, a.body, a.owner_user_id
                FROM `bigquery-public-data.stackoverflow.posts_questions` AS q 
                INNER JOIN `bigquery-public-data.stackoverflow.posts_answers` AS a
                    ON q.id = a.parent_id
                WHERE q.tags LIKE '%bigquery%'
                """

# Set up the query (cancel the query if it would use too much of
# your quota, with the limit set to 27 GB)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=27*10**10)
answers_query_job = client.query(answers_query, job_config=safe_config)

# API request - run the query, and return a pandas DataFrame
answers_results = answers_query_job.to_dataframe()

# Preview results
print(answers_results.head())

"""
5) Answer the question
You have the merge you need. But you want a list of users who have answered many questions... which requires more work beyond your previous result.

Write a new query that has a single row for each user who answered at least one question with a tag that includes the string "bigquery". Your results should have two columns:

user_id - contains the owner_user_id column from the posts_answers table
number_of_answers - contains the number of answers the user has written to "bigquery"-related questions
"""

# Your code here
bigquery_experts_query = """____"""
​
# Set up the query
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
bigquery_experts_query_job = ____  # Your code goes here
​
# API request - run the query, and return a pandas DataFrame
bigquery_experts_results = ____  # Your code goes here
​
# Preview results
print(bigquery_experts_results.head())
​
# Check your answer
q_5.check()
add Codeadd Markdown
For a hint or the solution, uncomment the appropriate line below.

add Codeadd Markdown
q_5.hint()
q_5.solution()
Hint: Start with SELECT a.owner_user_id AS user_id, COUNT(1) AS number_of_answers

Solution:


bigquery_experts_query = """
                         SELECT a.owner_user_id AS user_id, COUNT(1) AS number_of_answers
                         FROM `bigquery-public-data.stackoverflow.posts_questions` AS q
                         INNER JOIN `bigquery-public-data.stackoverflow.posts_answers` AS a
                             ON q.id = a.parent_Id
                         WHERE q.tags LIKE '%bigquery%'
                         GROUP BY a.owner_user_id
                         """

# Set up the query (cancel the query if it would use too much of
# your quota, with the limit set to 1 GB)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
bigquery_experts_query_job = client.query(
    bigquery_experts_query, job_config=safe_config)

# API request - run the query, and return a pandas DataFrame
bigquery_experts_results = bigquery_experts_query_job.to_dataframe()
