#############################################
##  Getting Started With SQL and BigQuery  ##
#############################################

from google.cloud import bigquery

####  Your first BigQuery commands  ####
# To use BigQuery, we'll import the Python package below:
from google.cloud import bigquery
# The first step in the workflow is to create a Client object. As you'll soon see, this Client object will play a central role in retrieving information from BigQuery datasets.

# Create a "Client" object
client = bigquery.Client()

"""
We'll work with a dataset of posts on Hacker News, a website focusing on computer science and cybersecurity news.

In BigQuery, each dataset is contained in a corresponding project. In this case, our hacker_news dataset is contained in the bigquery-public-data project. To access the dataset,

We begin by constructing a reference to the dataset with the dataset() method.
Next, we use the get_dataset() method, along with the reference we just constructed, to fetch the dataset.
"""

# Construct a reference to the "hacker_news" dataset
dataset_ref = client.dataset("hacker_news", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

"""
Every dataset is just a collection of tables. You can think of a dataset as a spreadsheet file containing multiple tables, all composed of rows and columns.

We use the list_tables() method to list the tables in the dataset.
"""

# List all the tables in the "hacker_news" dataset
tables = list(client.list_tables(dataset))

# Print names of all tables in the dataset (there are four!)
for table in tables:
    print(table.table_id)

# Similar to how we fetched a dataset, we can fetch a table. In the code cell below, we fetch the full table in the hacker_news dataset.

# Construct a reference to the "full" table
table_ref = dataset_ref.table("full")

# API request - fetch the table
table = client.get_table(table_ref)

####################
##  Table schema  ##
####################

# The structure of a table is called its schema. We need to understand a table's schema to effectively pull out the data we want.

# In this example, we'll investigate the full table that we fetched above.

# Print information on all the columns in the "full" table in the "hacker_news" dataset
table.schema

"""
Each SchemaField tells us about a specific column (which we also refer to as a field). In order, the information is :

* The name of the column
* The field type ( or datatype) in the column
* The mode of the column('NULLABLE' means that a column allows NULL values, and is the default)
* A description of the data in that column
"""

# The first field has the SchemaField:

SchemaField('by', 'string', 'NULLABLE',
            "The username of the item's author.", ())

"""
This tells us:

* the field ( or column) is called by,
* the data in this field is strings,
* NULL values are allowed, and
* it contains the usernames corresponding to each item's author.

We can use the list_rows() method to check just the first five lines of of the full table to make sure this is right. (Sometimes databases have outdated descriptions, so it's good to check.) This returns a BigQuery RowIterator object that can quickly be converted to a pandas DataFrame with the to_dataframe() method.
"""

# Preview the first five lines of the "full" table
client.list_rows(table, max_results=5).to_dataframe()

# The list_rows() method will also let us look at just the information in a specific column. If we want to see the first five entries in the by column, for example, we can do that!

# Preview the first five entries in the "by" column of the "full" table
client.list_rows(
    table, selected_fields=table.schema[:1], max_results=5).to_dataframe()


############################
##  Select, From & Where  ##
############################
"""
Now that you know how to access and examine a dataset, you're ready to write your first SQL query! As you'll soon see, SQL queries will help you sort through a massive dataset, to retrieve only the information that you need.

We'll begin by using the keywords SELECT, FROM, and WHERE to get data from specific columns based on conditions you specify.

For clarity, we'll work with a small imaginary dataset pet_records which contains just one table, called pets.
"""

####  SELECT ... FROM  ####
"""
The most basic SQL query selects a single column from a single table. To do this,

* specify the column you want after the word SELECT, and then
* specify the table after the word FROM.

For instance, to select the Name column(from the pets table in the pet_records database in the bigquery-public-data project), our query would appear as follows:

Note that when writing an SQL query, the argument we pass to FROM is not in single or double quotation marks(' or "). It is in backticks(`).
"""
####  WHERE ...  ####
"""
BigQuery datasets are large, so you'll usually want to return only the rows meeting specific conditions. You can do this using the WHERE clause.

The query below returns the entries from the Name column that are in rows where the Animal column has the text 'Cat'.

Example: What are all the U.S. cities in the OpenAQ dataset?
Now that you've got the basics down, let's work through an example with a real dataset. We'll use an OpenAQ dataset about air quality.

First, we'll set up everything we need to run queries and take a quick peek at what tables are in our database. (Since you learned how to do this in the previous tutorial, we have hidden the code. But if you'd like to take a peek, you need only click on the "Code" button below.)
"""

# Create a "Client" object
client = bigquery.Client()

# Construct a reference to the "openaq" dataset
dataset_ref = client.dataset("openaq", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

# List all the tables in the "openaq" dataset
tables = list(client.list_tables(dataset))

# Print names of all tables in the dataset (there's only one!)
for table in tables:
    print(table.table_id)

# Using Kaggle's public dataset BigQuery integration.
global_air_quality

# The dataset contains only one table, called global_air_quality. We'll fetch the table and take a peek at the first few rows to see what sort of data it contains. (Again, we have hidden the code. To take a peek, click on the "Code" button below.)

# Construct a reference to the "global_air_quality" table
table_ref = dataset_ref.table("global_air_quality")

# API request - fetch the table
table = client.get_table(table_ref)

# Preview the first five lines of the "global_air_quality" table
client.list_rows(table, max_results=5).to_dataframe()

# Everything looks good! So, let's put together a query. Say we want to select all the values from the city column that are in rows where the country column is 'US' (for "United States").

# Query to select all the items from the "city" column where the "country" column is 'US'
query = """
        SELECT city
        FROM `bigquery-public-data.openaq.global_air_quality`
        WHERE country = 'US'
        """

####  Submitting the query to the dataset  ####
# We're ready to use this query to get information from the OpenAQ dataset. As in the previous tutorial, the first step is to create a Client object.

# Create a "Client" object
client = bigquery.Client()
# We begin by setting up the query with the query() method. We run the method with the default parameters, but this method also allows us to specify more complicated settings that you can read about in the documentation. We'll revisit this later.

# Set up the query
query_job = client.query(query)
# Next, we run the query and convert the results to a pandas DataFrame.

# API request - run the query, and return a pandas DataFrame
us_cities = query_job.to_dataframe()

# Now we've got a pandas DataFrame called us_cities, which we can use like any other DataFrame.

# What five cities have the most measurements?

####  More queries  ####
# If you want multiple columns, you can select them with a comma between the names:

query = """
        SELECT city, country
        FROM `bigquery-public-data.openaq.global_air_quality`
        WHERE country = 'US'
        """
# You can select all columns with a * like this:

query = """
        SELECT *
        FROM `bigquery-public-data.openaq.global_air_quality`
        WHERE country = 'US'
        """

"""
Q & A: Notes on formatting
The formatting of the SQL query might feel unfamiliar. If you have any questions, you can ask in the comments section at the bottom of this page. Here are answers to two common questions:

Question: What's up with the triple quotation marks(\"""\)?
Answer: These tell Python that everything inside them is a single string, even though we have line breaks in it. The line breaks aren't necessary, but they make it easier to read your query.

Question: Do you need to capitalize SELECT and FROM?
Answer: No, SQL doesn't care about capitalization. However, it's customary to capitalize your SQL commands, and it makes your queries a bit easier to read.

Working with big datasets
BigQuery datasets can be huge. We allow you to do a lot of computation for free, but everyone has some limit.

Each Kaggle user can scan 5TB every 30 days for free. Once you hit that limit, you'll have to wait for it to reset.

The biggest dataset currently on Kaggle is 3TB, so you can go through your 30-day limit in a couple queries if you aren't careful.

Don't worry though: we'll teach you how to avoid scanning too much data at once, so that you don't run over your limit.

To begin,you can estimate the size of any query before running it. Here is an example using the (very large!) Hacker News dataset. To see how much data a query will scan, we create a QueryJobConfig object and set the dry_run parameter to True.
"""

# Query to get the score column from every row where the type column has value "job"
query = """
        SELECT score, title
        FROM `bigquery-public-data.hacker_news.full`
        WHERE type = "job"
        """

# Create a QueryJobConfig object to estimate size of query without running it
dry_run_config = bigquery.QueryJobConfig(dry_run=True)

# API request - dry run query to estimate costs
dry_run_query_job = client.query(query, job_config=dry_run_config)

print("This query will process {} bytes.".format(
    dry_run_query_job.total_bytes_processed))
# This query will process 494391991 bytes.
# You can also specify a parameter when running the query to limit how much data you are willing to scan. Here's an example with a low limit.

# Only run the query if it's less than 1 MB
ONE_MB = 1000*1000
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=ONE_MB)

# Set up the query (will only run if it's less than 1 MB)
safe_query_job = client.query(query, job_config=safe_config)

# API request - try to run the query, and return a pandas DataFrame
safe_query_job.to_dataframe()

# In this case, the query was cancelled, because the limit of 1 MB was exceeded. However, we can increase the limit to run the query successfully!

# Only run the query if it's less than 1 GB
ONE_GB = 1000*1000*1000
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=ONE_GB)

# Set up the query (will only run if it's less than 1 GB)
safe_query_job = client.query(query, job_config=safe_config)

# API request - try to run the query, and return a pandas DataFrame
job_post_scores = safe_query_job.to_dataframe()

# Print average score for job posts
job_post_scores.score.mean()

################################
##  Group By, Having & Count  ##
################################
"""
Now that you can select raw data, you're ready to learn how to group your data and count things within those groups. This can help you answer questions like:

How many of each kind of fruit has our store sold?
How many species of animal has the vet office treated?

To do this, you'll learn about three new techniques: GROUP BY, HAVING and COUNT(). Once again, we'll use this made-up table of information on pets.
"""

####  COUNT()  ####
"""
COUNT(), as you may have guessed from the name, returns a count of things. If you pass it the name of a column, it will return the number of entries in that column.

For instance, if we SELECT the COUNT() of the ID column in the pets table, it will return 4, because there are 4 ID's in the table.


COUNT() is an example of an aggregate function, which takes many values and returns one. (Other examples of aggregate functions include SUM(), AVG(), MIN(), and MAX().) As you'll notice in the picture above, aggregate functions introduce strange column names (like f0__). Later in this tutorial, you'll learn how to change the name to something more descriptive.
"""

####  GROUP BY  ####
"""
GROUP BY takes the name of one or more columns, and treats all rows with the same value in that column as a single group when you apply aggregate functions like COUNT().

For example, say we want to know how many of each type of animal we have in the pets table. We can use GROUP BY to group together rows that have the same value in the Animal column, while using COUNT() to find out how many ID's we have in each group.


It returns a table with three rows(one for each distinct animal). We can see that the pets table contains 1 rabbit, 1 dog, and 2 cats.
"""

####  GROUP BY ... HAVING  ####
"""
HAVING is used in combination with GROUP BY to ignore groups that don't meet certain criteria.

So this query, for example, will only include groups that have more than one ID in them.

Since only one group meets the specified criterion, the query will return a table with only one row.

Example: Which Hacker News comments generated the most discussion?

Ready to see an example on a real dataset? The Hacker News dataset contains information on stories and comments from the Hacker News social networking site.

We'll work with the comments table and begin by printing the first few rows. (We have hidden the corresponding code. To take a peek, click on the "Code" button below.)
"""


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
Let's use the table to see which comments generated the most replies. Since:

the parent column indicates the comment that was replied to, and
the id column has the unique ID used to identify each comment,
we can GROUP BY the parent column and COUNT() the id column in order to figure out the number of comments that were made as responses to a specific comment. (This might not make sense immediately - - take your time here to ensure that everything is clear!)

Furthermore, since we're only interested in popular comments, we'll look at comments with more than ten replies. So, we'll only return groups HAVING more than ten ID's.
"""

# Query to select comments that received more than 10 replies
query_popular = """
                SELECT parent, COUNT(id)
                FROM `bigquery-public-data.hacker_news.comments`
                GROUP BY parent
                HAVING COUNT(id) > 10
                """

# Now that our query is ready, let's run it and store the results in a pandas DataFrame:

# Set up the query (cancel the query if it would use too much of
# your quota, with the limit set to 10 GB)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
query_job = client.query(query_popular, job_config=safe_config)

# API request - run the query, and convert the results to a pandas DataFrame
popular_comments = query_job.to_dataframe()

# Print the first five rows of the DataFrame
popular_comments.head()

# Each row in the popular_comments DataFrame corresponds to a comment that received more than ten replies. For instance, the comment with ID 801208 received 56 replies.

####  Aliasing and other improvements  ####
"""
A couple hints to make your queries even better:

The column resulting from COUNT(id) was called f0__. That's not a very descriptive name. You can change the name by adding AS NumPosts after you specify the aggregation. This is called aliasing, and it will be covered in more detail in an upcoming lesson.
If you are ever unsure what to put inside the COUNT() function, you can do COUNT(1) to count the rows in each group. Most people find it especially readable, because we know it's not focusing on other columns. It also scans less data than if supplied column names(making it faster and using less of your data access quota).
"""
# Using these tricks, we can rewrite our query:

# Improved version of earlier query, now with aliasing & improved readability
query_improved = """
                 SELECT parent, COUNT(1) AS NumPosts
                 FROM `bigquery-public-data.hacker_news.comments`
                 GROUP BY parent
                 HAVING COUNT(1) > 10
                 """

safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
query_job = client.query(query_improved, job_config=safe_config)

# API request - run the query, and convert the results to a pandas DataFrame
improved_df = query_job.to_dataframe()

# Print the first five rows of the DataFrame
improved_df.head()

# Now you have the data you want, and it has descriptive names. That's good style.

####  Note on using GROUP BY  ####
"""
Note that because it tells SQL how to apply aggregate functions(like COUNT()), it doesn't make sense to use GROUP BY without an aggregate function. Similarly, if you have any GROUP BY clause, then all variables must be passed to either a

1. GROUP BY command, or
2. an aggregation function.
"""
# Consider the query below:

query_good = """
             SELECT parent, COUNT(id)
             FROM `bigquery-public-data.hacker_news.comments`
             GROUP BY parent
             """
"""
Note that there are two variables: parent and id.

    * parent was passed to a GROUP BY command ( in GROUP BY parent), and
    * id was passed to an aggregate function ( in COUNT(id)).
And this query won't work, because the author column isn't passed to an aggregate function or a GROUP BY clause:
"""
query_bad = """
            SELECT author, parent, COUNT(id)
            FROM `bigquery-public-data.hacker_news.comments`
            GROUP BY parent
            """
# If make this error, you'll get the error message SELECT list expression references column (column's name) which is neither grouped nor aggregated at.

##################
##  Ordered By  ##
##################
"""
So far, you've learned how to use several SQL clauses. For instance, you know how to use SELECT to pull specific columns from a table, along with WHERE to pull rows that meet specified criteria. You also know how to use aggregate functions like COUNT(), along with GROUP BY to treat multiple rows as a single group.

Now you'll learn how to change the order of your results using the ORDER BY clause, and you'll explore a popular use case by applying ordering to dates. To illustrate what you'll learn in this tutorial, we'll work with a slightly modified version of our familiar pets table.
"""

####  ORDER BY  ####
"""
ORDER BY is usually the last clause in your query, and it sorts the results returned by the rest of your query.

Notice that the rows are not ordered by the ID column. We can quickly remedy this with the query below.

The ORDER BY clause also works for columns containing text, where the results show up in alphabetical order.

You can reverse the order using the DESC argument(short for 'descending'). The next query sorts the table by the Animal column, where the values that are last in alphabetic order are returned first.
"""

####  Dates  ####
"""
Next, we'll talk about dates, because they come up very frequently in real-world databases. There are two ways that dates can be stored in BigQuery: as a DATE or as a DATETIME.

The DATE format has the year first, then the month, and then the day. It looks like this:

    YYYY-[M]M-[D]D
        * YYYY: Four-digit year
        * [M]M: One or two digit month
        * [D]D: One or two digit day

So 2019-01-10 is interpreted as January 10, 2019.

The DATETIME format is like the date format ... but with time added at the end.
"""
####  EXTRACT  ####
"""
Often you'll want to look at part of a date, like the year or the day. You can do this with EXTRACT. We'll illustrate this with a slightly different table, called pets_with_date.

The query below returns two columns, where column Day contains the day corresponding to each entry the Date column from the pets_with_date table:

SQL is very smart about dates, and we can ask for information beyond just extracting part of the cell. For example, this query returns one column with just the week in the year(between 1 and 53) for each date in the Date column:

You can find all the functions you can use with dates in BigQuery in this documentation under "Date and time functions".
"""
# Example: Which day of the week has the most fatal motor accidents?
"""
Let's use the US Traffic Fatality Records database, which contains information on traffic accidents in the US where at least one person died.

We'll investigate the accident_2015 table. Here is a view of the first few rows. (We have hidden the corresponding code. To take a peek, click on the "Code" button below.)
"""

# Create a "Client" object
client = bigquery.Client()

# Construct a reference to the "nhtsa_traffic_fatalities" dataset
dataset_ref = client.dataset(
    "nhtsa_traffic_fatalities", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

# Construct a reference to the "accident_2015" table
table_ref = dataset_ref.table("accident_2015")

# API request - fetch the table
table = client.get_table(table_ref)

# Preview the first five lines of the "accident_2015" table
client.list_rows(table, max_results=5).to_dataframe()

"""
Let's use the table to determine how the number of accidents varies with the day of the week. Since:

* the consecutive_number column contains a unique ID for each accident, and
* the timestamp_of_crash column contains the date of the accident in DATETIME format,
we can:

* EXTRACT the day of the week(as day_of_week in the query below) from the timestamp_of_crash column, and
* GROUP BY the day of the week, before we COUNT the consecutive_number column to determine the number of accidents for each day of the week.

Then we sort the table with an ORDER BY clause, so the days with the most accidents are returned first.
"""

# Query to find out the number of accidents for each day of the week
query = """
        SELECT COUNT(consecutive_number) AS num_accidents, 
               EXTRACT(DAYOFWEEK FROM timestamp_of_crash) AS day_of_week
        FROM `bigquery-public-data.nhtsa_traffic_fatalities.accident_2015`
        GROUP BY day_of_week
        ORDER BY num_accidents DESC
        """
# As usual, we run it as follows:

# Set up the query (cancel the query if it would use too much of
# your quota, with the limit set to 1 GB)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**9)
query_job = client.query(query, job_config=safe_config)

# API request - run the query, and convert the results to a pandas DataFrame
accidents_by_day = query_job.to_dataframe()

# Print the DataFrame
accidents_by_day

"""
Notice that the data is sorted by the num_accidents column, where the days with more traffic accidents appear first.

To map the numbers returned for the day_of_week column to the actual day, you might consult the BigQuery documentation on the DAYOFWEEK function. It says that it returns "an integer between 1 (Sunday) and 7 (Saturday), inclusively". So, in 2015, most fatal motor accidents in the US occured on Sunday and Saturday, while the fewest happened on Tuesday.
"""

#################
##  As & With  ##
#################