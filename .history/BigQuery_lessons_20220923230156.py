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
"""
With all that you've learned, your SQL queries are getting pretty long, which can make them hard understand ( and debug).

You are about to learn how to use AS and WITH to tidy up your queries and make them easier to read.

Along the way, we'll use the familiar pets table, but now it includes the ages of the animals.
"""
####  AS  ####
"""
You learned in an earlier tutorial how to use AS to rename the columns generated by your queries, which is also known as aliasing. This is similar to how Python uses as for aliasing when doing imports like import pandas as pd or import seaborn as sns.

To use AS in SQL, insert it right after the column you select. Here's an example of a query without an AS clause:

And here's an example of the same query, but with AS.

These queries return the same information, but in the second query the column returned by the COUNT() function will be called Number, rather than the default name of f0__.
"""
####  WITH ... AS  ####
"""
On its own, AS is a convenient way to clean up the data returned by your query. It's even more powerful when combined with WITH in what's called a "common table expression".

A common table expression ( or CTE) is a temporary table that you return within your query. CTEs are helpful for splitting your queries into readable chunks, and you can write queries against them.

For instance, you might want to use the pets table to ask questions about older animals in particular. So you can start by creating a CTE which only contains information about animals more than five years old like this:

While this incomplete query above won't return anything, it creates a CTE that we can then refer to(as Seniors) while writing the rest of the query.

We can finish the query by pulling the information that we want from the CTE. The complete query below first creates the CTE, and then returns all of the IDs from it.

You could do this without a CTE, but if this were the first part of a very long query, removing the CTE would make it much harder to follow.

Also, it's important to note that CTEs only exist inside the query where you create them, and you can't reference them in later queries. So, any query that uses a CTE is always broken into two parts: (1) first, we create the CTE, and then(2) we write a query that uses the CTE.
"""

# Example: How many Bitcoin transactions are made per month?
"""
We're going to use a CTE to find out how many Bitcoin transactions were made each day for the entire timespan of a bitcoin transaction dataset.

We'll investigate the transactions table. Here is a view of the first few rows. (The corresponding code is hidden, but you can un-hide it by clicking on the "Code" button below.)
"""

# Create a "Client" object
client = bigquery.Client()

# Construct a reference to the "crypto_bitcoin" dataset
dataset_ref = client.dataset("crypto_bitcoin", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

# Construct a reference to the "transactions" table
table_ref = dataset_ref.table("transactions")

# API request - fetch the table
table = client.get_table(table_ref)

# Preview the first five lines of the "transactions" table
client.list_rows(table, max_results=5).to_dataframe()

# Since the block_timestamp column contains the date of each transaction in DATETIME format, we'll convert these into DATE format using the DATE() command.

# We do that using a CTE, and then the next part of the query counts the number of transactions for each date and sorts the table so that earlier dates appear first.

# Query to select the number of transactions per date, sorted by date
query_with_CTE = """
                 WITH time AS
                 (
                     SELECT DATE(block_timestamp) AS trans_date
                     FROM `bigquery-public-data.crypto_bitcoin.transactions`
                 )
                 SELECT COUNT(1) AS transactions,
                        trans_date
                 FROM time
                 GROUP BY trans_date
                 ORDER BY trans_date
                 """

# Set up the query (cancel the query if it would use too much of
# your quota, with the limit set to 10 GB)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
query_job = client.query(query_with_CTE, job_config=safe_config)

# API request - run the query, and convert the results to a pandas DataFrame
transactions_by_date = query_job.to_dataframe()

# Print the first five rows
transactions_by_date.head()

# Since they're returned sorted, we can easily plot the raw results to show us the number of Bitcoin transactions per day over the whole timespan of this dataset.

transactions_by_date.set_index('trans_date').plot()

# As you can see, common table expressions(CTEs) let you shift a lot of your data cleaning into SQL. That's an especially good thing in the case of BigQuery, because it is vastly faster than doing the work in Pandas.

####################
##  Joining Data  ##
####################
"""
You have the tools to obtain data from a single table in whatever format you want it. But what if the data you want is spread across multiple tables?

That's where JOIN comes in! JOIN is incredibly important in practical SQL workflows. So let's get started.
"""

####  Example  ####
"""
We'll use our imaginary pets table, which has three columns:

    * ID - ID number for the pet
    * Name - name of the pet
    * Animal - type of animal
We'll also add another table, called owners. This table also has three columns:

    * ID - ID number for the owner(different from the ID number for the pet)
    * Name - name of the owner
    * Pet_ID - ID number for the pet that belongs to the owner(which matches the ID number for the pet in the pets table)

To get information that applies to a certain pet, we match the ID column in the pets table to the Pet_ID column in the owners table.

For example,

    * the pets table shows that Dr. Harris Bonkers is the pet with ID 1.
    * The owners table shows that Aubrey Little is the owner of the pet with ID 1.

Putting these two facts together, Dr. Harris Bonkers is owned by Aubrey Little.

Fortunately, we don't have to do this by hand to figure out which owner goes with which pet. In the next section, you'll learn how to use JOIN to create a new table combining information from the pets and owners tables.
"""
####  JOIN  ####
"""
Using JOIN, we can write a query to create a table with just two columns: the name of the pet and the name of the owner.


We combine information from both tables by matching rows where the ID column in the pets table matches the Pet_ID column in the owners table.

In the query, ON determines which column in each table to use to combine the tables. Notice that since the ID column exists in both tables, we have to clarify which one to use. We use p.ID to refer to the ID column from the pets table, and o.Pet_ID refers to the Pet_ID column from the owners table.

In general, when you're joining tables, it's a good habit to specify which table each of your columns comes from . That way, you don't have to pull up the schema every time you go back to read the query.

The type of JOIN we're using today is called an INNER JOIN. That means that a row will only be put in the final output table if the value in the columns you're using to combine them shows up in both the tables you're joining. For example, if Tom's ID number of 4 didn't exist in the pets table, we would only get 3 rows back from this query. There are other types of JOIN, but an INNER JOIN is very widely used, so it's a good one to start with .

Example: How many files are covered by each type of software license?
GitHub is the most popular place to collaborate on software projects. A GitHub repository (or repo) is a collection of files associated with a specific project.

Most repos on GitHub are shared under a specific legal license, which determines the legal restrictions on how they are used. For our example, we're going to look at how many different files have been released under each license.

We'll work with two tables in the database. The first table is the licenses table, which provides the name of each GitHub repo (in the repo_name column) and its corresponding license. Here's a view of the first five rows.


# Create a "Client" object
client = bigquery.Client()

# Construct a reference to the "github_repos" dataset
dataset_ref = client.dataset("github_repos", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

# Construct a reference to the "licenses" table
licenses_ref = dataset_ref.table("licenses")

# API request - fetch the table
licenses_table = client.get_table(licenses_ref)

# Preview the first five lines of the "licenses" table
client.list_rows(licenses_table, max_results=5).to_dataframe()
Using Kaggle's public dataset BigQuery integration.
/opt/conda/lib/python3.7/site-packages/ipykernel_launcher.py: 19: UserWarning: Cannot use bqstorage_client if max_results is set, reverting to fetching data with the tabledata.list endpoint.
repo_name	license
0	zoffixznet/perl6-App-Nopaste	artistic-2.0
1	vikt0rs/devstack-plugin-zmq	artistic-2.0
2	LegitTalon/nicolas	artistic-2.0
3	Arabidopsis-Information-Portal/Intern-Hello-World	artistic-2.0
4	andrewdonkin/falcon	artistic-2.0
The second table is the sample_files table, which provides, among other information, the GitHub repo that each file belongs to (in the repo_name column). The first several rows of this table are printed below.

# Construct a reference to the "sample_files" table
files_ref = dataset_ref.table("sample_files")

# API request - fetch the table
files_table = client.get_table(files_ref)

# Preview the first five lines of the "sample_files" table
client.list_rows(files_table, max_results=5).to_dataframe()
/opt/conda/lib/python3.7/site-packages/ipykernel_launcher.py: 8: UserWarning: Cannot use bqstorage_client if max_results is set, reverting to fetching data with the tabledata.list endpoint.

repo_name	ref	path	mode	id	symlink_target
0	git/git	refs/heads/master	RelNotes	40960	62615ffa4e97803da96aefbc798ab50f949a8db7	Documentation/RelNotes/2.10.0.txt
1	np/ling	refs/heads/master	tests/success/plug_compose.t/plug_compose.ll	40960	0c1605e4b447158085656487dc477f7670c4bac1	../../../fixtures/all/plug_compose.ll
2	np/ling	refs/heads/master	fixtures/strict-par-success/parallel_assoc_lef...	40960	b59bff84ec03d12fabd3b51a27ed7e39a180097e	../all/parallel_assoc_left.ll
3	np/ling	refs/heads/master	fixtures/sequence/parallel_assoc_2tensor2_left.ll	40960	f29523e3fb65702d99478e429eac6f801f32152b	../all/parallel_assoc_2tensor2_left.ll
4	np/ling	refs/heads/master	fixtures/success/my_dual.ll	40960	38a3af095088f90dfc956cb990e893909c3ab286	../all/my_dual.ll
Next, we write a query that uses information in both tables to determine how many files are released in each license.

# Query to determine the number of files per license, sorted by number of files
query = """
        SELECT L.license, COUNT(1) AS number_of_files
        FROM `bigquery-public-data.github_repos.sample_files` AS sf
        INNER JOIN `bigquery-public-data.github_repos.licenses` AS L
            ON sf.repo_name = L.repo_name
        GROUP BY L.license
        ORDER BY number_of_files DESC
        """

# Set up the query (cancel the query if it would use too much of
# your quota, with the limit set to 10 GB)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
query_job = client.query(query, job_config=safe_config)

# API request - run the query, and convert the results to a pandas DataFrame
file_count_by_license = query_job.to_dataframe()
/opt/conda/lib/python3.7/site-packages/google/cloud/bigquery/client.py: 440: UserWarning: Cannot create BigQuery Storage client, the dependency google-cloud-bigquery-storage is not installed.
"Cannot create BigQuery Storage client, the dependency "
It's a big query, and so we'll investigate each piece separately.


We'll begin with the JOIN(highlighted in blue above). This specifies the sources of data and how to join them. We use ON to specify that we combine the tables by matching the values in the repo_name columns in the tables.

Next, we'll talk about SELECT and GROUP BY(highlighted in yellow). The GROUP BY breaks the data into a different group for each license, before we COUNT the number of rows in the sample_files table that corresponds to each license. (Remember that you can count the number of rows with COUNT(1).)

Finally, the ORDER BY(highlighted in purple) sorts the results so that licenses with more files appear first.

It was a big query, but it gave us a nice table summarizing how many files have been committed under each license:

    # Print the DataFrame
file_count_by_license
