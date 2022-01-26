#!/usr/bin/env python
# coding: utf-8

# ## Assignment 1

# ## Case Study
# Twitter is a massive platform.  There are 300+ million users on Twitter, and it is a source of information for current events, social movements and, financial information.  It has been shown in a number of cases that information from Twitter can mobilize a large number of individuals.  From #blacklivesmatter to other forms of *hashtag* activism, social media can play an important role in informing and mobilizing individuals.
# 
# This same activity can be extended to financial information.  The introduction of "cashtags" to twitter has allowed individuals to connect and discuss stocks, but it has also given stock promoters a method for promoting low value stocks, to "pump and dump".  Some researchers have analyzed the use of cashtags on Twitter.  We will use a similar method to look at the data, but we will ask a slightly different question.
# 
# ### Reading
# Hentschel M, Alonso O. 2014. Follow the money: A study of cashtags on Twitter. *First Monday*. URL: https://firstmonday.org/ojs/index.php/fm/article/view/5385/4109
# 
# #### Supplementary Information
# 
# * Evans, L., Owda, M., Crockett, K., & Vilas, A. F. (2019). A methodology for the resolution of cashtag collisions on Twitter–A natural language processing & data fusion approach. *Expert Systems with Applications*, **127**, 353-369.
# * Evans, L., Owda, M., Crockett, K., & Vilas, A. F. (2021). [Credibility assessment of financial stock tweets](https://www.sciencedirect.com/science/article/pii/S0957417420310356). *Expert Systems with Applications*, **168**, 114351.
# * Cresci, S., Lillo, F., Regoli, D., Tardelli, S., & Tesconi, M. (2019). Cashtag Piggybacking: Uncovering Spam and Bot Activity in Stock Microblogs on Twitter. *ACM Transactions on the Web (TWEB)*, **13(2)**, 11.
# 
# #### Raw Data source
# I document the source of ticker data below.  The tweet data we use here comes from a dataset used in Cresci *et al* (2019) referenced above.  The data is available through Zenodo using the dataset's DOI: [10.5281/zenodo.2686861](https://doi.org/10.5281/zenodo.2686861). 
# 
# This is for your reference. I have already created the schema, tables, and loaded data to the database (please check the `loadnyse.py` file in [canvas page](https://canvas.ubc.ca/files/18519941/download?download_frd=1) of this assignment  to learn about the script used to do so).
# 
# ### Formulating the question
# 
# The question we want to ask specifically is whether *cashtag frequency is tied to increases in stock price*.
# 
# To do this we need to know a few things.  First, we need to understand the frequency of cashtags, and classify them in some way.  What aspects of a cashtag are important?  What elements of a tweet containing a cashtag are important?  How do we go from raw cashtag to something we can analyze?
# 
# In addition, what other information do we need to help us understand our data?  How do we know stock prices?

# ## Questions
# 
# ***1.  Identify elements of the potential dataset(s) that match each of the four Vs of Big Data: (Please edit here with your answer)***
# 
# rubric={reasoning:10}
# 
#   a.  Velocity: The frequency of recieving tweets is very high; however, we are going to use an available ststic dataset of tweets. If we used twitter API with high throughput, it would have been considred high velocity.
# 
#   b.  Veracity: As it is twitter data, the fields are generated via a code (no manual entry of the data into each field, it is supposed to be reliable. However, the content of the tweets might not be so reliable as there is all kinds of comments and views on twitter (This doesn't affect the reliability of tables though). Twitter data regarding financial markets is changing very fast and all the assumptions valid at this moment might change in matter of minutes. The information extracted from tweets is not very reliable and needs to be cross checked with other sources, i.e. stock price fluctuations' record from SEC.
# 
#   c.  Volume: The voulme of data is quite large. Of course one would be able to run it on a powerful machine but it can be marginally classified as big data by some measures. 
# 
#   d.  Variety: Most columns of the dataset are numbers or predefined values (categorical, date, html code links). The tweets' text however, is a text field with diverse elements. Neverthelesss, this column will be treated as text although very rich in information.

# 2. To find the stock price at a point in time for a cashtag (e.g., $A), we need to know which company uses that NYSE listing, and then find the listing at that time period.  A public dataset of stock listings is available [here](http://www.nasdaqtrader.com/trader.aspx?id=symboldirdefs).This [link](ftp://ftp.nasdaqtrader.com/symboldirectory/) opens it up in your file explorer. 
# 
# ***2a. You can download files directly into Python from an FTP service using the command FTP.  Read the file into Python and return the number of rows, and return the symbol of the stock ticker associated with the longest named entity.***
# 
# rubric={correctness:10}

# In[3]:


from ftplib import FTP
from io import StringIO
import csv

# The following lines are for your information.  they represent a recipe for downloading data directly from an FTP server:
session = FTP('ftp.nasdaqtrader.com')
session.login()
r = StringIO()
session.retrlines('RETR /SymbolDirectory/otherlisted.txt', lambda x: r.write(x+'\n'))

r.seek(0)
# reading file to python
csvfile = list(csv.DictReader(r, delimiter='|'))
# From here apply your solution.


# In[4]:


# Number of Rows
num_rows = len(csvfile)
print("Number of rows: " + str(num_rows))


# In[6]:


# Finding the symbol for the longest name

# Longest
lng = ''
num_lng = 0

for i in range(num_rows):
    if len(csvfile[i]['Security Name']) > len(lng):
        lng = csvfile[i]['Security Name']
        num_lng = i

print("Symbol of the longest named entity: " + csvfile[num_lng]['NASDAQ Symbol'])


# _Following 2a, you know how to pull data using FTP and python. Check out the python file `loadnyse.py` in [canvas page](https://canvas.ubc.ca/files/18519941/download?download_frd=1) of this assignment to see how I have loaded otherlisted.txt file to the database with the schema name `import` and table name `tickers`._
# 
# _In this file `loadnyse.py` you can also see how we loaded the twitter data that we pulled using [twitter API](https://zenodo.org/record/2686862#.YZ6Smy3r1QI) into the database (schema name: `import`, table name: `tweets`)_
# 
# _After the data got loaded to the database, I have taken a dump of `import` schema, which you can find in canvas page of this homework._
# 
# Checkout lecture2 to know how to take/load dumps from/to database. 

# ***2b. Load the dumps([import.sql](https://canvas.ubc.ca/files/18518713/download?download_frd=1)). You can paste the commands what you used in your terminal here. You should mask/remove your hostname for security reasons.***
# 
# rubric={correctness:5}

# In[7]:


import psycopg2

# Create a connection
# I provided the connection to the server through both methods (direct, and through .env file) to practice both

conString = {'host':'database-1.#####.amazonaws.com',
             'dbname':'#####',
             'user':'#####',
             'password':'#####',
             'port':'#####'}
conn = psycopg2.connect(**conString)
# Create a cursor
cur = conn.cursor()
# - Formulate your query
query = """CREATE SCHEMA IF NOT EXISTS classwork"""
# - Execute
cur.execute(query)
# - commiting.
conn.commit()


# In[8]:


## Here we create the table tickers
cur.execute("""CREATE TABLE IF NOT EXISTS classwork.tickers(
               actsymbol text PRIMARY KEY,
               securityname text,
               exchange text,
               cqssymbol text,
               etf text,
               roundlotsize text,
               testissue text,
               nasdaqsymbol text)""")
conn.commit()


# In[9]:


# Loading data into table tickers

from ftplib import FTP
from io import StringIO
import csv

session = FTP('ftp.nasdaqtrader.com')
session.login()
r = StringIO()
session.retrlines('RETR /SymbolDirectory/otherlisted.txt', lambda x: r.write(x+'\n'))
r.seek(0)
csvfile = list(csv.DictReader(r, delimiter='|'))

## Here we are reading each row and then inserting it to the table one at a time 
for row in csvfile:
    cur.execute("INSERT INTO classwork.tickers VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", list(row.values()))

conn.commit()


# In[10]:


# Create a connection (Schema for tweets)
conInfo = {'host':'database-1.#####.amazonaws.com',
             'dbname':'#####',
             'user':'#####',
             'password':'#####',
             'port':'#####'}
conn_tw = psycopg2.connect(**conInfo)
# Create a cursor
cur_tw = conn_tw.cursor()
# - Formulate your query
query_tw = """CREATE SCHEMA IF NOT EXISTS import"""
# - Execute
cur_tw.execute(query_tw)
# - commiting.
conn_tw.commit()


# In[11]:


## Here we create the table tweets
cur_tw.execute("""CREATE TABLE IF NOT EXISTS import.tweets(
               id text NOT NULL PRIMARY KEY,
               text text,
               userid text,
               inreplytostatusid text,
               inreplytouserid text,
               retweetedstatusid text,
               retweeteduserid text,
               lang text,
               source text,
               createdat text)""")
conn_tw.commit()


# _Following 2b now you have your database schema import with 2 tables `tickers` and `tweets`._

# ***2c.  In the database there is a table called `tickers`.  Connect to the database using Python.  Using a SQL query, return the number of rows in this table.***
# 
# rubric={correctness:15}

# In[12]:


# Ensure that you have a .env file in your folder.  This will include all of your connection string elements.
# DB_HOST=mbandtweet.xxxxxxx.us-east-1.rds.amazonaws.com
# DB_PORT=5432
# DB_USER=student
# DB_PASS=STUDENTPASSWORD
# From here apply your solution.

import os
import psycopg2

##Make sure you import and load your .env file
from dotenv import load_dotenv
load_dotenv()

conString = {'host':os.environ.get('DB_HOST'),
             'dbname':os.environ.get('DB_NAME'),
             'user':os.environ.get('DB_USER'),
             'password':os.environ.get('DB_PASS'),
             'port':os.environ.get('DB_PORT')}
print(conString["port"])


# In[13]:


# Rollback
conn.rollback()

query = """SELECT COUNT(*) FROM classwork.tickers"""
cur.execute(query)
row = cur.fetchall()
rows_n = int(row[0][0])
print("Number of rows: " + str(rows_n))


# ***2d. Use a SQL query to return the row with the longest company name.***
# 
# rubric={correctness:10}

# In[14]:


# Finding the symbol for the longest name
conn.rollback()

# Query
query = """SELECT securityname, nasdaqsymbol FROM classwork.tickers"""
cur.execute(query)
rows = cur.fetchall()


# In[15]:



# Longest
longest = ''
n = 0

for j in range(rows_n):
    if len(rows[j][0]) > len(longest):
        longest = rows[j][0]
        n = j

print("Symbol of the longest named entity: " + rows[n][1])


# 
# ***3. The output of an individual tweet may be a complex object, returned from the Twitter API.  This data is stored as a JSON object within a Postgres database in the cloud.  The table is in a schema called `import` in a table called `tweets`.  Connect to the database.  How many individual tweets are in our dataset?***
# 
# rubric={correctness:10}

# In[16]:


query_tw = """SELECT COUNT(*) FROM import.tweets"""
cur_tw.execute(query_tw)
row_tw = cur_tw.fetchall()
rows_n_tw = int(row_tw[0][0])
print("Number of rows: " + str(rows_n_tw))


# ***4. Currently all columns in the table `import.tweets` are coded as Postgres `text` columns.  How would you normalize these tables?  Use [`CREATE TABLE IF EXISTS`]() to generate the appropriate tables in the `import` schema.  What do the normalized tables look like?*** Type below all your create table SQL scripts.
# 
# rubric={correctness:10,reasoning:20}

# In[20]:


# This query creates a separate user table, as later we might want to categorize the users based on their activity

query_tb_1 = """CREATE TABLE IF NOT EXISTS import.users(
    userid INTEGER NOT NULL,
    PRIMARY KEY (userid)
);"""

cur.execute(query_tb_1)
conn.commit()

# This query builds the tweetz table which holds the main data regarding the tweets' texts, sources, dates, etc.
query_tb_2 = """CREATE TABLE IF NOT EXISTS import.tweetz(
    twid INTEGER NOT NULL,
    twtext TEXT, # We could have used VARCHAR(140) instead of TEXT for the "twtext" vairable considering the 140 character limit for tweets
    
    lang TEXT,
    twsource TEXT,
    createdat DATETIME,
    uid INTEGER,
    PRIMARY KEY (twid),
    CONSTRAINT fk_userid
        FOREIGN KEY (uid) REFERENCES users(userid) # linking the tweet back to the userid
);"""

cur.execute(query_tb_2)
conn.commit()


# This query builds the reply table which holds the information of replies to the tweets

query_tb_3 = """CREATE TABLE IF NOT EXISTS import.reply(
    id INTEGER NOT NULL IDENTITY(1,1),
    inreplytostatusid int, #reply info
    inreplytouserid int,
    tid int,
    PRIMARY KEY (id),
    CONSTRAINT fk_twid
        FOREIGN KEY (tid) REFERENCES tweetz(twid) # Linking it back to the relevant tweet
);"""

cur.execute(query_tb_3)
conn.commit()


# This query builds the reply table which holds the information of the retweets

query_tb_4 = """CREATE TABLE IF NOT EXISTS import.retweet(
    id INTEGER NOT NULL IDENTITY(1,1), 
    retweetedstatusid int, # We need the retweet info
    retweeteduserid int,
    tid int, # Linking it back to the relevant tweet
    PRIMARY KEY (id),
    CONSTRAINT fk_twid
        FOREIGN KEY (tid) REFERENCES tweetz(twid) # Linking it back to the relevant tweet
);"""

cur.execute(query_tb_4)
conn.commit()

