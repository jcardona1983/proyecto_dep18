# Project summary

## Requeriment
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming
app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an
easy way to query their data.

## Solution
In this project, Fact and dimension tables for a **star schema** are defined for a particular analytic focus, and an ETL pipeline 
that transfers data from files in two local directories into these tables. the tables and the data are in Postgres and the ETL is
built using Python and SQL.

## Purposes of the star schema
* Sparkify will have the possibility to do more straightforward queries meaning that the join logic for the Star Schema is simpler
  than the join logic of another highly normalized model.
* Has efficient Query performance. Star Schemas can provide performance enhancement for the read-only reporting applications than
  other types of schemas.
* Faster aggregations, which mean that simpler queries put against a Star Schema can produce an improved performance for all
  aggregation operations.
  
  
# Project files

1. **test.ipynb** displays the first few rows of each table and counts the records inserted in each table.
2. **create_tables.py** drops and creates the tables.
3. **etl.ipynb** reads and processes a single file from song_data and log_data and loads the data into the tables.
   This notebook contains detailed instructions on the ETL process for each of the tables.
4. **etl.py** reads and processes files from song_data and log_data and loads them into the tables.
5. **sql_queries.py** contains all the sql queries, and is imported into the last three files above.


# How does it work

### step 1: Run create_tables.py in a terminal to create all the tables

```bash
python create_tables.py
```

### step 2: Run etl.py in a terminal to load the data from the files to the tables

```bash
python etl.py
```

### step 3: Run the queries in the test.ipynb notebook to check if the data was loaded

1. Open the ***test.ipynb notebook***  
2. Load the sql module: ***%load_ext sql***
3. Connect to the data base: ***%sql postgresql://student:student@127.0.0.1/sparkifydb***
4. Run the queries

These are the queries you can find in the notebook:

* %sql SELECT * FROM songplays LIMIT 5
* %sql SELECT * FROM users LIMIT 5
* %sql SELECT * FROM songs LIMIT 5
* %sql SELECT * FROM artists LIMIT 5
* %sql SELECT * FROM time LIMIT 5

* %sql SELECT COUNT(*) FROM users
* %sql SELECT COUNT(*) FROM artists
* %sql SELECT COUNT(*) FROM songs
* %sql SELECT COUNT(*) FROM time
* %sql SELECT COUNT(*) FROM songplays


