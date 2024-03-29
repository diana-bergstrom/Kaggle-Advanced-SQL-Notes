--JOINs and UNIONs

--Question 1
--How long does it take for questions to receive answers on Stack Overflow?

SELECT q.id AS q_id,
  MIN(TIMESTAMP_DIFF(a.creation_date, q.creation_date, SECOND)) as time_to_answer
FROM `bigquery-public-data.stackoverflow.posts_questions` AS q
LEFT JOIN `bigquery-public-data.stackoverflow.posts_answers` AS a
  ON q.id = a.parent_id
WHERE q.creation_date >= '2018-01-01' and q.creation_date < '2018-02-01'
GROUP BY q_id
ORDER BY time_to_answer;

--Question 2
--You're interested in understanding the initial experiences that users typically have with the Stack Overflow website. Is it more common for users to first ask questions or provide answers? After signing up, how long does it take for users to first interact with the website? 

SELECT q.owner_user_id AS owner_user_id,
  MIN(q.creation_date) AS q_creation_date,
  MIN(a.creation_date) AS a_creation_date
FROM `bigquery-public-data.stackoverflow.posts_questions` AS q
FULL JOIN `bigquery-public-data.stackoverflow.posts_answers` AS a
  ON q.owner_user_id = a.owner_user_id 
WHERE q.creation_date >= '2019-01-01' AND q.creation_date < '2019-02-01'
AND a.creation_date >= '2019-01-01' AND a.creation_date < '2019-02-01'
GROUP BY owner_user_id;

--Question 3
--You're interested in understanding users who joined the site in January 2019. You want to track their activity on the site: when did they post their first questions and answers, if ever?

SELECT u.id AS id,
  MIN(q.creation_date) AS q_creation_date,
  MIN(a.creation_date) AS a_creation_date
FROM `bigquery-public-data.stackoverflow.posts_questions` AS q
FULL JOIN `bigquery-public-data.stackoverflow.posts_answers` AS a
  ON q.owner_user_id = a.owner_user_id
RIGHT JOIN `bigquery-public-data.stackoverflow.users` AS u
  ON a.owner_user_id = u.id
WHERE u.creation_date > '2019-01-01' AND u.creation_date < '2019-02-01'
GROUP BY u.id;

--Question 4
--Write a query that returns a table with a single column owner_user_id - the IDs of all users who posted at least one question or answer on January 1, 2019.  Each user ID should appear at most once. Your query must use a UNION.

SELECT DISTINCT owner_user_id
FROM (
  SELECT owner_user_id
  FROM `bigquery-public-data.stackoverflow.posts_questions`
  WHERE DATE(creation_date) = '2019-01-01'
  UNION DISTINCT
  SELECT owner_user_id
  FROM `bigquery-public-data.stackoverflow.posts_answers`
  WHERE DATE(creation_date) = '2019-01-01'
);

--Analytic Functions (a.k.a window functions)
--Notes:
--All analytic functions have an OVER clause which has three optional parts:
--1. PARTITION BY clause: this divides rows of a table into different groups (i.e. if you have an id assigned to individuals in a table, you may want to partition by id in an analytic function)
--  *if there is no PARTITION BY clause the query will treat the table as a single partition
--2. ORDER BY clause: defines an ordering within each partition (i.e. if you have dates stored in the table you are querying you may want to order chronologically)
--3. window frame clause: identifies the set of rows used in each calculation - can be written many ways - examples:
--  a. ROWS BETWEEN x PRECEEDING AND CURRENT ROW: x number of previous rows and the current row
--  b. ROWS BETWEEN x PRECEEDING AND y following: x number of previous rows and y number of following rows
--  c. ROWS BETWEEN UNBOUNDED PRECEEDING AND UNBOUNDED FOLLOWING: all rows in the partition
--There are different types of analytic functions, but three common ones are:
--  1. Analytic aggregate functions
--  2. Analytic navigation functions
--  3. Analytic numbering functions

--Question 1
--Say you work for a taxi company, and you're interested in predicting the demand for taxis. Towards this goal, you'd like to create a plot that shows a rolling average of the daily number of taxi trips. Return a data frame with columns for:
--  1. trip_date - contains one entry for each date from January 1, 2016, to March 31, 2016
--  2. avg_num_trips - shows the average number of daily trips, calculated over a window including the value for the current date, along with the values for the preceding 3 days and the following 3 days, as long as the days fit within the three-month time frame

WITH trips_by_day AS
  (
     SELECT 
      DATE(trip_start_timestamp) AS trip_date,
      COUNT(*) as num_trips
     FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
     WHERE trip_start_timestamp > '2016-01-01' AND trip_start_timestamp < '2016-04-01'
     GROUP BY trip_date
     ORDER BY trip_date
     )
      SELECT 
        trip_date,
       AVG(num_trips)
        OVER (
        ORDER BY trip_date
        ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING
        ) AS avg_num_trips
      FROM trips_by_day;

--This query creates a CTE (common table expression) for the number of trips between January 1 2016 and March 31 2016. Then, from that data, it returns the trip date and average number of daily trips calculated over a window of 3 days before and 3 days after any given date (provided that these dates are still within the Jan - March timeframe from the CTE)

--Question 2:
--Separate and order trips by community area for October 3rd 2013. Return a DataFrame with: pickup_community_area, trip_start_timestamp, trip_end_timestamp, and trip_number, where trip_number shows the order in which the trips were taken from their respective community areas. Use the RANK() function to answer this question.

WITH community_rank AS(
  SELECT pickup_community_area, trip_start_timestamp, trip_end_timestamp
  FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
  ORDER BY pickup_community_area
)
  SELECT pickup_community_area,
    trip_start_timestamp,
    trip_end_timestamp,
    RANK() OVER (PARTITION BY pickup_community_area ORDER BY trip_start_timestamp) AS trip_number
  FROM community_rank
  WHERE DATE(trip_start_timestamp) = '2013-10-03'
  ORDER BY pickup_community_area;

--To return the desired results, I first created a subquery with a CTE that orders the data I want by pickup_community_area. The next step was to pull the data I wanted from the subquery which included three columns in the original table of the dataset (pickup_community_area, trip_start_timestamp, trip_end_timestamp) as well as trip_number. I had to create the trip_number column using the RANK() window funciton, which ranks the trips within the pickup_community area based on the trip_start_timestamp of each trip. Finally, I ordered by pickup_community_area to sort the results based on the pickup_community_area.

--Question 3
--The challenge presents the following partial query:

 SELECT taxi_id,
  trip_start_timestamp,
  trip_end_timestamp,
  TIMESTAMP_DIFF(
    trip_start_timestamp, 
    ____
    OVER (
      PARTITION BY ____
      ORDER BY ____),
     MINUTE) as prev_break
FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
WHERE DATE(trip_start_timestamp) = '2013-10-03';

--The task is to find how much time elapses between trips by editing the query to include an additional prev_break column that shows the length of the break (in minutes) that the driver had before each trip started (which corresponds to the time between trip_start_timestamp of the current trip and trip_end_timestamp of the previous trip). The challenge specifies to partition the calculation by taxi_id, and order the results within each partition by trip_start_timestamp:

SELECT taxi_id,
  trip_start_timestamp,
  trip_end_timestamp,
  TIMESTAMP_DIFF(
    trip_start_timestamp,
    LAG(trip_end_timestamp)
      OVER (
        PARTITION BY taxi_id
        ORDER BY trip_start_timestamp),
    MINUTE) as prev_break
FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
WHERE DATE(trip_start_timestamp) = '2013-10-03' 
ORDER BY taxi_id;

--To finish the TIMESTAMP_DIFF call I added lag(trip_end_timestamp) so that the timestamp difference calculates the difference in minutes between a taxi id's start time for a ride and it's previous ride's end time. LAG functions are window functions so it is immediately followed with an OVER clause, in which I filled in the designations from the directions about partitioning by taxi_id and ordering by trip_start_timestamp to find the time difference per ride for each taxi's rides in order of trip start time. WHERE filters so that the query only returns rides started on October 3 2013 and lastly, I ordered results by taxi_id.

--Nested and Repeated Data
--To query repeated data the column containing that data needs to be used with the UNNEST function.
--STRUCTs are used to avoid expensive JOINs.

--Question 1
--Who had the most commits in 2016?
--Write a query to find the individuals with the most commits in this table in 2016. Your query should return a table with two columns:
--1. committer_name - contains the name of each individual with a commit (from 2016) in the table
--2. num_commits - shows the number of commits the individual has in the table (from 2016)
--Sort the table, so that people with more commits appear first.

SELECT
  committer.name AS committer_name,
  COUNT(commit) AS num_commits
FROM `bigquery-public-data.github_repos.sample_commits`
WHERE EXTRACT(YEAR from committer.date) > 2015 AND EXTRACT(YEAR from committer.date) < 2017
GROUP BY committer_name
ORDER BY num_commits DESC;

--Because the data in the committer field is stored as a RECORD data type, it must be specificied that we want to select name from the committer field (committer.name). This query counts the number of commits, groups by committer_name, and orders by number of commits in descending order so that the committers with the most commits appear first.

--Question 2
--Write a query to leverage the information in the languages table to determine which programming languages appear in the most repositories. The table returned by your query should have two columns:
--1. language_name - the name of the programming language
--2. num_repos - the number of repositories in the languages table that use the programming language
--Sort the table so that languages that appear in more repos are shown first.

SELECT 
  l.name AS language_name,
  COUNT(repo_name) AS num_repos
FROM `bigquery-public-data.github_repos.languages`,
UNNEST(language) as l
GROUP BY language_name
ORDER BY num_repos DESC;

--To return the desired results I needed to flatten the language column (an ARRAY) using UNNEST.

--Question 3
--Which languages are used in the repository with the most languages?
--For this question, you'll restrict your attention to the repository with name 'polyrabbit/polyglot'
--Write a query that returns a table with one row for each language in this repository. The table should have two columns:
--1. name - the name of the programming language
--2. bytes - the total number of bytes of that programming language
--Sort the table by the bytes column so that programming languages that take up more space in the repo appear first.

SELECT 
  l.name AS name,
  l.bytes AS bytes
FROM `bigquery-public-data.github_repos.languages`,
UNNEST(language) as l
WHERE repo_name = 'polyrabbit/polyglot'
ORDER BY bytes DESC;

--Similar to in question 2, data from the language column had to be unnested. With that in mind, the rest of the query is quite simple as we just needed to call for the name and bytes data where repo_name is polyrabbit/polyglot and order it by bytes in descending order so that the language types with the most bytes appear first

--Writing Efficient Queries
--There are functions that exist to help compare the efficiency of different queries - optimizing a query can save companies time and money. Two helpful functions for this are:
--1. show_amount_of_data_scanned() which shows the amount of data the query uses
--2. show_time_to_run() which prints how long it takes for the query to execute
--Tips to optimize query efficiency:
-- a. only select rows that need to be returned (as opposed to SELECT * FROM dataset)
-- b. read less data
-- c. avoid N:N JOINs
