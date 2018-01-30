-- ***************************************************************************
-- Loading Data:
-- create external table mapping for events.csv and mortality_events.csv

-- IMPORTANT NOTES:
-- You need to put events.csv and mortality.csv under hdfs directory 
-- '/input/events/events.csv' and '/input/mortality/mortality.csv'
-- 
-- To do this, run the following commands for events.csv, 
-- 1. sudo su - hdfs
-- 2. hdfs dfs -mkdir -p /input/events
-- 3. hdfs dfs -chown -R root /input
-- 4. exit 
-- 5. hdfs dfs -put /path-to-events.csv /input/events/
-- Follow the same steps 1 - 5 for mortality.csv, except that the path should be 
-- '/input/mortality'
-- ***************************************************************************
-- create events table 
DROP TABLE IF EXISTS events;
CREATE EXTERNAL TABLE events (
  patient_id STRING,
  event_id STRING,
  event_description STRING,
  time DATE,
  value DOUBLE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/input/sample_events';

-- create mortality events table 
DROP TABLE IF EXISTS mortality;
CREATE EXTERNAL TABLE mortality (
  patient_id STRING,
  time DATE,
  label INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/input/sample_mortality';

-- ******************************************************
-- Task 1:
-- By manipulating the above two tables, 
-- generate two views for alive and dead patients' events
-- ******************************************************
-- find events for alive patients

DROP VIEW IF EXISTS alive_events;
CREATE VIEW alive_events 
AS
SELECT events.patient_id, events.event_id, events.time FROM
events WHERE patient_id NOT IN (SELECT patient_id FROM mortality);

-- find events for dead patients
DROP VIEW IF EXISTS dead_events;
CREATE VIEW dead_events 
AS
SELECT events.patient_id, events.event_id, events.time FROM
events WHERE patient_id IN (SELECT patient_id FROM mortality);


-- ************************************************
-- Task 2: Event count metrics
-- Compute average, min and max of event counts 
-- for alive and dead patients respectively  
-- ************************************************
-- alive patients
DROP VIEW IF EXISTS alive_counts;
CREATE VIEW alive_counts
AS
SELECT patient_id, count(event_id) as event_count 
FROM alive_events 
GROUP BY patient_id;
SELECT avg(event_count), min(event_count), max(event_count) FROM alive_counts;




-- dead patients
-- ***** your code below *****
DROP VIEW IF EXISTS dead_counts;
CREATE VIEW dead_counts
AS
SELECT patient_id, count(event_id) as event_count 
FROM dead_events 
GROUP BY patient_id;
SELECT avg(event_count), min(event_count), max(event_count) FROM dead_counts;





-- ************************************************
-- Task 3: Encounter count metrics 
-- Compute average, min and max of encounter counts 
-- for alive and dead patients respectively
-- ************************************************
-- alive
-- ***** your code below *****
DROP VIEW IF EXISTS alive_encounter_counts;
CREATE VIEW alive_encounter_counts
AS
SELECT patient_id, count(distinct time) as event_count 
FROM alive_events 
GROUP BY patient_id;
SELECT avg(event_count), min(event_count), max(event_count) FROM alive_encounter_counts;




-- dead
-- ***** your code below *****
DROP VIEW IF EXISTS dead_encounter_counts;
CREATE VIEW dead_encounter_counts
AS
SELECT patient_id, count(distinct time) as event_count 
FROM dead_events 
GROUP BY patient_id;
SELECT avg(event_count), min(event_count), max(event_count) FROM dead_encounter_counts;


-- ************************************************
-- Task 4: Record length metrics
-- Compute average, median, min and max of record lengths
-- for alive and dead patients respectively
-- ************************************************
-- alive 
DROP VIEW IF EXISTS alive_rec_length_counts;
CREATE VIEW alive_rec_length_counts
AS
SELECT patient_id, datediff(max(time), min(time)) as record_length 
FROM alive_events
GROUP BY patient_id;

SELECT avg(record_length), percentile(record_length, 0.5), min(record_length), max(record_length)
FROM alive_rec_length_counts;

-- dead
DROP VIEW IF EXISTS dead_rec_length_counts;
CREATE VIEW dead_rec_length_counts
AS
SELECT patient_id, datediff(max(time), min(time)) as record_length 
FROM dead_events
GROUP BY patient_id;
SELECT avg(record_length), percentile(record_length, 0.5), min(record_length), max(record_length)
from dead_rec_length_counts;


-- ******************************************* 
-- Task 5: Common diag/lab/med
-- Compute the 5 most frequently occurring diag/lab/med
-- for alive and dead patients respectively
-- *******************************************
-- alive patients
---- diag
select event_id, count(*) as event_count from alive_events where event_id RLIKE 'DIAG' group by event_id order by event_count desc limit 5;


---- lab
select event_id, count(*) as event_count from alive_events where event_id RLIKE 'LAB' group by event_id order by event_count desc limit 5;

---- med
select event_id, count(*) as event_count from alive_events where event_id RLIKE 'DRUG' group by event_id order by event_count desc limit 5;



-- dead patients

---- diag
select event_id, count(*) as event_count from dead_events where event_id RLIKE 'DIAG' group by event_id order by event_count desc limit 5;

---- lab
select event_id, count(*) as event_count from dead_events where event_id RLIKE 'LAB' group by event_id order by event_count desc limit 5;

---- med
select event_id, count(*) as event_count from dead_events where event_id RLIKE 'DRUG' group by event_id order by event_count desc limit 5;








