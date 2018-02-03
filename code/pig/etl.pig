-- ***************************************************************************
-- TASK
-- Aggregate events into features of patient and generate training, testing data for mortality prediction.
-- Steps have been provided to guide you.
-- You can include as many intermediate steps as required to complete the calculations.
-- ***************************************************************************

-- ***************************************************************************
-- TESTS
-- To test, please change the LOAD path for events and mortality to ../../test/events.csv and ../../test/mortality.csv
-- 6 tests have been provided to test all the subparts in this exercise.
-- Manually compare the output of each test against the csv's in test/expected folder.
-- ***************************************************************************

-- register a python UDF for converting data into SVMLight format
-- import org.apache.pig.builtin.SubtractDuration;

REGISTER utils.py USING jython AS utils;

-- load events file
events = LOAD '../sample_test/sample_events.csv' USING PigStorage(',') AS (patientid:int, eventid:chararray, eventdesc:chararray, timestamp:chararray, value:float);

-- select required columns from events
events = FOREACH events GENERATE patientid, eventid, ToDate(timestamp, 'yyyy-MM-dd') AS etimestamp, value;

-- load mortality file
mortality = LOAD '../sample_test/sample_mortality.csv' USING PigStorage(',') as (patientid:int, timestamp:chararray, label:int);

mortality = FOREACH mortality GENERATE patientid, ToDate(timestamp, 'yyyy-MM-dd') AS mtimestamp, label;

-- To display the relation, use the ump command e.g. DUMP mortality;

-- ***************************************************************************
-- Compute the index dates for dead and alive patients
-- ***************************************************************************
eventswithmort = JOIN events BY patientid LEFT OUTER, mortality BY patientid;
deadevents = FILTER eventswithmort BY label is not null;
aliveevents = FILTER eventswithmort by label is null;

deadevents = FOREACH deadevents GENERATE events::patientid as patientid, events::eventid as eventid, events::value as value, mortality::label as label, -1 * DaysBetween(events::etimestamp, SubtractDuration(mortality::mtimestamp, 'P30D')) as time_difference;

patient_group = GROUP aliveevents BY events::patientid;
-- generates data in form of (patientid, index_date)
dates = FOREACH patient_group GENERATE group as patientid, MAX(aliveevents.etimestamp) as index_date;

aliveevents_with_dates = JOIN aliveevents BY events::patientid, dates by patientid;

aliveevents = FOREACH aliveevents_with_dates GENERATE aliveevents::events::patientid as patientid, aliveevents::events::eventid as eventid, aliveevents::events::value as value, 0 as label, -1 * DaysBetween(aliveevents::events::etimestamp, index_date) as time_difference;


--TEST-1
-- deadevents = ORDER deadevents BY patientid, eventid;
-- aliveevents = ORDER aliveevents BY patientid, eventid;
-- STORE aliveevents INTO 'aliveevents' USING PigStorage(',');
-- STORE deadevents INTO 'deadevents' USING PigStorage(',');

-- ***************************************************************************
-- Filter events within the observation window and remove events with missing values
-- ***************************************************************************
allevents = UNION aliveevents, deadevents;

filtered = FILTER allevents BY time_difference <= 2000;

filtered = FILTER filtered BY value is not null;

--TEST-2
-- filteredgrpd = GROUP filtered BY 1;
-- filtered = FOREACH filteredgrpd GENERATE FLATTEN(filtered);
-- filtered = ORDER filtered BY patientid, eventid,time_difference;
-- STORE filtered INTO 'filtered' USING PigStorage(',');

-- ***************************************************************************
-- Aggregate events to create features
-- ***************************************************************************
grouped_events = GROUP filtered BY (patientid, eventid);
featureswithid = FOREACH grouped_events GENERATE FLATTEN(group) as (patientid, eventid), COUNT(filtered.label) as featurevalue;

--TEST-3
-- featureswithid = ORDER featureswithid BY patientid, eventid;
-- STORE featureswithid INTO 'features_aggregate' USING PigStorage(',');

-- ***************************************************************************
-- Generate feature mapping
-- ***************************************************************************
distinct_features = FOREACH featureswithid GENERATE eventid;
distinct_features = DISTINCT distinct_features;
distinct_features = ORDER distinct_features by eventid;

features_by_number = GROUP distinct_features by 1;
indexed_features_by_number = FOREACH features_by_number GENERATE utils.bag_to_indexed(distinct_features);
all_features = FOREACH indexed_features_by_number GENERATE FLATTEN(indexes);
-- STORE all_features INTO 'features' using PigStorage(' ');

-- features = -- perform join of featureswithid and all_features by eventid and replace eventid with idx. It is of the -- form (patientid, idx, featurevalue)

features = JOIN featureswithid BY eventid, all_features by eventid;
features = FOREACH features GENERATE featureswithid::patientid as patientid, all_features::indexes::index as idx, featureswithid::featurevalue as featurevalue;

--TEST-4
features = ORDER features BY patientid, idx;
STORE features INTO 'features_map' USING PigStorage(',');


