
timesheet = LOAD '$dir/timesheet.csv' USING PigStorage(',') AS
(driverId:int, week:int, hoursLogged:int, milesLogged:int);

drivers = LOAD '$dir/drivers.csv' USING PigStorage(',') AS (driverId:int, name:chararray, ssn:int, location:chararray, certified:chararray, wagePlan:chararray);

join_data = JOIN timesheet BY (driverId), drivers BY (driverId);

grp = group join_data by (timesheet::driverId, drivers::name, drivers::wagePlan);

drivers_events = FOREACH grp GENERATE group, SUM($1.timesheet::hoursLogged), SUM($1.timesheet::milesLogged);


truck_events_subset = LIMIT drivers_events 100;

n = DISTINCT drivers_events;

dump n;

fs -rm -r /pig/data/aaa.txt
fs -rm -r /pig/data/aaa
fs -mkdir /pig/data/aaa
store n into '/pig/data/aaa' using PigStorage('|');




