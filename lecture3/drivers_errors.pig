 
truck_events = LOAD '$dir/truck_event_text_partition.csv' USING PigStorage(',') AS (driverId:int, truckId:int, eventTime:chararray, eventType:chararray,
longitude:double, latitude:double, eventKey:chararray, correlationId:long,
driverName:chararray, routeId:long, routeName:chararray, eventDate:chararray);

drivers = LOAD '/pig/data/drivers.csv' USING PigStorage(',') AS (driverId:int, name:chararray, ssn:int, location:chararray, certified:chararray, wagePlan:chararray);

join_data = JOIN truck_events BY (driverId), drivers BY (driverId);

ordered_data = ORDER drivers BY name asc;

filterd_errors = FILTER truck_events BY NOT(eventType MATCHES 'Normal');

drivers_errors = FOREACH filterd_errors GENERATE driverName, eventTime, eventType;

truck_events_subset = LIMIT drivers_errors 100;

dump drivers_errors;

