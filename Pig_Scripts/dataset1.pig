
-- Run this file in ubuntu terminal (or other supporting platforms) as "pig -f dataset1.pig"

Register 'util.py' USING jython as util;
DEFINE preprocess(year_str, org_airport_code, dest_airport_code) returns data
{
        -- load airline data from specified year
        airline = load 'airline_data/$year_str.csv' using PigStorage(',') 
            as (YEAR: int, MONTH: int, DAY_OF_MONTH: int, DAY_OF_WEEK: int, UNIQUE_CARRIER: chararray, TAIL_NUM, FL_NUM, ORIGIN_AIRPORT_ID, ORIGIN: chararray,
                ORIGIN_CITY_NAME, DEST_AIRPORT_ID, DEST: chararray, DEST_CITY_NAME, CRS_DEP_TIME: chararray, DEP_TIME: chararray, DEP_DELAY: int,
                TAXI_OUT, TAXI_IN, CRS_ARR_TIME, ARR_TIME, ARR_DELAY, CANCELLED: int, CANCELLATION_CODE, DIVERTED, CRS_ELAPSED_TIME, ACTUAL_ELAPSED_TIME,
                AIR_TIME, DISTANCE: int, CARRIER_DELAY, WEATHER_DELAY, NAS_DELAY, SECURITY_DELAY, LATE_AIRCRAFT_DELAY);

        -- keep only instances where flight was not cancelled and originate at JFK with Destination as LAX
        airline_flt = filter airline by CANCELLED == 0 and ORIGIN == '$org_airport_code' and DEST == '$dest_airport_code';

        -- Keep only necessary fields
        $data = foreach airline_flt generate DEP_DELAY as delay, MONTH, DAY_OF_MONTH, DAY_OF_WEEK, 
                                             util.get_hour(CRS_DEP_TIME) as hour, DISTANCE, UNIQUE_CARRIER, DEST,
                                             util.days_from_nearest_holiday(YEAR, MONTH, DAY_OF_MONTH) as hdays;
};

Data_2011 = preprocess('2011', 'JFK', 'LAX');
rmf Data_2011
store Data_2011 into 'Data_2011' using PigStorage(',');

Data_2012 = preprocess('2012', 'JFK', 'LAX');
rmf Data_2012
store Data_2012 into 'Data_2012' using PigStorage(',');

Data_2013 = preprocess('2013', 'JFK', 'LAX');
rmf Data_2013
store Data_2013 into 'Data_2013' using PigStorage(',');

Data_2014 = preprocess('2014', 'JFK', 'LAX');
rmf Data_2014
store Data_2014 into 'Data_2014' using PigStorage(',');

Data_2015 = preprocess('2015', 'JFK', 'LAX');
rmf Data_2015
store Data_2015 into 'Data_2015' using PigStorage(',');