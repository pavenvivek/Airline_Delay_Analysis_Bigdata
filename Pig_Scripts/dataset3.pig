
-- Run this file in ubuntu terminal (or other supporting platforms) as "pig -f dataset3.pig"

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

        -- Keep only the necessary fields
        airline2 = foreach airline_flt generate YEAR as year, MONTH as month, DAY_OF_MONTH as day, DAY_OF_WEEK as dow,
                        UNIQUE_CARRIER as carrier, ORIGIN as origin, DEST as dest, DISTANCE as distance,
                        CRS_DEP_TIME as time, DEP_DELAY as delay, util.to_date(YEAR, MONTH, DAY_OF_MONTH) as date;

        -- load weather data (give the path corresponding to your local system setup)
        weather = load 'weather/$year_str.csv' using PigStorage(',') 
                    as (station: chararray, date: chararray, metric, value, t1, t2, t3, time);

        -- keep only TMIN and TMAX weather observations from JFK
        origin_weather_tmin = filter weather by station == 'USW00094728' and metric == 'TMIN';
        origin_weather_tmax = filter weather by station == 'USW00094728' and metric == 'TMAX';
        origin_weather_prcp = filter weather by station == 'USW00094728' and metric == 'PRCP';
        origin_weather_snow = filter weather by station == 'USW00094728' and metric == 'SNOW';
        origin_weather_awnd = filter weather by station == 'USW00094728' and metric == 'AWND';

        -- keep only TMIN and TMAX weather observations from LAX
        dest_weather_tmin = filter weather by station == 'USW00023174' and metric == 'TMIN';
        dest_weather_tmax = filter weather by station == 'USW00023174' and metric == 'TMAX';
        dest_weather_prcp = filter weather by station == 'USW00023174' and metric == 'PRCP';
        dest_weather_snow = filter weather by station == 'USW00023174' and metric == 'SNOW';
        dest_weather_awnd = filter weather by station == 'USW00023174' and metric == 'AWND';

        joined = join airline2 by date, origin_weather_tmin by date, origin_weather_tmax by date, origin_weather_prcp by date, 
                                        origin_weather_snow by date, origin_weather_awnd by date,
                                        dest_weather_tmin by date, dest_weather_tmax by date, dest_weather_prcp by date, 
                                        dest_weather_snow by date, dest_weather_awnd by date;

        $data = foreach joined generate delay, month, day, dow, util.get_hour(airline2::time) as tod, distance, carrier, dest,
                                        util.days_from_nearest_holiday(year, month, day) as hdays,
                                        origin_weather_tmin::value as origin_temp_min, origin_weather_tmax::value as origin_temp_max,
                                        origin_weather_prcp::value as origin_prcp, origin_weather_snow::value as origin_snow, origin_weather_awnd::value as origin_wind,
                                        dest_weather_tmin::value as dest_temp_min, dest_weather_tmax::value as dest_temp_max,
                                        dest_weather_prcp::value as dest_prcp, dest_weather_snow::value as dest_snow, dest_weather_awnd::value as dest_wind;
};

Data_2011 = preprocess('2011', 'JFK', 'LAX');
rmf W_JFK_LAX_Data_2011
store Data_2011 into 'W_JFK_LAX_Data_2011' using PigStorage(',');

Data_2012 = preprocess('2012', 'JFK', 'LAX');
rmf W_JFK_LAX_Data_2012
store Data_2012 into 'W_JFK_LAX_Data_2012' using PigStorage(',');

Data_2013 = preprocess('2013', 'JFK', 'LAX');
rmf W_JFK_LAX_Data_2013
store Data_2013 into 'W_JFK_LAX_Data_2013' using PigStorage(',');

Data_2014 = preprocess('2014', 'JFK', 'LAX');
rmf W_JFK_LAX_Data_2014
store Data_2014 into 'W_JFK_LAX_Data_2014' using PigStorage(',');

Data_2015 = preprocess('2015', 'JFK', 'LAX');
rmf W_JFK_LAX_Data_2015
store Data_2015 into 'W_JFK_LAX_Data_2015' using PigStorage(',');