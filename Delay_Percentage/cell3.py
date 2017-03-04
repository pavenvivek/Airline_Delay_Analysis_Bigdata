from __future__ import division

cols = ['delay', 'month', 'day', 'dow', 'hour', 'distance', 'carrier', 'dest', 'days_from_holiday']
col_types = {'delay': int, 'month': int, 'day': int, 'dow': int, 'hour': int, 'distance': int, 
             'carrier': str, 'dest': str, 'days_from_holiday': int}

data_2011 = read_csv_from_hdfs('/home/paven/Bigdata/Project/Data_2011', cols, col_types)
data_2012 = read_csv_from_hdfs('/home/paven/Bigdata/Project/Data_2012', cols, col_types)
data_2013 = read_csv_from_hdfs('/home/paven/Bigdata/Project/Data_2013', cols, col_types)
data_2014 = read_csv_from_hdfs('/home/paven/Bigdata/Project/Data_2014', cols, col_types)
data_2015 = read_csv_from_hdfs('/home/paven/Bigdata/Project/Data_2015', cols, col_types)

data_2011['DepDelayed'] = data_2011['delay'].apply(lambda x: x>=15)
grouped = data_2011[['DepDelayed', 'carrier']].groupby('carrier').mean()
grouped.plot(kind='bar')

data_2012['DepDelayed'] = data_2012['delay'].apply(lambda x: x>=15)
grouped = data_2012[['DepDelayed', 'carrier']].groupby('carrier').mean()
grouped.plot(kind='bar')

data_2013['DepDelayed'] = data_2013['delay'].apply(lambda x: x>=15)
grouped = data_2013[['DepDelayed', 'carrier']].groupby('carrier').mean()
grouped.plot(kind='bar')

data_2014['DepDelayed'] = data_2014['delay'].apply(lambda x: x>=15)
grouped = data_2014[['DepDelayed', 'carrier']].groupby('carrier').mean()
grouped.plot(kind='bar')

data_2015['DepDelayed'] = data_2015['delay'].apply(lambda x: x>=15)
grouped = data_2015[['DepDelayed', 'carrier']].groupby('carrier').mean()
grouped.plot(kind='bar')

data_2015['DepDelayed'] = data_2015['delay'].apply(lambda x: x>=15)

grouped = data_2015[['DepDelayed', 'hour']].groupby('hour').mean()
grouped.plot(kind='bar')

grouped = data_2015[['DepDelayed', 'day']].groupby('day').mean()
grouped.plot(kind='bar')

grouped = data_2015[['DepDelayed', 'month']].groupby('month').mean()
grouped.plot(kind='bar')

grouped = data_2015[['DepDelayed', 'dow']].groupby('dow').mean()
grouped.plot(kind='bar')

grouped = data_2015[['DepDelayed', 'days_from_holiday']].groupby('days_from_holiday').mean()
grouped.plot(kind='bar')

