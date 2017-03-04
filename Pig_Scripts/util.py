#
# Python UDFs for our PIG script
#
from datetime import date

# get hour-of-day from HHMM field
@outputSchema("value: int")
def get_hour(val):
  return int(val.zfill(4)[:2])

# this array defines the dates of holiday in 2011, 2012, 2013, 2014 and 2015
holidays = [
        date(2011, 1, 1), date(2011, 1, 15), date(2011, 2, 19), date(2011, 5, 28), date(2011, 6, 7), date(2011, 7, 4), \
        date(2011, 9, 3), date(2011, 10, 8), date(2011, 11, 11), date(2011, 11, 22), date(2011, 12, 25), \
        date(2012, 1, 1), date(2012, 1, 21), date(2012, 2, 18), date(2012, 5, 22), date(2012, 5, 26), date(2012, 7, 4), \
        date(2012, 9, 1), date(2012, 10, 13), date(2012, 11, 11), date(2012, 11, 27), date(2012, 12, 25), \
        date(2013, 1, 1), date(2013, 1, 21), date(2013, 2, 18), date(2013, 5, 22), date(2013, 5, 26), date(2013, 7, 4), \
        date(2013, 9, 1), date(2013, 10, 13), date(2013, 11, 11), date(2013, 11, 27), date(2013, 12, 25), \
        date(2014, 1, 1), date(2014, 1, 21), date(2014, 2, 18), date(2014, 5, 22), date(2014, 5, 26), date(2014, 7, 4), \
        date(2014, 9, 1), date(2014, 10, 13), date(2014, 11, 11), date(2014, 11, 27), date(2014, 12, 25), \
        date(2015, 1, 1), date(2015, 1, 21), date(2015, 2, 18), date(2015, 5, 22), date(2015, 5, 26), date(2015, 7, 4), \
        date(2015, 9, 1), date(2015, 10, 13), date(2015, 11, 11), date(2015, 11, 27), date(2015, 12, 25) \
     ]
# get number of days from nearest holiday
@outputSchema("days: int")
def days_from_nearest_holiday(year, month, day):
  d = date(year, month, day)
  x = [(abs(d-h)).days for h in holidays]
  return min(x)

@outputSchema("date: chararray")
def to_date(year, month, day):
  s = "%04d%02d%02d" % (year, month, day)
  return s
