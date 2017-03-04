from __future__ import division

cols = ['delay', 'month', 'day', 'dow', 'hour', 'distance', 'carrier', 'dest', 'days_from_holiday']
col_types = {'delay': int, 'month': int, 'day': int, 'dow': int, 'hour': int, 'distance': int, 
             'carrier': str, 'dest': str, 'days_from_holiday': int}

data_2011 = read_csv_from_hdfs('/home/paven/Bigdata/Project/Data_2011', cols, col_types)
data_2012 = read_csv_from_hdfs('/home/paven/Bigdata/Project/Data_2012', cols, col_types)
data_2013 = read_csv_from_hdfs('/home/paven/Bigdata/Project/Data_2013', cols, col_types)
data_2014 = read_csv_from_hdfs('/home/paven/Bigdata/Project/Data_2014', cols, col_types)
data_2015 = read_csv_from_hdfs('/home/paven/Bigdata/Project/Data_2015', cols, col_types)

print "<================ Flight Details for JFK (New York) to LAX (Los Angeles) =================>\n"

######## American Airlines

total_records_2011 = data_2011[data_2011['carrier'] == "AA"]
delayed_records_2011 = data_2011[(data_2011['carrier'] == "AA") & (data_2011['delay'] >= 15)]

total_records_2012 = data_2012[data_2012['carrier'] == "AA"]
delayed_records_2012 = data_2012[(data_2012['carrier'] == "AA") & (data_2012['delay'] >= 15)]

total_records_2013 = data_2013[data_2013['carrier'] == "AA"]
delayed_records_2013 = data_2013[(data_2013['carrier'] == "AA") & (data_2013['delay'] >= 15)]

total_records_2014 = data_2014[data_2014['carrier'] == "AA"]
delayed_records_2014 = data_2014[(data_2014['carrier'] == "AA") & (data_2014['delay'] >= 15)]

total_records_2015 = data_2015[data_2015['carrier'] == "AA"]
delayed_records_2015 = data_2015[(data_2015['carrier'] == "AA") & (data_2015['delay'] >= 15)]

if (total_records_2011.shape[0] != 0):
    percent_delayed_2011 = (float(delayed_records_2011.shape[0])/float(total_records_2011.shape[0])) * 100
else:
    percent_delayed_2011 = "No Records Found !!!"

if (total_records_2012.shape[0] != 0):
    percent_delayed_2012 = (float(delayed_records_2012.shape[0])/float(total_records_2012.shape[0])) * 100
else:
    percent_delayed_2012 = "No Records Found !!!"
    
if (total_records_2013.shape[0] != 0):
    percent_delayed_2013 = (float(delayed_records_2013.shape[0])/float(total_records_2013.shape[0])) * 100
else:
    percent_delayed_2013 = "No Records Found !!!"

if (total_records_2014.shape[0] != 0):
    percent_delayed_2014 = (float(delayed_records_2014.shape[0])/float(total_records_2014.shape[0])) * 100
else:
    percent_delayed_2014 = "No Records Found !!!"

if (total_records_2015.shape[0] != 0):
    percent_delayed_2015 = (float(delayed_records_2015.shape[0])/float(total_records_2015.shape[0])) * 100
else:
    percent_delayed_2015 = "No Records Found !!!"

print ">>>>>>>>>>>> American Airlines Delay Percent >>>>>>>>>>>>>"

print "Percent of AA carriers delayed in 2011 = " + str(percent_delayed_2011) + "%"
print "Percent of AA carriers delayed in 2012 = " + str(percent_delayed_2012) + "%"
print "Percent of AA carriers delayed in 2013 = " + str(percent_delayed_2013) + "%"
print "Percent of AA carriers delayed in 2014 = " + str(percent_delayed_2014) + "%"
print "Percent of AA carriers delayed in 2015 = " + str(percent_delayed_2015) + "%\n"

delayed_records = delayed_records_2011.shape[0] + delayed_records_2012.shape[0] + delayed_records_2013.shape[0] + delayed_records_2014.shape[0] + delayed_records_2015.shape[0]
                
total_records = total_records_2011.shape[0] + total_records_2012.shape[0] + total_records_2013.shape[0] + total_records_2014.shape[0] + total_records_2015.shape[0]
                
percent_delayed = (float(delayed_records)/float(total_records)) * 100

print "Delayed AA Carriers (2011-2015) = " + str(delayed_records)
print "Total AA Carriers (2011-2015) = " + str(total_records)
print "Percent of AA carriers delayed in (2011-2015) = " + str(percent_delayed) + "%\n"
print "Average ticket fare for AA [JFK -> LAX (Quickest)] = 589$\n"

######## Delta Airlines

total_records_2011 = data_2011[data_2011['carrier'] == "DL"]
delayed_records_2011 = data_2011[(data_2011['carrier'] == "DL") & (data_2011['delay'] >= 15)]

total_records_2012 = data_2012[data_2012['carrier'] == "DL"]
delayed_records_2012 = data_2012[(data_2012['carrier'] == "DL") & (data_2012['delay'] >= 15)]

total_records_2013 = data_2013[data_2013['carrier'] == "DL"]
delayed_records_2013 = data_2013[(data_2013['carrier'] == "DL") & (data_2013['delay'] >= 15)]

total_records_2014 = data_2014[data_2014['carrier'] == "DL"]
delayed_records_2014 = data_2014[(data_2014['carrier'] == "DL") & (data_2014['delay'] >= 15)]

total_records_2015 = data_2015[data_2015['carrier'] == "DL"]
delayed_records_2015 = data_2015[(data_2015['carrier'] == "DL") & (data_2015['delay'] >= 15)]

if (total_records_2011.shape[0] != 0):
    percent_delayed_2011 = (float(delayed_records_2011.shape[0])/float(total_records_2011.shape[0])) * 100
else:
    percent_delayed_2011 = "No Records Found !!!"

if (total_records_2012.shape[0] != 0):
    percent_delayed_2012 = (float(delayed_records_2012.shape[0])/float(total_records_2012.shape[0])) * 100
else:
    percent_delayed_2012 = "No Records Found !!!"
    
if (total_records_2013.shape[0] != 0):
    percent_delayed_2013 = (float(delayed_records_2013.shape[0])/float(total_records_2013.shape[0])) * 100
else:
    percent_delayed_2013 = "No Records Found !!!"

if (total_records_2014.shape[0] != 0):
    percent_delayed_2014 = (float(delayed_records_2014.shape[0])/float(total_records_2014.shape[0])) * 100
else:
    percent_delayed_2014 = "No Records Found !!!"

if (total_records_2015.shape[0] != 0):
    percent_delayed_2015 = (float(delayed_records_2015.shape[0])/float(total_records_2015.shape[0])) * 100
else:
    percent_delayed_2015 = "No Records Found !!!"

print ">>>>>>>>>>>> Delta Airlines Delay Percent >>>>>>>>>>>>>"

print "Percent of DL carriers delayed in 2011 = " + str(percent_delayed_2011) + "%"
print "Percent of DL carriers delayed in 2012 = " + str(percent_delayed_2012) + "%"
print "Percent of DL carriers delayed in 2013 = " + str(percent_delayed_2013) + "%"
print "Percent of DL carriers delayed in 2014 = " + str(percent_delayed_2014) + "%"
print "Percent of DL carriers delayed in 2015 = " + str(percent_delayed_2015) + "%\n"

delayed_records = delayed_records_2011.shape[0] + delayed_records_2012.shape[0] + delayed_records_2013.shape[0] + delayed_records_2014.shape[0] + delayed_records_2015.shape[0]
                
total_records = total_records_2011.shape[0] + total_records_2012.shape[0] + total_records_2013.shape[0] + total_records_2014.shape[0] + total_records_2015.shape[0]
                
percent_delayed = (float(delayed_records)/float(total_records)) * 100

print "Delayed DL Carriers (2011-2015) = " + str(delayed_records)
print "Total DL Carriers (2011-2015) = " + str(total_records)
print "Percent of DL carriers delayed in (2011-2015) = " + str(percent_delayed) + "%\n"
print "Average ticket fare for DL [JFK -> LAX (Quickest)] = 588$\n"

######## JetBlue Airlines

total_records_2011 = data_2011[data_2011['carrier'] == "B6"]
delayed_records_2011 = data_2011[(data_2011['carrier'] == "B6") & (data_2011['delay'] >= 15)]

total_records_2012 = data_2012[data_2012['carrier'] == "B6"]
delayed_records_2012 = data_2012[(data_2012['carrier'] == "B6") & (data_2012['delay'] >= 15)]

total_records_2013 = data_2013[data_2013['carrier'] == "B6"]
delayed_records_2013 = data_2013[(data_2013['carrier'] == "B6") & (data_2013['delay'] >= 15)]

total_records_2014 = data_2014[data_2014['carrier'] == "B6"]
delayed_records_2014 = data_2014[(data_2014['carrier'] == "B6") & (data_2014['delay'] >= 15)]

total_records_2015 = data_2015[data_2015['carrier'] == "B6"]
delayed_records_2015 = data_2015[(data_2015['carrier'] == "B6") & (data_2015['delay'] >= 15)]

if (total_records_2011.shape[0] != 0):
    percent_delayed_2011 = (float(delayed_records_2011.shape[0])/float(total_records_2011.shape[0])) * 100
else:
    percent_delayed_2011 = "No Records Found !!!"

if (total_records_2012.shape[0] != 0):
    percent_delayed_2012 = (float(delayed_records_2012.shape[0])/float(total_records_2012.shape[0])) * 100
else:
    percent_delayed_2012 = "No Records Found !!!"
    
if (total_records_2013.shape[0] != 0):
    percent_delayed_2013 = (float(delayed_records_2013.shape[0])/float(total_records_2013.shape[0])) * 100
else:
    percent_delayed_2013 = "No Records Found !!!"

if (total_records_2014.shape[0] != 0):
    percent_delayed_2014 = (float(delayed_records_2014.shape[0])/float(total_records_2014.shape[0])) * 100
else:
    percent_delayed_2014 = "No Records Found !!!"

if (total_records_2015.shape[0] != 0):
    percent_delayed_2015 = (float(delayed_records_2015.shape[0])/float(total_records_2015.shape[0])) * 100
else:
    percent_delayed_2015 = "No Records Found !!!"

print ">>>>>>>>>>>> JetBlue Airlines Delay Percent >>>>>>>>>>>>>"

print "Percent of B6 carriers delayed in 2011 = " + str(percent_delayed_2011) + "%"
print "Percent of B6 carriers delayed in 2012 = " + str(percent_delayed_2012) + "%"
print "Percent of B6 carriers delayed in 2013 = " + str(percent_delayed_2013) + "%"
print "Percent of B6 carriers delayed in 2014 = " + str(percent_delayed_2014) + "%"
print "Percent of B6 carriers delayed in 2015 = " + str(percent_delayed_2015) + "%\n"

delayed_records = delayed_records_2011.shape[0] + delayed_records_2012.shape[0] + delayed_records_2013.shape[0] + delayed_records_2014.shape[0] + delayed_records_2015.shape[0]
                
total_records = total_records_2011.shape[0] + total_records_2012.shape[0] + total_records_2013.shape[0] + total_records_2014.shape[0] + total_records_2015.shape[0]
                
percent_delayed = (float(delayed_records)/float(total_records)) * 100

print "Delayed B6 Carriers (2011-2015) = " + str(delayed_records)
print "Total B6 Carriers (2011-2015) = " + str(total_records)
print "Percent of B6 carriers delayed in (2011-2015) = " + str(percent_delayed) + "%\n"
print "Average ticket fare for B6 [JFK -> LAX (Quickest)] = 556$\n"

######## United Airlines

total_records_2011 = data_2011[data_2011['carrier'] == "UA"]
delayed_records_2011 = data_2011[(data_2011['carrier'] == "UA") & (data_2011['delay'] >= 15)]

total_records_2012 = data_2012[data_2012['carrier'] == "UA"]
delayed_records_2012 = data_2012[(data_2012['carrier'] == "UA") & (data_2012['delay'] >= 15)]

total_records_2013 = data_2013[data_2013['carrier'] == "UA"]
delayed_records_2013 = data_2013[(data_2013['carrier'] == "UA") & (data_2013['delay'] >= 15)]

total_records_2014 = data_2014[data_2014['carrier'] == "UA"]
delayed_records_2014 = data_2014[(data_2014['carrier'] == "UA") & (data_2014['delay'] >= 15)]

total_records_2015 = data_2015[data_2015['carrier'] == "UA"]
delayed_records_2015 = data_2015[(data_2015['carrier'] == "UA") & (data_2015['delay'] >= 15)]

if (total_records_2011.shape[0] != 0):
    percent_delayed_2011 = (float(delayed_records_2011.shape[0])/float(total_records_2011.shape[0])) * 100
else:
    percent_delayed_2011 = "No Records Found !!!"

if (total_records_2012.shape[0] != 0):
    percent_delayed_2012 = (float(delayed_records_2012.shape[0])/float(total_records_2012.shape[0])) * 100
else:
    percent_delayed_2012 = "No Records Found !!!"
    
if (total_records_2013.shape[0] != 0):
    percent_delayed_2013 = (float(delayed_records_2013.shape[0])/float(total_records_2013.shape[0])) * 100
else:
    percent_delayed_2013 = "No Records Found !!!"

if (total_records_2014.shape[0] != 0):
    percent_delayed_2014 = (float(delayed_records_2014.shape[0])/float(total_records_2014.shape[0])) * 100
else:
    percent_delayed_2014 = "No Records Found !!!"

if (total_records_2015.shape[0] != 0):
    percent_delayed_2015 = (float(delayed_records_2015.shape[0])/float(total_records_2015.shape[0])) * 100
else:
    percent_delayed_2015 = "No Records Found !!!"

print ">>>>>>>>>>>> United Airlines Delay Percent >>>>>>>>>>>>>"

print "Percent of UA carriers delayed in 2011 = " + str(percent_delayed_2011) + "%"
print "Percent of UA carriers delayed in 2012 = " + str(percent_delayed_2012) + "%"
print "Percent of UA carriers delayed in 2013 = " + str(percent_delayed_2013) + "%"
print "Percent of UA carriers delayed in 2014 = " + str(percent_delayed_2014) + "%"
print "Percent of UA carriers delayed in 2015 = " + str(percent_delayed_2015) + "%\n"

delayed_records = delayed_records_2011.shape[0] + delayed_records_2012.shape[0] + delayed_records_2013.shape[0] + delayed_records_2014.shape[0] + delayed_records_2015.shape[0]
                
total_records = total_records_2011.shape[0] + total_records_2012.shape[0] + total_records_2013.shape[0] + total_records_2014.shape[0] + total_records_2015.shape[0]
                
percent_delayed = (float(delayed_records)/float(total_records)) * 100

print "Delayed UA Carriers (2011-2015) = " + str(delayed_records)
print "Total UA Carriers (2011-2015) = " + str(total_records)
print "Percent of UA carriers delayed in (2011-2015) = " + str(percent_delayed) + "%\n"
print "Average ticket fare for UA [JFK -> LAX (Quickest)] = 546$\n"

######## Virgin America Airlines

total_records_2011 = data_2011[data_2011['carrier'] == "VX"]
delayed_records_2011 = data_2011[(data_2011['carrier'] == "VX") & (data_2011['delay'] >= 15)]

total_records_2012 = data_2012[data_2012['carrier'] == "VX"]
delayed_records_2012 = data_2012[(data_2012['carrier'] == "VX") & (data_2012['delay'] >= 15)]

total_records_2013 = data_2013[data_2013['carrier'] == "VX"]
delayed_records_2013 = data_2013[(data_2013['carrier'] == "VX") & (data_2013['delay'] >= 15)]

total_records_2014 = data_2014[data_2014['carrier'] == "VX"]
delayed_records_2014 = data_2014[(data_2014['carrier'] == "VX") & (data_2014['delay'] >= 15)]

total_records_2015 = data_2015[data_2015['carrier'] == "VX"]
delayed_records_2015 = data_2015[(data_2015['carrier'] == "VX") & (data_2015['delay'] >= 15)]

if (total_records_2011.shape[0] != 0):
    percent_delayed_2011 = (float(delayed_records_2011.shape[0])/float(total_records_2011.shape[0])) * 100
else:
    percent_delayed_2011 = "No Records Found !!!"

if (total_records_2012.shape[0] != 0):
    percent_delayed_2012 = (float(delayed_records_2012.shape[0])/float(total_records_2012.shape[0])) * 100
else:
    percent_delayed_2012 = "No Records Found !!!"
    
if (total_records_2013.shape[0] != 0):
    percent_delayed_2013 = (float(delayed_records_2013.shape[0])/float(total_records_2013.shape[0])) * 100
else:
    percent_delayed_2013 = "No Records Found !!!"

if (total_records_2014.shape[0] != 0):
    percent_delayed_2014 = (float(delayed_records_2014.shape[0])/float(total_records_2014.shape[0])) * 100
else:
    percent_delayed_2014 = "No Records Found !!!"

if (total_records_2015.shape[0] != 0):
    percent_delayed_2015 = (float(delayed_records_2015.shape[0])/float(total_records_2015.shape[0])) * 100
else:
    percent_delayed_2015 = "No Records Found !!!"

print ">>>>>>>>>>>> Virgin America Airlines Delay Percent >>>>>>>>>>>>>"

print "Percent of VX carriers delayed in 2011 = " + str(percent_delayed_2011) + "%"
print "Percent of VX carriers delayed in 2012 = " + str(percent_delayed_2012) + "%"
print "Percent of VX carriers delayed in 2013 = " + str(percent_delayed_2013) + "%"
print "Percent of VX carriers delayed in 2014 = " + str(percent_delayed_2014) + "%"
print "Percent of VX carriers delayed in 2015 = " + str(percent_delayed_2015) + "%\n"

delayed_records = delayed_records_2011.shape[0] + delayed_records_2012.shape[0] + delayed_records_2013.shape[0] + delayed_records_2014.shape[0] + delayed_records_2015.shape[0]
                
total_records = total_records_2011.shape[0] + total_records_2012.shape[0] + total_records_2013.shape[0] + total_records_2014.shape[0] + total_records_2015.shape[0]
                
percent_delayed = (float(delayed_records)/float(total_records)) * 100

print "Delayed VX Carriers (2011-2015) = " + str(delayed_records)
print "Total VX Carriers (2011-2015) = " + str(total_records)
print "Percent of VX carriers delayed in (2011-2015) = " + str(percent_delayed) + "%\n"
print "Average ticket fare for VX [JFK -> LAX (Quickest)] = 546$\n"

