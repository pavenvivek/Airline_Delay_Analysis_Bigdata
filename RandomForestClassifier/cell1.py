import warnings
warnings.filterwarnings('ignore')

import sys
import random
import numpy as np

from sklearn import linear_model, cross_validation, metrics, svm
from sklearn.metrics import confusion_matrix, precision_recall_fscore_support, accuracy_score
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler

import pandas as pd
import matplotlib.pyplot as plt
%matplotlib inline

import pydoop.hdfs as hdfs

def read_csv_from_hdfs(path, cols, col_types=None):
  files = hdfs.ls(path);
  pieces = []
  for f in files:
    fhandle = hdfs.open(f)
    pieces.append(pd.read_csv(fhandle, names=cols, dtype=col_types))
    fhandle.close()
  return pd.concat(pieces, ignore_index=True)

# read files
cols = ['delay', 'month', 'day', 'dow', 'hour', 'distance', 'carrier', 'dest', 'days_from_holiday']
col_types = {'delay': int, 'month': int, 'day': int, 'dow': int, 'hour': int, 'distance': int, 
             'carrier': str, 'dest': str, 'days_from_holiday': int}

data_2014 = read_csv_from_hdfs('/home/paven/Bigdata/Project/Data_2014', cols, col_types)
data_2015 = read_csv_from_hdfs('/home/paven/Bigdata/Project/Data_2015', cols, col_types)

# Create training set and test set
cols = ['month', 'day', 'dow', 'hour', 'distance', 'days_from_holiday']
train_y = data_2014['delay'] >= 15
train_x = data_2014[cols]

test_y = data_2015['delay'] >= 15
test_x = data_2015[cols]

print train_x.shape

# Create Random Forest classifier with 100 trees
clf_rf = RandomForestClassifier(n_estimators=100, n_jobs=-1)
clf_rf.fit(train_x, train_y)

# Evaluate on test set
pr = clf_rf.predict(test_x)

# print results
cm = confusion_matrix(test_y, pr)
print "<-------  RandomForestClassifier -------->"
print "Confusion matrix:"
print pd.DataFrame(cm)
report_rf = precision_recall_fscore_support(list(test_y), list(pr), average='binary')
print "\n[-] Precision = %0.2f\n[-] Recall = %0.2f\n[-] F1 score = %0.2f\n[-] Accuracy = %0.2f" % \
        (report_rf[0], report_rf[1], report_rf[2], accuracy_score(list(test_y), list(pr)))
print "<---------------------------------------->\n"
