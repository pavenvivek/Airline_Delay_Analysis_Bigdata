from sklearn.preprocessing import OneHotEncoder

# read files
cols = ['delay', 'month', 'day', 'dow', 'hour', 'distance', 'carrier', 'dest', 'days_from_holiday']
col_types = {'delay': int, 'month': int, 'day': int, 'dow': int, 'hour': int, 'distance': int, 
             'carrier': str, 'dest': str, 'days_from_holiday': int}
data_2014 = read_csv_from_hdfs('/home/paven/Bigdata/Project/Data_2014', cols, col_types)
data_2015 = read_csv_from_hdfs('/home/paven/Bigdata/Project/Data_2015', cols, col_types)

# Create training set and test set
train_y = data_2014['delay'] >= 15
categ = [cols.index(x) for x in 'hour', 'month', 'day', 'dow', 'carrier', 'dest']
enc = OneHotEncoder(categorical_features = categ)
df = data_2014.drop('delay', axis=1)
df['carrier'] = pd.factorize(df['carrier'])[0]
df['dest'] = pd.factorize(df['dest'])[0]
train_x = enc.fit_transform(df)

test_y = data_2015['delay'] >= 15
df = data_2015.drop('delay', axis=1)
df['carrier'] = pd.factorize(df['carrier'])[0]
df['dest'] = pd.factorize(df['dest'])[0]
test_x = enc.transform(df)

print train_x.shape

# Create Random Forest classifier with 100 trees
clf_rf = RandomForestClassifier(n_estimators=100, n_jobs=-1)
clf_rf.fit(train_x.toarray(), train_y)

# Evaluate on test set
pr = clf_rf.predict(test_x.toarray())

# print results
cm = confusion_matrix(test_y, pr)
print "<-------  RandomForestClassifier -------->"
print "Confusion matrix:"
print pd.DataFrame(cm)
report_rf = precision_recall_fscore_support(list(test_y), list(pr), average='binary')
print "\n[-] Precision = %0.2f\n[-] Recall = %0.2f\n[-] F1 score = %0.2f\n[-] Accuracy = %0.2f" % \
        (report_rf[0], report_rf[1], report_rf[2], accuracy_score(list(test_y), list(pr)))
print "<---------------------------------------->\n"
