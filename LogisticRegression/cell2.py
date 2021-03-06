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

# Create logistic regression model with L2 regularization
clf_lr = linear_model.LogisticRegression(penalty='l2', class_weight='balanced')
clf_lr.fit(train_x.toarray(), train_y)

# Predict output labels on test set
pr = clf_lr.predict(test_x.toarray())

# display evaluation metrics
cm = confusion_matrix(test_y, pr)
print "<-------  LogisticRegression -------->"
print "Confusion matrix:"
print pd.DataFrame(cm)
report_lr = precision_recall_fscore_support(list(test_y), list(pr), average='binary')
print "\n[-] Precision = %0.2f\n[-] Recall = %0.2f\n[-] F1 score = %0.2f\n[-] Accuracy = %0.2f" % \
        (report_lr[0], report_lr[1], report_lr[2], accuracy_score(list(test_y), list(pr)))
print "<------------------------------------>\n"
