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

from sklearn.linear_model import LogisticRegression
from sklearn.naive_bayes import GaussianNB
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import VotingClassifier

clf1 = LogisticRegression(random_state=1)
clf2 = RandomForestClassifier(random_state=1)
clf3 = GaussianNB()

eclf = VotingClassifier(estimators=[('lr', clf1), ('rf', clf2), ('gnb', clf3)], voting='hard')

eclf.fit(train_x.toarray(), train_y)

# Evaluate on test set
pr = eclf.predict(test_x.toarray())

# print results
cm = confusion_matrix(test_y, pr)
print "<-------  VotingClassifier -------->"
print "Confusion matrix:"
print pd.DataFrame(cm)
report_svm = precision_recall_fscore_support(list(test_y), list(pr), average='binary')
print "\n[-] Precision = %0.2f\n[-] Recall = %0.2f\n[-] F1 score = %0.2f\n[-] Accuracy = %0.2f" % \
        (report_svm[0], report_svm[1], report_svm[2], accuracy_score(list(test_y), list(pr)))
print "<---------------------------------->\n"
