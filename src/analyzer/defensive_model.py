import os
import numpy as np
import pandas as pd
from joblib import dump, load
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import StandardScaler
from sklearn import metrics
from sklearn.neighbors import KNeighborsClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.svm import SVC
import json

SEED = 42
DATA_PATH = "./data/embeddings_ae.csv"
REPORT_PATH = "./result/defensive_model.txt"

#if os.path.exists(REPORT_PATH):
#    os.remove(REPORT_PATH)

data = pd.read_csv(DATA_PATH)
X, y, X_new, X_ae, n = [], [], [], [], []
X_result, y_result = [], []
print_re = {}
for row in data.values:
    #X.append(row[1:])
    #y.append(0)

    if os.path.exists(f"./server_data/test_dataset/test_tasks/{row[0]}.adjlist"):
        X.append(row[1:])
        y.append(0)
        n.append(f"{row[0]}.adjlist")
    """
    if os.path.exists(f"../server_data/malware/{row[0]}.adjlist"):
        X.append(row[1:])
        y.append(1)
        n.append(f"{row[0]}.adjlist")
    if os.path.exists(f"../server_data/new_malware/{row[0]}.adjlist"):
        #X_new.append(row[1:])
        X.append(row[1:])
        y.append(1)
        n.append(f"{row[0]}.adjlist")
    if os.path.exists(f"../server_data/AEs/{row[0]}.adjlist"):
        X_ae.append(row[1:])
"""
#X_train, X_test, y_train, y_test = train_test_split(
#    X, y, test_size=.3, random_state=SEED, stratify=y)
#only run test model
#X_train.extend(X_ae)
#y_train.extend([1] * len(X_ae))
#X_test.extend(X_new)
#y_test.extend([1] * len(X_new))
#X.extend(X_new)
#y.extend([1] * len(X_new))
#print(len(X_train), len(X_test))
#X.pop()
#y.pop()
#n.pop()
print(len(X), len(y))

#scaler = StandardScaler()
#X_train = scaler.fit_transform(X_train)
#X_test = scaler.transform(X_test)
#X_new = scaler.transform(X_new)
#dump(scaler, "model/defensive/scaler.joblib")
scaler = load("./model/defensive/scaler.joblib")
X = scaler.transform(X)

classifiers = {
    "NB": GaussianNB(),
    "DT": DecisionTreeClassifier(random_state=SEED, class_weight="balanced"),
    "KNN": KNeighborsClassifier(n_jobs=-1),
    "SVM": SVC(random_state=2020, probability=True, class_weight="balanced"),
    "RF": RandomForestClassifier(random_state=SEED, class_weight="balanced", n_jobs=-1),
}

hyperparam = [
    {},
    {"criterion": ["gini", "entropy"]},
    {"n_neighbors": [5, 100, 500], "weights": ["uniform", "distance"]},
    {"C": np.logspace(-3, 3, 7), "gamma": np.logspace(-3, 3, 7)},
    {"criterion": ["gini", "entropy"], "max_features": ["auto", "sqrt", "log2"], "n_estimators": [5, 10, 20, 50, 100, 200]},
]

if os.path.exists("result/pred_result.json"):
    os.remove("result/pred_result.json")


for (name, est), hyper in zip(classifiers.items(), hyperparam):
    clf = GridSearchCV(est, hyper, cv=5, n_jobs=-1)
    ###load model from file
    clf = load(f"./model/defensive/{name}.joblib")
    #clf.fit(X_train, y_train)

    #y_prob = clf.predict_proba(X)
    y_result = clf.predict(X)
    print(y_result)
    #print(len(y_result))
    print_re["filename"] = n[0]
    if y_result[-1] == 0:
        pre_re = "benign"
    else:
        pre_re = "malware"
    print_re[name] = pre_re


pre_result = json.dumps(print_re)
with open("./result/pred_result.json", 'w') as f:
    json.dump(pre_result, f)
f.close()


'''
    y_prob = clf.predict_proba(X_test)
    y_pred = clf.predict(X_test)
    y_pred_new = clf.predict(X_new)
    
    #dump(clf, f"model/defensive/{name}.joblib")
    #print(X, y_pred)
    
    with open(REPORT_PATH, 'a') as f:

        clf_rp = metrics.classification_report(y_test, y_pred, digits=4)
        cnf_matrix = metrics.confusion_matrix(y_test, y_pred)
        AUC = metrics.roc_auc_score(y_test, y_prob[:, 1])

        TN, FP, FN, TP = cnf_matrix.ravel()
        FPR = FP / (FP + TN)
        f.write(f"{name}\n")
        f.write(str(clf_rp) + '\n')
        f.write(str(cnf_matrix) + '\n\n')
        f.write(f"ROC AUC: {round(AUC, 4)}\n")
        f.write(f"FRP: {round(FPR, 4)}\n")
        f.write(f"Detect new malwares: {y_pred_new.sum()}/{len(X_new)}\n")
        f.write('-' * 88 + '\n')
    '''