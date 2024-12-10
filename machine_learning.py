import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder, StandardScaler, PolynomialFeatures
from sklearn.feature_selection import f_classif
from sklearn.model_selection import train_test_split
from sklearn.model_selection import TimeSeriesSplit
from sklearn.compose import ColumnTransformer
from sklearn.decomposition import PCA
from sklearn.feature_selection import SelectKBest, VarianceThreshold
from skopt.space import Real, Categorical, Integer
from sklearn.pipeline import Pipeline
# import models
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from sklearn.neighbors import KNeighborsClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis, QuadraticDiscriminantAnalysis
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import GridSearchCV, RandomizedSearchCV
from skopt import BayesSearchCV # see https://machinelearningmastery.com/
from sklearn.experimental import enable_halving_search_cv
from sklearn.model_selection import HalvingGridSearchCV # like racing methods in tidymodelsscikit-optimize-for-hyperparameter-tuning-in-machine-learning/
import machine_learning_helpers as mlh
import scipy.stats
# MLP, see https://stackoverflow.com/questions/77254670/migration-from-keras-wrappers-sci-learn-to-scikeras


# Read data
df_full = pd.read_csv("data/modeldata.csv", index_col=None)
df_full.shape


X_train, y_train, X_test, y_test = mlh.prepare_data(df_full, "mean5")
X_train.shape
y_train.value_counts()
X_train.columns[137]

X_train.iloc[:,137]
# use gridsearchcv with multiple pipelines https://github.com/scikit-learn/scikit-learn/issues/9631
# Create time series split on training data
tscv = TimeSeriesSplit(n_splits=10)
 
for i, (train_index, test_index) in enumerate(tscv.split(X_train, y_train)):
    print(f"Fold {i}:")
    print(f"  Train data size = {(len(train_index))}")
    print(f"  Test data size = {(len(test_index))}")
    
# Define preprocessing objects 
standardizer = StandardScaler()
selector = SelectKBest(score_func=f_classif)
pca = PCA()
variance_selector = VarianceThreshold()

# Define model objects
knn_model = KNeighborsClassifier()
logit_model =  LogisticRegression(penalty = "l2", 
                                  solver="newton-cg")
svm_model = SVC()
bayes_model = GaussianNB()
forest_model = RandomForestClassifier(n_estimators=2000)

# Define model pipeline
model = Pipeline([
    ("standardize", standardizer),
    ("check_variance", variance_selector),
    ("reduce_features", selector),
    ("classifier", knn_model)
])


parameters = [
   { # Hyperparameter grid for Naive Bayes
       "reduce_features": [selector],
       "reduce_features__k": [5,8,10,20,25,30,40,50,"all"],      
       "classifier": [bayes_model]
   },
   { # Hyperparameter grid for Multinomial regressionn
       "reduce_features": [selector],
       "reduce_features__k": [5,8,10,20,25,30,40,50,"all"],        
       "classifier": [logit_model],
       "classifier__C": [0.001,0.01,0.1,1,10,20,50,100] # C = 1/lambda, with lambda = regularization strength
   },
   { # Hyperparameter grid for SVM
       "reduce_features": [selector],
       "reduce_features__k": [5,8,10,20,25,30,40,50,"all"],    
       "classifier": [svm_model],
       "classifier__C": [0.001,0.01,0.1,1,10,20,50,100],
       "classifier__kernel": ["rbf", "sigmoid"],
       "classifier__gamma": ["scale", "auto",0.1, 0.2, 0.5, 1, 2, 5, 10,20]
   },
    { # Hyperparameter grid for SVM
       "reduce_features": [selector],
       "reduce_features__k": [5,8,10,20,25,30,40,50,"all"],    
       "classifier": [svm_model],
       "classifier__C": [0.001,0.01,0.1,1,10,20,50,100],
       "classifier__kernel": ["poly"],
       "classifier__gamma": ["scale", "auto",0.1, 0.2, 0.5, 1, 2, 5, 10,20],
       "classifier__degree": [2,3,4,5,6]
   },
    { # Hyperparameter grid for Random forests and K Best feature selection
        "reduce_features": [selector],
        "reduce_features__k": [5,8,10,20,25,30,40,50,"all"],     
        "classifier": [forest_model],
        "classifier__max_features": [None, "sqrt","log2",0.5,0.1,0.2,0.4,0.8],
        "classifier__max_depth": [1,2,3],
        #"classifier__min_samples_leaf": [1,5,10,100]
    }
]

grid_search = GridSearchCV(model, parameters, cv=tscv, verbose=10, scoring="accuracy", n_jobs=12) #, factor=1.5)
grid_result = grid_search.fit(X_train, y_train)
print("Best: %f using %s" % (grid_result.best_score_, grid_result.best_params_))
grid_result.best_params_

grid_result.best_score_

grid_result
halving_grid_result.best_score_
# Define grid for random search

n_components = Integer(5,70)

k_features= Integer(5,70)
k_features_forest = (10, 20, 40, 50, 80, 100, "all")
C_range = Real(0.0001, 100, "log-uniform")
n_neighbors = Integer(2,50)
max_features = Real(0.1,0.9, "uniform")
max_depth = Integer(1,6)


# Define a grid for Bayesian search
logit_pca = {
    "reduce_features": [pca],
    "reduce_features__n_components": n_components,
    "classifier": [logit_model],
    "classifier__C": C_range
}
logit_kbest = {
       "reduce_features": [selector],
       "reduce_features__k": k_features,        
       "classifier": [logit_model],
       "classifier__C": C_range
}
knn_pca = {
    "reduce_features": [pca],
    "reduce_features__n_components": n_components,
    "classifier": [knn_model],
    "classifier__n_neighbors": n_neighbors
}
knn_kbest = {
       "reduce_features": [selector],
       "reduce_features__k": k_features,        
       "classifier": [knn_model],
       "classifier__n_neighbors": n_neighbors
}
forest = {
    "reduce_features": [selector],
    "reduce_features__k": k_features_forest,
    "classifier": [forest_model],
    "classifier__max_features": max_features,
    "classifier__max_depth": max_depth,
}


grid_search_bayes = BayesSearchCV(model, [(logit_pca, 50), (logit_kbest, 50), (knn_pca, 50), (knn_kbest, 50), (forest, 50)], cv=tscv, verbose = 12, scoring="accuracy", n_jobs=12, random_state=15)

grid_bayes_result = grid_search_bayes.fit(X_train, y_train)

print("Best: %f using %s" % (grid_bayes_result.best_score_, grid_bayes_result.best_params_))
grid_bayes_result.best_params_

grid_bayes_result.best_score_

grid_bayes_result
