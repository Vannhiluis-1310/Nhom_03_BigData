# Complete Model Results Summary

Data: Olist E-Commerce Dataset | Train: 69,609 rows | Val: 14,916 rows | Test: 14,916 rows

---

## 1. CLASSIFICATION MODELS

| Model | Test Accuracy | Test F1 | Val Accuracy | Val F1 | Precision | Recall |
|---|---|---|---|---|---|---|
| **RandomForestClassifier** | **0.8261** | **0.7537** | 0.8089 | 0.7252 | 0.7822 | 0.8239 |
| RandomForest_CV (3-fold) | **0.8261** | **0.7537** | 0.8089 | 0.7252 | - | - |
| GBTClassifier_CV (3-fold) | 0.8255 | 0.7537 | 0.8085 | 0.7260 | - | - |
| DecisionTreeClassifier | 0.8239 | 0.7569 | 0.8062 | 0.7311 | 0.7823 | 0.8239 |
| GBTClassifier | 0.8236 | 0.7555 | 0.8079 | 0.7310 | - | - |
| LinearSVC | 0.8219 | 0.7415 | 0.8073 | 0.7213 | - | - |
| LogisticRegression | 0.8207 | 0.7421 | 0.8072 | 0.7241 | - | - |
| LogisticRegression_CV (3-fold) | 0.8207 | 0.7421 | 0.8072 | 0.7241 | - | - |
| NaiveBayes | 0.7794 | 0.7361 | 0.7752 | 0.7298 | - | - |

**Best classifier**: RandomForestClassifier (Test Accuracy: 82.61%, Test F1: 0.7537)

---

## 2. REGRESSION MODELS

| Model | Test RMSE | Test MAE | Test R2 | Val RMSE | Val MAE | Val R2 |
|---|---|---|---|---|---|---|
| LinearRegression_CV (3-fold) | **198.71** | **87.28** | **0.1898** | 188.53 | 89.92 | 0.2591 |
| LinearRegression | 198.83 | 87.34 | 0.1888 | 188.54 | 89.98 | 0.2590 |
| RandomForestRegressor | 198.92 | 92.19 | 0.1880 | 187.94 | 92.05 | 0.2637 |
| DecisionTreeRegressor | 208.10 | 97.39 | 0.1113 | 196.07 | 96.99 | 0.1986 |

**Best regressor**: LinearRegression_CV (Test RMSE: 198.71, Test R2: 0.1898)

Note: All regression models show low R2 values (< 0.27), indicating limited predictive power for the target variable.

---

## 3. CLUSTERING MODELS

| Model | Silhouette Score | k | Rows |
|---|---|---|---|
| **GaussianMixture** | **0.5996** | 6 | 96,096 |
| KMeans | 0.5813 | 6 | 96,096 |
| BisectingKMeans | 0.3829 | 6 | 96,096 |

**Best clustering**: GaussianMixture (Silhouette: 0.5996)

---

## 4. RECOMMENDATION MODEL

| Model | RMSE | Train Rows | Val Rows | Test Rows | Status |
|---|---|---|---|---|---|
| ALS | null | 69,598 | 15,207 | 14,980 | Evaluation not ready (NaN predictions) |

Note: ALS model produced null RMSE values. Evaluation was not completed successfully.

---

## 5. PATTERN MINING

| Model | Transactions | Frequent Itemsets | Association Rules |
|---|---|---|---|
| FPGrowth | 3,236 | 66 | 18 |

---

## 6. CROSSVALIDATOR RESULTS

### Best Hyperparameters

| Model | Tuning Method | Folds | Best Params | CV Avg Metric | Training Time | Grid Size |
|---|---|---|---|---|---|---|
| GBTClassifier | CrossValidator | 3 | maxIter=10, maxDepth=5, stepSize=0.1 | 0.7584 (acc) | 41.77s | 3 |
| RandomForest | CrossValidator | 3 | numTrees=50, maxBins=16 | 0.7585 (acc) | 19.49s | 4 |
| LogisticRegression | CrossValidator | 3 | regParam=0.01, maxIter=10 | 0.7547 (acc) | 16.76s | 9 |
| LinearRegression | CrossValidator | 3 | regParam=1.0, maxIter=100 | 0.2315 (r2) | 7.53s | 9 |

### CrossValidator vs TrainValidationSplit Comparison

| Model | CV Test Acc/R2 | TVS Test Acc/R2 | Winner |
|---|---|---|---|
| GBTClassifier | 0.8255 | 0.8236 | CV |
| RandomForest | 0.8261 | 0.8261 | Tie |
| LogisticRegression | 0.8207 | 0.8207 | Tie |
| LinearRegression | 0.1898 (R2) | 0.1888 (R2) | CV |

---

## 7. CLASSIFICATION NOTEBOOK EXECUTION STATUS

| Notebook | Status |
|---|---|
| 04_01_Classification_LogisticRegression | ok |
| 04_02_Classification_RandomForest | ok |
| 04_03_Classification_NaiveBayes | ok |
| 04_04_Classification_LinearSVC | ok |
| 04_05_Classification_GBTClassifier | ok |
| 04_06_Classification_DecisionTree | ok |

---

## 8. OVERALL RANKINGS

### By Model Family - Best Performers

| Rank | Family | Model | Key Metric |
|---|---|---|---|
| 1 | Classification | RandomForestClassifier | Accuracy: 82.61% |
| 2 | Clustering | GaussianMixture | Silhouette: 0.5996 |
| 3 | Regression | LinearRegression_CV | R2: 0.1898 |
| 4 | Pattern Mining | FPGrowth | 66 itemsets, 18 rules |
| 5 | Recommendation | ALS | Evaluation incomplete |
