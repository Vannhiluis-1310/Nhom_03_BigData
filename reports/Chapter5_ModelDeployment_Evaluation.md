# CHUONG 5: TRIEN KHAI MO HINH HOC MAY VA DANH GIA

Cap nhat tu dong tu `reports/model_metrics/*.json` luc 2026-04-07 23:37:11.

## 5.1. Nguyen tac bao cao

- So lieu trong chuong nay duoc sinh tu dong tu file metrics JSON hien hanh.
- Neu metrics thay doi sau retrain, can chay lai script sinh report de dong bo.
- Khong su dung so lieu tay/randomSplit cu neu khong ton tai trong metrics JSON.

## 5.2. Classification

| model_name             |     val_f1 |         f1 |   val_accuracy |   accuracy |   val_rmse |   rmse |   val_r2 |   r2 |   precision_at_k |   recall_at_k |   map_at_k |   ndcg_at_k |   train_rows |   val_rows |   test_rows | _file                                             |
|:-----------------------|-----------:|-----------:|---------------:|-----------:|-----------:|-------:|---------:|-----:|-----------------:|--------------:|-----------:|------------:|-------------:|-----------:|------------:|:--------------------------------------------------|
| DecisionTreeClassifier |   0.825833 |   0.83601  |       0.829177 |   0.838026 |        nan |    nan |      nan |  nan |              nan |           nan |        nan |         nan |       105187 |      14916 |       14916 | classification_decision_tree.json                 |
| GBTClassifier          |   0.849413 |   0.859795 |       0.854586 |   0.863703 |        nan |    nan |      nan |  nan |              nan |           nan |        nan |         nan |       105187 |      14916 |       14916 | classification_gbt_classifier.json                |
| GBTClassifier          | nan        | nan        |     nan        | nan        |        nan |    nan |      nan |  nan |              nan |           nan |        nan |         nan |          nan |        nan |         nan | tuning_classification_gbt_classifier_cv.json      |
| GBTClassifier_CV       |   0.667472 | nan        |       0.768184 | nan        |        nan |    nan |      nan |  nan |              nan |           nan |        nan |         nan |        68887 |      14546 |       14734 | classification_gbt_classifier_cv.json             |
| LinearSVC              |   0.856916 |   0.86346  |       0.865111 |   0.871279 |        nan |    nan |      nan |  nan |              nan |           nan |        nan |         nan |       105187 |      14916 |       14916 | classification_linear_svc.json                    |
| LogisticRegression     |   0.85723  |   0.863846 |       0.862899 |   0.868597 |        nan |    nan |      nan |  nan |              nan |           nan |        nan |         nan |       105187 |      14916 |       14916 | classification_logistic_regression.json           |
| LogisticRegression     | nan        | nan        |     nan        | nan        |        nan |    nan |      nan |  nan |              nan |           nan |        nan |         nan |          nan |        nan |         nan | tuning_classification_logistic_regression.json    |
| LogisticRegression     | nan        | nan        |     nan        | nan        |        nan |    nan |      nan |  nan |              nan |           nan |        nan |         nan |          nan |        nan |         nan | tuning_classification_logistic_regression_cv.json |
| LogisticRegression_CV  |   0.667472 | nan        |       0.768184 | nan        |        nan |    nan |      nan |  nan |              nan |           nan |        nan |         nan |        68887 |      14546 |       14734 | classification_logistic_regression_cv.json        |
| NaiveBayes             |   0.855097 |   0.860787 |       0.866519 |   0.871346 |        nan |    nan |      nan |  nan |              nan |           nan |        nan |         nan |       105187 |      14916 |       14916 | classification_naive_bayes.json                   |
| RandomForest           | nan        | nan        |     nan        | nan        |        nan |    nan |      nan |  nan |              nan |           nan |        nan |         nan |          nan |        nan |         nan | tuning_classification_random_forest_cv.json       |
| RandomForestClassifier |   0.831845 |   0.841803 |       0.832328 |   0.84111  |        nan |    nan |      nan |  nan |              nan |           nan |        nan |         nan |       105187 |      14916 |       14916 | classification_random_forest.json                 |
| RandomForest_CV        |   0.667472 | nan        |       0.768184 | nan        |        nan |    nan |      nan |  nan |              nan |           nan |        nan |         nan |        68887 |      14546 |       14734 | classification_random_forest_cv.json              |

## 5.3. Regression

| model_name            |   val_f1 |   f1 |   val_accuracy |   accuracy |   val_rmse |    rmse |     val_r2 |         r2 |   precision_at_k |   recall_at_k |   map_at_k |   ndcg_at_k |   train_rows |   val_rows |   test_rows | _file                                       |
|:----------------------|---------:|-----:|---------------:|-----------:|-----------:|--------:|-----------:|-----------:|-----------------:|--------------:|-----------:|------------:|-------------:|-----------:|------------:|:--------------------------------------------|
| DecisionTreeRegressor |      nan |  nan |            nan |        nan |    194.385 | 201.314 |   0.163296 |   0.157665 |              nan |           nan |        nan |         nan |        69294 |      14611 |       14761 | regression_decision_tree.json               |
| LinearRegression      |      nan |  nan |            nan |        nan |    195.514 | 202.177 |   0.153545 |   0.150427 |              nan |           nan |        nan |         nan |        69294 |      14611 |       14761 | regression_linear_regression.json           |
| LinearRegression      |      nan |  nan |            nan |        nan |    nan     | nan     | nan        | nan        |              nan |           nan |        nan |         nan |          nan |        nan |         nan | tuning_regression_linear_regression.json    |
| LinearRegression      |      nan |  nan |            nan |        nan |    nan     | nan     | nan        | nan        |              nan |           nan |        nan |         nan |          nan |        nan |         nan | tuning_regression_linear_regression_cv.json |
| LinearRegression_CV   |      nan |  nan |            nan |        nan |    195.518 | nan     |   0.153509 | nan        |              nan |           nan |        nan |         nan |        69294 |      14611 |       14761 | regression_linear_regression_cv.json        |
| RandomForestRegressor |      nan |  nan |            nan |        nan |    194.908 | 201.479 |   0.158781 |   0.156285 |              nan |           nan |        nan |         nan |        69294 |      14611 |       14761 | regression_random_forest.json               |

## 5.4. Clustering

| model_name      |   val_f1 |   f1 |   val_accuracy |   accuracy |   val_rmse |   rmse |   val_r2 |   r2 |   precision_at_k |   recall_at_k |   map_at_k |   ndcg_at_k |   train_rows |   val_rows |   test_rows | _file                            |
|:----------------|---------:|-----:|---------------:|-----------:|-----------:|-------:|---------:|-----:|-----------------:|--------------:|-----------:|------------:|-------------:|-----------:|------------:|:---------------------------------|
| BisectingKMeans |      nan |  nan |            nan |        nan |        nan |    nan |      nan |  nan |              nan |           nan |        nan |         nan |        65592 |      13799 |       13967 | clustering_bisecting_kmeans.json |
| GaussianMixture |      nan |  nan |            nan |        nan |        nan |    nan |      nan |  nan |              nan |           nan |        nan |         nan |        65592 |      13799 |       13967 | clustering_gaussian_mixture.json |
| KMeans          |      nan |  nan |            nan |        nan |        nan |    nan |      nan |  nan |              nan |           nan |        nan |         nan |        65592 |      13799 |       13967 | clustering_kmeans.json           |

## 5.5. Recommendation

| model_name   |   val_f1 |   f1 |   val_accuracy |   accuracy |   val_rmse |    rmse |   val_r2 |   r2 |   precision_at_k |   recall_at_k |    map_at_k |   ndcg_at_k |   train_rows |   val_rows |   test_rows | _file                   |
|:-------------|---------:|-----:|---------------:|-----------:|-----------:|--------:|---------:|-----:|-----------------:|--------------:|------------:|------------:|-------------:|-----------:|------------:|:------------------------|
| ALS          |      nan |  nan |            nan |        nan |    1.19965 | 1.22068 |      nan |  nan |      9.44465e-06 |   9.44465e-05 | 1.88893e-05 | 3.65369e-05 |        71277 |      15368 |       15342 | recommendation_als.json |

## 5.6. Pattern_Mining

| model_name   |   val_f1 |   f1 |   val_accuracy |   accuracy |   val_rmse |   rmse |   val_r2 |   r2 |   precision_at_k |   recall_at_k |   map_at_k |   ndcg_at_k |   train_rows |   val_rows |   test_rows | _file                 |
|:-------------|---------:|-----:|---------------:|-----------:|-----------:|-------:|---------:|-----:|-----------------:|--------------:|-----------:|------------:|-------------:|-----------:|------------:|:----------------------|
| FPGrowth     |      nan |  nan |            nan |        nan |        nan |    nan |      nan |  nan |              nan |           nan |        nan |         nan |          nan |        nan |         nan | pattern_fpgrowth.json |

## 5.7. Utilities

- Train/Val/Test split duoc doc truc tiep tu artifact feature va row count trong metrics JSON.
- CV/TVS duoc yeu cau xac nhan bang file tuning metrics rieng (neu co).

## 5.8. Kiem tra nhat quan

- File doi soat: `reports/report_metrics_consistency.json`.
- Mismatch = 0 la dieu kien de bao cao hop le voi artifact hien hanh.