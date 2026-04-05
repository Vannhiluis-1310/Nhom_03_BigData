# CHUONG 4: PHAN TICH DU LIEU VA PIPELINE HOC MAY

Ban cap nhat tu dong luc 2026-04-07 23:37:11.

## 4.1. Tong quan pipeline

- Data pipeline: notebooks 02 -> 03 -> 04_00x.
- Feature pipeline anti-leakage: split truoc fit, train-only fit cho supervised pipeline.
- Bao cao nay dong bo voi metrics JSON hien hanh.

## 4.2. Bang ket qua tong hop hien hanh

| model_family   | model_name             |     val_f1 |         f1 |   val_accuracy |   accuracy |   val_rmse |      rmse |     val_r2 |         r2 |   train_rows |   val_rows |   test_rows | _file                                             |
|:---------------|:-----------------------|-----------:|-----------:|---------------:|-----------:|-----------:|----------:|-----------:|-----------:|-------------:|-----------:|------------:|:--------------------------------------------------|
| classification | DecisionTreeClassifier |   0.825833 |   0.83601  |       0.829177 |   0.838026 |  nan       | nan       | nan        | nan        |       105187 |      14916 |       14916 | classification_decision_tree.json                 |
| classification | GBTClassifier          |   0.849413 |   0.859795 |       0.854586 |   0.863703 |  nan       | nan       | nan        | nan        |       105187 |      14916 |       14916 | classification_gbt_classifier.json                |
| classification | GBTClassifier          | nan        | nan        |     nan        | nan        |  nan       | nan       | nan        | nan        |          nan |        nan |         nan | tuning_classification_gbt_classifier_cv.json      |
| classification | GBTClassifier_CV       |   0.667472 | nan        |       0.768184 | nan        |  nan       | nan       | nan        | nan        |        68887 |      14546 |       14734 | classification_gbt_classifier_cv.json             |
| classification | LinearSVC              |   0.856916 |   0.86346  |       0.865111 |   0.871279 |  nan       | nan       | nan        | nan        |       105187 |      14916 |       14916 | classification_linear_svc.json                    |
| classification | LogisticRegression     |   0.85723  |   0.863846 |       0.862899 |   0.868597 |  nan       | nan       | nan        | nan        |       105187 |      14916 |       14916 | classification_logistic_regression.json           |
| classification | LogisticRegression     | nan        | nan        |     nan        | nan        |  nan       | nan       | nan        | nan        |          nan |        nan |         nan | tuning_classification_logistic_regression.json    |
| classification | LogisticRegression     | nan        | nan        |     nan        | nan        |  nan       | nan       | nan        | nan        |          nan |        nan |         nan | tuning_classification_logistic_regression_cv.json |
| classification | LogisticRegression_CV  |   0.667472 | nan        |       0.768184 | nan        |  nan       | nan       | nan        | nan        |        68887 |      14546 |       14734 | classification_logistic_regression_cv.json        |
| classification | NaiveBayes             |   0.855097 |   0.860787 |       0.866519 |   0.871346 |  nan       | nan       | nan        | nan        |       105187 |      14916 |       14916 | classification_naive_bayes.json                   |
| classification | RandomForest           | nan        | nan        |     nan        | nan        |  nan       | nan       | nan        | nan        |          nan |        nan |         nan | tuning_classification_random_forest_cv.json       |
| classification | RandomForestClassifier |   0.831845 |   0.841803 |       0.832328 |   0.84111  |  nan       | nan       | nan        | nan        |       105187 |      14916 |       14916 | classification_random_forest.json                 |
| classification | RandomForest_CV        |   0.667472 | nan        |       0.768184 | nan        |  nan       | nan       | nan        | nan        |        68887 |      14546 |       14734 | classification_random_forest_cv.json              |
| clustering     | BisectingKMeans        | nan        | nan        |     nan        | nan        |  nan       | nan       | nan        | nan        |        65592 |      13799 |       13967 | clustering_bisecting_kmeans.json                  |
| clustering     | GaussianMixture        | nan        | nan        |     nan        | nan        |  nan       | nan       | nan        | nan        |        65592 |      13799 |       13967 | clustering_gaussian_mixture.json                  |
| clustering     | KMeans                 | nan        | nan        |     nan        | nan        |  nan       | nan       | nan        | nan        |        65592 |      13799 |       13967 | clustering_kmeans.json                            |
| pattern_mining | FPGrowth               | nan        | nan        |     nan        | nan        |  nan       | nan       | nan        | nan        |          nan |        nan |         nan | pattern_fpgrowth.json                             |
| recommendation | ALS                    | nan        | nan        |     nan        | nan        |    1.19965 |   1.22068 | nan        | nan        |        71277 |      15368 |       15342 | recommendation_als.json                           |
| regression     | DecisionTreeRegressor  | nan        | nan        |     nan        | nan        |  194.385   | 201.314   |   0.163296 |   0.157665 |        69294 |      14611 |       14761 | regression_decision_tree.json                     |
| regression     | LinearRegression       | nan        | nan        |     nan        | nan        |  195.514   | 202.177   |   0.153545 |   0.150427 |        69294 |      14611 |       14761 | regression_linear_regression.json                 |
| regression     | LinearRegression       | nan        | nan        |     nan        | nan        |  nan       | nan       | nan        | nan        |          nan |        nan |         nan | tuning_regression_linear_regression.json          |
| regression     | LinearRegression       | nan        | nan        |     nan        | nan        |  nan       | nan       | nan        | nan        |          nan |        nan |         nan | tuning_regression_linear_regression_cv.json       |
| regression     | LinearRegression_CV    | nan        | nan        |     nan        | nan        |  195.518   | nan       |   0.153509 | nan        |        69294 |      14611 |       14761 | regression_linear_regression_cv.json              |
| regression     | RandomForestRegressor  | nan        | nan        |     nan        | nan        |  194.908   | 201.479   |   0.158781 |   0.156285 |        69294 |      14611 |       14761 | regression_random_forest.json                     |
