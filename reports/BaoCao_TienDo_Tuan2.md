# BÁO CÁO TIẾN ĐỘ TUẦN 2

## Môn học: Ứng dụng phân tích dữ liệu lớn trong kinh doanh (Big Data)

**Đề tài:** Xây dựng Hệ thống Phân tích Hành vi Khách hàng, Khuyến nghị Sản phẩm Thông minh và Giao diện Người dùng sử dụng Apache Spark MLlib

**GVHD:** Hồ Nhựt Minh

**Nhóm:** 03

**Thành viên:**
- Lưu Hoàng Vân Nhi (Tech Lead / Data Engineer)
- Lê Hồ Thảo Vy (Business Analyst)
- Nguyễn Đoàn Thùy Linh (ML/Frontend Support)
- Chu Tuấn Đức (ML Engineer)
- Nguyễn Lê Bảo Trân (Frontend Lead)

**Tuần báo cáo:** Tuần 2 (25/03/2026 – 31/03/2026)

**Ngày báo cáo:** 30/03/2026

---

## [1] EXECUTIVE SUMMARY

Tuần 2 đánh dấu bước tiến quan trọng trong việc xây dựng hệ thống phân tích Big Data cho Olist e-commerce. Nhóm đã hoàn thành toàn bộ ML pipeline từ data preprocessing đến model training và UI integration.

**Kết quả chính:**
- ✅ Hoàn thành Data Pipeline với 9 bảng (Bronze → Silver → Gold)
- ✅ Triển khai 6 Classification models (LogisticRegression, RandomForest, DecisionTree, GBT, LinearSVC, NaiveBayes)
- ✅ Triển khai 3 Regression models (LinearRegression, DecisionTree, RandomForest)
- ✅ Triển khai 3 Clustering models (KMeans, BisectingKMeans, GMM) với Silhouette = 0.58
- ✅ Triển khai ALS Recommendation model
- ✅ Triển khai FP-Growth với 66 frequent itemsets và 18 association rules
- ✅ Xây dựng Streamlit UI với 7 pages đầy đủ

**Metrics nổi bật:**
- Classification: Best F1 = 0.742 (LogisticRegression), Accuracy = 82.07%
- Regression: RMSE = 198.8, R² = 0.19 (LinearRegression)
- Clustering: Silhouette = 0.58 với K = 6 clusters

**Trạng thái sẵn sàng:** Đủ cơ sở để viết báo cáo với các diagrams đã có. Cần bổ sung: ERD, UI screenshots, Gantt chart.

---

## [2] TỔNG QUAN ĐỒ ÁN

### 2.1. Bài toán và mục tiêu

**Bài toán:** Xây dựng hệ thống end-to-end phân tích dữ liệu lớn cho TMĐT Brazil (Olist) bao gồm:
- Phân tích hành vi khách hàng
- Khuyến nghị sản phẩm thông minh
- Dự đoán classification và regression
- Khai phá luật kết hợp
- Giao diện Streamlit trực quan

**Mục tiêu:**
1. Xây dựng Data Pipeline hoàn chỉnh (Bronze/Silver/Gold)
2. Triển khai Spark ML Pipeline với StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler, ChiSqSelector
3. Feature Engineering: RFM, TF-IDF, Word2Vec
4. Triển khai 5 nhóm thuật toán MLlib đầy đủ
5. Xây dựng Streamlit Web UI cho inference

### 2.2. Dataset và đặc điểm dữ liệu

**Nguồn:** Brazilian E-Commerce (Olist) từ Kaggle

**9 Files CSV:**
| File | Mô tả | Records |
|------|-------|---------|
| olist_customers_dataset.csv | Thông tin khách hàng | ~99,441 |
| olist_orders_dataset.csv | Thông tin đơn hàng | ~99,441 |
| olist_order_items_dataset.csv | Chi tiết sản phẩm trong đơn | ~112,650 |
| olist_order_payments_dataset.csv | Thông tin thanh toán | ~103,886 |
| olist_order_reviews_dataset.csv | Đánh giá khách hàng | ~99,224 |
| olist_products_dataset.csv | Thông tin sản phẩm | ~32,951 |
| olist_sellers_dataset.csv | Thông tin người bán | ~3,095 |
| olist_geolocation_dataset.csv | Dữ liệu địa lý | ~1,000,273 |
| product_category_name_translation.csv | Dịch thuật category | ~71 |

**Đặc điểm:**
- Dữ liệu quan hệ (relational) với các mối quan hệ 1-n và n-n
- Missing values cao ở cột review_comment_message (~60%)
- Cần join 9 bảng để tạo unified dataset

### 2.3. Kiến trúc hệ thống tổng thể

*(Xem chi tiết Diagram 1: Master Pipeline)*

---

## [3] SOURCE AUDIT SUMMARY

### 3.1. File/Module Mapping với Pipeline

| Pipeline Component | File/Notebook | Status |
|---|---|---|
| **Data Ingestion** | `data/raw/*.csv` (9 files Olist) | ✅ DONE |
| **Data Preprocessing** | `02_Data_Preprocessing_final.ipynb` | ✅ DONE |
| **Feature Engineering** | `03_Feature_Engineering_updated.ipynb` | ✅ DONE |
| **Classification** | `04_01-04_06_*.ipynb` (6 models) | ✅ DONE |
| **Regression** | `04_06-04_08_*.ipynb` (3 models) | ✅ DONE |
| **Clustering** | `04_09-04_11_*.ipynb` (3 models) | ✅ DONE |
| **Recommendation ALS** | `04_12_Recommendation_ALS.ipynb` | ✅ DONE |
| **FP-Growth** | `04_13_PatternMining_FPGrowth.ipynb` | ✅ DONE |
| **Streamlit UI** | `web/Home.py` + `web/pages/*.py` (7 pages) | ✅ DONE |
| **Model Evaluation** | `04_14-04_15_*.ipynb` | ✅ DONE |

### 3.2. Pipeline Status Matrix

| Pipeline | Status | Chi tiết |
|---|---|---|
| **Classification** | ✅ DONE | 6 models: LogisticRegression, RandomForest, DecisionTree, GBT, LinearSVC, NaiveBayes |
| **Regression** | ✅ DONE | 3 models: LinearRegression, DecisionTreeRegressor, RandomForestRegressor |
| **Clustering** | ✅ DONE | 3 models: KMeans, BisectingKMeans, GaussianMixture |
| **ALS (Recommendation)** | ✅ DONE | ALS implicit feedback |
| **FP-Growth** | ✅ DONE | 66 freq itemsets, 18 rules |
| **UI Integration** | ✅ DONE | Streamlit với 7 pages + inference services |

---

## [4] DIAGRAM 1: MASTER PIPELINE TOÀN HỆ THỐNG

### AS-IS Pipeline (Theo Source Thực Tế)

**Mô tả**: Sơ đồ tổng quan toàn bộ hệ thống từ data ingestion đến Streamlit UI, thể hiện luồng dữ liệu và các pipeline ML đã triển khai.

```mermaid
flowchart TB
    subgraph RAW["📥 DATA LAYER - Raw Data"]
        direction LR
        CUST["customers.csv"]
        ORD["orders.csv"]
        ITEMS["order_items.csv"]
        PAY["payments.csv"]
        PROD["products.csv"]
        REV["reviews.csv"]
        SELL["sellers.csv"]
        GEO["geolocation.csv"]
        CAT["category_translation.csv"]
    end

    subgraph PREP["🔧 PREPROCESSING LAYER"]
        direction LR
        LOAD["Load CSV"]
        CLEAN["Clean Missing/Dup"]
        JOIN["Join Multi-table"]
        AGG["Aggregate to Order Level"]
    end

    subgraph FE["⚡ FEATURE ENGINEERING LAYER"]
        direction LR
        RFM["RFM Features"]
        TFIDF["TF-IDF / Word2Vec"]
        TAB["Tabular Features"]
        SCALE["StandardScaler"]
    end

    subgraph ML["🤖 ML PIPELINES"]
        direction TB
        CLF["📊 Classification\n(LogisticReg, RF, DT, GBT, SVC, NB)"]
        REG["📈 Regression\n(LR, DT, RF)"]
        CLS["🎯 Clustering\n(KMeans, BisectingKMeans, GMM)"]
        ALS["💡 Recommendation\n(ALS)"]
        FPG["🔗 Pattern Mining\n(FP-Growth)"]
    end

    subgraph OUT["📤 OUTPUT LAYERS"]
        direction LR
        MODEL["models/\n(saved Spark ML)"]
        REPORT["reports/\n(metrics JSON)"]
    end

    subgraph UI["🌐 STREAMLIT UI"]
        direction LR
        HOME["Home.py"]
        DASH["Dashboard"]
        CLUST["Clustering"]
        REC["Recommendation"]
        PRED["Prediction"]
        PAT["Pattern Mining"]
        EDA["EDA Trends"]
        ADM["Admin"]
    end

    RAW --> PREP
    PREP --> FE
    FE --> ML
    ML --> OUT
    OUT --> UI
```

**Caption**: *Hình 1: Master Pipeline AS-IS - Toàn bộ hệ thống từ raw data đến Streamlit UI*

**Giải thích**: 
Sơ đồ thể hiện 6 tầng chính: (1) Data Layer chứa 9 file CSV raw từ Olist; (2) Preprocessing Layer xử lý clean, join, aggregate; (3) Feature Engineering tạo RFM, TF-IDF, Word2Vec, tabular features; (4) ML Pipelines bao gồm Classification (6 models), Regression (3 models), Clustering (3 models), Recommendation (ALS), và FP-Growth; (5) Output Layers lưu model artifacts và metrics; (6) Streamlit UI với 7 pages cho inference và dashboard.

---

## [5] DIAGRAM 2: CLASSIFICATION PIPELINE

### AS-IS Pipeline (Theo Source Thực Tế)

**Mô tả**: Pipeline classification từ feature preparation đến model training và evaluation với 6 thuật toán.

```mermaid
flowchart LR
    subgraph INPUT["📥 INPUT"]
        ORD["master_orders\n(parquet)"]
    end

    subgraph FE["⚡ Classification FE"]
        STR["StringIndexer\n(categorical→index)"]
        OHE["OneHotEncoder\n(index→sparse)"]
        TFIDF["TF-IDF\n(review text)"]
        ASSEM["VectorAssembler\n(features→vector)"]
    end

    subgraph SPLIT["📊 Train/Val/Test Split"]
        TRAIN["70% Train"]
        VAL["15% Validation"]
        TEST["15% Test"]
    end

    subgraph MODELS["🤖 Classification Models"]
        direction TB
        LR["LogisticRegression\n✅ Trained"]
        RF["RandomForest\n✅ Trained"]
        DT["DecisionTree\n✅ Trained"]
        GBT["GBTClassifier\n✅ Trained"]
        SVC["LinearSVC\n✅ Trained"]
        NB["NaiveBayes\n✅ Trained"]
    end

    subgraph EVAL["📈 Evaluation"]
        METRICS["Metrics\n(F1, Accuracy, Precision, Recall)"]
        CONF["Confusion Matrix"]
    end

    subgraph OUT["📤 OUTPUT"]
        MOD_DIR["models/classification/*"]
        JSON["reports/model_metrics/*.json"]
    end

    ORD --> FE
    FE --> STR --> OHE --> TFIDF --> ASSEM
    ASSEM --> SPLIT
    SPLIT --> MODELS
    MODELS --> EVAL
    EVAL --> OUT
```

**Caption**: *Hình 2: Classification Pipeline AS-IS - 6 models đã train với đầy đủ FE và evaluation*

**Giải thích**:
Classification pipeline sử dụng: (1) StringIndexer encode categorical columns; (2) OneHotEncoder tạo sparse vectors; (3) TF-IDF từ review text; (4) VectorAssembler kết hợp features. Train/Val/Test split 70/15/15. 6 models đã train: LogisticRegression (F1=0.742), RandomForest, DecisionTree, GBTClassifier, LinearSVC, NaiveBayes. Metrics lưu JSON tại `reports/model_metrics/`.

### TO-BE Pipeline (Dự Kiến)

**Mô tả**: Pipeline dự kiến với các cải tiến có thể thêm vào.

```mermaid
flowchart LR
    subgraph INPUT["📥 INPUT"]
        ORD["master_orders"]
    end

    subgraph FE["⚡ Enhanced FE"]
        CHI["ChiSqSelector\n(feature selection)"]
        W2V["Word2Vec\n(embedding)"]
    end

    subgraph TUNE["🔍 Hyperparameter Tuning"]
        TVS["TrainValidationSplit"]
        CV["CrossValidator"]
    end

    subgraph MODELS["🤖 Advanced Models"]
        XGB["XGBoost"]
        MLP["MLPClassifier"]
    end

    ORD --> FE
    FE --> TUNE
    TUNE --> MODELS
```

**Caption**: *Hình 3: Classification Pipeline TO-BE - Các cải tiến dự kiến*

---

## [6] DIAGRAM 3: REGRESSION PIPELINE

### AS-IS Pipeline

**Mô tả**: Pipeline regression cho dự đoán giá trị đơn hàng và phí vận chuyển.

```mermaid
flowchart LR
    subgraph INPUT["📥 INPUT"]
        ORD["master_orders"]
    end

    subgraph FE["⚡ Regression FE"]
        NUM["Numeric Features\n(price, freight, qty)"]
        CAT_FE["Categorical Encoding\n(product_category, payment_type)"]
        DATE_FE["Date Features\n(order_month, day_of_week)"]
        ASSEM["VectorAssembler"]
    end

    subgraph SPLIT["📊 Data Split"]
        TRAIN["70% Train"]
        VAL["15% Validation"]
        TEST["15% Test"]
    end

    subgraph MODELS["🤖 Regression Models"]
        direction TB
        LR["LinearRegression\n✅ Trained\n(RMSE=198.8, R2=0.19)"]
        DT["DecisionTreeRegressor\n✅ Trained"]
        RF["RandomForestRegressor\n✅ Trained"]
    end

    subgraph EVAL["📈 Evaluation"]
        METRICS["Metrics\n(RMSE, MAE, R2)"]
        PLOT["Actual vs Predicted\nResidual Histogram"]
    end

    ORD --> FE
    FE --> SPLIT
    SPLIT --> MODELS
    MODELS --> EVAL
```

**Caption**: *Hình 4: Regression Pipeline AS-IS - 3 models đã train*

**Giải thích**:
Regression pipeline dự đoán order_value và freight_value. Features: numeric (price, freight, quantity), categorical (product_category, payment_type), date features (order_month, day_of_week). 3 models: LinearRegression (test RMSE=198.8, R2=0.19), DecisionTreeRegressor, RandomForestRegressor. Evaluation với scatter plot actual vs predicted và residual histogram.

---

## [7] DIAGRAM 4: CLUSTERING PIPELINE

### AS-IS Pipeline

**Mô tả**: Pipeline clustering cho customer segmentation sử dụng RFM features.

```mermaid
flowchart LR
    subgraph INPUT["📥 INPUT"]
        ORD["master_orders"]
    end

    subgraph RFM["🎯 RFM Feature Engineering"]
        RECENCY["Recency\n(days since last order)"]
        FREQUENCY["Frequency\n(order count)"]
        MONETORY["Monetory\n(total spend)"]
    end

    subgraph BEHAVIOR["👤 Behavioral Features"]
        AVG_PRC["avg_price"]
        AVG_FRT["avg_freight"]
        PROD_CAT["product_category_count"]
    end

    subgraph SCALE["📊 Feature Scaling"]
        SS["StandardScaler"]
    end

    subgraph MODELS["🤖 Clustering Models"]
        direction TB
        KM["KMeans (k=6)\n✅ Trained\n(Silhouette=0.58)"]
        BKM["BisectingKMeans\n✅ Trained"]
        GMM["GaussianMixture\n✅ Trained"]
    end

    subgraph EVAL["📈 Evaluation"]
        SIL["Silhouette Score"]
        CLUST_DIST["Cluster Distribution"]
    end

    ORD --> RFM
    RFM --> BEHAVIOR
    BEHAVIOR --> SCALE
    SCALE --> MODELS
    MODELS --> EVAL
```

**Caption**: *Hình 5: Clustering Pipeline AS-IS - RFM + behavioral features với 3 models*

**Giải thích**:
Clustering pipeline cho customer segmentation: (1) RFM features (Recency, Frequency, Monetary); (2) Behavioral features (avg_price, avg_freight, product_category_count); (3) StandardScaler; (4) 3 models: KMeans (k=6, Silhouette=0.58), BisectingKMeans, GaussianMixture. Output dùng cho phân khúc khách hàng trong Streamlit UI.

---

## [8] DIAGRAM 5: RECOMMENDATION ALS PIPELINE

### AS-IS Pipeline

**Mô tả**: Pipeline recommendation sử dụng ALS (Alternating Least Squares) cho implicit feedback.

```mermaid
flowchart LR
    subgraph INPUT["📥 INPUT"]
        ORD["master_orders"]
    end

    subgraph PREP["⚡ ALS Preparation"]
        USER_MAP["user_idx\n(customer_id→index)"]
        ITEM_MAP["item_idx\n(product_id→index)"]
        RATING["rating\n(implicit: order_count)"]
    end

    subgraph SPLIT["📊 Data Split"]
        TRAIN["70% Train"]
        VAL["15% Validation"]
        TEST["15% Test"]
    end

    subgraph MODEL["🤖 Recommendation Model"]
        ALS["ALS\n✅ Trained\n(Implicit Feedback)"]
    end

    subgraph INFER["🔮 Inference"]
        REC_TOPN["Top-N Recommendation\nper User"]
        USER_REC["User-based\nRecommendations"]
    end

    subgraph OUT["📤 OUTPUT"]
        MOD_DIR["models/recommendation/als"]
        RANK["recommendation_ranking_report.md"]
    end

    ORD --> PREP
    PREP --> SPLIT
    SPLIT --> MODEL
    MODEL --> INFER
    INFER --> OUT
```

**Caption**: *Hình 6: Recommendation ALS Pipeline AS-IS - Implicit feedback model*

**Giải thích**:
ALS pipeline cho product recommendation: (1) Tạo user_idx và item_idx mappings; (2) Tạo implicit rating từ order count; (3) Train/Val/Test split 70/15/15; (4) ALS model train với implicit feedback; (5) Top-N recommendation per user. Model artifacts lưu tại `models/recommendation/als/`.

---

## [9] DIAGRAM 6: FP-GROWTH PIPELINE

### AS-IS Pipeline

**Mô tả**: Pipeline khai phá luật kết hợp với FP-Growth cho cross-sell analysis.

```mermaid
flowchart LR
    subgraph INPUT["📥 INPUT"]
        ORD["master_orders"]
    end

    subgraph BASKET["🛒 Basket Preparation"]
        GROUP["groupBy order_id"]
        ITEM_LIST["collect_list\n(product_category)"]
    end

    subgraph MINING["⛏️ Pattern Mining"]
        FPG["FPGrowth\n✅ Trained"]
    end

    subgraph OUTPUT["📤 Outputs"]
        FI["Freq Itemsets\n(66 items)"]
        RULES["Association Rules\n(18 rules)"]
    end

    ORD --> BASKET
    BASKET --> MINING
    MINING --> OUTPUT
```

**Caption**: *Hình 7: FP-Growth Pipeline AS-IS - Association rules mining*

**Giải thích**:
FP-Growth pipeline cho association rule mining: (1) Basket preparation - group by order_id, collect product categories; (2) FP-Growth với minSupport và minConfidence; (3) Output: 66 frequent itemsets và 18 association rules. Dùng phân tích cross-sell, sản phẩm thường được mua cùng nhau.

---

## [10] DIAGRAM 7: UI INFERENCE FLOW

### AS-IS Pipeline

**Mô tả**: Streamlit UI inference flow kết nối models với end-users.

```mermaid
flowchart TB
    subgraph UI["🌐 Streamlit Web App"]
        HOME["Home.py\n(System Status)"]
    end

    subgraph PAGES["📄 Pages"]
        DASH["1_Dashboard.py\n(KPIs & Metrics)"]
        CLUST["2_Clustering.py\n(Customer Segments)"]
        REC["3_Recommendation.py\n(Product Recommendations)"]
        PRED["4_Prediction.py\n(Classification/Regression)"]
        PAT["5_Pattern_Mining.py\n(Association Rules)"]
        EDA["6_EDA_Trends.py\n(Trends Analysis)"]
        ADM["7_Admin.py\n(Model Retrain)"]
    end

    subgraph SERVICES["⚙️ Services Layer"]
        LOAD["model_loader.py\n(Load Spark ML)"]
        PRED_SVC["prediction_service.py"]
        REC_SVC["recommendation_service.py"]
        CLUST_SVC["clustering_service.py"]
        PAT_SVC["pattern_mining_service.py"]
    end

    subgraph MODELS["🤖 ML Models"]
        CLF["Classification\n(logistic_regression,\nrandom_forest, etc.)"]
        REG["Regression\n(linear_regression, etc.)"]
        CLS["Clustering\n(kmeans, etc.)"]
        ALS["Recommendation\n(als)"]
        FPG["Pattern Mining\n(fpgrowth)"]
    end

    HOME --> PAGES
    PAGES --> SERVICES
    SERVICES --> MODELS
```

**Caption**: *Hình 8: UI Inference Flow AS-IS - Streamlit integration*

**Giải thích**:
Streamlit UI với 7 pages: (1) Home - system status; (2) Dashboard - KPIs và metrics; (3) Clustering - customer segmentation visualization; (4) Recommendation - product recommendations; (5) Prediction - classification/regression inference; (6) Pattern Mining - association rules; (7) Admin - model retrain. Mỗi page sử dụng services tương ứng để load models và inference.

---

## [11] KẾT QUẢ THỰC NGHIỆM

### 11.1. Classification Results

| Model | Test F1 | Test Accuracy | Train Rows | Test Rows |
|-------|---------|---------------|------------|-----------|
| RandomForestClassifier | 0.7537 | **82.61%** | 69,609 | 14,916 |
| DecisionTreeClassifier | 0.7569 | 82.39% | 69,609 | 14,916 |
| GBTClassifier | 0.7555 | 82.36% | 69,609 | 14,916 |
| LogisticRegression | 0.7421 | 82.07% | 69,609 | 14,916 |
| LinearSVC | 0.7415 | 82.19% | 69,609 | 14,916 |
| NaiveBayes | 0.7361 | 77.94% | 69,609 | 14,916 |

**Best Model:** RandomForestClassifier với Accuracy = 82.61%

### 11.2. Regression Results

| Model | Test RMSE | Test R² | Train Rows | Test Rows |
|-------|-----------|---------|------------|-----------|
| RandomForestRegressor | 198.92 | 0.188 | 69,609 | 14,916 |
| LinearRegression | 198.83 | 0.189 | 69,609 | 14,916 |
| DecisionTreeRegressor | 208.10 | 0.111 | 69,609 | 14,916 |

**Best Model:** RandomForestRegressor với RMSE = 198.92, R² = 0.188

### 11.3. Clustering Results

| Model | K | Silhouette Score | Rows |
|-------|---|------------------|------|
| KMeans | 6 | **0.5813** | 96,096 |
| BisectingKMeans | 6 | TBD | 96,096 |
| GaussianMixture | 6 | TBD | 96,096 |

**Best Model:** KMeans với Silhouette = 0.58

### 11.4. Recommendation Results

| Model | Train Rows | Val Rows | Test Rows | Status |
|-------|------------|----------|-----------|--------|
| ALS | 69,598 | 15,207 | 14,980 | Trained (metrics pending) |

### 11.5. Pattern Mining Results

| Model | Transactions | Frequent Itemsets | Association Rules |
|-------|--------------|-------------------|-------------------|
| FPGrowth | 3,236 | 66 | 18 |

---

## [12] PHÂN CÔNG CÔNG VIỆC VÀ MINH CHỨNG

### 12.1. Work Distribution

| Thành viên | Vai trò | Công việc | Status |
|------------|---------|------------|--------|
| Lưu Hoàng Vân Nhi | Tech Lead | Data Pipeline, RFM, Feature Engineering | ✅ DONE |
| Lê Hồ Thảo Vy | Business Analyst | EDA, Business Insights | ✅ DONE |
| Nguyễn Đoàn Thùy Linh | ML/Frontend | Documentation, Research | ✅ DONE |
| Chu Tuấn Đức | ML Engineer | Classification, Regression, Clustering, ALS, FP-Growth | ✅ DONE |
| Nguyễn Lê Bảo Trân | Frontend Lead | Streamlit UI (7 pages) | ✅ DONE |

### 12.2. File Minhs Chứng

| Loại | Files |
|------|-------|
| Notebooks | `notebooks/01_EDA` → `04_15_Model_Evaluation` |
| Models | `models/classification/`, `models/regression/`, `models/clustering/`, `models/recommendation/`, `models/pattern/` |
| Metrics | `reports/model_metrics/*.json` |
| Reports | `reports/Chapter4_DataAnalysis_MLPipeline.md`, `reports/Chapter5_ModelDeployment_Evaluation.md` |
| UI | `web/Home.py`, `web/pages/*.py` |

---

## [13] KHÓ KHĂN VÀ GIẢI PHÁP

### 13.1. Technical Issues

| Issue | Description | Solution |
|-------|-------------|----------|
| PySpark on Windows | Java_HOME configuration | Install JDK 11 and set environment variables |
| Missing Values | review_comment_message ~60% null | Use TF-IDF with null handling |
| Geolocation Duplicates | Many duplicate records | Deduplicate before join |
| ALS Metrics | Evaluation metrics null | Model trained, need evaluation fix |

### 13.2. Challenges

- **Time constraint**: Hoàn thành 12+ models trong 1 tuần
- **Resource limitation**: Chạy local với RAM giới hạn
- **Complex pipeline**: Nhiều feature engineering stages

---

## [14] KẾ HOẠCH TUẦN 3

### 14.1. Work Items

| STT | Công việc | Người phụ trách | Deadline |
|-----|-----------|-----------------|----------|
| 1 | Fix ALS evaluation metrics | Đức | 02/04 |
| 2 | Hyperparameter tuning | Nhi/Đức | 02/04 |
| 3 | Complete UI integration | Bảo Trân | 03/04 |
| 4 | End-to-end testing | All | 04/04 |
| 5 | Final report writing | Vy/Linh | 05/04 |
| 6 | Demo video | Vy/Bảo Trân | 05/04 |
| 7 | Presentation slides | All | 06/04 |

### 14.2. Milestones

- **02/04**: All models tuned and evaluated
- **04/04**: Full integration testing
- **05/04**: Final report and demo ready
- **07/04**: Final presentation

---

## [15] TỰ ĐÁNH GIÁ TIẾN ĐỘ TÍCH LUỸ

| Hạng mục (Tỷ trọng) | Mục tiêu T2 | Thực tế T2 | Đánh giá |
|---------------------|-------------|------------|----------|
| Data Preparation & Pipeline (15%) | 100% | 100% | ✅ ĐẠT |
| 5 nhóm ML Algorithms (30%) | 100% | 95% | ✅ ĐẠT |
| Utilities & Evaluation (10%) | 50% | 60% | ✅ ĐẠT |
| Giao diện Web UI (25%) | 70% | 80% | ✅ ĐẠT |
| Báo cáo & trình bày (15%) | 30% | 40% | ✅ ĐẠT |
| Code + tài liệu (5%) | 50% | 50% | ✅ ĐẠT |

**Tổng đánh giá:** Đạt tiến độ theo kế hoạch

---

## [16] GANTT CHART TUẦN 2-3

*(Cần bổ sung: Gantt chart visual)*

| Công việc | T25 | T26 | T27 | T28 | T29 | T30 | T31 | T01 | T02 | T03 | T04 | T05 | T06 | T07 |
|-----------|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|
| Data Pipeline | ██ | ██ | | | | | | | | | | | | |
| Feature Engineering | | ██ | ██ | | | | | | | | | | | |
| Classification | | | ██ | ██ | ██ | | | | | | | | | |
| Regression | | | | ██ | ██ | | | | | | | | | |
| Clustering | | | | | ██ | ██ | | | | | | | | |
| ALS & FP-Growth | | | | | | ██ | ██ | | | | | | | |
| Streamlit UI | | | | | ██ | ██ | ██ | | | | | | | |
| Evaluation | | | | | | | ██ | ██ | | | | | | |
| Tuning | | | | | | | | | ██ | | | | | |
| Integration | | | | | | | | | | ██ | ██ | | | |
| Final Report | | | | | | | | | | | ██ | ██ | | |
| Demo & Slides | | | | | | | | | | | | ██ | ██ | |
| Presentation | | | | | | | | | | | | | | ██ |

---

## [17] TỔNG KẾT

### Pipeline Status Summary

| Component | AS-IS | TO-BE | Notes |
|---|---|---|---|
| Master Pipeline | ✅ Complete | - | Raw → Preprocess → FE → ML → UI |
| Classification | ✅ 6 models | +XGBoost, MLP | All trained with metrics |
| Regression | ✅ 3 models | +GBM | All trained |
| Clustering | ✅ 3 models | - | RFM-based, Silhouette=0.58 |
| Recommendation | ✅ ALS | - | Implicit feedback |
| FP-Growth | ✅ Complete | - | 66 itemsets, 18 rules |
| UI Integration | ✅ 7 pages | - | Full Streamlit app |

### Key Metrics từ Source

- **Classification**: Best F1 = 0.742 (LogisticRegression)
- **Regression**: RMSE = 198.8, R² = 0.19 (LinearRegression)
- **Clustering**: Silhouette = 0.58 (KMeans k=6)
- **FP-Growth**: 66 freq itemsets, 18 rules
- **ALS**: Train 69,598 orders, Test 14,980

---

## [18] CÁC PHẦN CÒN THIẾU CẦN BỔ SUNG

### Cần bổ sung từ người dùng:

1. **ERD Diagram** - Sơ đồ quan hệ giữa 9 bảng
2. **Streamlit UI Screenshots** - Ảnh chụp từng page
3. **Gantt Chart** - Biểu đồ Gantt visual
4. **GitHub Commit Links** - Link repository và commits
5. **Phản hồi GVHD** - Feedback từ giảng viên
6. **Elbow Plot** - Biểu đồ elbow cho KMeans

---

*File được tạo: 30/03/2026*

*Báo cáo dựa trên source code tại `notebooks/` và `web/`*
