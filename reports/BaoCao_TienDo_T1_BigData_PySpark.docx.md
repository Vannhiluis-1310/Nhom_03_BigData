

**TRƯỜNG ĐẠI HỌC CÔNG NGHỆ KỸ THUẬT TP.HCM**

**BÁO CÁO TIẾN ĐỘ TUẦN 1**

Môn học: Ứng dụng phân tích dữ liệu lớn trong kinh doanh (Big Data)

**Đề tài:**

*Xây dựng Hệ thống Phân tích Hành vi Khách hàng, Khuyến nghị Sản phẩm Thông minh và Giao diện Người dùng sử dụng Apache Spark MLlib*

**GVHD: Hồ Nhựt Minh**

**Hạn nộp: 07/04/2026**

| Tên đồ án: | Xây dựng Hệ thống Phân tích Hành vi Khách hàng, Khuyến nghị Sản phẩm Thông minh và Giao diện Người dùng sử dụng Apache Spark MLlib |
| :---- | :---- |
| **Nhóm:** | *\[Số nhóm\]* |
| **Thành viên:** | *\[Họ tên 1 – MSSV1\]* *\[Họ tên 2 – MSSV2\]* *\[Họ tên 3 – MSSV3\]* |
| **GVHD:** | Hồ Nhựt Minh |
| **Môn học:** | Ứng dụng phân tích dữ liệu lớn trong kinh doanh (Big Data) |
| **Công nghệ:** | Python 3.x, PySpark 3.x (MLlib), Jupyter Notebook / Databricks Community, Streamlit, Plotly / Seaborn, Git \+ GitHub |
| **Tuần báo cáo:** | Tuần 1 (18/03/2026 – 24/03/2026) |
| **Hạn nộp:** | 07/04/2026 |

# **1\. Tổng quan**

## **1.1. Mục tiêu đồ án**

*\[Xây dựng hệ thống end-to-end: pipeline xử lý dữ liệu (StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler (Spark Pipeline)), triển khai đầy đủ 5 nhóm thuật toán (Logistic Regression…), kèm giao diện Streamlit. Hạn nộp: 07/04/2026.\]*

## **1.2. Bộ dữ liệu**

*\[Brazilian E-Commerce (Olist): 9 file CSV, \~100.000 đơn hàng từ TMĐT Brazil. Bảng chính: orders, order\_items, customers, products, order\_reviews, sellers, payments, geolocation, category\_translation. Link: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce\]*

## **1.3. Công cụ và môi trường**

*\[Python 3.x, PySpark 3.x, Java JDK 11, Jupyter Notebook / Databricks Community, Streamlit, Plotly, Git \+ GitHub. Ghi rõ phiên bản.\]*

# **2\. Công việc đã thực hiện trong Tuần 1**

## **2.1. Khảo sát và tìm hiểu dữ liệu**

*\[Mô tả: cấu trúc 9 file CSV, quan hệ giữa các bảng (ERD), thống kê sơ bộ bằng Spark DataFrame (.printSchema(), .describe(), .count()). Tỷ lệ null cột review\_comment\_message (\~60%), geolocation trùng lặp. Đính kèm bảng/hình.\]*

## **2.2. Cài đặt môi trường phát triển**

*\[Cài Java JDK, PySpark, cấu hình JAVA\_HOME / SPARK\_HOME, Jupyter PySpark kernel, Streamlit. Vấn đề gặp phải và cách xử lý.\]*

## **2.3. Thiết kế kiến trúc hệ thống**

*\[Raw CSV → Spark DataFrame → Spark ML Pipeline (tiền xử lý \+ FE) → 5 nhóm MLlib model → Trained models (save/load) → Streamlit Web UI. Kèm hình vẽ.\]*

## **2.4. Tiền xử lý dữ liệu (bước đầu)**

*\[Join bằng Spark DataFrame (.join()), xử lý null (.dropna() / .fillna()), ép kiểu (.withColumn() \+ cast), tạo cột dẫn xuất. Kèm code snippet.\]*

# **3\. Phân công công việc Tuần 1**

| STT | Thành viên | Công việc được giao | Trạng thái | Ghi chú |
| ----- | ----- | ----- | ----- | ----- |
| 1 | *\[Họ tên 1\]* | Tìm hiểu dataset Olist, khảo sát cấu trúc 9 file CSV, EDA sơ bộ | Hoàn thành |  |
| 2 | *\[Họ tên 2\]* | Cài đặt môi trường: PySpark, Java JDK 11, Spark 3.x, Jupyter, Streamlit | Hoàn thành |  |
| 3 | *\[Họ tên 3\]* | Nghiên cứu tài liệu Big Data – PySpark, lập kế hoạch thuật toán | Hoàn thành |  |
| 4 | *\[Họ tên 1\]* | Join/Merge các bảng dữ liệu, xử lý missing values | Đang thực hiện | \~70% |
| 5 | *\[Họ tên 2\]* | Thiết kế wireframe giao diện Streamlit | Đang thực hiện | \~50% |
| 6 | *\[Họ tên 3\]* | Thiết kế kiến trúc hệ thống tổng thể | Đang thực hiện | \~60% |

# **4\. Khó khăn và giải pháp**

* Cài đặt PySpark trên Windows gặp lỗi JAVA\_HOME: giải quyết bằng cách cài JDK 11 (64-bit) và thiết lập biến môi trường.

* Cột review\_comment\_message có \~60% null: cần quyết định chiến lược xử lý trước khi chạy TF-IDF.

* Spark DataFrame không hỗ trợ trực tiếp một số thao tác EDA như Pandas: phải chuyển đổi toPandas() cho visualization.

* Bảng geolocation chứa nhiều bản ghi trùng lặp: cần deduplicate trước khi join.

* *\[Bổ sung thêm khó khăn thực tế của nhóm.\]*

# **5\. Kế hoạch Tuần 2 (25/03 – 31/03/2026)**

| STT | Công việc dự kiến | Người phụ trách | Deadline | Ưu tiên |
| ----- | ----- | ----- | ----- | ----- |
| 1 | Hoàn thành Spark Pipeline: Indexer, OHE, Assembler, Scaler, ChiSq | *\[Họ tên 1\]* | 28/03 | Cao |
| 2 | Feature Engineering: RFM \+ TF-IDF (pyspark.ml.feature) | *\[Họ tên 1\]* | 29/03 | Cao |
| 3 | Classification: LogReg, RF, NB, SVC, GBT (MLlib) \+ so sánh | *\[Họ tên 3\]* | 31/03 | Cao |
| 4 | Regression: Linear, DT, RF Regressor (MLlib) \+ đánh giá | *\[Họ tên 3\]* | 31/03 | Cao |
| 5 | Clustering: K-Means, Bisecting K-Means, GMM trên RFM | *\[Họ tên 1\]* | 30/03 | Cao |
| 6 | Dashboard \+ trang phân khúc KH trên Streamlit | *\[Họ tên 2\]* | 31/03 | Trung bình |
| 7 | CrossValidator \+ thống kê mô tả \+ Chi-square test | *\[Họ tên 3\]* | 31/03 | Trung bình |

**Mục tiêu cuối tuần 2:**

* Hoàn thành pipeline tiền xử lý \+ feature engineering đầy đủ (RFM, TF-IDF (pyspark.ml.feature)).

* Chạy xong Classification (Logistic Regression, Random Forest, Naive Bayes, LinearSVC, GBTClassifier) \+ Regression \+ Clustering với kết quả đánh giá ban đầu.

* Dashboard Streamlit hiển thị thống kê mô tả và biểu đồ clustering.

# **6\. Tự đánh giá tiến độ**

| Hạng mục (tỷ trọng) | Mục tiêu T1 | Thực tế T1 | Đúng tiến độ? | Ghi chú |
| ----- | ----- | ----- | ----- | ----- |
| Data Preparation & Pipeline (15%) | 30% | 25% | Gần đạt | Đang join \+ EDA |
| 5 nhóm ML Algorithms (30%) | 5% | 5% | Đạt | Nghiên cứu lý thuyết |
| Utilities & Evaluation (10%) | 0% | 0% | Đạt | Tuần 2–3 |
| Giao diện Web UI (25%) | 15% | 10% | Chậm nhẹ | Mới wireframe |
| Báo cáo & trình bày (15%) | 10% | 10% | Đạt | Đang viết |
| Code \+ tài liệu (5%) | 10% | 10% | Đạt | GitHub đã tạo |

**Nhận xét chung:**

*\[Nhóm tự đánh giá: tiến độ tổng thể, rủi ro (thời gian gấp 3 tuần, khối lượng lớn), cần hỗ trợ gì từ GVHD.\]*

| Xác nhận của GVHD *(Ký và ghi rõ họ tên)* | Nhóm trưởng *(Ký và ghi rõ họ tên)* |
| :---: | :---: |

**PHỤ LỤC: LƯU ĐỒ GANTT – KẾ HOẠCH TỔNG THỂ ĐỒ ÁN**

Big Data – PySpark  |  18/03/2026 – 07/04/2026 (3 tuần)  |  Hạn nộp: 07/04/2026

| STT | Giai đoạn (Tỷ trọng) | Công việc chi tiết | Phụ trách | Tuần 1 (18–24/3) |  | Tuần 2 (25–31/3) |  | Tuần 3 (01–07/4) |  |
| :---: | ----- | ----- | :---: | ----- | :---- | ----- | :---- | ----- | :---- |
| 1.1 | **Data Preparation** **& Pipeline** **(15%)** | Khảo sát dataset Olist (9 CSV), EDA sơ bộ, ERD | *\[TV1\]* |  |  |  |  |  |  |
| 1.2 |  | Join bảng (Spark DataFrame), xử lý missing values, type casting | *\[TV1\]* |  |  |  |  |  |  |
| 1.3 |  | Feature Engineering: RFM, biến dẫn xuất | *\[TV1\]* |  |  |  |  |  |  |
| 1.4 |  | TF-IDF / Word2Vec (pyspark.ml.feature) trên review text | *\[TV3\]* |  |  |  |  |  |  |
| 1.5 |  | Spark Pipeline: Scaler, Indexer, OHE, Assembler, ChiSqSelector | *\[TV1, TV3\]* |  |  |  |  |  |  |
| 2.1 | **Classification** **(30% chung** **ML Algorithms)** | LogisticRegression \+ NaiveBayes (MLlib) | *\[TV3\]* |  |  |  |  |  |  |
| 2.2 |  | RandomForest \+ GBTClassifier \+ LinearSVC (MLlib) | *\[TV3\]* |  |  |  |  |  |  |
| 2.3 |  | So sánh 5 mô hình, tuning hyperparameter (ParamGridBuilder) | *\[TV3\]* |  |  |  |  |  |  |
| 3.1 | **Regression** | LinearRegression \+ DecisionTreeRegressor \+ RFRegressor (MLlib) | *\[TV3\]* |  |  |  |  |  |  |
| 3.2 |  | Đánh giá RMSE, MAE, R² (RegressionEvaluator) \+ so sánh | *\[TV3\]* |  |  |  |  |  |  |
| 4.1 | **Clustering** | K-Means trên RFM features (MLlib) | *\[TV1\]* |  |  |  |  |  |  |
| 4.2 |  | Bisecting K-Means \+ GMM \+ so sánh Silhouette | *\[TV1\]* |  |  |  |  |  |  |
| 5.1 | **Recommendation** **(ALS)** | Chuẩn bị implicit feedback matrix cho ALS | *\[TV3\]* |  |  |  |  |  |  |
| 5.2 |  | Huấn luyện ALS \+ đánh giá (RankingMetrics) | *\[TV3\]* |  |  |  |  |  |  |
| 6.1 | **FP-Growth** | Xây dựng transaction dataset từ orders | *\[TV1\]* |  |  |  |  |  |  |
| 6.2 |  | FP-Growth \+ Association Rules (pyspark.ml.fpm) | *\[TV1\]* |  |  |  |  |  |  |
| 7.1 | **Utilities &** **Evaluation** **(10%)** | CrossValidator, TrainValidationSplit, thống kê mô tả | *\[TV1, TV3\]* |  |  |  |  |  |  |
| 7.2 |  | Chi-square test \+ tổng hợp metrics \+ so sánh tổng thể | *\[TV1\]* |  |  |  |  |  |  |
| 8.1 | **Giao diện** **Web UI** **(25%)** | Wireframe \+ thiết kế layout Streamlit | *\[TV2\]* |  |  |  |  |  |  |
| 8.2 |  | Dashboard thống kê \+ biểu đồ Clustering (Plotly) | *\[TV2\]* |  |  |  |  |  |  |
| 8.3 |  | Trang phân khúc KH \+ Hệ thống khuyến nghị ALS | *\[TV2\]* |  |  |  |  |  |  |
| 8.4 |  | Trang dự đoán \+ FP-Growth \+ xu hướng mua sắm | *\[TV2\]* |  |  |  |  |  |  |
| 8.5 |  | Trang Admin (upload, retrain) \+ responsive \+ deploy | *\[TV2\]* |  |  |  |  |  |  |
| 9.1 | **Tích hợp,** **Báo cáo &** **Trình bày** **(15% \+ 5%)** | Tích hợp Spark model vào Streamlit, kiểm thử end-to-end | *\[Cả nhóm\]* |  |  |  |  |  |  |
| 9.2 |  | Viết báo cáo Word/PDF (30–40 trang) | *\[TV3\]* |  |  |  |  |  |  |
| 9.3 |  | Quay video demo (3–5 phút) | *\[TV2\]* |  |  |  |  |  |  |
| 9.4 |  | Hoàn thiện GitHub \+ README \+ chuẩn bị trình bày 15 phút | *\[Cả nhóm\]* |  |  |  |  |  |  |

**Chú thích:**

|  | Data Preparation |  | Classification |  | Regression |  | Clustering |  | Recommendation |  | FP-Growth |
| :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- |
|  | Utilities |  | Web UI |  | Tích hợp |  | Báo cáo |  | Đã hoàn thành |  |  |

**Ghi chú:**

*Mỗi ô nửa tuần \~ 3–4 ngày. \[TV1\], \[TV2\], \[TV3\] \= thành viên nhóm — thay bằng tên thật.*

*Ô xanh nhạt (A9D18E) \= đã hoàn thành. Tỷ trọng ghi theo tiêu chí chấm điểm.*

*Tuần 3 (01–07/4): ưu tiên tích hợp, kiểm thử end-to-end, báo cáo, video demo, trình bày.*