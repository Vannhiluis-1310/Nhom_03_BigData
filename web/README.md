# Olist Streamlit Multipage App

## 1) Giới thiệu hệ thống

Ứng dụng này là lớp demo vận hành cho đồ án Olist, chạy trên **Streamlit + PySpark** và kết nối trực tiếp với:

- Dữ liệu parquet đã tiền xử lý.
- Model MLlib đã train (classification, regression, clustering, recommendation, pattern mining).
- Báo cáo metrics/metadata trong `reports/`.

App gồm 7 trang nghiệp vụ + 1 trang Home:

- `Home.py`
- `pages/1_Dashboard.py`
- `pages/2_Clustering.py`
- `pages/3_Recommendation.py`
- `pages/4_Prediction.py`
- `pages/5_Pattern_Mining.py`
- `pages/6_EDA_Trends.py`
- `pages/7_Admin.py`

## 2) Project structure

```text
web/
├── Home.py
├── README.md
├── config/
│   ├── feature_schemas.yaml
│   └── model_registry.yaml
├── pages/
│   ├── 1_Dashboard.py
│   ├── 2_Clustering.py
│   ├── 3_Recommendation.py
│   ├── 4_Prediction.py
│   ├── 5_Pattern_Mining.py
│   ├── 6_EDA_Trends.py
│   └── 7_Admin.py
├── services/
│   ├── admin_service.py
│   ├── clustering_service.py
│   ├── dashboard_service.py
│   ├── eda_service.py
│   ├── pattern_mining_service.py
│   ├── prediction_service.py
│   └── recommendation_service.py
└── utils/
    ├── components.py
    ├── config.py
    ├── data_loader.py
    ├── feature_builder.py
    ├── formatters.py
    ├── logger.py
    ├── model_loader.py
    ├── spark_helper.py
    └── style.py
```

## 3) Cách cài đặt

Từ thư mục root project:

```bash
pip install -r requirements.txt
```

## 4) Cách chạy Streamlit app

```bash
streamlit run web/Home.py
```

## 5) Cách đặt data/model artifacts

App dùng mapping tập trung tại `web/config/model_registry.yaml`.

- Nếu artifact đúng chuẩn mong muốn, giữ nguyên mapping.
- Nếu artifact thực tế khác vị trí/tên thư mục, chỉ cần sửa lại đường dẫn tương ứng trong file registry.

Hiện tại registry đã map sẵn với cấu trúc thực tế của repo này, ví dụ:

- `dashboard_base -> data/processed/silver/master_orders`
- `rfm_customer_features -> data/processed/gold/clustering_base`
- `association_rules -> reports/association_rules/association_rules`
- `frequent_itemsets -> reports/association_rules/frequent_itemsets`

## 6) Cách retrain từ Admin

Trong trang `7_Admin.py`:

1. (Tuỳ chọn) upload dữ liệu mới vào `data/raw`.
2. Bấm **Chạy preprocessing pipeline** để rebuild dữ liệu processed và các tập `train/val/test`.
3. Chọn model family và bấm **Retrain model** để chạy notebook retrain theo family.
4. Theo dõi log real-time ngay trên UI.

Các notebook retrain/preprocessing được lấy từ `model_registry.yaml` (mục `scripts`).

Lưu ý:

- Bước upload chỉ lưu file vào `data/raw`, chưa tự tạo `train/val/test`.
- Bước retrain luôn dùng các tập split hiện có trong `data/processed`.
- Nếu thay raw data hoặc upload file đặc biệt, nên chạy preprocessing trước rồi mới retrain để metrics `train/val/test` phản ánh đúng dữ liệu mới.

## 7) Các giả định quan trọng

1. Spark có thể chạy local (`master=local[2]`).
2. Artifacts model là Spark ML saved format (có thư mục `metadata/`).
3. Một số model/target trong UI có thể chưa có artifact thực tế, app sẽ báo thân thiện thay vì crash.
4. Classification hiện đang dùng target mặc định từ pipeline/model (thực tế thường là `is_low_review`).
5. Với Pattern Mining, thay threshold thấp hơn ngưỡng đã train có thể thiếu rule; app sẽ cảnh báo cần retrain.

## 8) Các giới hạn hiện tại

- Chưa có service API riêng (toàn bộ xử lý trong Streamlit + Spark local).
- Feature importance hiển thị tốt nhất cho tree-based model; với vector text lớn có thể hiển thị tên feature ở mức tổng quát.
- Item-based recommendation dùng fallback cosine/co-purchase khi không có item model riêng.
- Một số metadata nâng cao (training date/version chính xác) phụ thuộc vào file JSON bổ sung; nếu thiếu sẽ fallback sang Spark artifact metadata.
