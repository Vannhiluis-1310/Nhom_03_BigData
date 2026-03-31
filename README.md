# Olist BigData Streamlit Demo

## 1. Giới thiệu hệ thống

Đây là hệ thống demo end-to-end cho đồ án Olist, xây dựng bằng **Streamlit multipage + PySpark**.
Ứng dụng kết nối trực tiếp tới dữ liệu parquet đã xử lý và model Spark MLlib đã train, phục vụ:

- Dashboard vận hành
- Phân khúc khách hàng (clustering)
- Gợi ý sản phẩm (ALS)
- Dự đoán classification/regression
- Khai phá luật kết hợp (FP-Growth output)
- EDA & xu hướng
- Admin quản trị pipeline/model

## 2. Project structure

```text
Nhom03_PySpark_ProjectCuoiKy/
├── data/
│   ├── raw/
│   └── processed/
├── models/
├── notebooks/
├── reports/
├── src/
├── web/
│   ├── Home.py
│   ├── config/
│   ├── pages/
│   ├── services/
│   ├── utils/
│   └── README.md
└── requirements.txt
```

## 3. Cách cài đặt

```bash
pip install -r requirements.txt
```

## 4. Cách chạy Streamlit app

```bash
streamlit run web/Home.py
```

## 5. Cách đặt data/model artifacts

Registry tập trung tại `web/config/model_registry.yaml`.

- App ưu tiên đọc artifact theo mapping trong file này.
- Khi đường dẫn thực tế khác chuẩn mong muốn, chỉ cần sửa registry.
- Không hard-code rải rác trong page.

Ví dụ mapping đã có sẵn:

- `dashboard_base -> data/processed/silver/master_orders`
- `classification_inference_base -> data/processed/gold/classification_base`
- `association_rules -> reports/association_rules/association_rules`

## 6. Cách retrain từ Admin

Trên trang `7_Admin`:

1. Upload dữ liệu mới vào `data/raw`.
2. Chạy preprocessing pipeline.
3. Chọn model family và chạy retrain (thông qua notebook tương ứng).
4. Theo dõi progress/log trực tiếp trên UI.

## 7. Các giả định quan trọng

1. Spark chạy local được (`local[2]`).
2. Model artifacts là Spark saved model (có thư mục `metadata/`).
3. Một số model có thể chưa tồn tại trong repo hiện tại; app sẽ báo lỗi thân thiện.
4. Metadata/metrics ưu tiên lấy từ `reports/model_metrics/*.json` và spark metadata.
5. Feature schema inference ưu tiên từ `feature_schemas.yaml` và pipeline metadata.

## 8. Các giới hạn hiện tại

- Một số target UI chưa có model riêng (ví dụ repeat customer, delivery_days) nên dùng fallback.
- Pattern mining lọc threshold thấp hơn ngưỡng train có thể thiếu rule, cần retrain để đầy đủ.
- Feature importance cho model nhiều chiều (text hashing) chỉ mang tính tham khảo.
