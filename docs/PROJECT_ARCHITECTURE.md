# Kiến trúc project

## Mục tiêu

Project này phục vụ đồ án Big Data trên bộ dữ liệu Olist, với mục tiêu:

- xử lý dữ liệu bằng PySpark
- xây dựng các bài toán ML khác nhau trên cùng một hệ dữ liệu
- cung cấp một app Streamlit để demo vận hành và trình bày kết quả

## Các lớp chính trong project

### 1. Lớp dữ liệu

- `data/raw`: nơi chứa file CSV hoặc parquet đầu vào
- `data/bronze`: dữ liệu sau bước ingest
- `data/processed/silver`: dữ liệu đã làm sạch, chuẩn hóa, join theo bảng nghiệp vụ
- `data/processed/gold`: dataset theo từng bài toán
- `data/processed/features`: feature artifacts và pipeline phục vụ model

### 2. Lớp huấn luyện và đánh giá

- `notebooks/`: notebook train/evaluate theo từng bài toán
- `models/`: nơi lưu Spark model artifacts
- `reports/`: nơi lưu metrics JSON, báo cáo tổng hợp và các output khai thác mẫu

### 3. Lớp ứng dụng

- `web/`: Streamlit multipage app
- `web/services/`: nghiệp vụ đọc dữ liệu/model và xử lý cho từng trang
- `web/utils/`: config, loader, logger, spark helper, UI helpers
- `web/config/model_registry.yaml`: registry tập trung cho path dataset/model/report/pipeline

## Các nhóm bài toán hiện có

- Dashboard
- Classification
- Regression
- Clustering
- Recommendation
- Pattern mining
- EDA

## Kiến trúc vận hành hiện tại

Project đang chạy theo mô hình local:

- Spark chạy local
- Streamlit đọc dữ liệu từ file hệ thống
- Không có API backend riêng
- Không có object storage hoặc model registry ngoài filesystem

Điều này giúp project dễ demo, nhưng đổi lại:

- state phụ thuộc vào file local hiện có
- cần đảm bảo đã chạy preprocessing đúng trước khi retrain hoặc demo
- metrics và UI phụ thuộc vào artifact thực tế trên máy
