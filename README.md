# Nhom03 BigData Olist Project

Hệ thống phân tích dữ liệu và demo vận hành cho bộ dữ liệu Olist, xây dựng bằng `PySpark + Streamlit`.

Project này có 2 phần chính:

- Pipeline xử lý dữ liệu từ `raw -> bronze -> silver -> gold -> features`
- Ứng dụng Streamlit để xem dashboard, clustering, recommendation, prediction, pattern mining, EDA và quản trị pipeline/model

## Tài liệu nhanh

- [Tổng quan tài liệu](docs/README.md)
- [Kiến trúc project](docs/PROJECT_ARCHITECTURE.md)
- [Luồng dữ liệu và pipeline](docs/DATA_PIPELINE.md)
- [Tài liệu ứng dụng Streamlit](docs/STREAMLIT_APP.md)
- [Theo dõi lỗi](docs/ERROR_TRACKER.md)
- [Theo dõi tiến độ](docs/PROGRESS_TRACKER.md)
- [Hướng dẫn UV](docs/HUONG_DAN_UV.md)

## Chức năng chính

- Dashboard vận hành đơn hàng
- Phân khúc khách hàng bằng clustering
- Gợi ý sản phẩm bằng ALS
- Dự đoán classification và regression
- Khai phá luật kết hợp bằng FP-Growth
- EDA và phân tích xu hướng
- Admin để upload raw data, chạy preprocessing, retrain model và theo dõi trạng thái hệ thống

## Cấu trúc thư mục

```text
Nhom03_BigDataCuoiKy_Update_01/
├── data/
│   ├── raw/                  # File đầu vào do người dùng đưa vào
│   ├── bronze/               # Raw đã ingest
│   └── processed/
│       ├── silver/           # Bảng làm sạch, chuẩn hóa
│       ├── gold/             # Dataset cho từng bài toán
│       └── features/         # Feature artifacts và pipeline ML
├── docs/                     # Tài liệu dự án
├── models/                   # Spark ML artifacts
├── notebooks/                # Notebook train/evaluate theo từng bài toán
├── reports/                  # Metrics, báo cáo tổng hợp, output pattern mining
├── src/nhom03_pyspark_project/
│   ├── data/                 # Ingest, silver, gold, split
│   ├── features/             # Build feature pipeline
│   └── spark.py              # Khởi tạo SparkSession
├── web/                      # Streamlit multipage app
└── requirements.txt
```

## Cài đặt

```bash
pip install -r requirements.txt
```

Nếu dùng môi trường ảo, cần kích hoạt môi trường trước khi chạy.

## Cách chạy app

```bash
streamlit run web/Home.py
```

## Luồng chạy chuẩn của project

### 1. Nạp dữ liệu raw

- Đưa file vào `data/raw`
- Có thể làm trực tiếp hoặc dùng trang Admin để upload

### 2. Chạy preprocessing

Lệnh thực tế được gọi từ Admin là script:

```bash
python src/nhom03_pyspark_project/data/run_data_pipeline.py
```

Script này chạy tuần tự:

1. ingest raw vào bronze
2. build silver tables
3. build gold datasets
4. split gold thành `train/val/test`
5. build feature artifacts

Trước khi chạy preprocessing, pipeline sẽ đọc cấu hình mapping raw input tại:

- `config/raw_input_mapping.yaml`

File này cho phép map từng bảng raw sang file thực tế bạn muốn dùng để demo.
Ví dụ:

```yaml
orders: data/raw/test_orders_sample.csv
customers: data/raw/olist_customers_dataset.csv
```

Nếu không khai báo custom mapping, pipeline sẽ fallback về tên file mặc định.

### 3. Retrain model

Sau khi preprocessing xong, trang Admin sẽ gọi notebook/script retrain theo từng family:

- `classification`
- `regression`
- `clustering`
- `recommendation`
- `pattern_mining`

Lưu ý quan trọng:

- Upload file ở Admin chỉ lưu file vào `data/raw`
- Bước upload không tự tạo `train/val/test`
- Nếu file upload có tên lạ, pipeline chỉ dùng nó khi bạn map nó trong `config/raw_input_mapping.yaml`
- Retrain luôn dùng các tập split hiện có trong `data/processed`
- Nếu thay raw data hoặc upload file đặc biệt, cần chạy preprocessing trước rồi mới retrain

## Các dataset/artifact đang được app dùng

Đường dẫn được quản lý tập trung ở:

- `web/config/model_registry.yaml`

App không hard-code path trong từng page; nếu artifact thực tế đổi vị trí, chỉ cần sửa registry.

Ví dụ:

- `dashboard_base -> data/processed/silver/master_orders`
- `classification_inference_base -> data/processed/gold/classification_base_train`
- `regression_inference_base -> data/processed/gold/regression_base_train`
- `rfm_customer_features -> logical dataset của clustering`

Ghi chú về dữ liệu clustering:

- Repo hiện có `clustering_base_train`, `clustering_base_val`, `clustering_base_test`
- App đã được chỉnh để hiểu `rfm_customer_features` như một logical dataset ghép từ các split này khi cần

## Những điểm cần hiểu đúng khi dùng Admin

- `Chạy preprocessing pipeline`: rebuild dữ liệu processed và split
- `Retrain model`: train lại model từ dữ liệu processed hiện có
- `Tạo báo cáo metrics`: tổng hợp lại báo cáo từ artifact và metrics JSON

Vì vậy nếu UI đang hiển thị `train_rows`, `val_rows`, `test_rows`, đó là số liệu lấy từ metrics/report sau khi preprocessing + retrain hoàn tất, không phải do bước upload tự sinh ra.

## Trạng thái hiện tại của project

Project đang được tổ chức theo hướng demo vận hành nội bộ:

- Spark chạy local
- Model là Spark ML saved artifacts
- Streamlit đọc trực tiếp parquet/model từ local filesystem
- Một số chỗ có fallback để chịu được artifact hoặc dữ liệu không hoàn toàn đồng nhất

## Hạn chế hiện tại

- Chưa có API service riêng, toàn bộ đang chạy trong Streamlit + Spark local
- Một số mô hình trong UI trước đây từng được khai báo rộng hơn artifact thực tế; project đã được dọn lại theo artifact hiện có
- Một số logic hiển thị vẫn phụ thuộc vào metrics JSON/report sinh từ notebook
- Với dữ liệu mới hoặc file raw đặc biệt, cần chạy lại preprocessing để đồng bộ hoàn toàn toàn bộ app

## Gợi ý đọc tiếp

- Muốn hiểu tổng thể project: [docs/PROJECT_ARCHITECTURE.md](docs/PROJECT_ARCHITECTURE.md)
- Muốn hiểu data pipeline: [docs/DATA_PIPELINE.md](docs/DATA_PIPELINE.md)
- Muốn hiểu các trang Streamlit: [docs/STREAMLIT_APP.md](docs/STREAMLIT_APP.md)
