# Luồng dữ liệu và pipeline

## Script trung tâm

Luồng preprocessing chính được điều phối bởi:

- `src/nhom03_pyspark_project/data/run_data_pipeline.py`

Script này gọi lần lượt các bước:

1. ingest raw
2. build silver
3. build gold
4. split gold thành `train/val/test`
5. build features

## Chi tiết từng bước

### 1. Ingest raw

Mục đích:

- đọc dữ liệu từ `data/raw`
- chuẩn bị dữ liệu đầu vào để đưa vào các bước xử lý tiếp theo

### 2. Build silver

Mục đích:

- làm sạch dữ liệu
- chuẩn hóa schema
- tạo các bảng trung gian ổn định hơn để join

### 3. Build gold

Gold là dataset theo từng bài toán. Hiện project có các gold base chính:

- `classification_base`
- `regression_base`
- `clustering_base`
- `als_base`
- `fpgrowth_base`

### 4. Split gold

Sau khi build gold, project tách dữ liệu cho từng bài toán thành:

- `*_train`
- `*_val`
- `*_test`

Ví dụ:

- `classification_base_train`, `classification_base_val`, `classification_base_test`
- `regression_base_train`, `regression_base_val`, `regression_base_test`
- `clustering_base_train`, `clustering_base_val`, `clustering_base_test`

## Feature artifacts

Thư mục `data/processed/features` chứa:

- dữ liệu feature-ready
- pipeline ML đã fit cho một số bài toán

Ví dụ:

- `classification_fe_pipeline`
- `regression_fe_pipeline`
- `clustering_fe_pipeline`
- `clustering_fe`

## Hành vi thực tế trong Admin

Đây là chỗ dễ hiểu nhầm nhất:

- Upload file ở Admin chỉ lưu file vào `data/raw`
- Upload không tự tạo `train/val/test`
- Chỉ khi chạy preprocessing pipeline thì các tập split mới được rebuild
- Retrain model luôn dùng các tập split hiện có trong `data/processed`

## Trường hợp upload file đặc biệt

Nếu người dùng upload một file raw mới hoặc một file có tính chất đặc biệt:

1. lưu file vào `data/raw`
2. chạy preprocessing pipeline
3. kiểm tra lại `data/processed/gold` và `data/processed/features`
4. sau đó mới retrain

Nếu bỏ qua bước preprocessing mà retrain ngay, model sẽ tiếp tục dùng split cũ.
