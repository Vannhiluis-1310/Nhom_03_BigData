# Tài liệu ứng dụng Streamlit

## Điểm vào

App chạy từ:

```bash
streamlit run web/Home.py
```

## Cấu trúc app

### Trang chính

- `web/Home.py`

### Các trang nghiệp vụ

- `web/pages/1_Dashboard.py`
- `web/pages/2_Clustering.py`
- `web/pages/3_Recommendation.py`
- `web/pages/4_Prediction.py`
- `web/pages/5_Pattern_Mining.py`
- `web/pages/6_EDA_Trends.py`
- `web/pages/7_Admin.py`

## Registry và config

App đọc dataset/model/pipeline/report thông qua:

- `web/config/model_registry.yaml`

Ý nghĩa:

- không hard-code path rải rác trong từng page
- khi artifact đổi vị trí, có thể sửa ở một chỗ

## Vai trò của một số dataset key

### `dashboard_base`

- dùng cho dashboard vận hành

### `classification_inference_base`

- hiện dùng để lấy sample/default values cho form prediction classification
- không phải dataset train model

### `regression_inference_base`

- hiện dùng để lấy sample/default values cho form prediction regression
- không phải dataset train model

### `rfm_customer_features`

- dùng cho clustering page
- trong repo hiện tại, app đã hỗ trợ hiểu đây là logical dataset ghép từ các split clustering khi dataset full không tồn tại

## Ghi chú theo trang

### Dashboard

- đọc `dashboard_base`
- hiển thị KPI, revenue trend, payment, category, order status, địa lý

### Clustering

- dùng clustering model đã train
- UI hiện đã khóa `k = 6` để khớp với artifact hiện có và tránh lỗi mismatch

### Prediction

- chạy inference qua feature pipeline + model Spark ML
- default/input options được lấy từ dataset inference base

### Admin

Trang này có 3 hành động chính:

1. lưu file upload vào `data/raw`
2. chạy preprocessing pipeline
3. retrain model theo family

Điểm quan trọng:

- upload không tự sinh `train/val/test`
- retrain không tự split lại dữ liệu
- nếu muốn metrics mới phản ánh file raw vừa upload, phải chạy preprocessing trước

## Gợi ý vận hành an toàn

1. upload raw data
2. chạy preprocessing
3. kiểm tra `System Status`
4. retrain model cần dùng
5. tạo báo cáo metrics nếu cần
