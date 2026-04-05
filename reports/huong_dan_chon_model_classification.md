# Hướng dẫn đọc biểu đồ & chọn model Classification

## 1. Tại sao biểu đồ Weighted Metrics các model giống nhau?

Dữ liệu bị **class imbalance** (mất cân bằng lớp):
- **Class 0** (không mua lại): chiếm ~78% dữ liệu
- **Class 1** (mua lại): chỉ chiếm ~22% dữ liệu

Khi dùng **weighted metrics**, kết quả bị class 0 chi phối:
- `weighted_recall ≈ accuracy` vì class 0 quá đông
- Mọi model đều đoán gần như toàn bộ là class 0 → accuracy cao (~78%) nhưng thực tế không phân biệt được khách hàng mua lại

**Kết luận:** Weighted metrics KHÔNG phù hợp để so sánh model trong bài toán class imbalance. Cần dùng **per-class metrics** (metrics riêng cho từng class).

---

## 2. Biểu đồ Class 1 (Minority Class) — Cách đọc

Biểu đồ này tính **riêng cho class 1** (khách hàng mua lại) từ confusion matrix:

| Metric | Ý nghĩa | Công thức |
|--------|---------|-----------|
| **Precision (class 1)** | Trong số những người model dự đoán "sẽ mua lại", bao nhiêu % thực sự mua lại? | TP / (TP + FP) |
| **Recall (class 1)** | Trong số những người thực sự mua lại, model phát hiện được bao nhiêu %? | TP / (TP + FN) |
| **F1 (class 1)** | Trung bình điều hòa của Precision và Recall — cân bằng giữa 2 metric | 2 × P × R / (P + R) |

Trong đó:
- **TP** (True Positive): Model đoán "mua lại" → đúng, khách thực sự mua lại
- **FP** (False Positive): Model đoán "mua lại" → sai, khách không mua lại
- **FN** (False Negative): Model đoán "không mua lại" → sai, khách thực sự có mua lại

---

## 3. Kết quả TRƯỚC khi xử lý class imbalance

Khi chưa xử lý mất cân bằng dữ liệu, kết quả class 1 như sau:

| Model | Precision (class 1) | Recall (class 1) | F1 (class 1) |
|-------|---------------------|-------------------|---------------|
| LogisticRegression | ~0.81 | ~0.41 | ~0.55 |
| LinearSVC | ~0.78 | ~0.42 | ~0.55 |
| GBTClassifier | ~0.75 | ~0.43 | ~0.55 |
| NaiveBayes | ~0.72 | ~0.47 | ~0.57 |
| RandomForestClassifier | ~0.81 | ~0.31 | ~0.45 |

**Vấn đề:** Tất cả model đều có Recall class 1 dưới 50% — bỏ sót hơn nửa số khách mua lại.

---

## 4. Giải pháp: Oversampling class 1

### Oversampling là gì?
Khi dữ liệu bị mất cân bằng, model học thiên về class đa số (class 0) vì gặp nhiều mẫu class 0 hơn trong quá trình training. **Oversampling** là kỹ thuật **nhân bản (duplicate) các mẫu của class thiểu số (class 1)** lên cho đến khi số lượng cân bằng với class đa số.

### Cách thực hiện
```
Trước oversampling:  class_0 = 15,000 mẫu | class_1 = 4,200 mẫu  (tỉ lệ 3.6:1)
Sau oversampling:    class_0 = 15,000 mẫu | class_1 ≈ 15,000 mẫu (tỉ lệ 1:1)
```

- Class 1 được **sample lại với replacement** (lấy ngẫu nhiên có trả lại) với tỉ lệ = count_class0 / count_class1
- Chỉ áp dụng trên **tập training** — tập validation và test giữ nguyên để đánh giá khách quan
- `seed=42` để kết quả tái tạo được

### Tại sao oversampling cải thiện kết quả?
- Model được "thấy" số lượng class 1 ngang bằng class 0 khi training
- Model không còn thiên vị đoán class 0 nữa
- **Recall class 1 tăng** vì model học được pattern của class 1 tốt hơn
- **Precision có thể giảm nhẹ** vì model "mạnh dạn" đoán class 1 hơn → một số FP tăng
- **F1 thường tăng** vì Recall tăng nhiều hơn Precision giảm

---

## 5. Kết quả SAU khi áp dụng Oversampling

*(Chạy lại các notebook 04_01 → 04_06 rồi chạy 04_15 để cập nhật bảng này)*

| Model | Precision (class 1) | Recall (class 1) | F1 (class 1) | So với trước |
|-------|---------------------|-------------------|---------------|--------------|
| LogisticRegression | | | | |
| LinearSVC | | | | |
| GBTClassifier | | | | |
| NaiveBayes | | | | |
| RandomForestClassifier | | | | |
| DecisionTree | | | | |

> Sau khi chạy xong, điền kết quả vào bảng trên để so sánh trước/sau.

---

## 6. Phân tích từng model (trước oversampling)

### LogisticRegression
- Precision cao nhất (~0.81): ít bị nhầm khi đoán "mua lại"
- Recall trung bình (~0.41): bỏ sót ~59% khách mua lại
- Phù hợp khi **chi phí gửi nhầm khuyến mãi cao** (cần chính xác)

### LinearSVC
- Tương tự LogisticRegression, cân bằng hơn một chút
- F1 ngang nhau (~0.55)

### GBTClassifier
- Precision thấp hơn (~0.75) nhưng Recall cao hơn (~0.43)
- F1 ngang (~0.55)

### NaiveBayes
- **Recall cao nhất** (~0.47): phát hiện được nhiều khách mua lại nhất
- **F1 cao nhất** (~0.57): cân bằng tốt nhất
- Precision thấp hơn (~0.72) nhưng chấp nhận được

### RandomForestClassifier
- Precision cao (~0.81) nhưng **Recall thấp nhất** (~0.31)
- Bỏ sót ~69% khách mua lại
- F1 thấp nhất (~0.45)

---

## 7. Model nào tốt nhất?

Tùy thuộc vào **mục tiêu kinh doanh**:

### Ưu tiên Recall (tìm càng nhiều khách mua lại càng tốt):
> Chọn model có **Recall class 1 cao nhất**
> Chấp nhận Precision thấp hơn (gửi khuyến mãi cho một số người không mua lại cũng OK)

### Ưu tiên Precision (chính xác khi đoán "sẽ mua lại"):
> Chọn model có **Precision class 1 cao nhất**
> Phù hợp khi chi phí khuyến mãi cao, không muốn lãng phí

### Cân bằng cả hai:
> Chọn model có **F1 class 1 cao nhất**
> F1 là trung bình điều hòa, cân bằng giữa Precision và Recall

---

## 8. Tổng kết quy trình cải thiện

```
Bước 1: Train model bình thường
         → Phát hiện: Weighted metrics giống nhau, Recall class 1 < 50%

Bước 2: Phân tích nguyên nhân
         → Class imbalance: class 0 chiếm 78%, class 1 chỉ 22%

Bước 3: Áp dụng Oversampling class 1
         → Cân bằng dữ liệu training (class 0 ≈ class 1)

Bước 4: Train lại & đánh giá
         → So sánh per-class metrics (đặc biệt Recall & F1 class 1)

Bước 5: Chọn model tốt nhất dựa trên mục tiêu kinh doanh
```
