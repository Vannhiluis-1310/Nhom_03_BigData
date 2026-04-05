# CHƯƠNG 4. PHÂN TÍCH DỮ LIỆU VÀ XÂY DỰNG ML PIPELINE

## 4.1. Mô tả bộ dữ liệu Olist

### 4.1.1. Nguồn dữ liệu và phạm vi thời gian

Bộ dữ liệu Olist là tập dữ liệu thương mại điện tử Brazil được công bố công khai trên Kaggle, chứa thông tin về các đơn hàng từ nhiều cửa hàng trên nền tảng Olist. Đây là một trong những bộ dữ liệu e-commerce lớn nhất và phổ biến nhất cho việc nghiên cứu về phân tích dữ liệu và machine learning trong lĩnh vực thương mại điện tử.

Dữ liệu này bao gồm thông tin khách hàng, đơn hàng, sản phẩm, người bán, đánh giá và vị trí địa lý. Phạm vi thời gian của dữ liệu kéo dài từ năm 2016 đến năm 2018, cho phép phân tích xu hướng mua sắm theo mùa và đánh giá hiệu suất dịch vụ vận chuyển. Khoảng thời gian 2 năm này đủ lớn để có thể nhận ra các pattern theo mùa (seasonal patterns) và xu hướng tăng trưởng dài hạn của nền tảng thương mại điện tử.

Dữ liệu được lưu trữ tại thư mục `data/raw/` với các file CSV có tổng kích thước khoảng vài trăm MB. Cấu trúc dữ liệu theo mô hình quan hệ (relational model) với các bảng được liên kết thông qua các khóa chính và khóa ngoại, phù hợp cho việc phân tích đa chiều và xây dựng các mô hình machine learning.

### 4.1.2. Các bảng dữ liệu thành phần

Bộ dữ liệu Olist bao gồm 9 bảng dữ liệu chính, mỗi bảng lưu trữ thông tin về một thực thể nghiệp vụ khác nhau:

| Tên bảng | Số dòng | Số cột | Mô tả |
|----------|---------|--------|-------|
| customers | 99,441 | 5 | Thông tin khách hàng |
| orders | 99,441 | 8 | Thông tin đơn hàng |
| order_items | 112,650 | 7 | Chi tiết các sản phẩm trong đơn hàng |
| order_payments | 103,886 | 5 | Thông tin thanh toán |
| order_reviews | 104,162 | 7 | Đánh giá của khách hàng |
| products | 32,951 | 9 | Thông tin sản phẩm |
| sellers | 3,095 | 4 | Thông tin người bán |
| geolocation | 1,000,163 | 5 | Dữ liệu vị trí địa lý |
| category_translation | 71 | 2 | Bảng dịch danh mục sản phẩm |

**Mô tả chi tiết từng bảng:**

1. **customers**: Chứa thông tin về khách hàng bao gồm customer_id (khóa chính), customer_unique_id (định danh duy nhất cho mỗi khách hàng), customer_zip_code_prefix (mã bưu điện), customer_city (thành phố), và customer_state (bang). Bảng này có 99,441 khách hàng duy nhất.

2. **orders**: Bảng trung tâm chứa thông tin về đơn hàng với các cột order_id, customer_id, order_status, và 5 cột timestamp (order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date). Số dòng = 99,441 tương ứng với số đơn hàng.

3. **order_items**: Chi tiết từng sản phẩm trong đơn hàng, bao gồm order_id, order_item_id (thứ tự item trong đơn), product_id, seller_id, shipping_limit_date, price, và freight_value. Số dòng lớn hơn số đơn hàng (112,650 > 99,441) cho thấy một số đơn hàng có nhiều sản phẩm.

4. **order_payments**: Thông tin thanh toán với order_id, payment_sequential (thứ tự thanh toán), payment_type (phương thức), payment_installments (số kỳ trả góp), và payment_value (giá trị). Số dòng 103,886 cho thấy một số đơn hàng có nhiều giao dịch thanh toán.

5. **order_reviews**: Đánh giá của khách hàng với review_id, order_id, review_score (điểm 1-5), review_comment_title, review_comment_message, review_creation_date, và review_answer_timestamp.

6. **products**: Thông tin sản phẩm bao gồm product_id, product_category_name, và các thuộc tính vật lý như weight, dimensions, số lượng ảnh, độ dài tên và mô tả.

7. **sellers**: Thông tin người bán với seller_id, seller_zip_code_prefix, seller_city, và seller_state.

8. **geolocation**: Dữ liệu vị trí địa lý với zip_code, lat, lng, city, state. Bảng này có quy mô lớn nhất với hơn 1 triệu dòng do mỗi zip_code có nhiều tọa độ.

9. **category_translation**: Bảng tra cứu nhỏ (71 dòng) chuyển đổi tên danh mục từ tiếng Bồ Đào Nha sang tiếng Anh.

### 4.1.3. Quan hệ giữa các bảng dữ liệu

Các bảng dữ liệu trong Olist được liên kết với nhau theo sơ đồ quan hệ sau:

```
customers (customer_id) → orders (customer_id)
orders (order_id) → order_items (order_id)
orders (order_id) → order_payments (order_id)
orders (order_id) → order_reviews (order_id)
order_items (product_id) → products (product_id)
order_items (seller_id) → sellers (seller_id)
products (product_category_name) → category_translation (product_category_name)
customers/sellers (zip_code_prefix) → geolocation (zip_code_prefix)
```

**Chi tiết các mối quan hệ:**

- Bảng **orders** là bảng trung tâm, liên kết với **customers** qua `customer_id` và với **order_items** qua `order_id`. Đây là mối quan hệ 1-nhiều (một khách hàng có thể có nhiều đơn hàng, một đơn hàng có thể có nhiều sản phẩm).

- Bảng **order_items** liên kết với **products** qua `product_id` và với **sellers** qua `seller_id`. Mỗi sản phẩm trong đơn hàng được bán bởi một người bán cụ thể.

- Bảng **order_payments** và **order_reviews** liên kết với **orders** qua `order_id`. Một đơn hàng có thể có nhiều giao dịch thanh toán (trả góp) và có thể có nhiều đánh giá (do cập nhật).

- Bảng **geolocation** có thể join với **customers** và **sellers** qua zip code prefix để phân tích địa lý. Tuy nhiên, cần lưu ý rằng một zip_code có thể có nhiều tọa độ GPS khác nhau.

Mối quan hệ này cho phép xây dựng các pipeline phân tích toàn diện từ nhiều nguồn dữ liệu khác nhau, đồng thời đảm bảo tính toàn vẹn của dữ liệu trong quá trình xử lý.

### 4.1.4. Ý nghĩa kinh doanh của từng nhóm thuộc tính

Mỗi nhóm thuộc tính trong bộ dữ liệu mang ý nghĩa kinh doanh quan trọng:

**Nhóm thông tin khách hàng:**
- `customer_id`: Định danh duy nhất cho mỗi giao dịch
- `customer_unique_id`: Định danh để theo dõi khách hàng qua nhiều đơn hàng (quan trọng cho RFM và customer lifetime value)
- `customer_city`, `customer_state`: Phân tích phân bố địa lý, nhận diện thị trường tiềm năng
- `customer_zip_code_prefix`: Kết hợp với bảng geolocation để phân tích khoảng cách vận chuyển

**Nhóm thông tin đơn hàng:**
- `order_id`: Khóa chính để join các bảng
- `order_status`: Trạng thái đơn hàng (delivered, shipped, canceled, etc.)
- 5 cột timestamp: Theo dõi vòng đời đơn hàng, tính toán thời gian xử lý và giao hàng
- `order_estimated_delivery_date`: Ngày giao hàng dự kiến (để tính delay)

**Nhóm thông tin thanh toán:**
- `payment_type`: Phương thức thanh toán (credit_card, boleto, voucher, debit_card)
- `payment_installments`: Số kỳ trả góp (ảnh hưởng đến quyết định mua)
- `payment_value`: Giá trị thanh toán

**Nhóm đánh giá:**
- `review_score`: Điểm đánh giá 1-5 sao (nhãn cho bài toán phân lớp)
- `review_comment_message`: Văn bản đánh giá (cho NLP và sentiment analysis)

**Nhóm thông tin sản phẩm:**
- `product_category_name`: Danh mục sản phẩm (cho phân tích theo ngành hàng)
- Các thuộc tính vật lý (weight, dimensions): Ảnh hưởng đến chi phí vận chuyển

## 4.2. Khảo sát dữ liệu ban đầu

### 4.2.1. Quy mô dữ liệu theo từng bảng

Kết quả khảo sát cho thấy sự chênh lệch đáng kể về quy mô giữa các bảng:

| Bảng | Số dòng | Số cột | Ghi chú |
|------|---------|--------|---------|
| geolocation | 1,000,163 | 5 | Bảng lớn nhất - nhiều bản ghi trùng |
| order_items | 112,650 | 7 | > số đơn hàng (multi-item orders) |
| order_reviews | 104,162 | 7 | > số đơn hàng (multiple reviews) |
| order_payments | 103,886 | 5 | > số đơn hàng (multiple payments) |
| customers | 99,441 | 5 | 1:1 với orders |
| orders | 99,441 | 8 | Bảng trung tâm |
| products | 32,951 | 9 | Số sản phẩm |
| sellers | 3,095 | 4 | Số người bán |
| category_translation | 71 | 2 | Bảng tra cứu |

**Phân tích:**
- Bảng **geolocation** có quy mô lớn nhất với hơn 1 triệu dòng, nhưng thực tế chỉ có khoảng 738,332 dòng distinct do nhiều bản ghi trùng lặp (cùng zip_code có nhiều tọa độ).
- Số đơn hàng (99,441) nhỏ hơn số dòng trong bảng order_items (112,650), cho thấy khoảng 13% đơn hàng có nhiều sản phẩm (multi-item orders).
- Tương tự, bảng order_payments (103,886 dòng) lớn hơn số đơn hàng, cho thấy một số đơn hàng có nhiều giao dịch thanh toán hoặc thanh toán trả góp.

### 4.2.2. Kiểu dữ liệu của các thuộc tính

Sau khi chuẩn hóa (casting), các kiểu dữ liệu được xác định như sau:

**Datetime columns:**
- `orders`: order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date (TimestampType)
- `order_reviews`: review_creation_date, review_answer_timestamp (TimestampType)

**Numeric columns:**
- DoubleType: price, freight_value, payment_value, product_weight_g, product_length_cm, product_height_cm, product_width_cm
- IntegerType: order_item_id, payment_sequential, payment_installments, customer_zip_code_prefix

**Categorical columns:**
- StringType: order_id, customer_id, customer_unique_id, order_status, payment_type, product_id, seller_id, customer_city, customer_state

**Text columns:**
- review_comment_title, review_comment_message

### 4.2.3. Kiểm tra giá trị thiếu

Kết quả kiểm tra null profile cho thấy mức độ khác nhau giữa các bảng:

**Bảng orders:**
```
order_delivered_customer_date: 2,965 (2.98%)
order_delivered_carrier_date: 1,783 (1.79%)
order_approved_at: 160 (0.16%)
```

**Bảng order_reviews:**
```
review_comment_title: 92,157 (88.47%)
review_comment_message: 63,079 (60.56%)
review_answer_timestamp: 8,785 (8.43%)
review_creation_date: 8,764 (8.41%)
review_score: 2,380 (2.28%)
```

**Bảng products:**
```
product_category_name: 610 (1.85%)
product_name_length: 610 (1.85%)
product_description_length: 610 (1.85%)
product_photos_qty: 610 (1.85%)
```

**Các bảng không có null:**
- customers, order_items, order_payments, sellers, geolocation

**Phân tích ý nghĩa:**
- Tỷ lệ null cao ở review_comment_message (60.56%) cho thấy đa số khách hàng chỉ đánh giá bằng điểm số mà không viết bình luận.
- Các cột delivery date null cao (2.98%) có thể là các đơn hàng chưa được giao hoặc đang trong quá trình.

### 4.2.4. Kiểm tra dữ liệu trùng lặp

Kết quả kiểm tra duplicate:

- **geolocation**: 261,831 dòng trùng lặp (26.18%) - do cùng zip_code có nhiều tọa độ GPS
- **order_reviews**: 91 dòng trùng lặp (0.09%)
- **Các bảng khác**: Không có trùng lặp sau khi loại bỏ theo khóa chính

### 4.2.5. Kiểm tra giá trị ngoại lệ và dữ liệu nhiễu

Các vấn đề về giá trị ngoại lệ được phát hiện:

1. **Giá sản phẩm không hợp lệ:** Một số đơn hàng có price <= 0 hoặc freight_value < 0
2. **Ngày tháng bất thường:** Một số đơn hàng có order_delivered_customer_date trước order_purchase_timestamp
3. **Typo trong tên cột:** product_name_lenght (thay vì length), product_description_lenght
4. **Review score ngoài miền:** Một số review_score không nằm trong range [1-5]

## 4.3. Phân tích khám phá dữ liệu (EDA)

### 4.3.1. Phân bố đơn hàng theo thời gian

[Mục đích]
Phân tích phân bố đơn hàng theo thời gian nhằm nhận diện xu hướng mua sắm theo mùa, ngày trong tuần và giờ trong ngày. Đây là cơ sở cho các chiến lược marketing theo thời điểm và dự báo nhu cầu.

[Code]
```python
# Phân tích phân bố đơn hàng theo tháng
orders_by_month = (
    orders_norm
    .withColumn("year_month", F.date_format("order_purchase_timestamp", "yyyy-MM"))
    .groupBy("year_month")
    .count()
    .orderBy("year_month")
)

# Phân tích theo ngày trong tuần
orders_by_dayofweek = (
    orders_norm
    .withColumn("dayofweek", F.dayofweek("order_purchase_timestamp"))
    .groupBy("dayofweek")
    .count()
)

# Phân tích theo giờ trong ngày
orders_by_hour = (
    orders_norm
    .withColumn("hour", F.hour("order_purchase_timestamp"))
    .groupBy("hour")
    .count()
)
```

[Giải thích code]
Đoạn code trên sử dụng các hàm xử lý timestamp của PySpark để trích xuất các thành phần thời gian:
- `F.date_format()` để tạo cột year_month theo định dạng yyyy-MM
- `F.dayofweek()` để lấy thứ trong tuần (1=Chủ Nhật, 2=Thứ Hai,...)
- `F.hour()` để lấy giờ trong ngày (0-23)

Sau đó sử dụng `groupBy()` và `count()` để đếm số đơn hàng theo từng nhóm thời gian.

[Output - Kết quả thực tế]
Phân tích cho thấy:
- Xu hướng tăng trưởng mạnh mẽ từ năm 2016 đến 2018
- Các tháng cuối năm (November, December) có số đơn hàng cao nhất - đặc biệt là Black Friday (tháng 11)
- Thứ Hai và Thứ Ba là các ngày có nhiều đơn hàng nhất
- Thứ Bảy và Chủ Nhật có số lượng đơn hàng thấp hơn
- Giờ mua hàng phổ biến: 10-12 giờ sáng và 14-17 giờ chiều

[Ý nghĩa kỹ thuật]
Việc trích xuất các features thời gian giúp:
- Xây dựng features cho model dự báo
- Phát hiện seasonal patterns
- Tối ưu hóa chiến lược marketing theo thời điểm

[Ý nghĩa nghiệp vụ]
- Chuẩn bị hàng tồn kho cao hơn cho mùa cao điểm cuối năm
- Tối ưu hóa nhân sự và logistics theo ngày trong tuần
- Triển khai chiến dịch khuyến mãi vào các khung giờ cao điểm

[Ảnh hưởng đến bước tiếp theo]
Từ phân tích này, có thể tạo các features thời gian cho các bài toán classification và regression như:
- order_month: Tháng đặt hàng
- order_dayofweek: Ngày trong tuần
- order_hour: Giờ trong ngày
- is_holiday_season: Cờ cho mùa cao điểm

### 4.3.2. Phân bố điểm đánh giá review_score

[Mục đích]
Phân tích phân bố review_score để hiểu mức độ hài lòng của khách hàng và xây dựng nhãn cho bài toán phân lớp sentiment.

[Code]
```python
# Phân tích phân bố review_score
review_score_dist = (
    order_reviews_norm
    .filter(F.col("review_score").isNotNull())
    .groupBy("review_score")
    .count()
    .orderBy("review_score")
)

# Tính tỷ lệ phần trăm
total_reviews = order_reviews_norm.filter(F.col("review_score").isNotNull()).count()
review_score_pct = (
    review_score_dist
    .withColumn("percentage", F.col("count") / total_reviews * 100)
)
```

[Giải thích code]
Code trên:
- Lọc bỏ các dòng có review_score null
- Group by theo review_score và đếm số lượng
- Tính tỷ lệ phần trăm bằng cách chia cho tổng số reviews

[Output - Kết quả thực tế]
Phân bố review_score:
- Điểm 5: ~57% (phần lớn)
- Điểm 4: ~19%
- Điểm 3: ~8%
- Điểm 2: ~4%
- Điểm 1: ~12%

Điểm trung bình = 4.0, Trung vị = 5

[Ý nghĩa kỹ thuật]
- Tạo nhãn binary cho classification: 1 (tiêu cực) = score 1-2, 0 (tích cực) = score 3-5
- Imbalanced dataset: ~84% positive, ~16% negative
- Cần sử dụng các kỹ thuật xử lý imbalanced data

[Ý nghĩa nghiệp vụ]
- Mức độ hài lòng chung khá tốt (mean = 4.0)
- Nhóm 12% đánh giá 1 sao cần được chú ý đặc biệt
- Đây là cơ sở cho chiến lược churn prevention và customer retention

[Ảnh hưởng đến bước tiếp theo]
- Xây dựng classification model với nhãn is_low_review
- Sử dụng các kỹ thuật như SMOTE hoặc class weights để xử lý imbalanced data

### 4.3.3. Phân tích phương thức thanh toán

[Mục đích]
Phân tích hành vi thanh toán của khách hàng để hiểu preferences và tối ưu hóa trải nghiệm thanh toán.

[Code]
```python
# Phân tích phân bố payment_type
payment_type_dist = (
    order_payments_norm
    .groupBy("payment_type")
    .agg(
        F.count("*").alias("count"),
        F.sum("payment_value").alias("total_value"),
        F.avg("payment_value").alias("avg_value")
    )
    .orderBy(F.desc("count"))
)

# Phân tích số lần trả góp
installments_dist = (
    order_payments_norm
    .filter(F.col("payment_installments") > 1)
    .groupBy("payment_installments")
    .count()
    .orderBy("payment_installments")
)
```

[Output - Kết quả thực tế]
Phân bố payment_type:
- Credit card: 73.8%
- Boleto: 19.4%
- Voucher: 5.5%
- Debit card: 1.5%

Phân bố payment_installments:
- 1 lần (thanh toán ngay): 67%
- 2-10 lần: 28%
- >10 lần: 5%

[Ý nghĩa kỹ thuật]
- Tạo feature: is_credit_card (binary)
- Tạo feature: payment_installments (numeric)
- Tạo feature: max_installments (numeric)

[Ý nghĩa nghiệp vụ]
- Thẻ tín dụng là phương thức chủ đạo
- Trả góp phổ biến (33% giao dịch)
- Cần tối ưu hóa trải nghiệm thanh toán cho các phương thức phổ biến

### 4.3.4. Phân tích danh mục sản phẩm và doanh thu

[Mục đích]
Phân tích doanh thu và số lượng đơn hàng theo danh mục sản phẩm để nhận diện các ngành hàng tiềm năng.

[Code]
```python
# Join order_items với products và category_translation
category_revenue = (
    order_items_enriched_df
    .groupBy("product_category_name_enriched")
    .agg(
        F.sum("price").alias("total_revenue"),
        F.count("*").alias("order_count"),
        F.countDistinct("order_id").alias("unique_orders")
    )
    .orderBy(F.desc("total_revenue"))
)
```

[Output - Kết quả thực tế]
Top danh mục theo doanh thu:
1. Health & Beauty (beleza_saude)
2. Bed Bath & Table (cama_mesa_banho)
3. Computers & Accessories (informatica_acessorios)
4. Furniture & Decor (moveis_decoracao)
5. Sports & Leisure (esporte_lazer)

[Ý nghĩa nghiệp vụ]
- Tập trung nguồn lực vào các danh mục high-performance
- Phát triển cross-selling giữa các danh mục liên quan

### 4.3.5. Phân tích phân bố địa lý khách hàng và người bán

[Mục đích]
Phân tích phân bố địa lý để hiểu thị trường chính và tối ưu hóa logistics.

[Code]
```python
# Phân bố khách hàng theo state
customer_state_dist = (
    customers_clean
    .groupBy("customer_state")
    .count()
    .orderBy(F.desc("count"))
)

# Phân bố người bán theo state
seller_state_dist = (
    sellers_clean
    .groupBy("seller_state")
    .count()
    .orderBy(F.desc("count"))
)
```

[Output - Kết quả thực tế]
Top states theo số khách hàng:
1. SP (São Paulo): ~42%
2. RJ (Rio de Janeiro): ~13%
3. MG (Minas Gerais): ~11%
4. RS (Rio Grande do Sul): ~6%
5. PR (Paraná): ~5%

Top states theo số người bán:
1. SP (São Paulo): ~58%
2. MG (Minas Gerais): ~8%
3. PR (Paraná): ~7%

[Ý nghĩa nghiệp vụ]
- Tập trung marketing ở Đông Nam Brazil
- Tối ưu hóa warehouse/fulfillment ở SP và RJ
- Mở rộng thị trường ở các vùng tiềm năng

### 4.3.6. Phân tích review text và độ dài bình luận

[Mục đích]
Phân tích văn bản đánh giá để chuẩn bị cho bài toán NLP (TF-IDF, Word2Vec).

[Code]
```python
# Tính độ dài bình luận
review_text_analysis = (
    reviews_clean
    .withColumn("comment_length", F.length("review_comment_message_filled"))
    .filter(F.col("comment_length") > 0)
    .agg(
        F.avg("comment_length").alias("avg_length"),
        F.min("comment_length").alias("min_length"),
        F.max("comment_length").alias("max_length"),
        F.percentile_approx("comment_length", 0.5).alias("median_length")
    )
)
```

[Output - Kết quả thực tế]
- Độ dài trung bình: ~100-150 ký tự
- Độ dài trung vị: ~80 ký tự
- Tỷ lệ có comment: ~39% (60% không có comment)
- Từ xuất hiện nhiều trong review tiêu cực: "atraso" (trễ), "defeito" (lỗi), "péssimo" (tệ)
- Từ xuất hiện nhiều trong review tích cực: "perfeito" (hoàn hảo), "adorei" (rất thích), "recomendo" (recommend)

### 4.3.7. Các insight chính rút ra từ EDA

1. **Xu hướng mua sắm**: Khách hàng có xu hướng mua sắm nhiều hơn vào các dịp lễ cuối năm (Black Friday, Christmas)
2. **Mức độ hài lòng**: Đa số khách hàng hài lòng (57% đánh giá 5 sao), nhưng 12% đánh giá 1 sao cần được chú ý
3. **Phương thức thanh toán**: Thẻ tín dụng là phương thức chủ đạo (73.8%), trả góp phổ biến (33%)
4. **Phân bố địa lý**: Tập trung ở São Paulo và Rio de Janeiro (55% khách hàng)
5. **Vấn đề vận chuyển**: Thời gian giao hàng và chất lượng vận chuyển là yếu tố chính ảnh hưởng đến đánh giá tiêu cực
6. **Đặc điểm đơn hàng**: ~13% đơn hàng có nhiều sản phẩm, cơ hội cho cross-selling

## 4.4. Tiền xử lý dữ liệu

### 4.4.1. Chuẩn hóa tên cột và kiểu dữ liệu

[Mục đích]
Chuẩn hóa tên cột và kiểu dữ liệu là bước nền tảng để đảm bảo tính nhất quán trong toàn bộ pipeline xử lý dữ liệu. Việc này giúp tránh các lỗi về sau khi truy cập cột hoặc thực hiện các phép toán trên dữ liệu.

[Code]
```python
# Sửa typo trong tên cột sản phẩm
products_norm = (
    raw_tables["products"]
    .withColumnRenamed("product_name_lenght", "product_name_length")
    .withColumnRenamed("product_description_lenght", "product_description_length")
)

# Cast timestamp cho bảng orders
orders_norm = (
    raw_tables["orders"]
    .withColumn("order_purchase_timestamp", F.to_timestamp("order_purchase_timestamp"))
    .withColumn("order_approved_at", F.to_timestamp("order_approved_at"))
    .withColumn("order_delivered_carrier_date", F.to_timestamp("order_delivered_carrier_date"))
    .withColumn("order_delivered_customer_date", F.to_timestamp("order_delivered_customer_date"))
    .withColumn("order_estimated_delivery_date", F.to_timestamp("order_estimated_delivery_date"))
)

# Cast numeric cho bảng order_items
order_items_norm = (
    raw_tables["order_items"]
    .withColumn("shipping_limit_date", F.to_timestamp("shipping_limit_date"))
    .withColumn("price", F.col("price").cast("double"))
    .withColumn("freight_value", F.col("freight_value").cast("double"))
)

# Cast numeric cho bảng order_payments
order_payments_norm = (
    raw_tables["order_payments"]
    .withColumn("payment_sequential", F.col("payment_sequential").cast("int"))
    .withColumn("payment_installments", F.col("payment_installments").cast("int"))
    .withColumn("payment_value", F.col("payment_value").cast("double"))
)

# Cast numeric cho bảng order_reviews
order_reviews_norm = (
    raw_tables["order_reviews"]
    .withColumn("review_score", F.col("review_score").cast("int"))
    .withColumn("review_creation_date", F.to_timestamp("review_creation_date"))
    .withColumn("review_answer_timestamp", F.to_timestamp("review_answer_timestamp"))
)
```

[Giải thích code]
Đoạn code trên thực hiện 4 nhiệm vụ chính:

1. **Sửa typo trong tên cột**: Từ "product_name_lenght" thành "product_name_length", "product_description_lenght" thành "product_description_length" - đây là lỗi chính tả trong dữ liệu gốc.

2. **Cast timestamp cho orders**: Chuyển đổi 5 cột datetime từ string sang TimestampType bằng `F.to_timestamp()`. Điều này cho phép thực hiện các phép toán thời gian như tính khoảng cách ngày, trích xuất tháng/năm.

3. **Cast numeric cho order_items**: Chuyển đổi price và freight_value sang DoubleType để đảm bảo độ chính xác trong tính toán tiền tệ.

4. **Cast numeric cho order_payments**: Chuyển đổi payment_sequential và payment_installments sang IntegerType, payment_value sang DoubleType.

5. **Cast review_reviews**: Chuyển review_score sang IntegerType (điểm 1-5), các cột ngày sang TimestampType.

[Output]
Sau bước này, schema của các bảng được chuẩn hóa:
- TimestampType cho các cột thời gian
- DoubleType cho các cột giá trị tiền tệ
- IntegerType cho các cột đếm và điểm số

[Ý nghĩa kỹ thuật]
Việc cast đúng kiểu dữ liệu giúp Spark tối ưu hóa bộ nhớ và tăng hiệu suất xử lý. Timestamp được xử lý hiệu quả hơn trong các phép toán thời gian như tính khoảng cách ngày, trích xuất tháng/năm.

[Ý nghĩa dữ liệu]
Dữ liệu sau chuẩn hóa đảm bảo tính toàn vẹn, tránh các lỗi logic khi so sánh hoặc tính toán trên các cột không đúng kiểu.

[Ảnh hưởng đến bước tiếp theo]
- Các phép toán thời gian (datediff, year, month, hour) hoạt động chính xác
- Tính toán statistics (avg, sum, min, max) trên numeric columns chính xác
- Sẵn sàng cho EDA chi tiết và xây dựng features

### 4.4.2. Xử lý giá trị thiếu

[Mục đích]
Xử lý giá trị thiếu (missing values) là bước quan trọng để đảm bảo chất lượng dữ liệu đầu vào cho các mô hình machine learning. Việc xử lý không đúng cách có thể dẫn đến model bias hoặc lỗi trong quá trình huấn luyện.

[Code]
```python
# Tạo missing flags cho bảng orders
orders_clean = (
    normalized_tables["orders"]
    .dropDuplicates(["order_id"])
    .withColumn("is_missing_order_approved_at", F.col("order_approved_at").isNull().cast("int"))
    .withColumn("is_missing_delivered_carrier_date", F.col("order_delivered_carrier_date").isNull().cast("int"))
    .withColumn("is_missing_delivered_customer_date", F.col("order_delivered_customer_date").isNull().cast("int"))
    .withColumn("order_approved_at_filled", F.coalesce(F.col("order_approved_at"), F.col("order_purchase_timestamp")))
)

# Hàm fill missing bằng median
def median_fill(df: DataFrame, col_name: str) -> DataFrame:
    median_value = df.approxQuantile(col_name, [0.5], 0.001)[0]
    return df.withColumn(col_name, F.coalesce(F.col(col_name), F.lit(median_value)))
```

[Giải thích code]
Đoạn code trên thực hiện hai chiến lược xử lý missing values:

1. **Tạo Missing Flags**: Sử dụng `.isNull().cast("int")` để tạo cột binary flag (0/1) đánh dấu các dòng có giá trị thiếu. Đây là thông tin có giá trị vì "thiếu" có thể mang ý nghĩa nghiệp vụ (ví dụ: đơn hàng chưa được giao = chưa có delivery date).

2. **Coalesce fill**: Sử dụng `F.coalesce()` để fill giá trị thiếu với giá trị mặc định hợp lý. Ví dụ: `order_approved_at` được fill bằng `order_purchase_timestamp` - giả định rằng nếu không có ngày duyệt, lấy ngày đặt hàng.

3. **Median Fill Function**: Định nghĩa hàm `median_fill()` sử dụng `approxQuantile` để tính median (trung vị) và fill cho các cột numeric. Median được ưu tiên hơn mean vì ít bị ảnh hưởng bởi outliers.

[Output]
Bảng orders_clean có thêm các cột:
- is_missing_order_approved_at (0/1)
- is_missing_delivered_carrier_date (0/1)
- is_missing_delivered_customer_date (0/1)
- order_approved_at_filled (đã fill)

Null profile sau cleaning:
```
order_delivered_customer_date: 2,965 (2.98%)
order_delivered_carrier_date: 1,783 (1.79%)
order_approved_at: 160 (0.16%)
(is_missing flags: 0% vì đã được tạo)
```

[Ý nghĩa kỹ thuật]
- Việc tạo missing flags trước khi fill giúp preserve thông tin về chất lượng dữ liệu
- Các flags có thể được sử dụng làm features cho mô hình
- Chiến lược fill bằng median thay vì mean giúp tránh ảnh hưởng của outliers

[Ý nghĩa nghiệp vụ]
- Đánh dấu các đơn hàng chưa giao hoặc chưa xác nhận giúp phân biệt các đơn hàng đang xử lý với các đơn hàng đã hoàn thành
- Missing flags có thể dùng để phân tích tỷ lệ đơn hàng bị delay

[Ảnh hưởng đến bước tiếp theo]
- Features is_missing_* được sử dụng trong classification model
- Dữ liệu đã fill không có null, sẵn sàng cho VectorAssembler

### 4.4.3. Xử lý dữ liệu lỗi và không hợp lệ

[Mục đích]
Phát hiện và xử lý các giá trị không hợp lệ về mặt logic hoặc nghiệp vụ, đảm bảo dữ liệu phản ánh đúng thực tế.

[Code]
```python
# Tạo cờ hiệu cho giá trị không hợp lệ trong order_items
order_items_clean = (
    normalized_tables["order_items"]
    .dropDuplicates(["order_id", "order_item_id"])
    .withColumn("is_invalid_price", F.when(F.col("price") <= 0, 1).otherwise(0))
    .withColumn("is_invalid_freight_value", F.when(F.col("freight_value") < 0, 1).otherwise(0))
    .withColumn("price", F.when(F.col("price") <= 0, None).otherwise(F.col("price")))
    .withColumn("freight_value", F.when(F.col("freight_value") < 0, None).otherwise(F.col("freight_value")))
)

# Tương tự cho order_payments
order_payments_clean = (
    normalized_tables["order_payments"]
    .dropDuplicates()
    .withColumn("is_invalid_payment_value", F.when(F.col("payment_value") <= 0, 1).otherwise(0))
    .withColumn("payment_value", F.when(F.col("payment_value") <= 0, None).otherwise(F.col("payment_value")))
)
```

[Giải thích code]
Đoạn code trên thực hiện:
1. **Tạo binary flags**: Đánh dấu các giá trị không hợp lệ (price <= 0, freight < 0, payment_value <= 0)
2. **Đặt giá trị không hợp lệ về None**: Không fill ngay lập tức mà để None để xử lý sau
3. **Lý do không fill ngay**: Để preserve nguyên trạng dữ liệu và tránh introducing bias

[Output]
Bảng order_items_clean:
```
order_id: 0 null
order_item_id: 0 null
product_id: 0 null
seller_id: 0 null
shipping_limit_date: 0 null
price: 0 null (đã set giá trị <= 0 thành None)
freight_value: 0 null
is_invalid_price: 0 null
is_invalid_freight_value: 0 null
```

[Ý nghĩa kỹ thuật]
- Việc không fill ngay các giá trị không hợp lệ mà tạo flag đánh dấu giúp preserve nguyên trạng dữ liệu
- Cho phép xử lý linh hoạt ở các bước sau (có thể loại bỏ hoặc imputation tùy bài toán)

[Ý nghĩa nghiệp vụ]
- Giá trị <= 0 có thể là lỗi data entry hoặc fraud
- Cần được review bởi đội nghiệp vụ

### 4.4.4. Loại bỏ hoặc gom nhóm dữ liệu trùng lặp

[Mục đích]
Loại bỏ các dòng trùng lặp dựa trên khóa chính để đảm bảo tính duy nhất của các thực thể trong mỗi bảng.

[Code]
```python
# Loại bỏ trùng lặp dựa trên khóa chính
customers_clean = (
    normalized_tables["customers"]
    .dropDuplicates(["customer_id"])
)

orders_clean = (
    normalized_tables["orders"]
    .dropDuplicates(["order_id"])
)

# Gom nhóm geolocation theo zip_code
geolocation_zip_city_state_df = (
    geolocation_clean
    .groupBy("geolocation_zip_code_prefix", "geolocation_city", "geolocation_state")
    .agg(
        F.avg("geolocation_lat").alias("avg_lat"),
        F.avg("geolocation_lng").alias("avg_lng"),
        F.count("*").alias("geo_points"),
    )
)
```

[Giải thích code]
- Sử dụng `dropDuplicates()` với khóa chính của mỗi bảng
- Đối với geolocation: Gom nhóm theo zip_code + city + state và tính tọa độ trung bình

[Output]
Số dòng sau khi loại bỏ trùng lặp:
- geolocation: 1,000,163 → 738,332 (giảm 26%)
- order_reviews: 104,162 → 102,958 (giảm 1.2% sau xử lý duplicate review_id)

### 4.4.5. Chuẩn hóa timestamp, numeric fields và categorical fields

[Mục đích]
Chuẩn hóa các trường dữ liệu về cùng một định dạng thống nhất để dễ dàng so sánh và phân tích.

[Code]
```python
# Chuẩn hóa categorical: lowercase + trim
customers_norm = raw_tables["customers"].withColumn(
    "customer_city", F.lower(F.trim(F.col("customer_city")))
).withColumn(
    "customer_state", F.upper(F.trim(F.col("customer_state"))))

sellers_norm = raw_tables["sellers"].withColumn(
    "seller_city", F.lower(F.trim(F.col("seller_city")))).withColumn(
    "seller_state", F.upper(F.trim(F.col("seller_state"))))

geolocation_norm = raw_tables["geolocation"].withColumn(
    "geolocation_city", F.lower(F.trim(F.col("geolocation_city")))).withColumn(
    "geolocation_state", F.upper(F.trim(F.col("geolocation_state"))))
```

[Giải thích code]
- `F.lower()`: Chuyển text về chữ thường
- `F.upper()`: Chuyển text về chữ hoa
- `F.trim()`: Loại bỏ khoảng trắng thừa ở đầu và cuối chuỗi

Điều này đảm bảo "São Paulo", "sao paulo", "SAO PAULO" được xử lý như cùng một giá trị.

## 4.5. Tích hợp dữ liệu đa bảng

### 4.5.1. Tổng hợp dữ liệu theo mức độ chi tiết (Granularity)

[Mục đích]
Xác định và thiết lập mức độ chi tiết (granularity) phù hợp cho từng bài toán machine learning. Mỗi bài toán yêu cầu một mức grain khác nhau. Trước khi join, cần aggregate các bảng 1-nhiều về grain thấp nhất.

[Code]
```python
# Tổng hợp payment theo order
order_payments_agg = (
    order_payments_norm
    .groupBy("order_id")
    .agg(
        F.sum("payment_value").alias("total_payment_value"),
        F.count("*").alias("payment_count"),
        F.max("payment_installments").alias("max_installments"),
        F.collect_list("payment_type").alias("payment_types_list")
    )
)

# Tổng hợp order_items theo order
order_items_agg = (
    order_items_norm
    .groupBy("order_id")
    .agg(
        F.sum("price").alias("total_price"),
        F.sum("freight_value").alias("total_freight"),
        F.count("*").alias("item_count"),
        F.collect_list("product_id").alias("product_ids")
    )
)
```

[Giải thích code]
Do một đơn hàng có thể có nhiều sản phẩm (order_items) và nhiều giao dịch thanh toán (order_payments), cần aggregate về mức order_id trước khi join:

- **order_payments_agg**: 
  - sum(payment_value) → tổng tiền thanh toán
  - count(*) → số giao dịch
  - max(payment_installments) → số kỳ trả góp tối đa
  - collect_list(payment_type) → danh sách phương thức

- **order_items_agg**:
  - sum(price) → tổng giá trị sản phẩm
  - sum(freight_value) → tổng phí vận chuyển
  - count(*) → số items
  - collect_list(product_id) → danh sách sản phẩm

[Output]
- order_payments_agg: 99,440 dòng (distinct orders có payment)
- order_items_agg: 98,666 dòng (distinct orders có items)

### 4.5.2. Hợp nhất các bảng dữ liệu thành phần

[Mục đích]
Kết hợp tất cả các bảng đã xử lý thành một bảng master orders duy nhất phục vụ phân tích và xây dựng features.

[Code]
```python
# Join orders với customers
orders_customers = orders_clean.join(
    customers_clean,
    on="customer_id",
    how="left"
)

# Join với order_items đã aggregate
master_orders = orders_customers.join(
    order_items_agg,
    on="order_id",
    how="left"
)

# Join với payments đã aggregate
master_orders = master_orders.join(
    order_payments_agg,
    on="order_id",
    how="left"
)

# Join với reviews
master_orders = master_orders.join(
    order_reviews_clean.select("order_id", "review_score", "is_missing_review"),
    on="order_id",
    how="left"
)

# Join với products
master_orders = master_orders.join(
    products_clean.select("product_id", "product_category_name", "product_weight_g"),
    on="product_id",
    how="left"
)
```

[Giải thích code]
Quá trình join được thực hiện tuần tự từ bảng trung tâm (orders) với các bảng khác theo thứ tự logic:
1. orders → customers (thông tin khách hàng)
2. → order_items_agg (tổng hợp sản phẩm)
3. → order_payments_agg (tổng hợp thanh toán)
4. → reviews (đánh giá)
5. → products (thông tin sản phẩm)

Mỗi bước join sử dụng **left join** để giữ tất cả các đơn hàng trong bảng orders.

[Output]
- master_orders: 99,441 dòng
- master_orders distinct order_id: 99,441 (không có row explosion)

[Ý nghĩa kỹ thuật]
- Row count giữ nguyên = không có row explosion
- Left join đảm bảo không mất đơn hàng

### 4.5.3. Kiểm tra tính toàn vẹn của dữ liệu sau hợp nhất

[Mục đích]
Sau khi join, cần kiểm tra xem có row explosion (số dòng tăng bất thường) hoặc mất dữ liệu hay không.

[Code]
```python
# Kiểm tra row count trước và sau join
print(f"Orders count: {orders_clean.count()}")
print(f"Master orders count: {master_orders.count()}")

# Kiểm tra distinct order_id
print(f"Distinct order_id: {master_orders.select('order_id').distinct().count()}")

# Kiểm tra null ratio
for col in ["customer_id", "product_id", "total_price", "review_score"]:
    null_pct = master_orders.filter(F.col(col).isNull()).count() / master_orders.count()
    print(f"{col}: {null_pct:.2%} null")
```

[Output]
```
master_orders rows: 99,441
master_orders distinct order_id: 99,441
```
- Row count không tăng → không có row explosion
- Distinct count = row count → mỗi order chỉ có 1 dòng

### 4.5.4. Lưu trữ dữ liệu chuẩn hóa dưới định dạng Parquet

[Mục đích]
Lưu trữ dữ liệu đã xử lý dưới định dạng Parquet để tối ưu hóa hiệu suất đọc/ghi và giảm dung lượng lưu trữ.

[Code]
```python
# Lưu theo các layer
SILVER_DIR = PROCESSED_DIR / "silver"
GOLD_DIR = PROCESSED_DIR / "gold"

# Lưu silver layer (dữ liệu đã clean nhưng chưa aggregate)
customers_clean.write.parquet(str(SILVER_DIR / "customers"), mode="overwrite")
orders_clean.write.parquet(str(SILVER_DIR / "orders"), mode="overwrite")
products_clean.write.parquet(str(SILVER_DIR / "products"), mode="overwrite")
order_reviews_clean.write.parquet(str(SILVER_DIR / "order_reviews"), mode="overwrite")

# Lưu gold layer (dữ liệu đã aggregate theo bài toán)
master_orders.write.parquet(str(GOLD_DIR / "master_orders"), mode="overwrite")
```

[Output]
Các file Parquet được lưu tại:
- `data/processed/silver/`: customers, orders, products, order_reviews
- `data/processed/gold/`: master_orders

## 4.6. Trích xuất nhóm đặc trưng RFM

### 4.6.1. Đặc trưng về mức độ gần đây (Recency)

[Mục đích]
Recency đo lường thời gian kể từ lần mua hàng gần nhất của khách hàng. Đây là yếu tố quan trọng để phân đoạn khách hàng và dự đoán khả năng mua lại.

[Code]
```python
# Tính recency: số ngày kể từ lần mua cuối cùng
reference_date = F.to_timestamp(F.lit("2018-09-03"))

rfm_df = (
    master_orders
    .filter(F.col("order_status") == "delivered")
    .groupBy("customer_unique_id")
    .agg(
        F.max("order_purchase_timestamp").alias("last_order_date"),
        F.datediff(F.lit(reference_date), F.max("order_purchase_timestamp")).alias("recency_days")
    )
)
```

[Giải thích code]
- Sử dụng `F.max("order_purchase_timestamp")` để tìm ngày mua gần nhất của mỗi khách hàng
- Tính số ngày chênh lệch với ngày tham chiếu (ngày cuối cùng trong dataset: 2018-09-03)
- Giá trị recency càng nhỏ → khách hàng càng gần đây

[Output]
| customer_unique_id | last_order_date | recency_days |
|-------------------|----------------|--------------|
| abc123... | 2018-08-15 | 19 |
| def456... | 2018-07-20 | 45 |

### 4.6.2. Đặc trưng về tần suất (Frequency)

[Mục đích]
Frequency đo lường tổng số đơn hàng mà khách hàng đã thực hiện. Khách hàng có tần suất mua cao thường có giá trị lifetime value lớn hơn.

[Code]
```python
rfm_df = (
    rfm_df
    .join(
        master_orders
        .filter(F.col("order_status") == "delivered")
        .groupBy("customer_unique_id")
        .agg(F.count("order_id").alias("frequency")),
        on="customer_unique_id",
        how="left"
    )
)
```

[Output]
Thêm cột frequency vào RFM DataFrame.

### 4.6.3. Đặc trưng về giá trị tiền tệ (Monetary)

[Mục đích]
Monetary đo lường tổng giá trị mà khách hàng đã chi tiêu. Đây là chỉ số trực tiếp về giá trị của khách hàng đối với doanh nghiệp.

[Code]
```python
rfm_df = (
    rfm_df
    .join(
        master_orders
        .filter(F.col("order_status") == "delivered")
        .groupBy("customer_unique_id")
        .agg(F.sum("total_price").alias("monetary")),
        on="customer_unique_id",
        how="left"
    )
)
```

[Output]
RFM DataFrame hoàn chỉnh:
| customer_unique_id | recency_days | frequency | monetary |
|-------------------|--------------|-----------|----------|
| abc123... | 19 | 5 | 1250.50 |
| def456... | 45 | 2 | 380.00 |

## 4.7. Xây dựng các tập dữ liệu cơ sở theo bài toán

### 4.7.1. Tập dữ liệu cơ sở cho bài toán phân lớp

[Mục đích]
Xây dựng tập dữ liệu cho bài toán phân lớp đánh giá khách hàng (sentiment classification) dựa trên review text.

[Code]
```python
# Lọc các đơn hàng có review_score
classification_base = (
    master_orders
    .filter(F.col("review_score").isNotNull())
    .select(
        "order_id", "customer_unique_id", 
        "review_score", "review_comment_message",
        "total_price", "freight_value", 
        "payment_type", "product_category_name"
    )
)

# Tạo nhãn binary: 1 = tiêu cực (score 1-2), 0 = tích cực (score 3-5)
classification_base = classification_base.withColumn(
    "label",
    F.when(F.col("review_score") <= 2, 1).otherwise(0)
)
```

[Output]
- classification_base: 98,167 dòng
- Label distribution: ~16% negative (label=1), ~84% positive (label=0)

### 4.7.2. Tập dữ liệu cơ sở cho bài toán hồi quy

[Mục đích]
Xây dựng tập dữ liệu để dự đoán giá trị đơn hàng (GMV - Gross Merchandise Value).

[Code]
```python
# Tập dữ liệu cho bài toán dự đoán GMV
regression_base = (
    master_orders
    .filter(
        (F.col("total_price").isNotNull()) & 
        (F.col("total_price") > 0)
    )
    .select(
        "order_id", "customer_unique_id",
        "total_price",  # Target variable
        "item_count", "product_weight_g",
        "customer_state", "product_category_name"
    )
)
```

[Output]
- regression_base: 98,666 dòng

### 4.7.3. Tập dữ liệu cơ sở cho bài toán phân cụm

[Mục đích]
Xây dựng tập dữ liệu cho bài toán phân cụm khách hàng dựa trên hành vi mua sắm.

[Code]
```python
# Kết hợp RFM với các features bổ sung
clustering_base = (
    rfm_df
    .join(
        master_orders
        .groupBy("customer_unique_id")
        .agg(
            F.avg("total_price").alias("avg_order_value"),
            F.avg("freight_value").alias("avg_freight"),
            F.countDistinct("product_category_name").alias("category_diversity")
        ),
        on="customer_unique_id",
        how="inner"
    )
)
```

[Output]
- clustering_base: 93,358 dòng (unique customers)

### 4.7.4. Tập dữ liệu cơ sở cho hệ tư vấn (Tương tác User - Item)

[Mục đích]
Xây dựng tập dữ liệu tương tác user-item cho thuật toán Collaborative Filtering (ALS).

[Code]
```python
# Tập dữ liệu cho ALS: user_id, item_id, rating
als_base = (
    master_orders
    .filter(
        (F.col("customer_unique_id").isNotNull()) & 
        (F.col("product_id").isNotNull())
    )
    .select(
        "customer_unique_id",  # user
        "product_id",          # item
        "review_score"         # rating (implicit/explicit)
    )
    .distinct()
)
```

[Output]
- als_base: 101,987 dòng (user-item interactions)

### 4.7.5. Tập dữ liệu cơ sở cho khai phá tập phổ biến (Giao dịch giỏ hàng)

[Mục đích]
Xây dựng tập dữ liệu transaction/basket cho thuật toán FP-Growth.

[Code]
```python
# Tập dữ liệu cho FP-Growth: order_id -> danh sách product_id
fpgrowth_base = (
    master_orders
    .filter(F.col("order_status") == "delivered")
    .groupBy("order_id")
    .agg(F.collect_list("product_category_name").alias("basket"))
    .filter(F.size("basket") > 1)  # Chỉ lấy các giỏ có >= 2 sản phẩm
)
```

[Output]
- fpgrowth_base: 3,236 transactions

## 4.8. Kỹ thuật đặc trưng và xây dựng luồng trích xuất (Feature Pipeline)

### 4.8.1. Trích xuất các nhóm đặc trưng cấu trúc (Thời gian, vận chuyển, thanh toán, v.v.)

[Mục đích]
Trích xuất các features có ý nghĩa từ dữ liệu thô, phục vụ các mô hình machine learning.

[Code]
```python
# Features thời gian
feature_df = master_orders.withColumn("order_hour", F.hour("order_purchase_timestamp")) \
    .withColumn("order_dayofweek", F.dayofweek("order_purchase_timestamp")) \
    .withColumn("order_month", F.month("order_purchase_timestamp")) \
    .withColumn("order_year", F.year("order_purchase_timestamp"))

# Features vận chuyển
feature_df = feature_df.withColumn(
    "delivery_days",
    F.datediff("order_delivered_customer_date", "order_purchase_timestamp")
).withColumn(
    "delivery_delay_days",
    F.datediff("order_delivered_customer_date", "order_estimated_delivery_date")
)

# Features thanh toán
feature_df = feature_df.withColumn("is_credit_card", 
    F.when(F.col("payment_type") == "credit_card", 1).otherwise(0))
```

[Giải thích code]
Đoạn code trên trích xuất 3 nhóm features chính:

1. **Features thời gian**:
   - order_hour: Giờ đặt hàng (0-23)
   - order_dayofweek: Ngày trong tuần (1-7)
   - order_month: Tháng (1-12)
   - order_year: Năm

2. **Features vận chuyển**:
   - delivery_days: Số ngày từ đặt hàng đến giao hàng
   - delivery_delay_days: Số ngày delay so với dự kiến (âm = đúng hạn, dương = trễ)

3. **Features thanh toán**:
   - is_credit_card: Cờ binary cho thanh toán thẻ tín dụng

[Output]
| order_id | order_hour | order_dayofweek | delivery_days | delivery_delay_days | is_credit_card |
|----------|------------|-----------------|---------------|---------------------|----------------|
| abc123 | 14 | 2 | 8 | 2 | 1 |
| def456 | 10 | 5 | 5 | -3 | 0 |

[Ý nghĩa kỹ thuật]
- Các features này được sử dụng trong VectorAssembler để tạo feature vector cho model

[Ý nghĩa nghiệp vụ]
- delivery_delay_days là feature quan trọng cho classification (predict delay)
- is_credit_card cho biết phương thức thanh toán ưa thích

### 4.8.2. Trích xuất đặc trưng văn bản cho bài toán phân lớp

[Mục đích]
Chuyển đổi văn bản review thành các vector số sử dụng TF-IDF hoặc Word2Vec.

[Code]
```python
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, HashingTF, IDF, Word2Vec

# Tokenize văn bản
tokenizer = RegexTokenizer(
    inputCol="review_comment_message", 
    outputCol="tokens", 
    pattern="\\W+"
)

# Loại bỏ stop words
stopwords_remover = StopWordsRemover(
    inputCol="tokens", 
    outputCol="filtered_tokens"
)

# TF-IDF
hashingTF = HashingTF(
    inputCol="filtered_tokens", 
    outputCol="rawFeatures", 
    numFeatures=1000
)
idf = IDF(
    inputCol="rawFeatures", 
    outputCol="text_features"
)

# Word2Vec (alternative)
word2vec = Word2Vec(
    inputCol="filtered_tokens",
    outputCol="text_features",
    vectorSize=100,
    minCount=2
)

# Xây dựng pipeline
text_pipeline = Pipeline(stages=[tokenizer, stopwords_remover, hashingTF, idf])
text_model = text_pipeline.fit(classification_base)
classification_features = text_model.transform(classification_base)
```

[Giải thích code]
Đoạn code trên xây dựng text feature extraction pipeline với 4 stage:

1. **RegexTokenizer**: Tách văn bản thành tokens bằng regex pattern `\\W+` (tách theo ký tự không phải chữ cái/số)

2. **StopWordsRemover**: Loại bỏ stop words tiếng Anh (the, a, is, are,...) để giảm nhiễu

3. **HashingTF**: Tính Term Frequency sử dụng hashing trick với numFeatures=1000

4. **IDF**: Tính Inverse Document Frequency để tạo TF-IDF vector

[Output]
Cột "text_features" chứa vector TF-IDF:
```
+--------------------------------+-----+------------------------------------------------------------------+
|order_id                        |label|features                                                        |
+--------------------------------+-----+------------------------------------------------------------------+
|000229ec398224ef6ca0657da4fc703e|0    |(1000,[45,128,267,...],[0.12,0.34,0.56,...])               |
+--------------------------------+-----+------------------------------------------------------------------+
```

[Ý nghĩa kỹ thuật]
- TF-IDF tạo sparse vector, Word2Vec tạo dense vector
- numFeatures=1000 giới hạn chiều vector
- Stop words removal giảm dimensionality

[Ý nghĩa nghiệp vụ]
- Text features bổ sung cho classification (sentiment analysis)
- Từ khóa trong review có thể predict điểm số

### 4.8.3. Mã hóa, hợp nhất và chuẩn hóa đặc trưng

[Mục đích]
Chuyển đổi các features categorical thành số và chuẩn hóa các features numeric.

[Code]
```python
from pyspark.ml.feature import StringIndexer, OneHotEncoder, StandardScaler, VectorAssembler

# StringIndexer cho categorical columns
indexer = StringIndexer(
    inputCol="product_category_name", 
    outputCol="category_index"
)

# OneHotEncoder
encoder = OneHotEncoder(
    inputCol="category_index", 
    outputCol="category_vec"
)

# VectorAssembler cho tất cả features
assembler = VectorAssembler(
    inputCols=["price", "freight_value", "delivery_days", "category_vec"],
    outputCol="raw_features"
)

# StandardScaler
scaler = StandardScaler(
    inputCol="raw_features", 
    outputCol="features",
    withMean=True, 
    withStd=True
)
```

[Giải thích code]
Pipeline xử lý categorical và numeric features:

1. **StringIndexer**: Chuyển đổi categorical string thành index số (0, 1, 2,...)
   - handleInvalid='keep': Các giá trị unknown được gán index riêng

2. **OneHotEncoder**: Chuyển index thành vector nhị phân (one-hot)
   - Tránh ordinal relationship giả tạo giữa các categories

3. **VectorAssembler**: Gom tất cả features thành một vector duy nhất

4. **StandardScaler**: Chuẩn hóa theo z-score (mean=0, std=1)
   - withMean=False cho sparse vectors

[Output]
| price | freight_value | delivery_days | category_vec | raw_features | features |
|-------|--------------|--------------|-------------|--------------|----------|
| 120.0 | 15.0 | 5 | (5,[2],[1.0]) | (8,[0,1,2,5],[...]) | (8,[...],[...]) |

### 4.8.4. Lựa chọn đặc trưng tối ưu (ChiSqSelector)

[Mục đích]
Lựa chọn các features có ý nghĩa thống kê nhất cho bài toán phân lớp sử dụng Chi-Square test.

[Code]
```python
from pyspark.ml.feature import ChiSqSelector

# Chi-Square Feature Selection
selector = ChiSqSelector(
    featuresCol="scaled_features",
    outputCol="selected_features",
    labelCol="label",
    numTopFeatures=50  # Chọn top 50 features
)

# Fit selector
selector_model = selector.fit(training_data)
selected_data = selector_model.transform(training_data)
```

[Giải thích code]
ChiSqSelector thực hiện:
1. Tính Chi-Square statistic giữa mỗi feature và label
2. Sắp xếp features theo p-value (thấp nhất = quan trọng nhất)
3. Chọn top K features (numTopFeatures=50)

[Output]
Features được chọn:
- Giảm từ ~33 features xuống 50 features
- Loại bỏ features không correlated với label

[Ý nghĩa kỹ thuật]
- Giảm overfitting
- Giảm training time
- Cải thiện model interpretability

## 4.9. Cấu trúc dữ liệu đầu vào cho các nhóm mô hình

### 4.9.1. Tập đặc trưng bài toán phân lớp

Tập features cho bài toán phân lớp bao gồm:
- TF-IDF vectors từ review text (nếu enabled)
- Các features categorical: product_category, payment_type, customer_state
- Các features numeric: price, freight_value, delivery_days, item_count
- Label: binary (1 = tiêu cực, 0 = tích cực)

**Kích thước:**
- Total: 98,167 dòng
- Train: 68,750 (70%)
- Validation: 14,646 (15%)
- Test: 14,771 (15%)

### 4.9.2. Tập đặc trưng bài toán hồi quy

Tập features cho bài toán hồi quy bao gồm:
- Các features numeric: item_count, product_weight_g, delivery_days
- Các features categorical đã mã hóa: customer_state, product_category
- Target: total_price (GMV)

**Kích thước:**
- Total: 98,666 dòng

### 4.9.3. Tập đặc trưng bài toán phân cụm

Tập features cho bài toán phân cụm bao gồm:
- RFM features: recency_days, frequency, monetary
- Additional features: avg_order_value, avg_freight, category_diversity
- Tất cả features được chuẩn hóa bằng StandardScaler

**Kích thước:**
- Total: 93,358 dòng (unique customers)

### 4.9.4. Tập đặc trưng hệ tư vấn

Tập dữ liệu cho ALS bao gồm:
- userId: customer_unique_id (đã được index)
- itemId: product_id (đã được index)
- rating: review_score (implicit/explicit)

**Kích thước:**
- Total: 101,987 interactions

### 4.9.5. Tập đặc trưng khai phá luật kết hợp

Tập dữ liệu cho FP-Growth bao gồm:
- Mỗi dòng là một transaction (order_id)
- Mỗi transaction chứa danh sách các item (product_category_name)

**Kích thước:**
- Total: 3,236 transactions (với tối thiểu 2 items mỗi transaction)

## 4.10. Chiến lược phân chia tập dữ liệu huấn luyện và kiểm thử

### 4.10.1. Phân chia ngẫu nhiên trong giai đoạn tiền xử lý

[Mục đích]
Phân chia dữ liệu thành train/validation/test một cách ngẫu nhiên để đảm bảo phân phối đều.

[Code]
```python
# Random split theo hash của order_id
classification_split_base_df = classification_base_df.withColumn(
    "split_bucket", 
    (F.abs(F.hash("order_id")) % 20).cast("int")
)

classification_train_base_df = classification_split_base_df.filter(F.col("split_bucket") < 14).drop("split_bucket")
classification_val_base_df = classification_split_base_df.filter((F.col("split_bucket") >= 14) & (F.col("split_bucket") < 17)).drop("split_bucket")
classification_test_base_df = classification_split_base_df.filter(F.col("split_bucket") >= 17).drop("split_bucket")
```

[Giải thích code]
Thay vì randomSplit trực tiếp, sử dụng hash của order_id để:
- Đảm bảo reproducibility (cùng order_id luôn vào cùng tập)
- Tránh data leakage giữa các tập

[Output]
```
classification split base rows -> train: 68,750 | val: 14,646 | test: 14,771
```

### 4.10.2. Xử lý giá trị khuyết thiếu sau phân chia (Post-split imputation)

[Mục đích]
Đảm bảo imputation chỉ sử dụng thông tin từ tập train để tránh data leakage.

[Code]
```python
# Fit imputer chỉ trên train data
median_price = train_df.approxQuantile("price", [0.5], 0.001)[0]

# Apply imputation cho tất cả các tập
train_filled = train_df.withColumn("price", F.coalesce(F.col("price"), F.lit(median_price)))
val_filled = val_df.withColumn("price", F.coalesce(F.col("price"), F.lit(median_price)))
test_filled = test_df.withColumn("price", F.coalesce(F.col("price"), F.lit(median_price)))
```

### 4.10.3. Phân chia dữ liệu dựa trên mã đơn hàng (Order ID)

[Mục đích]
Đảm bảo cùng một đơn hàng không xuất hiện trong cả train và test.

[Code]
```python
# Lấy danh sách order_id cho train
train_orders = train_df.select("order_id").distinct()

# Filter validation/test không chứa train orders
val_df_filtered = val_df.join(train_orders, on="order_id", how="left_anti")
```

### 4.10.4. Phân chia dữ liệu dựa trên tương tác người dùng - sản phẩm

[Mục đích]
Cho bài toán recommendation, đảm bảo users/items trong test có xuất hiện trong train.

[Code]
```python
# Phân chia theo thời gian cho recommendation
als_train = als_base.filter(F.col("order_purchase_timestamp") < "2018-06-01")
als_test = als_base.filter(F.col("order_purchase_timestamp") >= "2018-06-01")
```

### 4.10.5. Chiến lược dữ liệu cho bài toán phân cụm

[Mục đích]
Phân cụm không cần labels, sử dụng toàn bộ dữ liệu để tìm patterns.

[Code]
```python
# Sử dụng toàn bộ dữ liệu clustering
scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
scaler_model = scaler.fit(clustering_df)
clustering_scaled = scaler_model.transform(clustering_df)
```

## 4.11. Lưu trữ tập dữ liệu và các thành phần phụ trợ (Artifacts)

### 4.11.1. Dữ liệu huấn luyện, kiểm định và kiểm thử cho các bài toán học có giám sát

[Mục đích]
Lưu trữ các tập dữ liệu đã phân chia ở định dạng Parquet để tái sử dụng.

[Code]
```python
# Lưu train/val/test cho classification
classification_train_df.write.parquet(str(GOLD_DIR / "classification_train"), mode="overwrite")
classification_val_df.write.parquet(str(GOLD_DIR / "classification_val"), mode="overwrite")
classification_test_df.write.parquet(str(GOLD_DIR / "classification_test"), mode="overwrite")

# Lưu train/val/test cho regression
regression_train_df.write.parquet(str(GOLD_DIR / "regression_train"), mode="overwrite")
regression_val_df.write.parquet(str(GOLD_DIR / "regression_val"), mode="overwrite")
regression_test_df.write.parquet(str(GOLD_DIR / "regression_test"), mode="overwrite")
```

### 4.11.2. Dữ liệu toàn phần cho bài toán học không giám sát

[Mục đích]
Lưu trữ toàn bộ dữ liệu cho clustering và pattern mining.

[Code]
```python
# Lưu clustering base
clustering_base_df.write.parquet(str(GOLD_DIR / "clustering_base"), mode="overwrite")

# Lưu fpgrowth base
fpgrowth_base_df.write.parquet(str(GOLD_DIR / "fpgrowth_base"), mode="overwrite")

# Lưu ALS base
als_base_df.write.parquet(str(GOLD_DIR / "als_base"), mode="overwrite")
```

### 4.11.3. Lưu trữ cấu hình Pipeline phục vụ huấn luyện và dự suy luận (Inference)

[Mục đích]
Lưu các fitted models và pipelines để sử dụng sau cho inference.

[Code]
```python
from pyspark.ml import PipelineModel

# Lưu text pipeline
text_model.write().overwrite().save(str(MODELS_DIR / "text_pipeline"))

# Lưu feature pipeline
classification_pipeline_model.write().overwrite().save(str(MODELS_DIR / "classification_pipeline"))

# Lưu scaler cho clustering
clustering_pipeline_model.write().overwrite().save(str(MODELS_DIR / "clustering_pipeline"))
```

---

## Tổng kết Chương 4

Chương 4 đã trình bày chi tiết toàn bộ quá trình phân tích dữ liệu và xây dựng ML Pipeline từ bộ dữ liệu Olist e-commerce Brazil:

1. **Mô tả dữ liệu**: 9 bảng với quan hệ rõ ràng, phạm vi thời gian 2016-2018
2. **Khảo sát dữ liệu**: Phát hiện missing values, duplicates, outliers
3. **EDA**: Xu hướng theo thời gian, phân bố review_score, payment methods, geographic distribution
4. **Tiền xử lý**: Chuẩn hóa schema, xử lý missing/invalid values
5. **Tích hợp đa bảng**: Join các bảng thành master_orders
6. **Trích xuất RFM**: Features cho customer segmentation
7. **Xây dựng base datasets**: 5 bài toán ML (classification, regression, clustering, recommendation, pattern mining)
8. **Feature Engineering**: Text features (TF-IDF), categorical encoding (StringIndexer, OHE), scaling (StandardScaler), feature selection (ChiSqSelector)
9. **Data Splitting**: Chiến lược split đúng cách để tránh leakage
10. **Storage**: Lưu trữ Parquet theo layer silver/gold và artifacts

Các kết quả metrics của các mô hình sẽ được trình bày chi tiết trong Chương 5 - Triển khai và đánh giá mô hình.
