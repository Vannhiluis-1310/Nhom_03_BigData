# PROGRESS TRACKER - Big Data Project

> Theo dõi tiến độ chi tiết của từng thành viên và module.

---

## THÀNH VIÊN & VAI TRÒ

| STT | Họ tên | Vai trò | Nhiệm vụ chính |
|-----|--------|---------|----------------|
| 1 | Lưu Hoàng Vân Nhi | Tech Lead / Data Engineer | Pipeline, Data Processing, RFM |
| 2 | Lê Hồ Thảo Vy | Business Analyst | EDA, Insights, Báo cáo |
| 3 | Nguyễn Đoàn Thùy Linh | ML/Frontend Support | Documentation, Research |
| 4 | Chu Tuấn Đức | ML Engineer | Classification, Regression, Clustering |
| 5 | Nguyễn Lê Bảo Trân | Frontend Lead | Streamlit Web UI |

---

## WEEKLY PROGRESS

### Week 1: Foundation (18-24/03/2026)

| Module | Công việc | Người | Trạng thái | Ghi chú |
|--------|-----------|-------|------------|---------|
| **Environment** | Setup môi trường (UV, PySpark, Jupyter) | Nhi | ✅ Hoàn thành | PySpark 4.1.1, Python 3.12 |
| **Structure** | Tạo folder structure đồng bộ | Nhi | ✅ Hoàn thành | Tất cả subfolders đã tạo |
| **Core Files** | README, CONTRIBUTING, pyproject.toml | Nhi | ✅ Hoàn thành | |
| **Config** | Spark config utilities | Nhi | ✅ Hoàn thành | src/utils/spark_config.py |
| **Guides** | Hướng dẫn UV chi tiết | Nhi | ✅ Hoàn thành | docs/HUONG_DAN_UV.md |
| **Notebooks** | Setup Environment notebook | Nhi | ✅ Hoàn thành | notebooks/00_Setup_Environment.ipynb |
| **Dataset** | Download Olist dataset | Nhi | ⏳ Chờ | Cần Kaggle API token |

**Week 1 Deliverables:**
- [x] Project structure đồng bộ
- [x] Requirements & dependencies setup
- [x] Core documentation
- [ ] EDA complete (10 CSV files)
- [ ] ERD diagram
- [ ] Architecture diagram

---

### Week 2: Core Development (25-31/03/2026)

| Module | Công việc | Người | Trạng thái | Deadline |
|--------|-----------|-------|------------|----------|
| **Data** | Join tables (9 bảng) | Nhi | ⏳ Pending | 25/03 |
| **Data** | Xử lý missing values | Nhi | ⏳ Pending | 26/03 |
| **Features** | RFM Feature Engineering | Nhi | ⏳ Pending | 27/03 |
| **Pipeline** | Spark ML Pipeline | Nhi | ⏳ Pending | 28/03 |
| **Classification** | 5 models (LogReg, RF, NB, SVC, GBT) | Đức | ⏳ Pending | 29-31/03 |
| **Regression** | 3 models (Linear, DT, RF) | Đức | ⏳ Pending | 30-31/03 |
| **Clustering** | 3 models (K-Means, Bisecting, GMM) | Nhi | ⏳ Pending | 31/03 |
| **Recommendation** | ALS Model | Đức | ⏳ Pending | 31/03 |
| **Pattern Mining** | FP-Growth | Đức | ⏳ Pending | 31/03 |
| **Streamlit** | Dashboard pages | Bảo Trân | ⏳ Pending | 26-31/03 |
| **Analysis** | Statistical analysis, Chi-square | Linh | ⏳ Pending | 26-27/03 |
| **Insights** | Business insights drafts | Vy | ⏳ Pending | 27-31/03 |

**Week 2 Deliverables:**
- [ ] Unified dataset (joined tables)
- [ ] RFM features
- [ ] ML Pipeline ready
- [ ] All ML models trained (5+3+3+1+1 = 12 models)
- [ ] Model comparison metrics
- [ ] Streamlit basic pages
- [ ] Business insights drafts

---

### Week 3: Integration & Finalization (01-07/04/2026)

| Module | Công việc | Người | Trạng thái | Deadline |
|--------|-----------|-------|------------|----------|
| **ML Tuning** | Hyperparameter tuning | Nhi/Đức | ⏳ Pending | 02/04 |
| **Integration** | ML → Streamlit | Đức/Bảo Trân | ⏳ Pending | 03/04 |
| **Testing** | Unit + Integration tests | Đức | ⏳ Pending | 04/04 |
| **Reports** | Báo cáo Chương 1-4 | Vy/Linh | ⏳ Pending | 05/04 |
| **Documentation** | README, GitHub | Nhi | ⏳ Pending | 04/04 |
| **Presentation** | Slides + Video demo | Vy/Bảo Trân | ⏳ Pending | 05-06/04 |
| **Final Review** | Code review, Q&A prep | All | ⏳ Pending | 06-07/04 |

**Week 3 Deliverables:**
- [ ] Tuned ML models
- [ ] Integrated Streamlit app
- [ ] All tests passing
- [ ] Final report (30-40 pages)
- [ ] Deployed app
- [ ] Video demo (3-5 min)
- [ ] Presentation slides (15 slides)

---

## ML MODELS TRACKER

### Classification (Target: review_score prediction)

| Model | Trạng thái | Accuracy | Precision | Recall | F1 | Ghi chú |
|-------|------------|----------|-----------|--------|----|---------|
| Logistic Regression | ⏳ Pending | - | - | - | - | |
| Random Forest | ⏳ Pending | - | - | - | - | |
| Naive Bayes | ⏳ Pending | - | - | - | - | |
| LinearSVC | ⏳ Pending | - | - | - | - | |
| GBT Classifier | ⏳ Pending | - | - | - | - | |

### Regression (Target: order_value / freight_value)

| Model | Trạng thái | RMSE | MAE | R² | Ghi chú |
|-------|------------|------|-----|----|---------|
| Linear Regression | ⏳ Pending | - | - | - | |
| Decision Tree | ⏳ Pending | - | - | - | |
| Random Forest | ⏳ Pending | - | - | - | |

### Clustering (Target: Customer Segmentation)

| Model | Trạng thái | K | Silhouette | Ghi chú |
|-------|------------|---|------------|---------|
| K-Means | ⏳ Pending | - | - | |
| Bisecting K-Means | ⏳ Pending | - | - | |
| Gaussian Mixture | ⏳ Pending | - | - | |

### Recommendation

| Model | Trạng thái | Precision@10 | Recall@10 | Ghi chú |
|-------|------------|-------------|-----------|---------|
| ALS | ⏳ Pending | - | - | |

### Pattern Mining

| Model | Trạng thái | Rules Found | Top Rule | Ghi chú |
|-------|------------|-------------|----------|---------|
| FP-Growth | ⏳ Pending | - | - | |

---

## STREAMLIT PAGES TRACKER

| Page | Mô tả | Trạng thái | Người |
|------|-------|------------|-------|
| Home/Dashboard | Metrics, charts overview | ⏳ Pending | Bảo Trân |
| Segmentation | RFM visualization, customer personas | ⏳ Pending | Bảo Trân |
| Recommendation | Top-N product recommendations | ⏳ Pending | Bảo Trân |
| Prediction | Classification/Regression predictions | ⏳ Pending | Bảo Trân |
| Market Basket | FP-Growth rules visualization | ⏳ Pending | Bảo Trân |
| Admin | Upload, retrain, reports | ⏳ Pending | Bảo Trân |

---

## FILES TRACKER

| File | Trạng thái | Người | Link |
|------|-------------|-------|------|
| README.md | ✅ Hoàn thành | Nhi | /README.md |
| CONTRIBUTING.md | ✅ Hoàn thành | Nhi | /CONTRIBUTING.md |
| pyproject.toml | ✅ Hoàn thành | Nhi | /pyproject.toml |
| setup.py | ✅ Hoàn thành | Nhi | /setup.py |
| requirements.txt | ✅ Hoàn thành | Nhi | /requirements.txt |
| ERROR_TRACKER.md | ✅ Hoàn thành | Nhi | /docs/ERROR_TRACKER.md |
| PROGRESS_TRACKER.md | ✅ Hoàn thành | Nhi | /docs/PROGRESS_TRACKER.md |
| HUONG_DAN_UV.md | ✅ Hoàn thành | Nhi | /docs/HUONG_DAN_UV.md |
| 00_Setup_Environment.ipynb | ✅ Hoàn thành | Nhi | /notebooks/ |
| EDA_report.md | ⏳ Pending | Vy | /reports/ |
| Business_insights.md | ⏳ Pending | Vy | /reports/ |
| ML_comparison.md | ⏳ Pending | Đức | /reports/ |
| Final_report.docx | ⏳ Pending | Vy/Linh | /reports/ |

---

## NEXT ACTIONS

### Immediate (Before next session)
1. Download Olist dataset từ Kaggle
2. Load và khám phá 10 CSV files
3. Bắt đầu EDA

### This Week (Week 1 remaining)
- [ ] Hoàn thành EDA toàn bộ dataset
- [ ] Vẽ ERD diagram
- [ ] Vẽ architecture diagram
- [ ] Join tables thành unified DataFrame
- [ ] Xử lý missing values

---

*Last updated: 2026-03-20*
