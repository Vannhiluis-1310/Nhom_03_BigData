# ERROR TRACKER - Big Data Project

> File này ghi lại tất cả các lỗi và khó khăn gặp phải trong quá triình phát triển dự án.

---

## TIẾN ĐỘ TỔNG QUAN

### ✅ Đã hoàn thành (Week 1 - Foundation)

| Module | Chi tiết | Người phụ trách |
|--------|----------|------------------|
| **Folder Structure** | Tạo đầy đủ thư mục theo kiến trúc | Nhi |
| **Dependencies** | requirements.txt với 15 packages | Nhi |
| **Core Files** | README.md, CONTRIBUTING.md, pyproject.toml, setup.py | Nhi |
| **Spark Config** | src/utils/spark_config.py | Nhi |
| **Setup Guide** | docs/HUONG_DAN_UV.md | Nhi |
| **Jupyter Notebook** | notebooks/00_Setup_Environment.ipynb | Nhi |
| **Environment Setup** | UV venv + dependencies đã cài đặt thành công | Nhi |

### 📁 Cấu trúc dự án
```
Nhom03_PySpark_ProjectCuoiKy/
├── app/                    # Streamlit pages, components, utils
├── config/                 # Configuration files
├── data/
│   ├── processed/          # Dữ liệu đã xử lý
│   └── raw/                # Dữ liệu thô (10 CSV files Olist)
├── docs/                   # ERROR_TRACKER.md, PROGRESS_TRACKER.md, HUONG_DAN_UV.md
├── models/                 # Lưu trained models
│   ├── classification/
│   ├── clustering/
│   ├── pipeline/
│   ├── recommendation/
│   └── regression/
├── notebooks/              # Jupyter notebooks (.venv/)
├── reports/                # Báo cáo
├── src/                    # Source code
│   ├── data/               # Data loading & processing
│   ├── evaluation/         # Model evaluation
│   ├── features/           # Feature engineering
│   ├── models/             # Model training
│   ├── pipeline/           # ML Pipelines
│   └── utils/              # Utilities (spark_config.py)
├── tests/                  # Unit & integration tests
├── .gitignore
├── CONTRIBUTING.md
├── pyproject.toml
├── README.md
├── requirements.txt
└── setup.py
```

---

## Lỗi & Cảnh báo đã biết

### [2026-03-20] UV Commands sai trên Windows PowerShell
- **Môi trường**: Windows PowerShell
- **Module**: Environment Setup
- **Mô tả lỗi**: Nhiều lệnh UV không hoạt động đúng trên PowerShell

| Lệnh sai | Lệnh đúng | Ghi chú |
|----------|-----------|---------|
| `uv list` | `uv pip list` | UV subcommands cần prefix `pip` |
| `uv install` | `uv pip install` | UV subcommands cần prefix `pip` |
| `uv pip install` (không args) | `uv pip install -r requirements.txt` | Cần chỉ định package hoặc file |
| `rm -rf .venv` | `Remove-Item -Recurse -Force .venv` | PowerShell không hỗ trợ `rm -rf` |

- **Nguyên nhân**: UV commands trên Windows PowerShell cần cú pháp khác Linux/Mac
- **Giải pháp**: Sử dụng `uv pip` thay vì `uv` cho các operations, dùng PowerShell cmdlets thay vì Unix commands
- **Trạng thái**: [x] Đã giải quyết - Đã cài đặt thành công 182 packages

### [2026-03-20] Typo trong requirements.txt path
- **Môi trường**: Windows PowerShell
- **Module**: Environment Setup
- **Mô tả lỗi**: Gõ nhầm `requirenments.txt` thay vì `requirements.txt`
- **Error message**: `error: File not found: \`requirenments.txt\``
- **Nguyên nhân**: Lỗi đánh máy
- **Giải pháp**: Sử dụng đường dẫn đầy đủ để tránh nhầm lẫn
- **Trạng thái**: [x] Đã giải quyết

### [2026-03-20] LSP Warning trong spark_config.py
- **Môi trường**: VS Code / PyLance
- **Module**: src/utils/spark_config.py
- **Mô tả lỗi**: LSP/Type checker báo lỗi type trên dòng `SparkSession.builder` nhưng code chạy bình thường
- **Nguyên nhân**: PySpark sử dụng method chaining pattern không được PyLance nhận diện đúng
- **Giải pháp**: Ignore warning này, code vẫn hoạt động tốt
- **Trạng thái**: [x] Đã xác nhận (Không cần fix)

### [2026-03-20] .venv activation error khi chưa tạo venv
- **Môi trường**: Windows PowerShell
- **Module**: Environment Setup
- **Mô tả lỗi**: `.venv\Scripts\activate` báo lỗi "The module '.venv' could not be loaded"
- **Error message**: `The module '.venv' could not be loaded. For more information, run 'Import-Module .venv'.`
- **Nguyên nhân**: Chưa tạo virtual environment trước khi activate
- **Giải pháp**: Chạy `uv venv` trước, sau đó mới activate
- **Trạng thái**: [x] Đã giải quyết

---

## Lỗi chưa giải quyết

*(Danh sách các lỗi đang chờ xử lý)*

---

## Lỗi đang xử lý

*(Danh sách các lỗi đang được giải quyết)*

---

## Lỗi đã giải quyết

| Ngày | Lỗi | Giải pháp |
|------|------|-----------|
| 2026-03-20 | UV commands sai trên PowerShell | Dùng `uv pip` prefix, PowerShell cmdlets |
| 2026-03-20 | Typo `requirenments.txt` | Dùng đường dẫn đầy đủ |
| 2026-03-20 | LSP Warning trong spark_config.py | Xác nhận là false positive |
| 2026-03-20 | .venv activation trước khi tạo | Tạo venv bằng `uv venv` trước |

---

## Khó khăn chung

| Ngày | Khó khăn | Ảnh hưởng | Trạng thái |
|------|----------|-----------|-------------|
| 2026-03-20 | Đồng bộ folder structure giữa các máy | Cần clear structure để team cùng làm | ✅ Đã giải quyết |
| 2026-03-20 | UV commands khác nhau Linux vs Windows | Cần hướng dẫn riêng cho Windows | ✅ Đã xác định |

---

## Checklist tiến độ

- [x] Setup môi trường (requirements.txt, UV setup)
- [x] Tạo folder structure đồng bộ
- [x] Core files (README, CONTRIBUTING, pyproject.toml)
- [x] Spark config utilities
- [x] UV Environment Setup (182 packages installed)
- [ ] EDA toàn bộ dataset (10 CSV files)
- [ ] Join tables + xử lý missing values
- [ ] RFM Feature Engineering
- [ ] Spark ML Pipeline
- [ ] Classification (5 models: LogReg, RF, NB, SVC, GBT)
- [ ] Regression (3 models: LinearReg, DT, RF)
- [ ] Clustering (3 models: K-Means, Bisecting, GMM)
- [ ] ALS Recommendation
- [ ] FP-Growth
- [ ] Streamlit Dashboard
- [ ] Integration (ML models → Streamlit)
- [ ] Báo cáo Word/PDF
- [ ] Presentation slides + Video demo

---

## Ghi chú quan trọng - UV trên Windows

### Lệnh UV đã test thành công trên Windows PowerShell:

```powershell
# 1. Tạo virtual environment
uv venv

# 2. Activate (PowerShell)
.\.venv\Scripts\Activate

# 3. Cài packages
uv pip install -r C:\full\path\to\requirements.txt

# 4. Kiểm tra packages đã cài
uv pip list

# 5. Xóa venv (PowerShell)
Remove-Item -Recurse -Force .venv
```

### Lệnh SAI (không hoạt động):
- `uv list` → phải là `uv pip list`
- `uv install` → phải là `uv pip install`
- `rm -rf .venv` → phải là `Remove-Item -Recurse -Force .venv`

---

## Cách sử dụng file này

- **Ngày**: Ngày gặp lỗi (format: YYYY-MM-DD)
- **Môi trường**: Môi trường xảy ra lỗi (Windows, Jupyter, Terminal, etc.)
- **Module**: Module/code đang làm việc (data, ml, app, etc.)
- **Người gặp**: Thành viên gặp lỗi

### Mẫu báo cáo lỗi mới
```
### [YYYY-MM-DD] Mô tả ngắn gọn lỗi
- **Môi trường**: 
- **Module**: 
- **Người gặp**: 
- **Mô tả lỗi**:
- **Error message**:
- **Nguyên nhân**:
- **Giải pháp**:
- **Trạng thái**: [ ] Chưa giải quyết [ ] Đang xử lý [ ] Đã giải quyết
- **Ghi chú thêm**:
```

---

*Last updated: 2026-03-20*
