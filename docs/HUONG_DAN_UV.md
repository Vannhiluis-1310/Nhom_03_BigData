# Hướng dẫn sử dụng UV Package Manager

## UV là gì?

**UV** là một ultra-fast Python package installer và resolver, được viết bằng Rust. UV nhanh hơn pip từ 10 đến 100 lần và bao gồm tất cả các tính năng cần thiết cho Python project management.

### Tại sao nên dùng UV?

| Tính năng | UV | pip |
|-----------|-----|-----|
| Tốc độ | Cực nhanh (Rust) | Chậm |
| Lock file | Có | Không |
| Virtual env | Tích hợp sẵn | Cần virtualenv |
| Dependency resolution | Nhanh, chính xác | Chậm |
| Cross-platform | Windows, Mac, Linux | Windows, Mac, Linux |

---

## 1. Cài đặt UV

### Linux/Mac

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Windows (PowerShell)

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

### Sử dụng pip

```bash
pip install uv
```

### Sử dụng npm

```bash
npm install -g uv
```

---

## 2. Tạo và quản lý Virtual Environment

### Tạo virtual environment mới

```bash
# Tạo venv trong thư mục hiện tại
uv venv

# Tạo venv với tên tùy chỉnh
uv venv myenv

# Tạo venv với Python version cụ thể
uv venv --python 3.11 myenv
```

### Activate virtual environment

```bash
# Linux/Mac
source .venv/bin/activate

# Windows (Command Prompt)
.venv\Scripts\activate.bat

# Windows (PowerShell)
.venv\Scripts\Activate.ps1
```

### Deactivate

```bash
deactivate
```

---

## 3. Cài đặt Packages

### Cài đặt từ requirements.txt

```bash
uv pip install -r requirements.txt
```

### Cài đặt package đơn lẻ

```bash
uv pip install pyspark
uv pip install pandas>=2.0.0
uv pip install "numpy>=1.24,<2.0"
```

### Cài đặt nhiều packages

```bash
uv pip install pyspark pandas numpy scikit-learn streamlit
```

### Cài đặt với specific version

```bash
uv pip install pyspark==3.5.0
```

### Uninstall package

```bash
uv pip uninstall pyspark
uv pip uninstall pandas numpy
```

### List installed packages

```bash
uv pip freeze
```

---

## 4. Sync Project với Lock File

### Tạo lock file

```bash
uv pip freeze > requirements.txt
```

### Sync environment

```bash
# Đảm bảo environment khớp với requirements.txt
uv pip sync
```

---

## 5. Run Scripts với UV

### Chạy Python script trực tiếp

```bash
uv run python script.py
```

### Chạy với dependencies tạm thời

```bash
uv run --with pyspark python script.py
```

### Chạy Jupyter Notebook

```bash
uv run jupyter notebook
```

### Chạy Streamlit app

```bash
uv run streamlit run app/main.py
```

---

## 6. UV cho Project có pyproject.toml

### Cài đặt project editable mode

```bash
uv pip install -e .
```

### Cài đặt với dev dependencies

```bash
uv pip install -e ".[dev]"
uv pip install -e ".[mlops]"
```

### Cài đặt tất cả extras

```bash
uv pip install -e ".[dev,mlops]"
```

---

## 7. Common Workflow cho Dự án này

### Lần đầu clone repository

```bash
# Clone project
git clone <repo-url>
cd Nhom03_PySpark_ProjectCuoiKy

# Cài đặt UV (nếu chưa có)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Tạo virtual environment
uv venv

# Activate
source .venv/bin/activate

# Cài dependencies
uv pip install -r requirements.txt
```

### Khi bắt đầu làm việc

```bash
# Activate environment
source .venv/bin/activate

# Pull latest changes
git pull origin main
```

### Khi thêm package mới

```bash
# Cài package mới
uv pip install new-package

# Cập nhật requirements.txt
uv pip freeze > requirements.txt

# Commit changes
git add requirements.txt
git commit -m "chore: add new-package"
```

---

## 8. Troubleshooting

### Lỗi "uv: command not found"

Đảm bảo UV đã được thêm vào PATH. Thử:

```bash
# Reload shell
exec "$SHELL"

# Hoặc kiểm tra installation
which uv
uv --version
```

### Lỗi "Cannot find venv"

```bash
# Tạo venv mới
uv venv
```

### Lỗi permission (Linux)

```bash
# Thêm quyền execute
chmod +x ~/.local/bin/uv
```

---

## 9. So sánh UV và pip Commands

| pip | UV |
|-----|-----|
| `pip install pkg` | `uv pip install pkg` |
| `pip install -r req.txt` | `uv pip install -r req.txt` |
| `pip freeze > req.txt` | `uv pip freeze > req.txt` |
| `pip uninstall pkg` | `uv pip uninstall pkg` |
| `pip list` | `uv pip freeze` |
| `python -m venv venv` | `uv venv` |
| - | `uv run python script.py` |
| - | `uv run --with pkg script.py` |

---

## 10. Resources

- Documentation: https://docs.astral.sh/uv/
- GitHub: https://github.com/astral-sh/uv
- Discord: https://discord.gg/astral-sh
