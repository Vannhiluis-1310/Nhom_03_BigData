from setuptools import setup, find_packages

setup(
    name="nhom03-pyspark-project",
    version="1.0.0",
    description="Brazilian E-Commerce Analytics with Apache Spark MLlib",
    author="Nhom 03",
    author_email="nhom03@example.com",
    url="https://github.com/nhom03/bigdata-project",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.9",
    install_requires=[
        "pyspark==3.5.8",
        "pandas>=2.0.0",
        "numpy>=1.24.0",
        "scikit-learn>=1.3.0",
        "streamlit>=1.28.0",
        "plotly>=5.18.0",
        "seaborn>=0.12.0",
        "matplotlib>=3.7.0",
        "jupyter>=1.0.0",
        "ipykernel>=6.25.0",
        "python-dotenv>=1.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-spark>=0.6.0",
            "black>=23.0.0",
            "ruff>=0.1.0",
        ],
        "mlops": [
            "mlflow>=2.8.0",
            "great-expectations>=0.18.0",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)
