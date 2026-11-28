# [K1] Ghi chú cấu hình môi trường PySpark + Parquet + FAISS

> Người phụ trách: Nguyễn Duy Khang (`26410055`) · Kiểm thử: `02_pyspark_gigo/spark_env_check.py` → **6/6 PASS** (18/11/2026)

## 1. Phiên bản đã chốt

| Thành phần | Phiên bản | Ghi chú |
| :--- | :--- | :--- |
| Python | 3.12 | |
| Java | OpenJDK 17 | Spark 3.5 chạy JDK 8/11/17 |
| PySpark | 3.5.9 (`>=3.5,<4.0`) | Khớp Colab |
| PyArrow | 26.0.1 | Arrow bật, **tắt fallback** để lộ lỗi |
| pandas | 2.3.x (`<3.0`) | PySpark 3.5 chưa hỗ trợ pandas 3 |
| setuptools | bắt buộc | Python 3.12 bỏ `distutils`; PySpark 3.5 vẫn import nó |
| faiss-cpu | 1.16.0 | |

Cấu hình Spark chuẩn dùng chung (xem `K1_CONF` và `build_spark()` trong `spark_env_check.py`; cấu hình tinh chỉnh
cho lát cắt triệu dòng ở `spark_tuning.py`, xem [huong_dan_toi_uu_spark_va_lat_cat.md](huong_dan_toi_uu_spark_va_lat_cat.md)):

```python
.config("spark.driver.memory", "4g")                                   # chỉnh theo RAM máy
.config("spark.sql.execution.arrow.pyspark.enabled", "true")
.config("spark.sql.execution.arrow.pyspark.fallback.enabled", "false")
.config("spark.sql.parquet.compression.codec", "snappy")
.config("spark.sql.session.timeZone", "UTC")
```

## 2. Local (Linux / Ubuntu)

```bash
sudo apt-get install -y openjdk-17-jdk-headless python3-venv
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

export SPARK_DRIVER_MEMORY=4g                                # mặc định của script là 1g
python 02_pyspark_gigo/spark_env_check.py                    # local[*]
python 02_pyspark_gigo/spark_env_check.py --hdfs             # + HDFS (cần HADOOP_CONF_DIR) hoặc --hdfs hdfs://host:port
```

**Windows:** Spark cần `winutils.exe` + `hadoop.dll` và biến `HADOOP_HOME` thì mới ghi được Parquet
(lỗi `HADOOP_HOME and hadoop.home.dir are unset`). Nên chạy Spark trên Linux hoặc Colab.

## 3. Google Colab

```python
!apt-get -qq install -y openjdk-17-jdk-headless > /dev/null
import os; os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
!pip -q install "pyspark>=3.5,<4.0" "pandas>=2.2,<3.0" setuptools pyarrow faiss-cpu psutil
!python 02_pyspark_gigo/spark_env_check.py
```
