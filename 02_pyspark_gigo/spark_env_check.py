"""[K1] Kiểm tra môi trường PySpark + PyArrow/Parquet + FAISS (+ HDFS tùy chọn).

Chạy:
    python 02_pyspark_gigo/spark_env_check.py                       # local
    python 02_pyspark_gigo/spark_env_check.py --hdfs                # + HDFS (fs.defaultFS)
    python 02_pyspark_gigo/spark_env_check.py --hdfs hdfs://localhost:9000 --driver-memory 2g

Các bước kiểm tra:
    1. Thông tin hệ thống: Python, Java, Spark, PyArrow, FAISS, số lõi CPU, RAM.
    2. Ghi/đọc Parquet nén snappy, phân vùng theo `year_month` (schema long-format mẫu).
    3. Arrow: chuyển đổi Spark <-> Pandas với Arrow bắt buộc (tắt fallback).
    4. FAISS: IndexFlatL2 tìm đúng chính vector truy vấn (recall@1 = 1.0).
    5. (Tùy chọn) HDFS: ghi/đọc Parquet trên HDFS.

Mã thoát: 0 nếu mọi bước PASS, 1 nếu có bước FAIL.
"""

from __future__ import annotations

import argparse
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import time
import traceback
from dataclasses import dataclass, field
from pathlib import Path

# Schema long-format chuẩn (khớp Data Contract dự kiến ở [K2]).
SAMPLE_ROWS = 200_000
N_SENSORS = 207
YEAR_MONTHS = ("2012-03", "2012-04")


@dataclass
class CheckResult:
    name: str
    ok: bool
    seconds: float
    detail: str = ""
    error: str = ""


@dataclass
class Report:
    results: list[CheckResult] = field(default_factory=list)

    def run(self, name: str, fn, *args, **kwargs):
        start = time.perf_counter()
        try:
            detail = fn(*args, **kwargs) or ""
            self.results.append(CheckResult(name, True, time.perf_counter() - start, detail))
            print(f"[PASS] {name} ({time.perf_counter() - start:.2f}s) {detail}")
        except Exception as exc:  # noqa: BLE001 — muốn ghi nhận mọi lỗi môi trường
            err = f"{type(exc).__name__}: {str(exc).splitlines()[0][:300]}"
            self.results.append(CheckResult(name, False, time.perf_counter() - start, error=err))
            print(f"[FAIL] {name}: {err}")
            if os.environ.get("ENV_CHECK_TRACEBACK"):
                traceback.print_exc()

    @property
    def ok(self) -> bool:
        return all(r.ok for r in self.results)


# --------------------------------------------------------------------------- #
# 1. Thông tin hệ thống
# --------------------------------------------------------------------------- #
def java_version() -> str:
    java_home = os.environ.get("JAVA_HOME")
    java = Path(java_home, "bin", "java") if java_home else shutil.which("java")
    if not java:
        raise RuntimeError("Không tìm thấy Java (đặt JAVA_HOME hoặc thêm java vào PATH; cần JDK 11/17)")
    out = subprocess.run([str(java), "-version"], capture_output=True, text=True, check=True)
    return (out.stderr or out.stdout).splitlines()[0]


def print_system_info() -> None:
    import psutil

    vm = psutil.virtual_memory()
    rows = [
        ("OS", f"{platform.system()} {platform.release()} ({platform.machine()})"),
        ("Python", sys.version.split()[0]),
        ("CPU (logical / physical)", f"{psutil.cpu_count()} / {psutil.cpu_count(logical=False)}"),
        ("RAM total / available", f"{vm.total / 2**30:.1f} GB / {vm.available / 2**30:.1f} GB"),
        ("JAVA_HOME", os.environ.get("JAVA_HOME", "<unset>")),
        ("HADOOP_HOME", os.environ.get("HADOOP_HOME", "<unset>")),
        ("HADOOP_CONF_DIR", os.environ.get("HADOOP_CONF_DIR", "<unset>")),
        ("PYSPARK_PYTHON", os.environ.get("PYSPARK_PYTHON", "<unset>")),
    ]
    width = max(len(k) for k, _ in rows)
    print("=" * 72)
    print("THÔNG TIN HỆ THỐNG")
    print("=" * 72)
    for k, v in rows:
        print(f"  {k:<{width}} : {v}")
    print("=" * 72)


def check_versions() -> str:
    import faiss
    import numpy
    import pandas
    import pyarrow
    import pyspark

    return (
        f"java='{java_version()}' pyspark={pyspark.__version__} pyarrow={pyarrow.__version__} "
        f"faiss={faiss.__version__} numpy={numpy.__version__} pandas={pandas.__version__}"
    )


# --------------------------------------------------------------------------- #
# 2–3. Spark: Parquet + Arrow
# --------------------------------------------------------------------------- #
K1_CONF = {
    "spark.sql.execution.arrow.pyspark.enabled": "true",
    # Tắt fallback để lỗi Arrow lộ ra thay vì âm thầm quay về đường chậm.
    "spark.sql.execution.arrow.pyspark.fallback.enabled": "false",
    "spark.sql.parquet.compression.codec": "snappy",
    "spark.sql.session.timeZone": "UTC",
    "spark.sql.shuffle.partitions": "8",
    "spark.ui.showConsoleProgress": "false",
}


def build_spark(master: str, driver_memory: str, conf: dict[str, str] | None = None,
                app_name: str = "Nhom11-K1-spark_env_check"):
    """SparkSession với cấu hình chuẩn K1_CONF; `conf` ghi đè/bổ sung (xem spark_tuning.py, [K4])."""
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.master(master).appName(app_name).config("spark.driver.memory", driver_memory)
    for key, value in {**K1_CONF, **(conf or {})}.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def sample_long_df(spark, rows: int):
    """DataFrame long-format giả lập METR-LA: timestamp, sensor_id, speed, year_month, hour, day_of_week."""
    from pyspark.sql import functions as F

    start_ts = 1330560000  # 2012-03-01 00:00:00 UTC (mốc bắt đầu METR-LA)
    n_steps = rows // N_SENSORS + 1
    return (
        spark.range(rows)
        .withColumn("sensor_id", (F.col("id") % N_SENSORS + 773_000).cast("string"))
        .withColumn("step", (F.col("id") / N_SENSORS).cast("long"))
        # Trải đều mốc 5 phút qua cả YEAR_MONTHS để có >= 2 partition.
        .withColumn(
            "timestamp",
            F.timestamp_seconds(F.lit(start_ts) + F.col("step") * F.lit(int(61 * 86400 / n_steps))),
        )
        .withColumn("speed", F.round(F.lit(65.0) - F.rand(seed=11) * 40, 2).cast("float"))
        .withColumn("year_month", F.date_format("timestamp", "yyyy-MM"))
        .withColumn("hour", F.hour("timestamp").cast("byte"))
        .withColumn("day_of_week", F.dayofweek("timestamp").cast("byte"))
        .drop("id", "step")
    )


def check_parquet_rw(spark, out_uri: str, rows: int, local_dir: Path | None) -> str:
    df = sample_long_df(spark, rows)
    df.write.mode("overwrite").partitionBy("year_month").parquet(out_uri)

    back = spark.read.parquet(out_uri)
    n = back.count()
    if n != rows:
        raise AssertionError(f"Số dòng đọc lại {n} != {rows}")
    parts = sorted(r.year_month for r in back.select("year_month").distinct().collect())
    if parts != list(YEAR_MONTHS):
        raise AssertionError(f"Partition year_month = {parts}, kỳ vọng {list(YEAR_MONTHS)}")
    expected_cols = {"timestamp", "sensor_id", "speed", "year_month", "hour", "day_of_week"}
    if set(back.columns) != expected_cols:
        raise AssertionError(f"Cột = {back.columns}")

    detail = f"rows={n:,} partitions={parts}"
    if local_dir is not None:  # Xác minh codec bằng metadata PyArrow (chỉ với file cục bộ)
        import pyarrow.parquet as pq

        files = list(local_dir.rglob("*.parquet"))
        codecs = {pq.ParquetFile(f).metadata.row_group(0).column(0).compression for f in files}
        if codecs != {"SNAPPY"}:
            raise AssertionError(f"Codec = {codecs}, kỳ vọng SNAPPY")
        size_mb = sum(f.stat().st_size for f in files) / 2**20
        detail += f" files={len(files)} size={size_mb:.2f}MB codec=SNAPPY"
    return detail


def check_arrow(spark) -> str:
    import numpy as np
    import pandas as pd

    pdf = pd.DataFrame({"sensor_id": np.arange(50_000) % N_SENSORS, "speed": np.random.rand(50_000) * 70})
    sdf = spark.createDataFrame(pdf)  # pandas -> Spark qua Arrow
    agg = sdf.groupBy("sensor_id").avg("speed").toPandas()  # Spark -> pandas qua Arrow
    if len(agg) != N_SENSORS:
        raise AssertionError(f"groupBy trả {len(agg)} nhóm, kỳ vọng {N_SENSORS}")
    return f"pandas->spark->pandas OK ({len(pdf):,} rows, {len(agg)} groups, fallback=disabled)"


# --------------------------------------------------------------------------- #
# 4. FAISS
# --------------------------------------------------------------------------- #
def check_faiss(n: int = 10_000, d: int = 12, k: int = 5) -> str:
    import faiss
    import numpy as np

    rng = np.random.default_rng(11)
    xb = rng.random((n, d), dtype=np.float32)
    index = faiss.IndexFlatL2(d)
    index.add(xb)
    q_ids = rng.choice(n, size=100, replace=False)
    _, idx = index.search(xb[q_ids], k)
    recall1 = float((idx[:, 0] == q_ids).mean())
    if recall1 != 1.0:
        raise AssertionError(f"IndexFlatL2 recall@1 = {recall1}, kỳ vọng 1.0")
    return f"IndexFlatL2 ntotal={index.ntotal:,} d={d} recall@1={recall1:.2f} omp_threads={faiss.omp_get_max_threads()}"


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--master", default="local[*]", help="Spark master (mặc định local[*])")
    p.add_argument("--driver-memory", default=os.environ.get("SPARK_DRIVER_MEMORY", "1g"))
    p.add_argument("--rows", type=int, default=SAMPLE_ROWS, help="Số dòng mẫu cho bài test Parquet")
    p.add_argument(
        "--hdfs",
        nargs="?",
        const="default",
        default=None,
        help="Kiểm tra thêm HDFS. Không kèm giá trị = dùng fs.defaultFS; hoặc truyền URI, vd hdfs://localhost:9000",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    print_system_info()
    report = Report()
    report.run("Phiên bản thư viện & Java", check_versions)
    report.run("FAISS IndexFlatL2", check_faiss)

    holder: dict = {}

    def init_spark() -> str:
        holder["spark"] = build_spark(args.master, args.driver_memory)
        sc = holder["spark"].sparkContext
        return (
            f"spark={holder['spark'].version} master={sc.master} "
            f"defaultParallelism={sc.defaultParallelism} driver.memory={args.driver_memory}"
        )

    def hdfs_base(spark) -> str:
        default_fs = spark.sparkContext._jsc.hadoopConfiguration().get("fs.defaultFS")
        base = default_fs if args.hdfs == "default" else args.hdfs
        if not base or not base.startswith("hdfs://"):
            raise RuntimeError(f"fs.defaultFS = {base!r}; đặt HADOOP_CONF_DIR hoặc truyền --hdfs hdfs://host:port")
        return base.rstrip("/")

    tmp = Path(tempfile.mkdtemp(prefix="k1_env_check_"))
    report.run("Khởi tạo SparkSession", init_spark)
    spark = holder.get("spark")
    try:
        if spark is not None:
            local_out = tmp / "metr_la_env_check.parquet"
            report.run("Parquet snappy + partitionBy(year_month) [local]",
                       check_parquet_rw, spark, local_out.resolve().as_uri(), args.rows, local_out)
            report.run("Arrow pandas <-> Spark", check_arrow, spark)
            if args.hdfs:
                report.run("Parquet snappy + partitionBy(year_month) [HDFS]",
                           lambda: check_parquet_rw(
                               spark, f"{hdfs_base(spark)}/tmp/k1_env_check/metr_la_env_check.parquet",
                               args.rows, None))
    finally:
        if spark is not None:
            spark.stop()
        shutil.rmtree(tmp, ignore_errors=True)

    print("=" * 72)
    passed = sum(r.ok for r in report.results)
    print(f"KẾT QUẢ: {passed}/{len(report.results)} PASS")
    for r in report.results:
        if not r.ok:
            print(f"  - FAIL {r.name}: {r.error}")
    if not report.ok and platform.system() == "Windows" and not os.environ.get("HADOOP_HOME"):
        print("  Gợi ý: Spark trên Windows cần winutils.exe + hadoop.dll và HADOOP_HOME để ghi Parquet.")
    print("=" * 72)
    return 0 if report.ok else 1


if __name__ == "__main__":
    sys.exit(main())
