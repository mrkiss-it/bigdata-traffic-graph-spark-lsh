"""[K3] Vectorizer cửa sổ trượt + smoke-test Exact KNN vs. Spark LSH vs. FAISS HNSW (Nhóm 11).

Vectorizer (PySpark):
    Đọc Parquet long-format theo Data Contract [K2] (timestamp, sensor_id, speed). Với mỗi cảm biến, gom
    `--window` mốc 5 phút liên tiếp (mặc định 12 mốc = 60 phút) thành một vector 12 chiều, đơn vị mph.
    Cửa sổ chỉ được giữ khi:
      - đủ `--window` giá trị không null/NaN và trải đúng (window-1)*5 phút. Cửa sổ đi qua chỗ hở thời gian
        (vd giờ DST bị thiếu của PEMS-BAY 2017-03-12 02:00–02:55) bị loại;
      - không chứa 0.0 (tín hiệu mất kết nối, xem gigo_audit_report.md). Dùng --keep-zero với dữ liệu đã
        làm sạch/chuẩn hóa, nơi 0.0 có thể là giá trị hợp lệ.
    Cửa sổ lấy theo bước `--stride` mốc, căn theo đồng hồ chung để mọi cảm biến có cùng mốc kết thúc.
    Mặc định stride = window (không chồng lấn), tránh các cửa sổ lệch nhau 5 phút gần như trùng nhau.

So sánh (khoảng cách Euclid L2, k = --k):
    exact_knn   faiss.IndexFlatL2 vét cạn lấy REFINE_FACTOR*k ứng viên, sau đó tính lại khoảng cách các ứng viên
                bằng float64 và giữ k phần tử gần nhất (xem run_exact()). Ground truth tính riêng bằng numpy
                float64; các chỉ mục FAISS dùng vector đã trừ trung bình (xem center()).
    spark_lsh   pyspark.ml.feature.BucketedRandomProjectionLSH, truy vấn từng vector bằng
                approxNearestNeighbors (mỗi truy vấn là một Spark job quét toàn bộ tập đã băm).
    spark_lsh_join  cùng mô hình LSH, truy vấn theo lô bằng approxSimilarityJoin (equi-join theo bucket),
                lấy top-k mỗi truy vấn. Ngưỡng khoảng cách mặc định là phân vị --join-quantile của khoảng cách
                láng giềng thứ k chính xác, tức là được hiệu chỉnh từ ground truth.
    faiss_hnsw  faiss.IndexHNSWFlat (M = --hnsw-m, efConstruction = --ef-construction), quét efSearch.

candidate_frac là tỷ lệ vector chỉ mục phải tính khoảng cách thật cho mỗi truy vấn (1.0 với vét cạn;
LSH: vector trùng bucket với truy vấn ở ít nhất một bảng băm; HNSW: số lần tính khoảng cách / n_base).

Recall@k tính theo khoảng cách để không phạt các vector trùng nhau: một kết quả được tính là đúng khi khoảng
cách của nó không lớn hơn khoảng cách láng giềng thứ k chính xác (cộng sai số 1e-3 mph).
Truy vấn lấy ngẫu nhiên từ tập cửa sổ và được loại khỏi tập chỉ mục.

Chạy:
    python 03_benchmarks_similarity/smoke_test_lsh_faiss.py
    python 03_benchmarks_similarity/smoke_test_lsh_faiss.py --n-base 10000 --n-queries 200 --lsh-queries 50
    python 03_benchmarks_similarity/smoke_test_lsh_faiss.py --input data/interim/metr_la_cleaned.parquet --keep-zero

Mã thoát: 0 nếu mọi phương pháp chạy xong và exact_knn tự khớp ground truth (recall = 1.0), 1 nếu ngược lại.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
PROJECT = HERE.parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(PROJECT / "02_pyspark_gigo"))

from benchmark_profiler import append_csv, environment_info, measure  # noqa: E402

STEP_SECONDS = 300  # lưới 5 phút của METR-LA / PEMS-BAY
CLEANED_SOURCE = PROJECT / "data/interim/metr_la_cleaned.parquet"
RAW_SOURCE = PROJECT / "data/interim/metr_la_sample.parquet"
DEFAULT_OUTPUT = HERE / "smoke_lsh_faiss_results.csv"
DIST_EPS = 1e-3
REFINE_FACTOR = 4
RESULT_FIELDS = [
    "run_id", "timestamp_utc", "dataset", "n_windows", "n_base", "n_queries", "dim", "k",
    "method", "params", "status", "build_s", "index_mb", "build_peak_rss_mb",
    "queries_timed", "recall_at_k", "candidate_frac", "lat_mean_ms", "lat_p50_ms", "lat_p95_ms", "qps",
]


# --------------------------------------------------------------------------- #
# Vectorizer cửa sổ trượt (PySpark)
# --------------------------------------------------------------------------- #
def default_source() -> Path:
    """Ưu tiên dữ liệu sạch của [T5]; chưa có thì dùng Parquet thô của [K2] (loại cửa sổ chứa 0.0)."""
    return CLEANED_SOURCE if CLEANED_SOURCE.exists() else RAW_SOURCE


def to_uri(path: str | Path) -> str:
    path = str(path)
    return path if "://" in path else Path(path).resolve().as_uri()


def sliding_windows(spark, source: str | Path, window: int = 12, stride: int | None = None,
                    keep_zero: bool = False):
    """DataFrame (sensor_id, start_ts, end_ts, values: array<double> độ dài `window`)."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    stride = stride or window
    # Dùng timestampdiff trên TIMESTAMP_NTZ để không phụ thuộc spark.sql.session.timeZone (tránh lệch DST).
    slot = F.expr(
        f"floor(timestampdiff(SECOND, TIMESTAMP_NTZ '1970-01-01 00:00:00', cast(timestamp as timestamp_ntz)) / {STEP_SECONDS})"
    )
    w = Window.partitionBy("sensor_id").orderBy("timestamp").rowsBetween(-(window - 1), 0)
    df = (
        spark.read.parquet(to_uri(source))
        .select("sensor_id", "timestamp", "speed")
        .withColumn("speed", F.when(F.isnan("speed"), None).otherwise(F.col("speed")))
        .select(
            "sensor_id",
            F.col("timestamp").alias("end_ts"),
            slot.alias("slot"),
            F.min("timestamp").over(w).alias("start_ts"),
            F.collect_list("speed").over(w).alias("values"),  # bỏ qua null -> cửa sổ thiếu bị loại
        )
        .where(((F.col("slot") + 1) % stride) == 0)
        .where(F.size("values") == window)
        .where(
            F.expr("timestampdiff(SECOND, cast(start_ts as timestamp_ntz), cast(end_ts as timestamp_ntz))")
            == (window - 1) * STEP_SECONDS
        )
    )
    if not keep_zero:
        df = df.where(F.forall("values", lambda x: x != 0.0))
    return df.select("sensor_id", "start_ts", "end_ts", "values")


def windows_to_numpy(df, window: int, scratch: str | Path | None = None) -> np.ndarray:
    """Thu cửa sổ về driver thành ma trận float32 (n, window), thứ tự (end_ts, sensor_id).

    Không dùng toPandas(): cột array<double> thành mảng object (một ndarray mỗi dòng) và kết quả phải nằm
    trọn trong heap JVM driver trước khi sang Python. Với stride 1, cách đó cần ~6,3 GB RAM cho 6,3M cửa sổ
    METR-LA và hết bộ nhớ với ~16M cửa sổ PEMS-BAY ([K4]). Ở đây executor ghi `window` cột float32 ra Parquet
    tạm (sắp xếp toàn cục, mỗi part-file một khoảng liên tiếp), rồi pyarrow đọc thẳng từng file vào ma trận
    đã cấp phát sẵn.
    """
    import shutil
    import tempfile

    import pyarrow.parquet as pq
    from pyspark.sql import functions as F

    tmp_root = Path(scratch) if scratch else PROJECT / "data/interim/bench/tmp"
    tmp_root.mkdir(parents=True, exist_ok=True)
    out = Path(tempfile.mkdtemp(prefix="windows_", dir=tmp_root))
    try:
        cols = [F.col("values")[i].cast("float").alias(f"v{i}") for i in range(window)]
        df.orderBy("end_ts", "sensor_id").select(*cols).write.mode("overwrite").parquet(to_uri(out))
        files = sorted(out.glob("part-*.parquet"))
        n = sum(pq.ParquetFile(f).metadata.num_rows for f in files)
        if n == 0:
            raise RuntimeError("Không tạo được cửa sổ nào; kiểm tra nguồn dữ liệu và --keep-zero")
        x = np.empty((n, window), dtype=np.float32)
        pos = 0
        for f in files:
            table = pq.read_table(f)
            for j in range(window):
                x[pos:pos + table.num_rows, j] = table.column(j).to_numpy()
            pos += table.num_rows
        return x
    finally:
        shutil.rmtree(out, ignore_errors=True)


def split_base_queries(x: np.ndarray, n_base: int | None, n_queries: int, seed: int):
    """Lấy ngẫu nhiên n_queries truy vấn rồi n_base vector chỉ mục từ phần còn lại (không giao nhau)."""
    rng = np.random.default_rng(seed)
    order = rng.permutation(len(x))
    n_base = len(x) - n_queries if n_base is None else n_base
    if n_queries + n_base > len(x):
        raise ValueError(f"Cần {n_queries + n_base} cửa sổ nhưng chỉ có {len(x)}")
    return x[order[n_queries:n_queries + n_base]], x[order[:n_queries]]


# --------------------------------------------------------------------------- #
# Đánh giá
# --------------------------------------------------------------------------- #
def recall_at_k(found_dists: list[np.ndarray], true_kth: np.ndarray, k: int) -> float:
    hits = [np.count_nonzero(d[:k] <= t + DIST_EPS) for d, t in zip(found_dists, true_kth)]
    return float(np.sum(hits) / (k * len(true_kth)))


def latency_stats(seconds: list[float]) -> dict:
    ms = np.asarray(seconds) * 1000
    return {
        "queries_timed": len(ms),
        "lat_mean_ms": round(float(ms.mean()), 4),
        "lat_p50_ms": round(float(np.percentile(ms, 50)), 4),
        "lat_p95_ms": round(float(np.percentile(ms, 95)), 4),
        "qps": round(1000 / float(ms.mean()), 2),
    }


def faiss_index_mb(index) -> float:
    import faiss

    return round(faiss.serialize_index(index).size / 1024 ** 2, 3)


def search_one_by_one(index, queries: np.ndarray, k: int) -> tuple[list[np.ndarray], list[float]]:
    dists, times = [], []
    for q in queries:
        start = time.perf_counter()
        d, _ = index.search(q[None, :], k)
        times.append(time.perf_counter() - start)
        dists.append(np.sqrt(np.maximum(d[0], 0)))  # faiss trả về bình phương L2
    return dists, times


def exact_kth(base: np.ndarray, queries: np.ndarray, k: int) -> np.ndarray:
    """Ground truth: khoảng cách láng giềng thứ k, vét cạn bằng numpy float64."""
    b = base.astype(np.float64)
    return np.array([np.partition(np.sqrt(((b - q) ** 2).sum(axis=1)), k - 1)[k - 1]
                     for q in queries.astype(np.float64)])


def center(base: np.ndarray, queries: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Trừ vector trung bình của tập chỉ mục. Khoảng cách L2 không đổi nhưng chuẩn ‖x‖ nhỏ đi, giảm sai số
    float32 của FAISS (IndexFlatL2 tính ‖x‖² + ‖y‖² − 2x·y; với vận tốc ~58 mph sai số lên tới ~1e-3)."""
    mu = base.astype(np.float64).mean(axis=0)
    return (np.ascontiguousarray((base - mu).astype(np.float32)),
            np.ascontiguousarray((queries - mu).astype(np.float32)))


def run_exact(base: np.ndarray, queries: np.ndarray, true_kth: np.ndarray, k: int,
              raw_base: np.ndarray | None = None, raw_queries: np.ndarray | None = None) -> dict:
    """Vét cạn FAISS + tinh chỉnh float64. Riêng IndexFlatL2 (float32, khai triển chuẩn) đảo thứ tự các láng giềng
    cách nhau < ~1e-3 mph, nên trên PEMS-BAY 1,4M vector chỉ đạt recall 0,995 so với ground truth float64."""
    import faiss

    with measure() as m:
        index = faiss.IndexFlatL2(base.shape[1])
        index.add(base)
    index.search(queries[:10], k)  # khởi động
    ref_base = base if raw_base is None else raw_base
    ref_queries = queries if raw_queries is None else raw_queries
    dists, times = [], []
    for q, q_ref in zip(queries, ref_queries):
        start = time.perf_counter()
        _, ids = index.search(q[None, :], REFINE_FACTOR * k)
        cand = ref_base[ids[0][ids[0] >= 0]].astype(np.float64)
        d = np.sort(np.sqrt(((cand - q_ref.astype(np.float64)) ** 2).sum(axis=1)))[:k]
        times.append(time.perf_counter() - start)
        dists.append(d)
    return {
        "method": "exact_knn", "params": f"faiss.IndexFlatL2+refine_fp64(x{REFINE_FACTOR})",
        "build_s": round(m.wall_s, 4),
        "index_mb": faiss_index_mb(index), "build_peak_rss_mb": round(m.peak_rss_mb, 1),
        "recall_at_k": recall_at_k(dists, true_kth, k), "candidate_frac": 1.0, **latency_stats(times),
    }


def run_hnsw(base: np.ndarray, queries: np.ndarray, true_kth: np.ndarray, k: int, m_links: int,
             ef_construction: int, ef_search: list[int]) -> list[dict]:
    import faiss

    with measure() as m:
        index = faiss.IndexHNSWFlat(base.shape[1], m_links)
        index.hnsw.efConstruction = ef_construction
        index.add(base)
    size_mb = faiss_index_mb(index)
    rows = []
    for ef in ef_search:
        index.hnsw.efSearch = ef
        index.search(queries[:10], k)  # khởi động
        faiss.cvar.hnsw_stats.reset()
        dists, times = search_one_by_one(index, queries, k)
        ndis = faiss.cvar.hnsw_stats.ndis / (len(queries) * len(base))
        rows.append({
            "method": "faiss_hnsw", "params": f"M={m_links};efConstruction={ef_construction};efSearch={ef}",
            "build_s": round(m.wall_s, 4), "index_mb": size_mb, "build_peak_rss_mb": round(m.peak_rss_mb, 1),
            "recall_at_k": recall_at_k(dists, true_kth, k), "candidate_frac": round(ndis, 6),
            **latency_stats(times),
        })
    return rows


def spark_cached_mb(spark) -> float:
    infos = spark.sparkContext._jsc.sc().getRDDStorageInfo()
    return round(sum(info.memSize() + info.diskSize() for info in infos) / 1024 ** 2, 3)


def safe_unpersist(df) -> None:
    """Giải phóng cache đồng bộ; bỏ qua lỗi nếu JVM driver đã sập (vd sau OOM)."""
    if df is None:
        return
    try:
        df.unpersist(blocking=True)
    except Exception:  # noqa: BLE001 — không che mất bản ghi status=oom đã ghi nhận
        pass


def lsh_candidate_frac(model, base: np.ndarray, queries: np.ndarray, bucket_length: float) -> float:
    """Tái tạo hàm băm của Spark BRP, h_j(x) = floor(x·v_j / r), để đếm số ứng viên trùng bucket."""
    v = np.array([list(u.toArray()) for u in model._java_obj.randUnitVectors()])  # (numHashTables, dim)
    hb = np.floor(base.astype(np.float64) @ v.T / bucket_length)
    hq = np.floor(queries.astype(np.float64) @ v.T / bucket_length)
    return float(np.mean([(hb == h).any(axis=1).mean() for h in hq]))


def vectors_df(spark, x: np.ndarray, id_offset: int = 0):
    import pandas as pd
    from pyspark.ml.functions import array_to_vector

    pdf = pd.DataFrame({"id": np.arange(id_offset, id_offset + len(x)), "values": list(x.astype(np.float64))})
    return spark.createDataFrame(pdf).select("id", array_to_vector("values").alias("features"))


def run_spark_lsh(spark, base: np.ndarray, queries: np.ndarray, true_kth: np.ndarray, k: int,
                  bucket_length: float, num_tables: int, lsh_queries: int, join_quantile: float | None,
                  seed: int, join_skip_reason: str = "") -> list[dict]:
    """Xây mô hình BRP-LSH một lần rồi đo hai cách truy vấn: từng vector và join theo lô."""
    from pyspark.ml.feature import BucketedRandomProjectionLSH
    from pyspark.ml.linalg import Vectors
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    params = f"bucketLength={bucket_length:g};numHashTables={num_tables}"
    base_df = vectors_df(spark, base).repartition(spark.sparkContext.defaultParallelism).cache()
    hashed = None
    rows = []
    try:
        base_df.count()
        raw_mb = spark_cached_mb(spark)  # index_mb = phần cache tăng thêm của bảng đã băm (vector + hash)

        lsh = BucketedRandomProjectionLSH(inputCol="features", outputCol="hashes",
                                          bucketLength=bucket_length, numHashTables=num_tables, seed=seed)
        with measure() as m:
            model = lsh.fit(base_df)
            hashed = model.transform(base_df).cache()
            hashed.count()
        index_mb = round(spark_cached_mb(spark) - raw_mb, 3)
        common = {"build_s": round(m.wall_s, 4), "index_mb": index_mb, "build_peak_rss_mb": round(m.peak_rss_mb, 1)}
        cand = round(lsh_candidate_frac(model, base, queries, bucket_length), 6)

        # 1) approxNearestNeighbors: một Spark job cho mỗi truy vấn.
        if lsh_queries > 0:
            model.approxNearestNeighbors(hashed, Vectors.dense(queries[0].astype(float)), k).count()  # khởi động
            dists, times = [], []
            for q in queries[:lsh_queries]:
                start = time.perf_counter()
                res = model.approxNearestNeighbors(hashed, Vectors.dense(q.astype(float)), k, distCol="dist")
                found = np.sort([r["dist"] for r in res.select("dist").collect()])
                times.append(time.perf_counter() - start)
                dists.append(found)
            rows.append({"method": "spark_lsh", "params": params, **common, "candidate_frac": cand,
                         "recall_at_k": recall_at_k(dists, true_kth[:lsh_queries], k), **latency_stats(times)})

        # 2) approxSimilarityJoin: toàn bộ truy vấn trong một job, ngưỡng hiệu chỉnh từ ground truth.
        if join_quantile is not None and join_skip_reason:
            rows.append({"method": "spark_lsh_join", "params": f"{params};{join_skip_reason}", **common,
                         "status": "skipped", "candidate_frac": cand, "queries_timed": 0, "lat_mean_ms": ""})
        elif join_quantile is not None:
            threshold = float(np.quantile(true_kth, join_quantile))
            q_df = vectors_df(spark, queries).cache()
            try:
                q_df.count()
                row = {"method": "spark_lsh_join", "params": f"{params};threshold={threshold:.3f}(q{join_quantile:g})",
                       **common, "candidate_frac": cand, "queries_timed": len(queries)}
                start = time.perf_counter()
                try:
                    joined = (
                        model.approxSimilarityJoin(hashed, q_df, threshold, distCol="dist")
                        .select(F.col("datasetB.id").alias("qid"), "dist")
                        .withColumn("rank", F.row_number().over(Window.partitionBy("qid").orderBy("dist")))
                        .where(F.col("rank") <= k)
                        .collect()
                    )
                except Exception as exc:  # noqa: BLE001 — OOM của join là một kết quả cần ghi lại, không dừng cả lượt đo
                    msg = str(exc)
                    row["status"] = "oom" if "OutOfMemory" in msg or "UNABLE_TO_ACQUIRE_MEMORY" in msg else "error"
                    row["lat_mean_ms"] = round((time.perf_counter() - start) * 1000 / len(queries), 4)
                    print(f"   spark_lsh_join {params}: {row['status']} sau {time.perf_counter() - start:.1f}s")
                else:
                    ms = (time.perf_counter() - start) * 1000 / len(queries)
                    per_query: dict[int, list[float]] = {}
                    for r in joined:
                        per_query.setdefault(r["qid"], []).append(r["dist"])
                    dists = [np.sort(per_query.get(i, [])) for i in range(len(queries))]
                    row.update({"recall_at_k": recall_at_k(dists, true_kth, k), "lat_mean_ms": round(ms, 4),
                                "qps": round(1000 / ms, 2)})
                rows.append(row)
            finally:
                safe_unpersist(q_df)
    finally:
        safe_unpersist(hashed)
        safe_unpersist(base_df)
    return rows


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def add_common_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--input", default=None, help="Parquet long-format (mặc định: cleaned của [T5] nếu có, "
                                                 "không thì metr_la_sample.parquet của [K2])")
    p.add_argument("--keep-zero", action="store_true", help="Giữ cửa sổ chứa 0.0 (dữ liệu đã làm sạch)")
    p.add_argument("--window", type=int, default=12, help="Số mốc 5 phút mỗi vector (12 = 60 phút)")
    p.add_argument("--stride", type=int, default=None, help="Bước giữa hai cửa sổ, tính bằng mốc (mặc định = window)")
    p.add_argument("--n-queries", type=int, default=200)
    p.add_argument("--lsh-queries", type=int, default=50, help="Số truy vấn đo với approxNearestNeighbors")
    p.add_argument("-k", "--k", type=int, default=10)
    p.add_argument("--join-quantile", type=float, default=0.95,
                   help="Phân vị khoảng cách láng giềng thứ k làm ngưỡng approxSimilarityJoin (âm để bỏ qua)")
    p.add_argument("--ef-construction", type=int, default=40)
    p.add_argument("--ef-search", type=int, nargs="+", default=[16, 32, 64, 128])
    p.add_argument("--seed", type=int, default=11)
    p.add_argument("--master", default="local[*]")
    p.add_argument("--driver-memory", default="4g")
    p.add_argument("--shuffle-partitions", type=int, default=None,
                   help="spark.sql.shuffle.partitions; mặc định 8 × số lõi theo [K4] (48 trên VM 6 lõi). "
                        "[K1] đặt 8, quá ít cho approxSimilarityJoin quy mô lớn")


def validate_common_args(p: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if args.window < 1 or (args.stride is not None and args.stride < 1):
        p.error("--window và --stride phải >= 1")
    if args.n_queries < 1 or args.k < 1:
        p.error("--n-queries và -k phải >= 1")
    if args.lsh_queries < 0:
        p.error("--lsh-queries phải >= 0 (0 để bỏ qua approxNearestNeighbors)")
    if args.join_quantile > 1.0:
        p.error("--join-quantile phải <= 1.0 (hoặc âm để bỏ qua approxSimilarityJoin)")
    if args.shuffle_partitions is not None and args.shuffle_partitions < 1:
        p.error("--shuffle-partitions phải >= 1")
    if args.ef_construction < 1 or any(ef < 1 for ef in args.ef_search):
        p.error("--ef-construction và --ef-search phải >= 1")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    add_common_args(p)
    p.add_argument("--n-base", type=int, default=10_000, help="Số vector trong chỉ mục")
    p.add_argument("--bucket-length", type=float, default=4.0, help="Độ rộng bucket r của BRP-LSH")
    p.add_argument("--hash-tables", type=int, default=8, help="Số bảng băm L (OR-amplification)")
    p.add_argument("--hnsw-m", type=int, default=32, help="Số cạnh mỗi nút của đồ thị HNSW")
    p.add_argument("--output", default=str(DEFAULT_OUTPUT))
    args = p.parse_args()
    validate_common_args(p, args)
    if args.n_base < args.k:
        p.error("--n-base phải >= -k")
    if args.bucket_length <= 0 or args.hash_tables < 1 or args.hnsw_m < 1:
        p.error("--bucket-length phải > 0, --hash-tables và --hnsw-m phải >= 1")
    return args


def build_session(args, app_name: str = "Nhom11-K3-lsh_faiss"):
    from spark_tuning import build_tuned_spark  # cấu hình [K1] + tinh chỉnh quy mô triệu dòng [K4]

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    return build_tuned_spark(args.master, args.driver_memory, app_name, args.shuffle_partitions)


def load_vectors(spark, args) -> tuple[np.ndarray, str, float]:
    source = Path(args.input) if args.input else default_source()
    keep_zero = args.keep_zero
    start = time.perf_counter()
    x = windows_to_numpy(sliding_windows(spark, source, args.window, args.stride, keep_zero), args.window)
    elapsed = time.perf_counter() - start
    print(f"Nguồn: {source} | keep_zero={keep_zero} | {len(x):,} cửa sổ × {x.shape[1]} chiều "
          f"| vectorize {elapsed:.1f}s")
    return x, source.name, elapsed


def evaluate(spark, x: np.ndarray, n_base: int | None, args) -> list[dict]:
    import faiss

    faiss.omp_set_num_threads(os.cpu_count() or 1)
    base, queries = split_base_queries(x, n_base, args.n_queries, args.seed)
    true_kth = exact_kth(base, queries, args.k)
    base_c, queries_c = center(base, queries)
    rows = [run_exact(base_c, queries_c, true_kth, args.k, raw_base=base, raw_queries=queries)]
    rows += run_spark_lsh(spark, base, queries, true_kth, args.k, args.bucket_length, args.hash_tables,
                          min(args.lsh_queries, len(queries)),
                          args.join_quantile if args.join_quantile >= 0 else None, args.seed)
    rows += run_hnsw(base_c, queries_c, true_kth, args.k, args.hnsw_m, args.ef_construction, args.ef_search)
    meta = {"n_base": len(base), "n_queries": len(queries), "dim": base.shape[1], "k": args.k}
    return with_status([{**meta, **r} for r in rows])


def fmt(value, spec: str) -> str:
    return format(value, spec) if isinstance(value, (int, float)) else "—"


def with_status(rows: list[dict]) -> list[dict]:
    return [{"status": "ok", "recall_at_k": "", "lat_p50_ms": "", "lat_p95_ms": "", "qps": "", **r} for r in rows]


def print_table(rows: list[dict]) -> None:
    print(f"\n{'n_base':>8} {'method':<15} {'params':<52} {'status':<6} {'build_s':>8} {'index_mb':>9} "
          f"{'recall':>7} {'cand':>8} {'lat_ms':>9} {'p95_ms':>9}")
    for r in rows:
        print(f"{r['n_base']:>8} {r['method']:<15} {r['params']:<52} {r['status']:<6} {r['build_s']:>8.3f} "
              f"{r['index_mb']:>9.3f} {fmt(r['recall_at_k'], '>7.3f')} {r['candidate_frac']:>8.4f} "
              f"{fmt(r['lat_mean_ms'], '>9.3f')} {fmt(r['lat_p95_ms'], '>9.3f')}")


def finish(rows: list[dict], output: str, dataset: str, n_windows: int) -> int:
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    env = environment_info()
    env["faiss"] = __import__("faiss").__version__
    out = [{"run_id": run_id, "timestamp_utc": run_id, "dataset": dataset, "n_windows": n_windows,
            **{f: r.get(f, "") for f in RESULT_FIELDS if f not in ("run_id", "timestamp_utc", "dataset", "n_windows")},
            **env} for r in rows]
    append_csv(output, out, RESULT_FIELDS + list(env.keys()))
    print_table(rows)
    print(f"\nĐã ghi {len(out)} dòng vào {output} (run_id={run_id})")
    exact_ok = all(r["recall_at_k"] == 1.0 for r in rows if r["method"] == "exact_knn")
    return 0 if exact_ok else 1


def main() -> int:
    args = parse_args()
    spark = build_session(args)
    try:
        x, dataset, _ = load_vectors(spark, args)
        rows = evaluate(spark, x, args.n_base, args)
    finally:
        spark.stop()
    return finish(rows, args.output, dataset, len(x))


if __name__ == "__main__":
    sys.exit(main())
