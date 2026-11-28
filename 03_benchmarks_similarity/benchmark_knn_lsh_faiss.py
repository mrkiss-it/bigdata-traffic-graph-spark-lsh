"""[K6] Đối chứng Exact KNN vs. Spark LSH vs. FAISS HNSW theo quy mô tập vector (Nhóm 11, Tuần 6).

Dùng lại vectorizer cửa sổ trượt và các hàm đo của smoke_test_lsh_faiss.py ([K3]); script này bổ sung:
    - Quét quy mô tập chỉ mục --sizes (mặc định 10K, 100K và "all" = toàn bộ cửa sổ trừ truy vấn).
      Cùng một tập truy vấn cho mọi quy mô và mọi phương pháp (lấy ngẫu nhiên, loại khỏi chỉ mục).
    - Quét cấu hình Spark LSH --lsh-configs "bucketLength:numHashTables" và FAISS HNSW --hnsw-m × --ef-search.
    - Nhiều bộ dữ liệu: --input nhận nhiều đường dẫn Parquet long-format (vd METR-LA và PEMS-BAY).
    - Ghi CSV knn_lsh_faiss_results.csv sau mỗi mốc quy mô (lỗi ở mốc sau không làm mất kết quả mốc trước)
      và in bảng Markdown để dán vào nhật ký đo đạc. approxSimilarityJoin hết bộ nhớ ghi status=oom.

Chỉ số mỗi dòng: build_s (thời gian lập chỉ mục), index_mb, recall_at_k, candidate_frac, độ trễ mỗi truy vấn
(mean/p50/p95 ms), qps. Định nghĩa chi tiết ở docstring của smoke_test_lsh_faiss.py.

Chạy:
    python 03_benchmarks_similarity/benchmark_knn_lsh_faiss.py
    python 03_benchmarks_similarity/benchmark_knn_lsh_faiss.py --sizes 10000 100000 all --lsh-configs 4:8 8:8 8:16
    python 03_benchmarks_similarity/benchmark_knn_lsh_faiss.py \\
        --input data/interim/metr_la_sample.parquet data/interim/pems_bay_sample.parquet --sizes all
    python 03_benchmarks_similarity/benchmark_knn_lsh_faiss.py --stride 1 --sizes 1000000 all   # mẻ lớn Tuần 7

Mã thoát: 0 nếu exact_knn khớp ground truth (recall = 1.0) ở mọi quy mô, 1 nếu ngược lại.
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
sys.path.insert(0, str(HERE))

import smoke_test_lsh_faiss as k3  # noqa: E402
from benchmark_profiler import append_csv, environment_info  # noqa: E402

DEFAULT_OUTPUT = HERE / "knn_lsh_faiss_results.csv"
FIELDS = k3.RESULT_FIELDS[:4] + ["stride", "vectorize_s"] + k3.RESULT_FIELDS[4:]


def parse_lsh_config(text: str) -> tuple[float, int]:
    try:
        r, tables = text.split(":")
        r_val, t_val = float(r), int(tables)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Cấu hình LSH phải có dạng bucketLength:numHashTables, nhận '{text}'") from exc
    if r_val <= 0 or t_val < 1:
        raise argparse.ArgumentTypeError(f"Cấu hình LSH cần bucketLength > 0 và numHashTables >= 1, nhận '{text}'")
    return r_val, t_val


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0], conflict_handler="resolve")
    k3.add_common_args(p)
    p.set_defaults(lsh_queries=30)
    p.add_argument("--input", nargs="+", default=None, help="Một hoặc nhiều Parquet long-format")
    p.add_argument("--sizes", nargs="+", default=["10000", "100000", "all"], help="Số vector chỉ mục hoặc 'all'")
    p.add_argument("--lsh-configs", nargs="+", type=parse_lsh_config, default=None,
                   help="Danh sách bucketLength:numHashTables (mặc định 2:8 4:8 8:8)")
    p.add_argument("--hnsw-m", type=int, nargs="+", default=[16, 32])
    p.add_argument("--skip-lsh", action="store_true", help="Chỉ chạy exact_knn và FAISS HNSW")
    p.add_argument("--join-max-base", type=int, default=600_000,
                   help="Bỏ qua approxSimilarityJoin khi n_base lớn hơn ngưỡng này (status=skipped). Với "
                        "driver 4g, join trên PEMS-BAY 1,41M vector hết bộ nhớ và làm sập JVM driver")
    p.add_argument("--output", default=str(DEFAULT_OUTPUT))
    args = p.parse_args()
    k3.validate_common_args(p, args)
    if any(m < 1 for m in args.hnsw_m):
        p.error("--hnsw-m phải >= 1")
    if args.join_max_base < 0:
        p.error("--join-max-base phải >= 0")
    for s in args.sizes:
        if s != "all":
            try:
                n = int(s)
            except ValueError:
                p.error(f"--sizes không hợp lệ: '{s}' (cần số nguyên hoặc 'all')")
            if n < args.k:
                p.error(f"--sizes {n} phải >= -k ({args.k})")
    args.lsh_configs = args.lsh_configs or [(2.0, 8), (4.0, 8), (8.0, 8)]
    return args


def resolve_size(size: str, available: int, k: int = 1) -> int:
    n = available if size == "all" else int(size)
    if n < k:
        raise ValueError(f"--sizes {size} ({n:,}) nhỏ hơn -k ({k})")
    if n > available:
        raise ValueError(f"--sizes {size} vượt số cửa sổ khả dụng ({available:,})")
    return n


def run_dataset(spark, source: Path, args, save) -> list[dict]:
    import faiss

    start = time.perf_counter()
    x = k3.windows_to_numpy(k3.sliding_windows(spark, source, args.window, args.stride, args.keep_zero),
                            args.window)
    vectorize_s = round(time.perf_counter() - start, 2)
    print(f"\n=== {source.name}: {len(x):,} cửa sổ × {x.shape[1]} chiều | vectorize {vectorize_s}s ===")

    # Cố định tập truy vấn; tập chỉ mục quy mô n là n phần tử đầu của phần còn lại (lồng nhau theo quy mô).
    pool, queries = k3.split_base_queries(x, None, args.n_queries, args.seed)
    faiss.omp_set_num_threads(os.cpu_count() or 1)
    rows = []
    for size in args.sizes:
        n = resolve_size(size, len(pool), args.k)
        base = np.ascontiguousarray(pool[:n])
        t0 = time.perf_counter()
        true_kth = k3.exact_kth(base, queries, args.k)
        print(f"-- n_base={n:,}: ground truth {time.perf_counter() - t0:.1f}s", flush=True)
        base_c, queries_c = k3.center(base, queries)
        size_rows = [k3.run_exact(base_c, queries_c, true_kth, args.k, raw_base=base, raw_queries=queries)]
        if not args.skip_lsh:
            for r, tables in args.lsh_configs:
                skip = f"n_base>{args.join_max_base}" if n > args.join_max_base else ""
                size_rows += k3.run_spark_lsh(spark, base, queries, true_kth, args.k, r, tables,
                                              min(args.lsh_queries, len(queries)),
                                              args.join_quantile if args.join_quantile >= 0 else None, args.seed,
                                              join_skip_reason=skip)
                print(f"   spark_lsh r={r:g} L={tables} xong", flush=True)
        for m_links in args.hnsw_m:
            size_rows += k3.run_hnsw(base_c, queries_c, true_kth, args.k, m_links, args.ef_construction,
                                     args.ef_search)
        meta = {"n_windows": len(x), "stride": args.stride or args.window, "vectorize_s": vectorize_s,
                "n_base": n, "n_queries": len(queries), "dim": x.shape[1], "k": args.k}
        size_rows = k3.with_status([{**meta, **row, "dataset": source.name} for row in size_rows])
        save(size_rows)
        k3.print_table(size_rows)
        rows += size_rows
    return rows


def markdown_table(rows: list[dict]) -> str:
    fmt = k3.fmt
    lines = ["| Dataset | n_base | Phương pháp | Tham số | Trạng thái | Build (s) | Index (MB) | Recall@k | "
             "Ứng viên | Trễ TB (ms) | p95 (ms) |",
             "| :--- | ---: | :--- | :--- | :---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for r in rows:
        lines.append(
            f"| {r['dataset']} | {r['n_base']:,} | `{r['method']}` | `{r['params']}` | {r['status']} | "
            f"{r['build_s']:.3f} | {r['index_mb']:.2f} | {fmt(r['recall_at_k'], '.3f')} | "
            f"{r['candidate_frac']:.2%} | {fmt(r['lat_mean_ms'], '.3f')} | {fmt(r['lat_p95_ms'], '.3f')} |"
        )
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    sources = [Path(p) for p in args.input] if args.input else [k3.default_source()]
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    env = {**environment_info(), "faiss": __import__("faiss").__version__}

    def save(rows: list[dict]) -> None:
        out = [{"run_id": run_id, "timestamp_utc": run_id, **{f: r.get(f, "") for f in FIELDS[2:]}, **env}
               for r in rows]
        append_csv(args.output, out, FIELDS + list(env.keys()))

    spark = k3.build_session(args, app_name="Nhom11-K6-knn_lsh_faiss")
    rows: list[dict] = []
    try:
        for source in sources:
            rows += run_dataset(spark, source, args, save)
    finally:
        spark.stop()

    print(f"\nĐã ghi {len(rows)} dòng vào {args.output} (run_id={run_id})\n")
    print(markdown_table(rows))
    exact_ok = all(r["recall_at_k"] == 1.0 for r in rows if r["method"] == "exact_knn")
    return 0 if exact_ok else 1


if __name__ == "__main__":
    sys.exit(main())
