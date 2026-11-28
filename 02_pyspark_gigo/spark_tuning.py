"""[K4] Cấu hình PySpark tinh chỉnh cho lát cắt 1M → 17M dòng và benchmark trước/sau (Nhóm 11).

Ba hồ sơ cấu hình (profile) được so sánh trên cùng lát cắt, cùng tác vụ:
    spark_default  chỉ master + driver memory, còn lại là mặc định Spark 3.5 (Arrow tắt, 200 shuffle partitions).
    k1_baseline    cấu hình chuẩn K1_CONF của spark_env_check.py ([K1]): Arrow bật, tắt fallback,
                   8 shuffle partitions.
    tuned          K1_CONF + TUNED_CONF bên dưới, dùng cho benchmark quy mô triệu dòng ([D3], [K6]).

Tác vụ đo (mỗi tác vụ tính từ lúc đọc Parquet tới khi có kết quả cuối):
    groupby_sensor_hour  bỏ zero sentinel, trung bình speed theo (sensor_id, giờ)  — từ [D1]
    rolling_mean_60min   trung bình trượt 12 bước theo cảm biến (Window)            — từ [D1]
    vectorize_windows    vectorizer cửa sổ trượt 60 phút của [K3] với stride 1, đếm số cửa sổ
    export_windows       vectorizer stride 1 rồi đưa ma trận float32 về driver qua Parquet trung gian
                         (smoke_test_lsh_faiss.windows_to_numpy), đường dữ liệu của [K6] ở Tuần 7

Lát cắt: N dòng đầu theo thứ tự thời gian của nguồn (mặc định PEMS-BAY từ [K2]), cắt bằng
smoke_test_pandas_vs_spark.prepare_slice ([D1]) vào data/interim/bench/, dùng chung với [D3].
Mỗi cặp (profile, lát cắt) chạy trong một tiến trình con riêng (JVM mới, cấu hình khởi động áp dụng được),
1 lần khởi động + --repeats lần đo; ghi CSV spark_tuning_results.csv và in bảng median.

Chạy (trên lab VM / Linux, cần dữ liệu [K2]):
    python 02_pyspark_gigo/spark_tuning.py
    python 02_pyspark_gigo/spark_tuning.py --sizes 1000000 all --repeats 3 --profiles k1_baseline tuned
    python 02_pyspark_gigo/spark_tuning.py --print-conf          # in cấu hình tuned cho máy hiện tại

Dùng trong script khác:
    from spark_tuning import build_tuned_spark
    spark = build_tuned_spark("local[*]", "4g", app_name="Nhom11-...")

Mã thoát: 0 nếu mọi tiến trình con chạy xong và checksum giữa các profile khớp, 1 nếu ngược lại.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import subprocess
import sys
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
PROJECT = HERE.parent
BENCH = PROJECT / "03_benchmarks_similarity"
sys.path[:0] = [str(HERE), str(BENCH)]

from spark_env_check import K1_CONF, build_spark  # noqa: E402

PROFILES = ("spark_default", "k1_baseline", "tuned")
TASKS = ("groupby_sensor_hour", "rolling_mean_60min", "vectorize_windows", "export_windows")
DEFAULT_SOURCE = PROJECT / "data/interim/pems_bay_sample.parquet"
DEFAULT_SIZES = ("1000000", "5000000", "10000000", "all")
DEFAULT_OUTPUT = HERE / "spark_tuning_results.csv"
RESULT_PREFIX = "RESULT_JSON "
CHECKSUM_REL_TOL = 1e-9
FIELDS = ["run_id", "run_started_utc", "dataset", "profile", "rows", "task", "repeat", "status",
          "result_rows", "checksum", "wall_s", "peak_rss_mb", "peak_python_mb", "peak_children_mb",
          "shuffle_partitions", "driver_memory", "error"]


def tuned_conf(cores: int | None = None) -> dict[str, str]:
    """Cấu hình bổ sung lên K1_CONF cho một máy local[*]; lý do từng khóa xem huong_dan_toi_uu_spark_va_lat_cat.md."""
    cores = cores or os.cpu_count() or 1
    return {
        # 8 partition cố định (K1) lệch tải khi số lõi không chia hết và mỗi partition quá to cho Window 17M dòng.
        # Quét 4×/8×/16×/~33× số lõi trên 17M dòng (6 lõi): 8× nhanh nhất cho vectorizer/export và ít RAM
        # hơn 200 partition; AQE gộp các partition nhỏ còn lại.
        "spark.sql.shuffle.partitions": str(8 * cores),
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "16m",
        # Kryo nhanh và gọn hơn Java serialization cho shuffle/cache của RDD (approxNearestNeighbors, LSH).
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        # Giới hạn kết quả gửi về driver; collect lớn hơn nên đi qua Parquet trung gian (export_windows).
        "spark.driver.maxResultSize": "1g",
    }


def build_tuned_spark(master: str = "local[*]", driver_memory: str = "4g", app_name: str = "Nhom11-K4-tuned",
                      shuffle_partitions: int | None = None):
    conf = tuned_conf()
    if shuffle_partitions is not None:
        if shuffle_partitions < 1:
            raise ValueError(f"shuffle_partitions phải >= 1, nhận {shuffle_partitions}")
        conf["spark.sql.shuffle.partitions"] = str(shuffle_partitions)
    return build_spark(master, driver_memory, conf, app_name)


def build_profile(profile: str, master: str, driver_memory: str, shuffle_partitions: int | None = None):
    if profile == "spark_default":
        from pyspark.sql import SparkSession

        spark = (SparkSession.builder.master(master).appName("Nhom11-K4-spark_default")
                 .config("spark.driver.memory", driver_memory)
                 .config("spark.ui.showConsoleProgress", "false").getOrCreate())
        spark.sparkContext.setLogLevel("ERROR")
        return spark
    if profile == "k1_baseline":
        return build_spark(master, driver_memory, app_name="Nhom11-K4-k1_baseline")
    if profile == "tuned":
        return build_tuned_spark(master, driver_memory, "Nhom11-K4-tuned", shuffle_partitions)
    raise ValueError(profile)


# --------------------------------------------------------------------------- #
# Tác vụ
# --------------------------------------------------------------------------- #
def run_task(spark, task: str, path: Path, scratch: Path) -> tuple[int, float]:
    from pyspark.sql import functions as F

    import smoke_test_lsh_faiss as k3
    import smoke_test_pandas_vs_spark as d1

    if task in ("groupby_sensor_hour", "rolling_mean_60min"):
        # URI file:// để không bị hiểu là đường dẫn HDFS khi HADOOP_CONF_DIR đặt fs.defaultFS=hdfs://
        return d1.spark_task(spark, task, k3.to_uri(path))
    windows = k3.sliding_windows(spark, path, window=12, stride=1)
    if task == "vectorize_windows":
        row = windows.agg(F.count(F.lit(1)), F.sum(F.col("values")[0])).first()
        return int(row[0]), float(row[1] or 0.0)
    if task == "export_windows":
        x = k3.windows_to_numpy(windows, 12, scratch)
        return len(x), float(x[:, 0].astype("float64").sum())
    raise ValueError(task)


def worker(args: argparse.Namespace) -> int:
    from benchmark_profiler import measure

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    path = Path(args.worker_slice)
    scratch = Path(args.slice_dir) / "tmp"
    spark = build_profile(args.worker_profile, args.master, args.driver_memory, args.tuned_shuffle_partitions)
    meta = {"shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions")}
    try:
        for task in args.tasks:
            run_task(spark, task, path, scratch)  # khởi động: page cache, codegen
            for repeat in range(1, args.repeats + 1):
                with measure() as m:
                    result_rows, checksum = run_task(spark, task, path, scratch)
                rec = {"task": task, "repeat": repeat, "result_rows": result_rows, "checksum": checksum,
                       **meta, **m.as_dict()}
                print(RESULT_PREFIX + json.dumps(rec), flush=True)
    finally:
        spark.stop()
    return 0


# --------------------------------------------------------------------------- #
# Tiến trình cha
# --------------------------------------------------------------------------- #
def run_worker(args: argparse.Namespace, profile: str, slice_path: Path) -> tuple[list[dict], str, str]:
    cmd = [sys.executable, str(Path(__file__).resolve()), "--worker-profile", profile,
           "--worker-slice", str(slice_path), "--tasks", *args.tasks, "--repeats", str(args.repeats),
           "--master", args.master, "--driver-memory", args.driver_memory, "--slice-dir", str(args.slice_dir)]
    if args.tuned_shuffle_partitions:
        cmd += ["--tuned-shuffle-partitions", str(args.tuned_shuffle_partitions)]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=args.timeout_s)
    except subprocess.TimeoutExpired as exc:
        out = exc.stdout.decode() if isinstance(exc.stdout, bytes) else (exc.stdout or "")
        return parse_records(out), "timeout", f"quá {args.timeout_s}s"
    records = parse_records(proc.stdout)
    if proc.returncode == 0:
        return records, "ok", ""
    tail = proc.stderr.strip().splitlines()[-40:]
    status = "oom" if any("OutOfMemory" in line or "MemoryError" in line for line in tail) else (
        "killed" if proc.returncode in (-9, 137) else "error")
    return records, status, next((line for line in reversed(tail) if "Error" in line), " ".join(tail[-1:]))[:300]


def parse_records(stdout: str) -> list[dict]:
    return [json.loads(line[len(RESULT_PREFIX):]) for line in stdout.splitlines() if line.startswith(RESULT_PREFIX)]


def resolve_sizes(raw: list[str], source: Path) -> list[int]:
    import pyarrow.dataset as ds

    if source.is_file():
        files = [source]
    else:
        parts = sorted(p for p in source.iterdir() if p.is_dir() and p.name.startswith("year_month="))
        files = [f for p in parts for f in sorted(p.glob("*.parquet"))] if parts else sorted(source.rglob("*.parquet"))
    total = ds.dataset(files, format="parquet").count_rows()
    sizes = set()
    for s in raw:
        n = total if s == "all" else int(s)
        if n < 1:
            raise ValueError(f"--sizes phải >= 1, nhận '{s}'")
        sizes.add(min(n, total))
    return sorted(sizes)


def summarise(rows: list[dict]) -> list[dict]:
    failed_keys = {(r["rows"], r["task"], r["profile"]) for r in rows if r["status"] != "ok"}
    groups = defaultdict(list)
    for r in rows:
        key = (r["rows"], r["task"], r["profile"])
        if r["status"] == "ok" and key not in failed_keys:
            groups[key].append(r)
    out = []
    for (n, task, profile), rs in sorted(groups.items()):
        out.append({"rows": n, "task": task, "profile": profile,
                    "wall_s": statistics.median(r["wall_s"] for r in rs),
                    "peak_rss_mb": statistics.median(r["peak_rss_mb"] for r in rs),
                    "result_rows": rs[0]["result_rows"], "checksum": rs[0]["checksum"],
                    "repeats_checks": [(r["result_rows"], r["checksum"]) for r in rs]})
    return out


def checksum_problems(summary: list[dict]) -> list[str]:
    by_key = defaultdict(list)
    for s in summary:
        by_key[(s["rows"], s["task"])].append(s)
    problems = []
    for (n, task), ss in by_key.items():
        ref = ss[0]
        for s in ss:
            for rows_i, chk_i in s.get("repeats_checks", [(s["result_rows"], s["checksum"])]):
                if rows_i != ref["result_rows"] or not math.isclose(
                        chk_i, ref["checksum"], rel_tol=CHECKSUM_REL_TOL):
                    if s["profile"] == ref["profile"]:
                        problems.append(f"{task} @ {n:,}: các lần lặp của {s['profile']} cho kết quả khác nhau")
                    else:
                        problems.append(f"{task} @ {n:,}: {ref['profile']} và {s['profile']} cho kết quả khác nhau")
                    break
    return problems


def markdown_table(summary: list[dict], failures: list[dict], profiles: list[str]) -> str:
    cell = {(s["rows"], s["task"], s["profile"]): s for s in summary}
    failed = {}
    fail_rows_tasks = set()
    for f in failures:
        if f.get("failed_task"):
            failed[(f["rows"], f["failed_task"], f["profile"])] = f["status"]
            fail_rows_tasks.add((f["rows"], f["failed_task"]))
        for t in f.get("skipped_tasks", ()):
            failed[(f["rows"], t, f["profile"])] = "skipped"
            fail_rows_tasks.add((f["rows"], t))
    head = " | ".join(f"{p} (s)" for p in profiles)
    lines = [f"| Số dòng | Tác vụ | {head} | RAM tuned (MB) | Tăng tốc tuned/k1 | Tăng tốc tuned/default |",
             "| ---: | :--- | " + " | ".join("---:" for _ in profiles) + " | ---: | ---: | ---: |"]
    for n, task in sorted({(s["rows"], s["task"]) for s in summary} | fail_rows_tasks,
                          key=lambda k: (k[0], TASKS.index(k[1]))):
        vals = []
        for p in profiles:
            s = cell.get((n, task, p))
            vals.append(f"{s['wall_s']:.2f}" if s else failed.get((n, task, p), "—"))
        tuned, k1, dflt = (cell.get((n, task, p)) for p in ("tuned", "k1_baseline", "spark_default"))
        ram = f"{tuned['peak_rss_mb']:.0f}" if tuned else "—"
        sp_k1 = f"{k1['wall_s'] / tuned['wall_s']:.2f}×" if tuned and k1 else "—"
        sp_df = f"{dflt['wall_s'] / tuned['wall_s']:.2f}×" if tuned and dflt else "—"
        lines.append(f"| {n:,} | `{task}` | " + " | ".join(vals) + f" | {ram} | {sp_k1} | {sp_df} |")
    return "\n".join(lines)


def main_parent(args: argparse.Namespace) -> int:
    import smoke_test_pandas_vs_spark as d1
    from benchmark_profiler import append_csv, environment_info

    run_id = uuid.uuid4().hex[:8]
    started = datetime.now(timezone.utc).isoformat(timespec="seconds")
    env = environment_info()
    sizes = resolve_sizes(args.sizes, args.source)
    dataset = d1.slice_name(args.source).removesuffix("_sample")
    print(f"[K4] run {run_id} | {dataset} | sizes {sizes} | profiles {args.profiles} | "
          f"{env['cpu_logical']} CPU, {env['total_ram_gb']} GB RAM, driver {args.driver_memory}", flush=True)

    rows, failures = [], []
    for n in sizes:
        slice_path = d1.prepare_slice(args.source, n, args.slice_dir)
        for profile in args.profiles:
            records, status, error = run_worker(args, profile, slice_path)
            out = [{"run_id": run_id, "run_started_utc": started, "dataset": dataset, "profile": profile,
                    "rows": n, "status": "ok", "driver_memory": args.driver_memory, "error": "", **r}
                   for r in records]
            if status != "ok":
                counts = Counter(r["task"] for r in records)
                pending = [t for t in args.tasks if counts[t] < args.repeats]
                failed_task = pending[0] if pending else args.tasks[-1]
                skipped_tasks = pending[1:] if pending else []
                failures.append({"rows": n, "profile": profile, "status": status,
                                 "failed_task": failed_task, "skipped_tasks": skipped_tasks})
                out.append({"run_id": run_id, "run_started_utc": started, "dataset": dataset, "profile": profile,
                            "rows": n, "task": failed_task, "status": status, "driver_memory": args.driver_memory,
                            "error": error})
            append_csv(args.output, [{f: r.get(f, "") for f in FIELDS} | env for r in out],
                       FIELDS + list(env.keys()))
            rows += out
            print(f"  {n:>11,} {profile:<14} {status:<8} {len(records)} lần đo {error}", flush=True)

    summary = summarise(rows)
    problems = checksum_problems(summary)
    print(f"\nĐã ghi {len(rows)} dòng vào {args.output} (run_id={run_id})\n")
    print(markdown_table(summary, failures, args.profiles))
    for p in problems:
        print("CHECKSUM:", p)
    return 0 if not problems and not failures else 1


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    p.add_argument("--sizes", nargs="+", default=list(DEFAULT_SIZES), help="Số dòng mỗi lát cắt hoặc 'all'")
    p.add_argument("--profiles", nargs="+", choices=PROFILES, default=list(PROFILES))
    p.add_argument("--tasks", nargs="+", choices=TASKS, default=list(TASKS))
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--master", default="local[*]")
    p.add_argument("--driver-memory", default=os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
    p.add_argument("--slice-dir", type=Path, default=PROJECT / "data/interim/bench")
    p.add_argument("--timeout-s", type=float, default=1800)
    p.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    p.add_argument("--tuned-shuffle-partitions", type=int, default=None,
                   help="Ghi đè spark.sql.shuffle.partitions của profile tuned (để quét tham số)")
    p.add_argument("--print-conf", action="store_true", help="In cấu hình tuned cho máy này rồi thoát")
    p.add_argument("--worker-profile", choices=PROFILES, help=argparse.SUPPRESS)
    p.add_argument("--worker-slice", help=argparse.SUPPRESS)
    args = p.parse_args()
    if args.repeats < 1 or args.timeout_s <= 0:
        p.error("--repeats phải >= 1 và --timeout-s phải > 0")
    if args.tuned_shuffle_partitions is not None and args.tuned_shuffle_partitions < 1:
        p.error("--tuned-shuffle-partitions phải >= 1")
    for s in args.sizes:
        if s != "all":
            try:
                n = int(s)
            except ValueError:
                p.error(f"--sizes không hợp lệ: '{s}' (cần số nguyên >= 1 hoặc 'all')")
            if n < 1:
                p.error(f"--sizes phải >= 1, nhận {n}")
    return args


def main() -> int:
    args = parse_args()
    if args.print_conf:
        for key, value in {**K1_CONF, **tuned_conf()}.items():
            print(f"{key} = {value}")
        return 0
    if args.worker_profile:
        return worker(args)
    if not args.source.exists():
        print(f"Không thấy {args.source}. Chạy 02_pyspark_gigo/csv_to_partitioned_parquet.py trước.")
        return 1
    return main_parent(args)


if __name__ == "__main__":
    sys.exit(main())
