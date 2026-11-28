# [K4] Tối ưu hóa PySpark và hướng dẫn nạp lát cắt 1M → 17M dòng

> Người phụ trách: Nguyễn Duy Khang (`26410055`) · Script: [spark_tuning.py](spark_tuning.py) · Kết quả: [spark_tuning_results.csv](spark_tuning_results.csv), [spark_tuning_sweep.csv](spark_tuning_sweep.csv)
> Áp dụng cho benchmark Pandas vs. PySpark `[D3]` và đối chứng KNN/LSH/HNSW quy mô triệu vector `[K6]` (Tuần 7).

## 1. Tóm tắt

- **Thay đổi có tác động lớn nhất không phải là một khóa cấu hình mà là cách đưa vector về driver.** Với `--stride 1`,
  `windows_to_numpy()` bản cũ (`toPandas()`) cần 6,3 GB RAM cho 6,28M vector METR-LA và hết heap JVM với 16,9M vector
  PEMS-BAY. Bản mới ghi Parquet tạm rồi đọc bằng pyarrow: METR-LA 53 s / 6,3 GB → 23 s / 3,4 GB; PEMS-BAY chạy được,
  50 s / 4,8 GB (driver 2g: 3,9 GB). Đây là điều kiện để `[K6]` đo ở quy mô triệu vector trong Tuần 7 (§4).
- **Cấu hình `tuned` (`spark_tuning.py`) thay 8 shuffle partition cố định của `[K1]` bằng 8 × số lõi + AQE + Kryo.**
  Ở 17M dòng, so với `[K1]`: nhanh hơn 1,18–1,33× ở các tác vụ Window/sắp xếp và RAM đỉnh giảm 31–40%. So với mặc định
  Spark: ngang tốc độ (0,94–1,13×) nhưng RAM đỉnh giảm 30–42%. Ở 1M dòng, ba cấu hình chênh nhau không quá 10% (§3).
- **Lát cắt 1M → 17M dòng** cắt theo thời gian bằng `prepare_slice()` của `[D1]`, lưu chung ở `data/interim/bench/`;
  lệnh cho `[D3]` và `[K6]`, cùng giới hạn bộ nhớ HNSW ở mốc 16,9M vector, nằm ở §5.
- Cấu hình yêu cầu trong kế hoạch Tuần 5 (tắt Arrow fallback, `driver-memory=4g`) đã có trong `K1_CONF` từ `[K1]` và
  được giữ nguyên trong `tuned`.

## 2. Cấu hình đề xuất

`tuned` = cấu hình chuẩn `K1_CONF` của `[K1]` cộng các khóa dưới đây (`tuned_conf()` trong [spark_tuning.py](spark_tuning.py)).
In cấu hình áp dụng cho máy đang dùng bằng `python 02_pyspark_gigo/spark_tuning.py --print-conf`.

| Khóa | Mặc định Spark 3.5 | `k1_baseline` | `tuned` | Lý do |
| :--- | :--- | :--- | :--- | :--- |
| `spark.driver.memory` | 1g | 4g | 4g | Đặt khi khởi động JVM. VM 7,8 GB: 4g cho job chỉ chạy Spark; 2g khi FAISS chạy cùng tiến trình (§4). |
| `spark.sql.execution.arrow.pyspark.enabled` | false | true | true | `toPandas()`/`createDataFrame()` đi qua Arrow theo cột. |
| `spark.sql.execution.arrow.pyspark.fallback.enabled` | true | false | false | Lỗi Arrow phải lộ ra, không âm thầm quay về đường chậm. |
| `spark.sql.shuffle.partitions` | 200 | 8 | 8 × số lõi (48 trên VM 6 lõi) | Xem §3.2. |
| `spark.sql.adaptive.enabled`, `...coalescePartitions.enabled` | true | true | true (ghi rõ) | AQE gộp partition shuffle nhỏ sau khi biết kích thước thật. |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | 64m | 64m | 16m | Dữ liệu nhỏ trên đĩa (PEMS-BAY 25,4 MB Snappy), partition sau gộp không nên quá to. |
| `spark.serializer` | Java | Java | Kryo | Nhanh và gọn hơn cho shuffle/cache RDD (đường `pyspark.ml` của LSH). |
| `spark.driver.maxResultSize` | 1g | 1g | 1g (ghi rõ) | Kết quả lớn đi qua Parquet trung gian thay vì `collect` (§4), nên giữ giới hạn để lỗi sớm. |

Dùng trong script khác:

```python
import sys; sys.path.insert(0, "02_pyspark_gigo")
from spark_tuning import build_tuned_spark

spark = build_tuned_spark("local[*]", "4g", app_name="Nhom11-...")
```

`build_spark(master, driver_memory, conf=None, app_name=...)` của `[K1]` nay nhận thêm `conf` để ghi đè; các script cũ
gọi `build_spark(master, driver_memory)` không đổi hành vi.

## 3. Đo trước/sau

**Thiết lập.** Lab VM Ubuntu 24.04, 6 vCPU, 7,75 GB RAM, Python 3.12, PySpark 3.5.9 `local[*]`, driver 4g.
Nguồn PEMS-BAY `[K2]` (`pems_bay_sample.parquet`, 16.937.700 dòng, dữ liệu thô). Mỗi cặp (profile, lát cắt) chạy trong
một JVM riêng; mỗi tác vụ chạy 1 lần khởi động rồi 3 lần đo, bảng ghi trung vị. RAM là đỉnh RSS cả cây tiến trình
(Python + JVM). Checksum (số dòng kết quả, tổng giá trị) khớp giữa ba profile ở mọi ô.

### 3.1. Kết quả chính

Run `4f0bc93b` (26/11/2026). "Mặc định / tuned" > 1 nghĩa là `tuned` nhanh hơn.

| Số dòng | Tác vụ | Mặc định (s) | K1 (s) | Tuned (s) | K1 / tuned | Mặc định / tuned | RAM mặc định (MB) | RAM K1 (MB) | RAM tuned (MB) |
| ---: | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1.000.000 | `groupby_sensor_hour` | 0,67 | 0,63 | 0,66 | 0,95× | 1,01× | 592 | 597 | 615 |
| 1.000.000 | `rolling_mean_60min` | 1,25 | 1,12 | 1,14 | 0,99× | 1,10× | 1.440 | 1.169 | 1.292 |
| 1.000.000 | `vectorize_windows` | 1,56 | 1,61 | 1,56 | 1,03× | 1,00× | 1.508 | 1.480 | 1.334 |
| 1.000.000 | `export_windows` | 3,23 | 3,45 | 3,24 | 1,06× | 1,00× | 1.966 | 1.726 | 1.716 |
| 5.000.000 | `groupby_sensor_hour` | 1,00 | 0,86 | 0,94 | 0,92× | 1,07× | 889 | 978 | 813 |
| 5.000.000 | `rolling_mean_60min` | 2,97 | 3,47 | 2,95 | 1,18× | 1,01× | 2.056 | 1.596 | 1.997 |
| 5.000.000 | `vectorize_windows` | 5,05 | 5,48 | 5,04 | 1,09× | 1,00× | 2.345 | 2.429 | 2.193 |
| 5.000.000 | `export_windows` | 13,04 | 14,31 | 13,23 | 1,08× | 0,99× | 3.353 | 3.121 | 2.816 |
| 10.000.000 | `groupby_sensor_hour` | 0,98 | 0,96 | 1,05 | 0,92× | 0,94× | 1.285 | 1.239 | 1.343 |
| 10.000.000 | `rolling_mean_60min` | 4,96 | 5,38 | 5,84 | 0,92× | 0,85× | 2.972 | 2.326 | 2.092 |
| 10.000.000 | `vectorize_windows` | 7,30 | 9,24 | 8,38 | 1,10× | 0,87× | 3.668 | 3.040 | 2.788 |
| 10.000.000 | `export_windows` | 22,84 | 27,60 | 22,69 | 1,22× | 1,01× | 5.350 | 4.088 | 3.389 |
| 16.937.700 | `groupby_sensor_hour` | 1,30 | 1,32 | 1,26 | 1,05× | 1,04× | 1.478 | 1.501 | 1.706 |
| 16.937.700 | `rolling_mean_60min` | 7,33 | 8,92 | 7,58 | 1,18× | 0,97× | 3.489 | 2.970 | 2.038 |
| 16.937.700 | `vectorize_windows` | 11,50 | 15,00 | 12,22 | 1,23× | 0,94× | 4.045 | 3.976 | 2.398 |
| 16.937.700 | `export_windows` | 40,71 | 47,75 | 36,00 | 1,33× | 1,13× | 4.979 | 5.273 | 3.504 |

### 3.2. Chọn số shuffle partition

Profile `tuned` với `spark.sql.shuffle.partitions` thay đổi (`--tuned-shuffle-partitions`), các khóa khác giữ nguyên,
toàn bộ PEMS-BAY 16.937.700 dòng, trung vị 3 lần đo. Mỗi giá trị là một lượt chạy riêng
([spark_tuning_sweep.csv](spark_tuning_sweep.csv); giá trị 24 lấy từ lượt đầu khi `tuned` còn dùng 4 × số lõi).

| Shuffle partitions | `rolling_mean_60min` (s) | `vectorize_windows` (s) | `export_windows` (s) | RAM `vectorize` (MB) | RAM `export` (MB) |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 24 (4 × lõi) | 7,53 | 12,16 | 36,74 | 2.706 | 3.901 |
| **48 (8 × lõi)** | 7,87 | **10,21** | **34,15** | 2.643 | **3.728** |
| 96 (16 × lõi) | 8,01 | 10,51 | 35,68 | 2.574 | 3.812 |
| 200 | 7,59 | 12,29 | 35,58 | 3.042 | 4.244 |

48 nhanh nhất cho hai tác vụ của `[K6]` và RAM thấp, nên `tuned_conf()` lấy 8 × số lõi. Cùng cấu hình 48 partition,
`vectorize_windows` đo 10,21 s ở lượt quét nhưng 12,22 s ở lượt chính §3.1: chênh lệch giữa hai lượt chạy (hai JVM
khác nhau) tới ~20%, nên các khác biệt nhỏ hơn mức đó trong bảng này không đủ để kết luận.

### 3.3. Nhận xét

1. **Ở 1M dòng, cấu hình không quan trọng.** Mọi khác biệt nằm trong ±10%; thời gian bị chi phối bởi chi phí lập
   kế hoạch và khởi chạy task của Spark, không phải xử lý dữ liệu.
2. **8 partition cố định của `[K1]` là điểm nghẽn từ 10M dòng.** Ở 17M, `tuned` nhanh hơn `k1_baseline` 1,18–1,33×
   ở ba tác vụ dùng Window/sắp xếp, và giảm RAM đỉnh 31–40% (`vectorize` 3.976 → 2.398 MB, `export` 5.273 → 3.504 MB).
   Với 6 lõi, 8 task tạo hai đợt (đợt sau chỉ dùng 2/6 lõi), mỗi task phải sắp xếp 1/8 dữ liệu.
3. **So với mặc định Spark, `tuned` ngang tốc độ nhưng tiết kiệm RAM.** Trên 17M, tỉ lệ thời gian 0,94–1,13× (chỉ
   `export_windows` nhanh hơn rõ, 1,13×; lượt đầu cũng cho 1,14×), còn RAM đỉnh giảm 30–42% ở ba tác vụ Window
   (`vectorize` 4.045 → 2.398 MB, `rolling` 3.489 → 2.038 MB, `export` 4.979 → 3.504 MB). Trên VM 7,8 GB, phần RAM
   này là chỗ cho FAISS ở `[K6]`. Ở mốc 10M, `rolling`/`vectorize` của `tuned` chậm hơn mặc định 15–18%, cùng cỡ với
dao động giữa các lượt chạy (§3.2).
4. **`rolling_mean_60min` không hưởng lợi từ số partition.** Ngay cả `tuned` với 200 partition (bằng mặc định) vẫn
   7,59 s so với 6,46–7,48 s (trung vị 7,33 s) của mặc định, nên phần chênh nằm ở các khóa khác (Kryo, `advisoryPartitionSizeInBytes`,
   Arrow, múi giờ phiên) mà lượt đo này chưa tách riêng. Với tác vụ này nên giữ mặc định hoặc `k1_baseline` + 48 partition.
5. **`groupby_sensor_hour` không đổi** (≤ 1,7 s ở mọi cấu hình): kết quả chỉ 7.800 nhóm, shuffle rất nhỏ.
6. **Khuyến nghị:** dùng `build_tuned_spark()` cho `[K6]` và mọi bước vectorize/export ở quy mô triệu dòng. `[D3]`
   đã chạy xong với `k1_baseline`; chạy lại bằng `tuned` chỉ đổi kết quả PySpark 0,92–1,18× ở `groupby`/`rolling`,
   không làm thay đổi kết luận Pandas vs. PySpark, nên không bắt buộc.

## 4. Đưa vector về driver: Parquet trung gian thay cho `toPandas()`

Vectorizer `[K3]` trả DataFrame có cột `values: array<double>`. Bản cũ của `windows_to_numpy()` gọi `toPandas()`:
toàn bộ kết quả phải nằm trong heap JVM driver trước khi chuyển sang Python, và cột mảng thành mảng `object` (một
`ndarray` cho mỗi dòng). Với `--stride 1` (mẻ triệu vector của Tuần 7) cách này không dùng được trên VM 7,8 GB.

Bản mới: executor ghi 12 cột `float32` ra Parquet tạm (đã `orderBy(end_ts, sensor_id)`, mỗi part-file là một khoảng
liên tiếp), rồi pyarrow đọc thẳng từng file vào ma trận `numpy` cấp phát sẵn; thư mục tạm bị xóa sau đó. Thứ tự dòng
giữ nguyên như bản cũ nên tập truy vấn/chỉ mục của `[K3]`/`[K6]` không đổi.

Đo một lần trên VM (script thăm dò riêng, cùng vectorizer, `--stride 1`, driver 4g trừ khi ghi khác):

| Dữ liệu | Số vector | Cách đưa về driver | Driver | Thời gian (s) | RAM đỉnh cả cây (MB) | RAM JVM (MB) |
| :--- | ---: | :--- | :---: | ---: | ---: | ---: |
| METR-LA | 6.275.355 | `toPandas()` cột mảng (bản cũ) | 4g | 53,0 | 6.265 | 4.249 |
| METR-LA | 6.275.355 | Arrow `_collect_as_arrow()` cột mảng | 4g | 24,0 | 5.921 | 4.468 |
| METR-LA | 6.275.355 | `toPandas()` trên 12 cột `float` | 4g | 20,5 | 4.914 | 3.650 |
| METR-LA | 6.275.355 | **Parquet trung gian (bản mới)** | 4g | 22,8 | **3.422** | 2.751 |
| PEMS-BAY | 16.924.298 | `toPandas()` cột mảng (bản cũ) | 4g | lỗi `OutOfMemoryError: Java heap space` | — | — |
| PEMS-BAY | 16.924.298 | Arrow `_collect_as_arrow()` cột mảng | 4g | lỗi (`Py4JJavaError` khi nhận kết quả) | — | — |
| PEMS-BAY | 16.924.298 | **Parquet trung gian (bản mới)** | 4g | 50,2 | 4.836 | 4.607 |
| PEMS-BAY | 16.924.298 | **Parquet trung gian (bản mới)** | 2g | 51,4 | **3.893** | 2.517 |

Chỉ đếm số cửa sổ (không đưa về driver) mất 10,4 s với METR-LA. Phần còn lại của bản mới là ghi/đọc Parquet tạm.
`toPandas()` trên 12 cột số nhanh nhất ở METR-LA nhưng vẫn giữ toàn bộ kết quả trong heap driver; bản Parquet là cách
duy nhất có RAM không phụ thuộc vào heap JVM, nên được chọn.

Tác vụ `export_windows` ở §3 dùng chính đường mới này.

## 5. Hướng dẫn nạp lát cắt

### 5.1. Lát cắt được tạo thế nào

`prepare_slice()` của `[D1]` (`03_benchmarks_similarity/smoke_test_pandas_vs_spark.py`) lấy **N dòng đầu theo thứ tự
thời gian** của nguồn (partition `year_month` tăng dần, trong file đã sắp theo `(timestamp, sensor_id)`), giữ 3 cột
`timestamp, sensor_id, speed` và ghi một file Snappy `data/interim/bench/<nguồn>_<N>.parquet`. File đã có, đủ N dòng và
mới hơn nguồn thì được dùng lại. `[D3]`, `spark_tuning.py` và `[K6]` dùng chung thư mục này.

Vì cắt theo thời gian, mỗi lát cắt là **một khoảng thời gian liên tục của toàn bộ cảm biến**, nên vectorizer cửa sổ
trượt chạy đúng trên lát cắt (chỉ mốc cuối có thể thiếu một phần cảm biến khi N không chia hết cho số cảm biến).

| Nguồn | Cảm biến | Lát cắt | Số mốc 5 phút | Khoảng thời gian | Cửa sổ 60 phút (stride 1, bỏ cửa sổ chứa 0) |
| :--- | ---: | ---: | ---: | :--- | ---: |
| PEMS-BAY | 325 | 1.000.000 | 3.077 | ~10,7 ngày từ 01/01/2017 | 996.425 |
| PEMS-BAY | 325 | 5.000.000 | 15.385 | ~53 ngày | 4.996.425 |
| PEMS-BAY | 325 | 10.000.000 | 30.770 | ~107 ngày | 9.992.850 |
| PEMS-BAY | 325 | 16.937.700 (`all`) | 52.116 | 01/01 – 30/06/2017 | 16.924.298 |
| METR-LA | 207 | 7.094.304 (`all`) | 34.272 | 01/03 – 27/06/2012 | 6.275.355 |

Số cửa sổ PEMS-BAY lấy từ `export_windows` (cột `result_rows`); METR-LA từ lượt đo §4. METR-LA mất nhiều cửa sổ hơn vì
8,11% ô là 0.0 (mất tín hiệu); PEMS-BAY gần như không có.

### 5.2. Lệnh cho `[D3]` (Duy)

```bash
# Lát cắt mặc định 1M, 5M, 10M, all của PEMS-BAY; driver 4g
python 03_benchmarks_similarity/benchmark_scaling_pandas_vs_spark.py --driver-memory 4g --sizes 1000000 5000000 10000000 all
```

- Trên máy có `HADOOP_CONF_DIR` (lab VM), Spark hiểu đường dẫn không có scheme là HDFS. `spark_task()` của `[D1]` đọc
  `str(path)` nên sẽ báo `PATH_NOT_FOUND hdfs://...`. Chạy với `env -u HADOOP_CONF_DIR python ...` hoặc truyền URI
  `file://` như `spark_tuning.py` (quy tắc 5 của [Data Contract](parquet_schema_contract.md)).
- Muốn dùng cấu hình `tuned` cho Spark trong `[D3]`: thay `build_spark(...)` bằng `build_tuned_spark(...)` trong
  `worker()` của `[D1]`. Với `groupby`/`rolling` của `[D3]`, mức thay đổi trong §3.1 là 0,92–1,18× so với
  `k1_baseline` (§3.3 mục 6), nên không bắt buộc chạy lại.

### 5.3. Lệnh cho `[K6]` Tuần 7 (stride 1, triệu vector)

```bash
# METR-LA 6,28M vector; PEMS-BAY 16,9M vector. Bỏ Spark LSH ở quy mô này (mỗi truy vấn quét toàn bộ tập, xem 05_report_slides/draft_c2_spark_lsh_math.md §2.4.5).
python 03_benchmarks_similarity/benchmark_knn_lsh_faiss.py --stride 1 --skip-lsh --driver-memory 2g \
    --input data/interim/metr_la_sample.parquet --sizes 1000000 all --hnsw-m 32
python 03_benchmarks_similarity/benchmark_knn_lsh_faiss.py --stride 1 --skip-lsh --driver-memory 2g \
    --input data/interim/pems_bay_sample.parquet --sizes 1000000 5000000 --hnsw-m 32
```

Giới hạn bộ nhớ khi FAISS chạy cùng tiến trình với Spark driver:

- Ma trận vector `N × 12 × 4` byte, cộng một bản đã trừ trung bình: 2 × 0,81 GB với N = 16,9M.
- HNSW `M=32` tầng 0 tốn `N·(4d + 8M)` = 304 byte/vector: 1,5 GB với 5M, **5,1 GB với 16,9M**. Cộng JVM driver và
  ma trận, mốc 16,9M với `M=32` vượt 7,8 GB của VM. `M=16` (176 byte/vector, 3,0 GB) vừa bộ nhớ nhưng recall chỉ
  0,948 ở 1,41M vector (nhật ký `[K6]`). Vì vậy mốc `all` của PEMS-BAY cần máy ≥ 12 GB RAM, hoặc dừng ở 5M trên VM.
- Driver 2g đủ cho bước vectorize + export 16,9M vector (§4) và nhường RAM cho FAISS.

## 6. Hạn chế

- Một máy `local[*]`: các khóa liên quan tới mạng/cluster (`spark.executor.*`, `spark.locality.wait`, broadcast join)
  không đo được. Kết luận về số partition gắn với 6 lõi; `tuned_conf()` tính theo số lõi của máy chạy.
- Mỗi ô là trung vị 3 lần đo trong một lượt chạy; chênh lệch dưới ~10% giữa các profile nằm trong mức dao động giữa
  các JVM (xem cột `repeat` trong CSV).
- Dữ liệu thô `[K2]`: lát cắt chưa qua làm sạch GIGO `[T5]`. Khi `[T5]` merge, thời gian các tác vụ không đổi đáng kể
  (cùng số dòng) nhưng số cửa sổ hợp lệ sẽ tăng vì không còn phải bỏ cửa sổ chứa 0.
- Bảng §4 là lần đo đơn lẻ bằng script thăm dò (không nằm trong repo); đường mới được đo lặp lại ở tác vụ `export_windows`.
