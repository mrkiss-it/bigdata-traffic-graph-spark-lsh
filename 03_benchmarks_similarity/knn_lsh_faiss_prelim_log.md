# [K6] Nhật ký đo đạc sơ bộ — Exact KNN vs. Spark LSH vs. FAISS HNSW

> Người phụ trách: Nguyễn Duy Khang (`26410055`) · Ngày đo: `25/11/2026` · Script: [benchmark_knn_lsh_faiss.py](./benchmark_knn_lsh_faiss.py) (dùng vectorizer và hàm đo của [smoke_test_lsh_faiss.py](./smoke_test_lsh_faiss.py) `[K3]`)
> Số liệu đầy đủ (90 dòng, `run_id=20261125T174133Z`): [knn_lsh_faiss_results.csv](./knn_lsh_faiss_results.csv) · Smoke-test 10K vector `[K3]`: [smoke_lsh_faiss_results.csv](./smoke_lsh_faiss_results.csv)
> Đây là lượt đo **sơ bộ** (1 lần chạy, chưa lặp lại), dùng để chọn tham số và kịch bản cho mẻ đo chính thức Tuần 7.

## 1. Thiết lập

| Hạng mục | Giá trị |
| :--- | :--- |
| Máy | Ubuntu 24.04, 6 vCPU, 7,75 GB RAM; Python 3.12.3, PySpark 3.5.9 `local[*]` (`driver.memory=4g`, `shuffle.partitions=48`), JDK 17, faiss-cpu 1.16.0 |
| Nguồn | Parquet `[K2]` chưa làm sạch: `metr_la_sample.parquet`, `pems_bay_sample.parquet`. `metr_la_cleaned.parquet` của `[T5]` chưa có trên `main`, nên cửa sổ chứa `0.0` bị loại |
| Vector | Cửa sổ 12 mốc × 5 phút = 60 phút, 12 chiều, đơn vị mph, `stride = 12` (không chồng lấn). METR-LA: **523.899** vector (vectorize 17,1 s); PEMS-BAY: **1.410.954** vector (34,9 s) |
| Truy vấn | 200 vector lấy ngẫu nhiên (`seed=11`), loại khỏi chỉ mục; `k = 10`. `spark_lsh` chỉ đo 30 truy vấn đầu vì mỗi truy vấn là một Spark job |
| Quy mô chỉ mục | 10.000 → 100.000 → toàn bộ (lồng nhau, cùng tập truy vấn) |
| Ground truth | Vét cạn numpy `float64` |
| Cấu hình | Spark BRP-LSH `bucketLength` ∈ {2, 4, 8}, `numHashTables = 8`; FAISS HNSW `M` ∈ {16, 32}, `efConstruction = 40`, `efSearch` ∈ {16, 32, 64, 128} |

Lệnh chạy:

```bash
python 03_benchmarks_similarity/smoke_test_lsh_faiss.py
python 03_benchmarks_similarity/benchmark_knn_lsh_faiss.py \
    --input data/interim/metr_la_sample.parquet data/interim/pems_bay_sample.parquet
```

Chỉ số: **Recall@10** tính theo khoảng cách (kết quả đúng khi khoảng cách ≤ khoảng cách láng giềng thật thứ 10 + `1e-3`); **Ứng viên** = tỷ lệ vector chỉ mục phải tính khoảng cách thật cho mỗi truy vấn; **Trễ** = thời gian một truy vấn đơn lẻ (ms). Với `spark_lsh_join` là thời gian cả lô 200 truy vấn chia cho 200.

## 2. Kết quả rút gọn

Mỗi phương pháp lấy một cấu hình đại diện: Spark LSH `bucketLength=4` (recall ≈ 0,95–1,0); HNSW `M=32, efSearch=64`.

| Dataset | n_base | Phương pháp | Recall@10 | Ứng viên | Trễ TB (ms) | p95 (ms) | Build (s) | Index (MB) |
| :--- | ---: | :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| METR-LA | 10.000 | Exact KNN | 1,000 | 100% | 0,117 | 0,130 | 0,000 | 0,46 |
| | | Spark LSH (từng truy vấn) | 0,953 | 82,2% | 110,4 | 137,5 | 0,25 | 7,25 |
| | | Spark LSH (join theo lô) | 0,956 | 82,2% | 19,7 | — | 0,25 | 7,25 |
| | | FAISS HNSW | 1,000 | 7,19% | 0,059 | 0,071 | 0,13 | 3,05 |
| METR-LA | 100.000 | Exact KNN | 1,000 | 100% | 0,381 | 0,375 | 0,002 | 4,58 |
| | | Spark LSH (từng truy vấn) | 0,977 | 82,0% | 282,5 | 307,8 | 0,58 | 72,5 |
| | | Spark LSH (join theo lô) | 0,957 | 82,0% | 186,8 | — | 0,58 | 72,5 |
| | | FAISS HNSW | 1,000 | 1,13% | 0,082 | 0,098 | 1,22 | 30,5 |
| METR-LA | 523.699 | Exact KNN | 1,000 | 100% | 1,523 | 1,688 | 0,008 | 24,0 |
| | | Spark LSH (từng truy vấn) | 0,987 | 82,0% | 281,4 | 421,7 | 6,83 | 379,6 |
| | | Spark LSH (join theo lô) | 0,955 | 82,0% | 987,5 | — | 6,83 | 379,6 |
| | | FAISS HNSW | 0,998 | 0,27% | 0,126 | 0,153 | 8,08 | 159,9 |
| PEMS-BAY | 10.000 | Exact KNN | 1,000 | 100% | 0,425 | 0,120 | 0,002 | 0,46 |
| | | Spark LSH (từng truy vấn) | 1,000 | 89,8% | 82,2 | 105,4 | 0,15 | 7,25 |
| | | Spark LSH (join theo lô) | 0,980 | 89,8% | 15,6 | — | 0,15 | 7,25 |
| | | FAISS HNSW | 1,000 | 5,70% | 0,055 | 0,068 | 0,12 | 3,05 |
| PEMS-BAY | 100.000 | Exact KNN | 1,000 | 100% | 0,350 | 0,376 | 0,002 | 4,58 |
| | | Spark LSH (từng truy vấn) | 1,000 | 89,9% | 293,0 | 335,5 | 0,50 | 72,5 |
| | | Spark LSH (join theo lô) | 0,961 | 89,9% | 168,6 | — | 0,50 | 72,5 |
| | | FAISS HNSW | 0,999 | 0,99% | 0,080 | 0,101 | 1,13 | 30,5 |
| PEMS-BAY | 1.410.754 | Exact KNN | 1,000 | 100% | 5,186 | 5,418 | 0,089 | 64,6 |
| | | Spark LSH (từng truy vấn) | 1,000 | 90,0% | 488,2 | 534,7 | 2,02 | 1.022,6 |
| | | Spark LSH (join theo lô) | *skipped* — OOM, xem §4 | | | | | |
| | | FAISS HNSW | 0,981 | 0,10% | 0,148 | 0,186 | 28,63 | 430,7 |

Trễ trung bình của Exact KNN ở mốc METR-LA 100.000 (0,381 ms so với p95 0,375 ms) và PEMS-BAY 10.000 (0,425 ms so với p95 0,120 ms) lớn hơn p95 do vài truy vấn bị nhiễu hệ thống; nên so sánh theo trung vị/p95 trong CSV.

## 3. Quan sát chính

1. **FAISS HNSW giữ độ trễ gần như không đổi khi quy mô tăng.** Từ 10K lên 1,41M vector (×141), trễ HNSW `M=32, efSearch=64` chỉ tăng từ 0,055 lên 0,148 ms (×2,7), trong khi Exact KNN tăng từ ~0,1 lên 5,19 ms, gần tuyến tính theo N. Ở mốc lớn nhất, HNSW nhanh hơn vét cạn **12×** (METR-LA) và **35×** (PEMS-BAY), đồng thời chỉ tính khoảng cách trên **0,10–0,27%** tập dữ liệu.
2. **Spark BRP-LSH gần như không lọc được ứng viên trên vector 12 chiều.** Tỷ lệ ứng viên chỉ phụ thuộc `bucketLength` và phân bố dữ liệu, **không giảm theo N**: với `r=2` là 65% (METR-LA) và 81% (PEMS-BAY), với `r=4` là 82% và 90%, với `r=8` là 93% và 97%. Mọi cấu hình đạt recall ≥ 0,95 đều phải tính lại khoảng cách trên ≥ 81% dữ liệu. Nguyên nhân là BRP của Spark chỉ có khuếch đại OR (mỗi bảng băm một hàm chiếu, không có khuếch đại AND); phân tích ở Mục 2.4.5 của [draft_c2_spark_lsh_math.md](../05_report_slides/draft_c2_spark_lsh_math.md).
3. **`approxNearestNeighbors` bị chi phối bởi chi phí Spark job.** Mỗi truy vấn tốn 80–490 ms. Con số này tăng chậm theo N vì phần lớn là chi phí lập lịch job và quét bảng đã cache, chậm hơn Exact KNN một máy khoảng 100–1.000 lần. Spark LSH không phù hợp cho tra cứu trực tuyến từng truy vấn.
4. **`approxSimilarityJoin` có lợi ở quy mô nhỏ nhưng bùng nổ theo N × Q × tỷ lệ ứng viên.** Ở 10K, join theo lô rẻ hơn truy vấn từng cái 5–6× (16–24 ms so với 82–141 ms mỗi truy vấn). Ở 524K, join đắt hơn (~1 s mỗi truy vấn) vì phải sinh ~200 × 430K cặp ứng viên. Ở 1,41M, join hết bộ nhớ (§4). Recall của join bị giới hạn bởi ngưỡng khoảng cách (phân vị 0,95 của khoảng cách láng giềng thứ 10 thật), trong thực nghiệm đạt tối đa 0,9795 (≤ 0,98). Lưu ý: ở một vài mốc (vd METR-LA 10K `r=2`), recall của `spark_lsh_join` (đo trên 200 truy vấn) cao hơn `spark_lsh` (chỉ đo 30 truy vấn đầu) do nhiễu chọn mẫu nhỏ; mẻ đo Tuần 7 cần đối chiếu thêm trên cùng tập truy vấn.
5. **Bộ nhớ:** bảng Spark đã băm (vector + 8 giá trị băm, dạng đối tượng `DenseVector`) chiếm 1.022,6 MB cho 1,41M vector, gấp 16× chỉ mục FAISS Flat (64,6 MB) và 2,4× HNSW `M=32` (430,7 MB). Dung lượng HNSW khớp công thức `N·(4d + 8M)` byte cho tầng 0: 1,41M × 304 B ≈ 409 MiB, phần còn lại (~5%) là các tầng trên. Cột "Index (MB)" tính theo MiB (1024² byte). *Lưu ý:* cột `build_peak_rss_mb` trong CSV là RSS cộng dồn cả tiến trình Python + Spark JVM qua nhiều mốc nối tiếp, không dùng để so sánh trực tiếp dung lượng bộ nhớ giữa các thuật toán (dùng `index_mb`).
6. **HNSW cần đồ thị chất lượng cao hơn khi dữ liệu dày.** Trên PEMS-BAY 1,41M, `M=16` dừng ở recall 0,948 dù tăng `efSearch` lên 128; `M=32` đạt 0,992. Thời gian lập chỉ mục tăng từ 0,12 s lên 28,6 s (`M=32`). Mẻ Tuần 7 nên thử thêm `efConstruction` 100–200.

## 4. Sự cố gặp phải và cách xử lý

| Sự cố | Nguyên nhân | Xử lý trong code |
| :--- | :--- | :--- |
| Recall của FAISS HNSW "dừng" ở ~0,97 trên 10K vector dù tăng `efSearch` | `IndexFlatL2` (dùng làm ground truth ban đầu) tính `‖x‖² + ‖y‖² − 2x·y` ở float32. Với vận tốc ~58 mph, sai số khoảng cách tới ~1e-3, đủ đảo thứ tự các láng giềng sát nhau. Thực ra HNSW đúng, ground truth sai | Ground truth vét cạn numpy `float64`; trừ vector trung bình trước khi lập chỉ mục FAISS (`center()`) |
| Exact KNN chỉ đạt recall 0,995 trên PEMS-BAY 1,41M | Như trên: tập dày có nhiều láng giềng cách nhau < 1e-3 mph | `exact_knn` lấy 4k ứng viên từ `IndexFlatL2` rồi tính lại bằng `float64` (`refine_fp64(x4)`) |
| `approxSimilarityJoin` OOM trên METR-LA 524K (lượt chạy đầu) | `[K1]` đặt `spark.sql.shuffle.partitions = 8`, mỗi tác vụ phải giữ quá nhiều cặp ứng viên khi khử trùng lặp | `--shuffle-partitions` mặc định 48: METR-LA 524K chạy xong |
| `approxSimilarityJoin` OOM trên PEMS-BAY 1,41M (`r=2`, sau 146 s); JVM driver sau đó hết heap và chết ở cấu hình kế tiếp | ~200 × 1,15M cặp ứng viên vượt `driver.memory=4g` ở chế độ `local[*]` | Bắt lỗi, ghi `status=oom`; `--join-max-base` (mặc định 600.000) bỏ qua join ở quy mô lớn hơn với `status=skipped`, để các phương pháp khác vẫn chạy xong |
| Mất toàn bộ kết quả khi tiến trình chết giữa chừng | CSV chỉ ghi ở cuối | Ghi CSV sau mỗi mốc quy mô |

## 5. Chuẩn bị cho mẻ đo chính thức Tuần 7

- [ ] Chạy lại trên `metr_la_cleaned.parquet` của `[T5]` khi PR được merge (`--input ... --keep-zero` nếu dữ liệu đã chuẩn hóa), so sánh với kết quả trên dữ liệu thô ở trên.
- [ ] Lặp lại ≥ 3 lần mỗi cấu hình, báo cáo trung vị (lượt này chỉ 1 lần).
- [ ] Mở rộng lên `--stride 1`: METR-LA cho **6.275.355** vector, PEMS-BAY **16.924.298** vector, đúng quy mô "hàng triệu vector" của đề cương. `[K4]` đã đổi `windows_to_numpy()` sang Parquet trung gian để bước này chạy được trên VM 7,8 GB; giới hạn bộ nhớ HNSW và lệnh chạy ở [huong_dan_toi_uu_spark_va_lat_cat.md](../02_pyspark_gigo/huong_dan_toi_uu_spark_va_lat_cat.md) §5.3.
- [ ] HNSW: thêm `efConstruction` ∈ {100, 200}; vẽ đường cong Recall@10 – độ trễ cho Chương 3 (dữ liệu có sẵn trong CSV).
- [ ] Spark LSH: thử `bucketLength` < 2 cùng `numHashTables` 16–32 để tìm điểm có tỷ lệ ứng viên < 50% mà recall vẫn chấp nhận được; đo join theo lô với `driver.memory` lớn hơn hoặc trên YARN.
- [ ] Chạy lại smoke-test `smoke_test_lsh_faiss.py` với cấu hình `build_tuned_spark` (Kryo) + `windows_to_numpy()` Parquet mới từ `[K4]` trước khi chạy mẻ Tuần 7.
- [ ] Bàn giao CSV cho `[D4]`/Duy để đưa tab "LSH/ANN" vào Dashboard (trực quan hóa dung lượng bộ nhớ bằng cột `index_mb`, không dùng `build_peak_rss_mb`).
