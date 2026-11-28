# ĐỒ ÁN MÔN HỌC: PHÂN TÍCH DỮ LIỆU LỚN (BIG DATA ANALYTICS)
## PIPELINE XỬ LÝ DỮ LIỆU LỚN GIAO THÔNG ĐỒ THỊ KHÔNG–THỜI GIAN BẰNG APACHE SPARK & LSH/ANN (METR-LA/PEMS-BAY VÀ CASE STUDY TP.HCM)

> **Đơn vị đào tạo:** Trường Đại học Công nghệ Thông tin (UIT) – ĐHQG-HCM (CITD)  
> **Giảng viên hướng dẫn:** TS. Hà Minh Tân  
> **Nhóm thực hiện:** Nhóm 11 *(4 thành viên – HK1, Năm học 2026–2027)*  
> **Phạm vi thực hiện:** Đồ án độc lập, trọn vẹn cho môn *Phân tích dữ liệu lớn* (`Tuần 4 – Tuần 9`, hoàn tất trước `20/12/2026`)

---

## 1. Thành viên Nhóm 11

| STT (Lớp) | Họ và Tên | MSSV | Vai Trò Chuyên Trách Trong Đồ Án |
| :---: | :--- | :---: | :--- |
| 1 *(52)* | **Trần Hoàng Hôn** *(NT)* | `26410046` | Trưởng nhóm / PM, Kiến trúc đồ thị `OSMnx` TP.HCM, Mở đầu + Chương 1 & biên tập Báo cáo $\ge 40$ trang |
| 2 *(55)* | **Nguyễn Duy Khang** | `26410055` | Kỹ sư **Apache Spark (PySpark) / Parquet** & Thuật toán tìm kiếm lớn (**KNN vs. LSH vs. ANN FAISS**), Chương 2 (`> 13` trang) |
| 3 *(70)* | **Phạm Thành Trung** | `26410141` | Kỹ sư Dữ liệu (`METR-LA`, `PEMS-BAY`, `Open-Meteo`) & Pipeline tiền xử lý **GIGO**; OLS chỉ với dữ liệu giao thông–thời tiết khớp địa điểm và thời gian |
| 4 *(47)* | **Vũ Văn Duy** | `26410031` | Kỹ sư **Benchmark Pandas vs. PySpark (`1M–17M` dòng)**, **Web Dashboard (`Streamlit/Folium`)** & Slide/Video |

---

## 2. Cấu trúc Hồ sơ Đồ án (Chuẩn hóa theo Bộ Biểu mẫu CITD/UIT)

Toàn bộ biểu mẫu tuân thủ chuẩn văn bản CITD/UIT và đáp ứng đầy đủ rubric `YeuCau` trong [Final_Project.xlsx](../Final_Project.xlsx):

| STT | Tệp tài liệu | Mục đích & Chuẩn tham chiếu |
| :-: | :--- | :--- |
| 1 | [De_Cuong_Chi_Tiet_Do_An_Phan_Tich_Du_Lieu_Lon.md](De_Cuong_Chi_Tiet_Do_An_Phan_Tich_Du_Lieu_Lon.md) | Thuyết minh Đề cương chi tiết 7 mục + lộ trình `Tuần 4 – Tuần 9` (Markdown). |
| 2 | [De_Cuong_Chi_Tiet_Do_An_Phan_Tich_Du_Lieu_Lon.docx](De_Cuong_Chi_Tiet_Do_An_Phan_Tich_Du_Lieu_Lon.docx) | Bản Đề cương Word định dạng bảng chuẩn **CITD/UIT** (`Table 0` Quốc hiệu + `Table 1` 8x2) sẵn sàng ký duyệt. |
| 3 | [Ke_Hoach_Thuc_Hien_Do_An_Phan_Tich_Du_Lieu_Lon.xlsx](Ke_Hoach_Thuc_Hien_Do_An_Phan_Tich_Du_Lieu_Lon.xlsx) | Bảng theo dõi tiến độ `Tuần 4 – Tuần 9` (`11/11 – 20/12/2026`, cột `SV tự đánh giá` / `GV đánh giá`). |
| - | [Ke_Hoach_Thuc_Hien_Tuan_4.md](Ke_Hoach_Thuc_Hien_Tuan_4.md) \| [Tuan_5.md](Ke_Hoach_Thuc_Hien_Tuan_5.md) \| [Tuan_6.md](Ke_Hoach_Thuc_Hien_Tuan_6.md) | Kế hoạch chi tiết từng tuần: phân công 4 thành viên, ma trận phụ thuộc, sơ đồ Mermaid và checklist DoD. |
| 4 | [Bao_Cao_Tuan_4_Phan_Tich_Du_Lieu_Lon.docx](Bao_Cao_Tuan_4_Phan_Tich_Du_Lieu_Lon.docx) | Báo cáo tiến độ Tuần 4 chính thức (chuẩn biểu mẫu CITD/UIT, bảng 3 cột). |
| 5 | [Bao_Cao_Tuan_5_Phan_Tich_Du_Lieu_Lon.docx](Bao_Cao_Tuan_5_Phan_Tich_Du_Lieu_Lon.docx) | Báo cáo tiến độ Tuần 5 chính thức (chuẩn biểu mẫu CITD/UIT, cập nhật nghiệm thu PR #6). |
| 6 | [Mau_Bao_Cao_Tuan_Phan_Tich_Du_Lieu_Lon.docx](Mau_Bao_Cao_Tuan_Phan_Tich_Du_Lieu_Lon.docx) | Mẫu báo cáo tiến độ tuần chuẩn CITD điền sẵn danh sách 4 thành viên Nhóm 11. |
| 7 | [Khung_Bao_Cao_Do_An_Phan_Tich_Du_Lieu_Lon.docx](Khung_Bao_Cao_Do_An_Phan_Tich_Du_Lieu_Lon.docx) | Khung quyển Báo cáo Word chuẩn bìa/style CITD, dàn ý **> 40 trang** khớp sheet `YeuCau` (`Mở đầu`, `Ch1 + Gap`, `Ch2 > 13p`, `Ch3 > 13p`, `Kết luận ~2p`, `TLTK`). |
| 8 | [Khung_Slide_Bao_Cao_Phan_Tich_Du_Lieu_Lon.pptx](Khung_Slide_Bao_Cao_Phan_Tich_Du_Lieu_Lon.pptx) | Bộ khung **26 Slide** (đạt ngưỡng `25–30 slide`) kế thừa theme CITD, phân công đều 4 thành viên phục vụ Video thuyết trình 20–30 phút. |

### Cấu trúc Phân hệ Mã nguồn & Tài liệu trên Repository

| Thư mục | Mục đích & Các thành phần chính |
| :--- | :--- |
| `01_data_ingestion/` | Thu thập dữ liệu giao thông thô, trích xuất cấu trúc đồ thị mạng lưới đường Hàng Xanh (`20` nút giao, `37` cạnh) và cào dữ liệu thời tiết Open-Meteo. |
| `02_pyspark_gigo/` | Pipeline tiền xử lý PySpark, kiểm tra môi trường (`spark_env_check.py`), Data Contract lưu trữ cột Parquet phân vùng tháng theo chuẩn Snappy, cấu hình Spark tinh chỉnh cho lát cắt $1\text{M} \to 17\text{M}$ dòng (`spark_tuning.py`, `[K4]`). |
| `03_benchmarks_similarity/` | Khung đo kiểm tài nguyên `benchmark_profiler.py`, benchmark Pandas vs. PySpark $1\text{M} \to 17\text{M}$ dòng (`benchmark_scaling_pandas_vs_spark.py`), vectorizer cửa sổ trượt & đối chứng Exact KNN vs. Spark LSH vs. FAISS HNSW (`smoke_test_lsh_faiss.py`, `[K3]`; `benchmark_knn_lsh_faiss.py`, `[K6]`). |
| `04_hcm_dashboard/` | Trực quan hóa bản đồ GIS Folium (`build_gis_inspection_map.py`), kiểm chứng tọa độ 20 nút Hàng Xanh và 207 cảm biến METR-LA. |
| `05_report_slides/` | Bản thảo học thuật Mở đầu & Chương 1, chuyên đề Chương 2 (đồ thị $G=(V, E, W)$ và kiến trúc Spark/Parquet + toán học LSH/HNSW `draft_c2_spark_lsh_math.md`, `[K5]`), biên bản họp GVHD và tài liệu khảo sát 5 công trình IEEE/ACM. |

---

## 3. Kiến trúc Hệ thống Độc lập (Pipeline Big Data)

```text
[ LÕI DỮ LIỆU LỚN: METR-LA (~7.1M) & PEMS-BAY (~17M) ] ──┐
[ CASE STUDY TP.HCM: OSMnx (Hàng Xanh) + Open-Meteo  ] ──┼──> [ PySpark GIGO & Columnar Parquet ]
                                                                     │
          ┌──────────────────────────────────────────────────────────┴───────────────────────────────┐
          ▼                                                                                          ▼
[ BENCHMARK HIỆU NĂNG (1M -> 17M DÒNG) ]                                       [ TRUY VẤN LỚN & TRỰC QUAN HÓA ]
• Pandas đơn máy vs. PySpark phân tán                                          • KNN vét cạn vs. LSH vs. ANN (FAISS)
• Wall-clock Time, Peak RAM, Speedup                                           • Case Study TP.HCM (đồ thị + bối cảnh thời tiết) + Dashboard
```

> **Ghép dữ liệu để phân tích OLS:** Không ghép thời tiết Hàng Xanh với METR-LA/PEMS-BAY tại California. Hồi quy mưa–tốc độ chỉ hợp lệ khi dữ liệu cùng khu vực và kỳ quan sát; nếu chưa có tốc độ Hàng Xanh thì dashboard chỉ trình bày thời tiết như bối cảnh GIS.

Nguồn và quy chuẩn tái tạo dữ liệu được ghi nhận chi tiết tại [01_data_ingestion/data_sources.md](01_data_ingestion/data_sources.md). CSV nguồn thô được lưu trữ tại `data/raw/`. Nhóm chuẩn bị 2 hình thức chuyển đổi Parquet phục vụ các mục đích khác nhau:
- **Kiểm thử & Tiền xử lý cục bộ (`[T1]`):** Tạo bằng script [convert_traffic_to_parquet.py](01_data_ingestion/convert_traffic_to_parquet.py) (Pandas/PyArrow, nén Snappy, không partition, 3 cột: `timestamp[us]`, `sensor_id`, `speed`).
- **Chuẩn hóa phân tán PySpark & Phân vùng tháng (`[K2]`):** Tuân thủ chặt chẽ [Data Contract](02_pyspark_gigo/parquet_schema_contract.md), tạo bằng script PySpark [csv_to_partitioned_parquet.py](02_pyspark_gigo/csv_to_partitioned_parquet.py) (6 cột: `timestamp` NTZ microsecond, `sensor_id`, `speed`, `hour`, `day_of_week`, `year_month`, tự động partition theo `year_month` tại `data/interim/<dataset>_sample.parquet/`).

---

## 4. Checklist Nghiệm thu Bắt buộc (Hạn chót 20/12/2026)

- [x] Chốt đề tài độc lập Hướng 3 & cập nhật Hàng 33 [Final_Project.xlsx](../Final_Project.xlsx)
- [x] **Tuần 4 (`11/11 – 17/11`):** Tải `METR-LA` & `PEMS-BAY`, trích xuất đồ thị `OSMnx` TP.HCM & API `Open-Meteo` (Đã hoàn thành 100%, xuất bản Báo cáo Tuần 4)
- [x] **Tuần 5 (`18/11 – 24/11`):** Pipeline **PySpark GIGO + Parquet**, hoàn thành **Mở đầu + Chương 1**, bản đồ GIS Folium & hoàn tất các PR #1–#4, PR #6 (Đạt ~92%)
- [ ] **Tuần 6 (`25/11 – 01/12`):** Thi giữa kỳ 90p, hoàn thành **Chương 2 (> 13 trang)** *(Phần Benchmark `Pandas vs. PySpark` 1M–17M dòng đã hoàn thành trước tại PR #6)*
- [ ] **Tuần 7 (`02/12 – 08/12`):** Đối chứng **KNN vs. LSH vs. ANN (FAISS)**, Case Study TP.HCM & hoàn thành **Chương 3 (> 13 trang)**
- [ ] **Tuần 8–9 (`09/12 – 20/12`):** Web Dashboard `Streamlit/Folium`, xuất **Báo cáo > 40 trang (Word + PDF đồng nhất)**, **Slide 26 trang**, **Video Demo (10–15p)** & **Video Thuyết trình 4 thành viên (20–30p)**.
