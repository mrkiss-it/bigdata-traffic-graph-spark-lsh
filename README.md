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
| 3 *(70)* | **Phạm Thành Trung** | `26410141` | Kỹ sư Dữ liệu (`METR-LA`, `PEMS-BAY`, `Open-Meteo`) & Pipeline tiền xử lý **GIGO** + OLS tương quan |
| 4 *(47)* | **Vũ Văn Duy** | `26410031` | Kỹ sư **Benchmark Pandas vs. PySpark (`1M–17M` dòng)**, **Web Dashboard (`Streamlit/Folium`)** & Slide/Video |

---

## 2. Cấu trúc Hồ sơ Đồ án (Chuẩn hóa theo Bộ Biểu mẫu CITD/UIT)

Toàn bộ biểu mẫu tuân thủ chuẩn văn bản CITD/UIT và đáp ứng đầy đủ rubric `YeuCau` trong [Final_Project.xlsx](../Final_Project.xlsx):

| STT | Tệp tài liệu | Mục đích & Chuẩn tham chiếu |
| :-: | :--- | :--- |
| 1 | [De_Cuong_Chi_Tiet_Do_An_Phan_Tich_Du_Lieu_Lon.md](De_Cuong_Chi_Tiet_Do_An_Phan_Tich_Du_Lieu_Lon.md) | Thuyết minh Đề cương chi tiết 7 mục + lộ trình `Tuần 4 – Tuần 9` (Markdown). |
| 2 | [De_Cuong_Chi_Tiet_Do_An_Phan_Tich_Du_Lieu_Lon.docx](De_Cuong_Chi_Tiet_Do_An_Phan_Tich_Du_Lieu_Lon.docx) | Bản Đề cương Word định dạng bảng chuẩn **CITD/UIT** (`Table 0` Quốc hiệu + `Table 1` 8x2) sẵn sàng ký duyệt. |
| 3 | [Ke_Hoach_Thuc_Hien_Do_An_Phan_Tich_Du_Lieu_Lon.xlsx](Ke_Hoach_Thuc_Hien_Do_An_Phan_Tich_Du_Lieu_Lon.xlsx) | Bảng theo dõi tiến độ `Tuần 4 – Tuần 9` (`11/11 – 20/12/2026`, cột `SV tự đánh giá` / `GV đánh giá`). |
| 4 | [Mau_Bao_Cao_Tuan_Phan_Tich_Du_Lieu_Lon.docx](Mau_Bao_Cao_Tuan_Phan_Tich_Du_Lieu_Lon.docx) | Mẫu báo cáo tiến độ tuần chuẩn CITD điền sẵn danh sách 4 thành viên Nhóm 11. |
| 5 | [Khung_Bao_Cao_Do_An_Phan_Tich_Du_Lieu_Lon.docx](Khung_Bao_Cao_Do_An_Phan_Tich_Du_Lieu_Lon.docx) | Khung quyển Báo cáo Word chuẩn bìa/style CITD, dàn ý **> 40 trang** khớp sheet `YeuCau` (`Mở đầu`, `Ch1 + Gap`, `Ch2 > 13p`, `Ch3 > 13p`, `Kết luận ~2p`, `TLTK`). |
| 6 | [Khung_Slide_Bao_Cao_Phan_Tich_Du_Lieu_Lon.pptx](Khung_Slide_Bao_Cao_Phan_Tich_Du_Lieu_Lon.pptx) | Bộ khung **26 Slide** (đạt ngưỡng `25–30 slide`) kế thừa theme CITD, phân công đều 4 thành viên phục vụ Video thuyết trình 20–30 phút. |

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
• Wall-clock Time, Peak RAM, Speedup                                           • Case Study TP.HCM (OLS Mưa) + Dashboard
```

---

## 4. Checklist Nghiệm thu Bắt buộc (Hạn chót 20/12/2026)

- [x] Chốt đề tài độc lập Hướng 3 & cập nhật Hàng 33 [Final_Project.xlsx](../Final_Project.xlsx)
- [ ] **Tuần 4 (`11/11 – 17/11`):** Tải `METR-LA` & `PEMS-BAY`, trích xuất đồ thị `OSMnx` TP.HCM & API `Open-Meteo`
- [ ] **Tuần 5 (`18/11 – 24/11`):** Pipeline **PySpark GIGO + Parquet**, hoàn thành **Mở đầu + Chương 1**
- [ ] **Tuần 6 (`25/11 – 01/12`):** Thi giữa kỳ 90p, Benchmark **Pandas vs. PySpark (`1M–17M` dòng)** & hoàn thành **Chương 2 (> 13 trang)**
- [ ] **Tuần 7 (`02/12 – 08/12`):** Đối chứng **KNN vs. LSH vs. ANN (FAISS)**, Case Study TP.HCM & hoàn thành **Chương 3 (> 13 trang)**
- [ ] **Tuần 8–9 (`09/12 – 20/12`):** Web Dashboard `Streamlit/Folium`, xuất **Báo cáo > 40 trang (Word + PDF đồng nhất)**, **Slide 26 trang**, **Video Demo (10–15p)** & **Video Thuyết trình 4 thành viên (20–30p)**.
