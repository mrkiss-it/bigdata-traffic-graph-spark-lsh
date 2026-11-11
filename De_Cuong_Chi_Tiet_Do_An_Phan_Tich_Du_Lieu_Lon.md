# ĐỀ CƯƠNG CHI TIẾT ĐỒ ÁN MÔN HỌC
## MÔN: PHÂN TÍCH DỮ LIỆU LỚN (BIG DATA ANALYTICS)
### HỌC KỲ 1 – NĂM HỌC 2026 – 2027

---

**ĐẠI HỌC QUỐC GIA TP. HỒ CHÍ MINH**  
**TRƯỜNG ĐẠI HỌC CÔNG NGHỆ THÔNG TIN**  
**TRUNG TÂM PHÁT TRIỂN CÔNG NGHỆ THÔNG TIN (CITD)**

---

### THÔNG TIN CHUNG

* **TÊN ĐỀ TÀI (Tiếng Việt):**  
  **Xây dựng pipeline xử lý dữ liệu lớn mạng lưới giao thông trên cấu trúc đồ thị không–thời gian bằng Apache Spark và kỹ thuật tìm kiếm láng giềng gần đúng (LSH/ANN): Thực nghiệm đối chứng trên METR-LA/PEMS-BAY và ứng dụng phân tích tại TP.HCM kết hợp thời tiết**
* **TÊN ĐỀ TÀI (Tiếng Anh):**  
  *Building a Distributed Big Data Processing Pipeline on Spatio-Temporal Traffic Network Graphs Using Apache Spark and Approximate Nearest Neighbor Search (LSH/ANN): Benchmark Evaluation on METR-LA/PEMS-BAY and Case Study in Ho Chi Minh City with Weather Data*
* **Cán bộ hướng dẫn (CBHD):** TS. Hà Minh Tân *(Giảng viên môn Phân tích dữ liệu lớn – ĐHQG-HCM)*
* **Mã nhóm trên danh sách lớp (`Final_Project.xlsx`):** **Nhóm 11** *(4 thành viên theo đúng quy chuẩn `2 - 4 người`)*
* **Thời gian thực hiện:** Từ ngày **11/11/2026** đến ngày **20/12/2026** *(Lộ trình gọn 6 tuần: Tuần 4 – Tuần 9)*
* **Sinh viên thực hiện (Nhóm 11 – 4 thành viên):**

| STT (Lớp) | Họ và Tên Sinh Viên | Mã Số Sinh Viên | Lớp / Nhóm | Vai Trò Đảm Trách Trong Đồ Án |
| :---: | :--- | :---: | :---: | :--- |
| 1 *(52)* | **Trần Hoàng Hôn** | `26410046` | Nhóm 11 *(NT)* | **Trưởng nhóm / PM & Kiến trúc Đồ thị TP.HCM:** Điều phối tiến độ chung, trích xuất và chuẩn hóa cấu trúc đồ thị mạng lưới đường TP.HCM (`OSMnx` – khu vực Hàng Xanh/Bình Thạnh), viết Phần Mở đầu + Chương 1 & biên tập quyển Báo cáo $\ge 40$ trang (Word + PDF). |
| 2 *(55)* | **Nguyễn Duy Khang** | `26410055` | Nhóm 11 | **Kỹ sư Xử lý Phân tán & Thuật toán Big Data:** Thiết kế pipeline phân tán **Apache Spark (PySpark)** và định dạng **Parquet**; cài đặt & đối chứng thuật toán tìm kiếm láng giềng quy mô lớn (**KNN vét cạn** vs. **LSH** vs. **ANN FAISS**), viết **Chương 2 (> 13 trang)**. |
| 3 *(70)* | **Phạm Thành Trung** | `26410141` | Nhóm 11 | **Kỹ sư Thu thập & Tiền xử lý GIGO:** Nạp và quản trị bộ dữ liệu `METR-LA` ($\sim 7.1\text{M}$ dòng) & `PEMS-BAY` ($\sim 17\text{M}$ dòng); xây dựng module thu thập API thời tiết `Open-Meteo`; cài đặt pipeline làm sạch **GIGO** (Imputation, IQR/Z-score, Min-Max/Z-score, OLS tương quan), viết phần **Dữ liệu & Kịch bản Tiền xử lý (Chương 3)**. |
| 4 *(47)* | **Vũ Văn Duy** | `26410031` | Nhóm 11 | **Kỹ sư Benchmark Hiệu năng & Trực quan hóa:** Thực hiện kịch bản đo kiểm tài nguyên (**Pandas vs. PySpark** qua các mốc $1\text{M} \to 17\text{M}$ dòng); xây dựng **Web Dashboard trực quan** (`Streamlit/Folium`); hoàn thiện **Chương 3 (> 13 trang)**, Slide 26 trang & điều phối Video demo. |

---

## I. NỘI DUNG ĐỀ TÀI

### 1. Tổng quan đề tài & Tính cấp thiết

Trong kỷ nguyên đô thị thông minh (*Intelligent Transportation Systems – ITS*), dữ liệu phát sinh từ hệ thống cảm biến vòng từ (*loop detectors*), thiết bị GPS phương tiện và trạm khí tượng đô thị đang tăng trưởng theo cấp số nhân. Việc xử lý chuỗi thời gian vận tốc gắn trên mạng lưới đường bộ có cấu trúc phi Euclid đặt ra hai bài toán cốt lõi của **Phân tích Dữ liệu lớn (Big Data Analytics)**: *(1) Làm sạch và biến đổi hiệu quả hàng chục triệu bản ghi chuỗi thời gian nhiễu cao mà không làm tràn bộ nhớ đơn máy*; và *(2) Tìm kiếm nhanh các trạng thái/mẫu hình ùn tắc tương đồng trong không gian đa chiều để khoanh vùng điểm nghẽn và phân tích quy luật lan truyền*.

Bài toán hội tụ đầy đủ **5 đặc trưng (5V) của Dữ liệu lớn**:
* **Volume (Dung lượng lớn):** Kết hợp tập dữ liệu cảm biến giao thông chuẩn quốc tế **`PEMS-BAY` ($\sim 16.93\text{ triệu bản ghi}$)** và **`METR-LA` ($\sim 7.1\text{ triệu bản ghi}$)** cùng mạng lưới đồ thị đường bộ TP.HCM (`OSMnx`).
* **Velocity (Tốc độ cập nhật):** Chuỗi dữ liệu giao thông cập nhật liên tục theo bước 5 phút (*time-series intervals*) kết hợp API thời tiết `Open-Meteo`, đòi hỏi cơ chế xử lý cửa sổ trượt (*Sliding Window*) và truy vấn tìm kiếm tương đồng đạt độ trễ mili-giây.
* **Variety (Đa dạng nguồn & cấu trúc):** Dung hợp dữ liệu chuỗi thời gian đa biến (`.h5` / `.csv`), cấu trúc **đồ thị không–thời gian $G = (V, E, W)$** (`.pkl` / `.npy` / `OSMnx`), dữ liệu **JSON REST API** (`Open-Meteo`), và định dạng lưu trữ cột **Apache Parquet**.
* **Veracity (Độ nhiễu cao – Nguyên lý GIGO):** Dữ liệu cảm biến thực tế trong `METR-LA` và `PEMS-BAY` chứa nhiều giá trị bằng `0` bất thường (*cảm biến mất tín hiệu*), giá trị khuyết `NaN` và ngoại lai. Theo nguyên lý **GIGO (*Garbage In, Garbage Out*)**, dữ liệu bắt buộc phải trải qua quy trình kiểm toán và làm sạch toán học nghiêm ngặt trước khi tính toán khoảng cách hay phân tích xu hướng.
* **Value (Giá trị khai phá):** Cho phép đối chứng định lượng giới hạn của công cụ đơn máy (**Pandas**) so với xử lý phân tán (**Apache Spark / PySpark**), thay thế thuật toán **KNN vét cạn $O(N^2)$** bằng **LSH** và **ANN (FAISS)** trên quy mô triệu vector, đồng thời minh họa thực tiễn qua **Case Study bản đồ cụm Hàng Xanh – TP.HCM kết hợp thời tiết**.

#### Khoảng trống nghiên cứu & thực tiễn (Research Gap):
1. Phần lớn công cụ phân tích thông thường xử lý trực tiếp trên RAM đơn máy (`Pandas / NumPy`) nên gặp nút thắt tràn bộ nhớ (*Out-of-Memory*) và suy giảm hiệu năng nghiêm trọng khi bảng dữ liệu vượt mốc nhiều triệu dòng.
2. Thuật toán tìm kiếm láng giềng chính xác (**Exact KNN**) có độ phức tạp $O(N \cdot d)$ cho mỗi truy vấn, không đáp ứng được yêu cầu tra cứu trạng thái kẹt xe tương đồng ở quy mô hàng triệu cửa sổ thời gian đồ thị.
3. Các nghiên cứu thuần benchmark thuật toán trên `METR-LA/PEMS-BAY` thường thiếu bước kiểm chứng chuyển giao trên mạng lưới đô thị thực tế tại Việt Nam (như TP.HCM có mật độ nút giao dày đặc và tác động rõ rệt của mưa ngập).

#### Khảo sát các nghiên cứu & công nghệ liên quan (Trích dẫn chuẩn IEEE/ACM):
* **2 Công trình nền tảng Dữ liệu lớn Giao thông & Xử lý Phân tán:**
  1. **[1] Li, Y. et al. (2018), *ICLR* – "Diffusion Convolutional Recurrent Neural Network: Data-Driven Traffic Forecasting":** Thiết lập chuẩn hóa cấu trúc đồ thị có hướng từ mạng lưới giao thông thực địa và công bố hai bộ dữ liệu chuẩn quốc tế **`METR-LA`** (207 cảm biến) và **`PEMS-BAY`** (325 cảm biến).
  2. **[2] Zaharia, M. et al. (2016), *Communications of the ACM* – "Apache Spark: A Unified Engine for Big Data Processing":** Chứng minh kiến trúc tính toán phân tán trong bộ nhớ (**RDD/DataFrame DAG** và **Catalyst Optimizer**) vượt trội hoàn toàn so với MapReduce đọc/ghi đĩa cứng khi xử lý dữ liệu quy mô lớn.
* **2 Công trình về Tìm kiếm Tương đồng Quy mô lớn & Đồ thị Đô thị:**
  3. **[3] Johnson, J., Douze, M., & Jégou, H. (2019), *IEEE Transactions on Big Data* – "Billion-scale similarity search with GPUs" (FAISS) & Indyk, P., Motwani, R. (1998), *STOC* (LSH):** Giải quyết bài toán truy vấn láng giềng gần đúng quy mô triệu–tỷ vector thông qua **Locality-Sensitive Hashing (LSH)**, **Inverted File with Product Quantization (IVF-PQ)** và đồ thị phân cấp **HNSW**.
  4. **[4] Boeing, G. (2017), *Computers, Environment and Urban Systems* – "OSMnx" & Tạp chí Giao thông Vận tải Việt Nam (2023):** Chuẩn hóa mạng lưới đường bộ OpenStreetMap thành đồ thị có hướng tại đô thị và phân tích tương quan suy giảm vận tốc giao thông tại TP.HCM dưới tác động của vũ lượng mưa lớn.

#### Sơ đồ tổng quan Kiến trúc Hệ thống (Pipeline Big Data Độc lập):
```text
┌─────────────────────────────────────────────────────────────────────────────────┐
│ 1. DATA INGESTION (Thu thập đa nguồn - Volume & Variety)                        │
│   ├── Lõi Benchmark Dữ liệu lớn: METR-LA (~7.1M dòng) & PEMS-BAY (~17M dòng)    │
│   └── Case Study TP.HCM: Đồ thị OSMnx (Hàng Xanh – Bình Thạnh) + API Open-Meteo │
└───────────────────────────────────────┬─────────────────────────────────────────┘
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ 2. DISTRIBUTED PREPROCESSING & GIGO (Apache Spark / PySpark + Parquet)          │
│   ├── Chuyển đổi định dạng lưu trữ cột Parquet (Partitioning + Snappy)          │
│   ├── Kiểm toán & Xử lý GIGO: Missing/Zero Sensors (Interpolation), Outlier IQR │
│   └── Chuẩn hóa thang đo (Min-Max / Z-score), OLS tương quan & Vector hóa cửa sổ│
└───────────────────┬─────────────────────────────────────┬───────────────────────┘
                    ▼                                     ▼
┌─────────────────────────────────────────┐ ┌─────────────────────────────────────┐
│ 3A. SCALABLE ANALYTICS & BENCHMARKS     │ │ 3B. CASE STUDY TP.HCM & DASHBOARD   │
│  • Đối chứng Pandas vs. PySpark (1M-17M)│ │  • Ánh xạ đồ thị Hàng Xanh + Mưa    │
│  • Đối chứng KNN vét cạn vs. LSH vs. ANN│ │  • Thống kê cụm nghẽn & hồi quy OLS │
│    (Recall@K, Latency ms, Index RAM)    │ │  • Web Dashboard (Streamlit/Folium) │
└─────────────────────────────────────────┘ └─────────────────────────────────────┘
```

---

### 2. Mục tiêu của đề tài

#### Mục tiêu tổng quát:
Xây dựng hoàn chỉnh một **pipeline phân tích dữ liệu lớn phân tán trên Apache Spark (PySpark)** kết hợp với các **thuật toán tìm kiếm láng giềng gần đúng quy mô lớn (LSH/ANN)** trên nền cấu trúc đồ thị không–thời gian; kiểm chứng toàn diện hiệu năng và độ chính xác trên tập dữ liệu thực tế hàng chục triệu bản ghi (`METR-LA / PEMS-BAY`), đồng thời triển khai minh họa ứng dụng trực quan cho mạng lưới giao thông TP.HCM (cụm Hàng Xanh) kết hợp dữ liệu thời tiết.

#### 4 Mục tiêu kỹ thuật cụ thể:
1. **Xử lý Phân tán & Chuẩn hóa GIGO Quy mô 1M–17M Dòng:** Vận hành pipeline **PySpark + Columnar Parquet** xử lý sạch $100\%$ giá trị `0`/khuyết thiếu cảm biến, loại bỏ ngoại lai (IQR/Z-score) và chuẩn hóa đa biến không tràn bộ nhớ.
2. **Định lượng Hiệu năng Hệ thống (Pandas vs. PySpark):** Thiết lập thực nghiệm đối chứng thời gian xử lý (*Wall-clock Time*), mức tiêu thụ RAM (*Peak RAM*) và hệ số tăng tốc (*Speedup*) ở các lát cắt $1\text{M}, 5\text{M}, 10\text{M}, 17\text{M}$ dòng.
3. **Làm chủ Thuật toán Tìm kiếm Tương đồng Lớn (KNN vs. LSH vs. ANN FAISS):** Chứng minh bằng lý thuyết và thực nghiệm sự đánh đổi giữa tốc độ truy vấn (*Query Latency*) và độ chính xác (*Recall@K*) khi tìm kiếm mẫu hình ùn tắc trên không gian triệu vector.
4. **Trực quan hóa Case Study TP.HCM & Hoàn thiện Hồ sơ Chuẩn Học thuật:** Triển khai Web Dashboard (`Streamlit/Folium`) phân tích nút giao Hàng Xanh kết hợp API thời tiết `Open-Meteo` (hồi quy OLS/tương quan), hoàn chỉnh quyển Báo cáo $\ge 40$ trang, 26 Slides và 2 Video nộp **TS. Hà Minh Tân**.

---

### 3. Phương pháp thực hiện & Cơ sở toán học (Trọng tâm Chương 2 > 13 trang)

* **Kiến trúc Lưu trữ & Tính toán Phân tán:**
  * Nền tảng GFS/HDFS, cơ chế MapReduce (*Map – Shuffle & Sort – Reduce*) và kiến trúc thực thi trong bộ nhớ của **Apache Spark** (*RDD lineage, DataFrame DAG, Catalyst Optimizer, Partitioning với Parquet*).
* **Nguyên lý GIGO & Toán học Tiền xử lý / Hồi quy OLS:**
  * *Điền khuyết (Imputation):* Nội suy tuyến tính chuỗi thời gian và `Forward-fill` giới hạn cửa sổ theo từng mã cảm biến/cạnh đồ thị.
  * *Phát hiện ngoại lai:* Khoảng tứ phân vị **IQR** ($[Q_1 - 1.5\cdot IQR, Q_3 + 1.5\cdot IQR]$) và ngưỡng vật lý vận tốc giao thông.
  * *Chuẩn hóa thang đo:* **Min-Max Scaling** $x' = \frac{x - x_{\min}}{x_{\max} - x_{\min}}$ và **Z-score Standardization** $z = \frac{x - \mu}{\sigma}$.
  * *Hồi quy tuyến tính OLS (*Ordinary Least Squares*):* Cực tiểu hóa tổng bình phương sai số $SSE = \sum (y_i - \hat{y}_i)^2$ và đánh giá $R^2$ để định lượng mối quan hệ giữa lượng mưa (`Open-Meteo`) với tốc độ lưu thông.
* **Biểu diễn Đồ thị Không–Thời gian $G = (V, E, W)$:**
  * Xây dựng ma trận kề trọng số dựa trên khoảng cách mạng lưới đường bộ theo hàm nhân Gaussian chuẩn hóa:
    $$W_{ij} = \begin{cases} \exp\left(-\dfrac{\text{dist}(v_i, v_j)^2}{\sigma^2}\right), & \text{nếu } \text{dist}(v_i, v_j) \le \kappa \text{ và } \exp(\cdot) \ge \epsilon \\ 0, & \text{ngược lại} \end{cases}$$
* **Thuật toán Tìm kiếm Láng giềng Quy mô lớn (KNN vs. LSH vs. ANN FAISS):**
  * **Baseline Exact KNN:** Đo khoảng cách Euclidean / Cosine ($O(N \cdot d)$ mỗi truy vấn) — nút thắt cổ chai khi $N$ đạt hàng triệu vector trượt.
  * **LSH (*Locality-Sensitive Hashing*):** Định lý hàm băm $(r_1, r_2, p_1, p_2)$-sensitive, kỹ thuật *Random Hyperplane Projection* (cho Cosine Similarity) và *p-stable LSH* (cho khoảng cách $L_2$).
  * **ANN (*FAISS*):** Phân vùng không gian Voronoi (**IVF**), nén mã vector bằng Lượng tử hóa tích (**Product Quantization – PQ**), và duyệt đồ thị thế giới nhỏ phân cấp nhiều tầng (**HNSW**).

---

### 4. Các nội dung chính của đề tài & Khung Báo cáo $> 40$ trang

#### 6 Nội dung thực hiện chính:
* **Nội dung 1: Khảo sát lý thuyết, thu thập & phân tích dữ liệu đa nguồn (`METR-LA`, `PEMS-BAY`, `OSMnx`, `Open-Meteo`)**
  * *Thực hiện cái gì:* Tải và kiểm tra schema tập `METR-LA` ($\sim 7.1\text{M}$ dòng) và `PEMS-BAY` ($\sim 17\text{M}$ dòng); trích xuất đồ thị `OSMnx` cụm Hàng Xanh TP.HCM và dữ liệu lịch sử mưa/độ ẩm từ `Open-Meteo`.
  * *Thực hiện như thế nào:* Sử dụng Python (`h5py`, `osmnx`, `requests`, `pyspark`) chuyển đổi sang cấu trúc lưu trữ cột **Parquet**.
  * *Dự kiến kết quả:* Kho dữ liệu thô chuẩn hóa và báo cáo kiểm toán chất lượng ban đầu (EDA).
* **Nội dung 2: Thiết kế Pipeline Tiền xử lý GIGO phân tán trên Apache Spark**
  * *Thực hiện cái gì:* Xử lý triệt để cảm biến báo `0`/`NaN`, lọc ngoại lai IQR, chuẩn hóa Min-Max/Z-score và tạo vector cửa sổ trượt không–thời gian.
  * *Thực hiện như thế nào:* Lập trình bằng **PySpark DataFrame / Spark SQL Window Functions** trên Colab/Local Spark Cluster.
  * *Dự kiến kết quả:* Tập dữ liệu sạch $100\%$ đạt chuẩn GIGO, lưu dạng Parquet phân vùng tối ưu.
* **Nội dung 3: Thực nghiệm Benchmark Hiệu năng Tính toán (Pandas vs. PySpark)**
  * *Thực hiện cái gì:* Đo đạc và đối chứng tốc độ xử lý (*Runtime*) và đỉnh tiêu thụ bộ nhớ (*Peak RAM*) ở các ngưỡng `1M -> 5M -> 10M -> 17M` bản ghi.
  * *Thực hiện như thế nào:* Chạy cùng một bộ tác vụ Join, Aggregation, Window Rolling và Scaling trên Pandas đơn luồng và PySpark phân tán.
  * *Dự kiến kết quả:* Bảng và biểu đồ chứng minh điểm gãy Out-of-Memory của Pandas và hệ số tăng tốc (*Speedup*) của PySpark.
* **Nội dung 4: Cài đặt & Đối chứng Thuật toán Tìm kiếm Tương đồng (KNN vs. LSH vs. ANN FAISS)**
  * *Thực hiện cái gì:* Truy vấn tìm $K$ khung giờ có trạng thái mạng lưới ùn tắc tương đồng nhất trên tập hàng triệu vector cửa sổ trượt.
  * *Thực hiện như thế nào:* Thực nghiệm đối chiếu **KNN vét cạn**, **MinHash/Random Projection LSH** (PySpark/Python) và **FAISS (`IndexFlatL2`, `IVF-PQ`, `HNSW`)**.
  * *Dự kiến kết quả:* Đường cong đánh đổi (*Trade-off curves*) giữa `Query Latency (ms)`, `Recall@K` và `Memory Footprint`.
* **Nội dung 5: Case Study Đồ thị TP.HCM + Thời tiết & Web Dashboard Trực quan**
  * *Thực hiện cái gì:* Mô hình hóa đồ thị Hàng Xanh (`OSMnx`), kiểm định tương quan OLS giữa lượng mưa `Open-Meteo` và mức suy giảm vận tốc đô thị; đóng gói giao diện Web tương tác.
  * *Thực hiện như thế nào:* Xây dựng ứng dụng **Streamlit + Folium** hiển thị bản đồ mạng lưới, biểu đồ Benchmark Spark/LSH/ANN và kịch bản tra cứu điểm nghẽn Hàng Xanh.
  * *Dự kiến kết quả:* Web Dashboard chạy trực tiếp phục vụ quay Video Demo code 10–15 phút.
* **Nội dung 6: Biên tập Quyển Báo cáo $> 40$ trang, Slide $25\text{–}30$ trang & Video Thuyết trình**
  * *Thực hiện cái gì:* Hoàn thiện quyển báo cáo Word + PDF đồng nhất, bộ Slide 26 trang chuẩn CITD và ghi hình đầy đủ 4 thành viên thuyết trình.
  * *Thực hiện như thế nào:* Áp dụng bộ biểu mẫu CITD/UIT đã chuẩn hóa theo đúng rubric `YeuCau` của TS. Hà Minh Tân.
  * *Dự kiến kết quả:* Hoàn tất 100% hồ sơ nộp trên Google Drive lớp trước `20/12/2026`.

#### Cấu trúc Quyển Báo cáo $\ge 40$ trang (Khớp 100% sheet `YeuCau`):
1. **Mở đầu (~2–3 trang):** Bối cảnh bài toán, giá trị thực tiễn, lý do chọn đề tài và phạm vi thực hiện.
2. **Chương 1 – Tổng quan bài toán & Các công nghệ liên quan (~8–10 trang):** Đặc trưng 5V Big Data, Tổng quan công trình liên quan *(trích dẫn IEEE/ACM)*, Kiến trúc tổng thể và **Khoảng trống nghiên cứu (Research Gap)**.
3. **Chương 2 – Phương pháp áp dụng cho bài toán (> 13 trang – Trọng tâm):** Kiến trúc HDFS/Parquet/PySpark, Toán học GIGO & OLS, Mô hình đồ thị $G=(V,E,W)$ & Kernel Gaussian, Lý thuyết tìm kiếm tương đồng **KNN vét cạn vs. LSH vs. ANN (FAISS)**.
4. **Chương 3 – Triển khai hệ thống, Thực nghiệm & Đánh giá (> 13 trang – Trọng tâm):** Mô tả dữ liệu & môi trường; **KB1:** Kiểm toán GIGO & EDA; **KB2:** Benchmark Pandas vs. PySpark (`1M–17M` dòng); **KB3:** Đối chứng KNN vs. LSH vs. ANN (`Recall@K`, `Latency`, `RAM`); **KB4:** Case Study TP.HCM (`OSMnx` + `Open-Meteo`) & Web Dashboard.
5. **Kết luận & Hướng phát triển (~2–3 trang)** + **Tài liệu tham khảo (Chuẩn IEEE/ACM)**.

---

### 5. Giới hạn của đề tài

* **Giới hạn phạm vi dữ liệu thực nghiệm:** Lõi kiểm thử quy mô lớn sử dụng dữ liệu cảm biến thực tế **`METR-LA`** ($\sim 7.1\text{M}$ dòng) và **`PEMS-BAY`** ($\sim 17\text{M}$ dòng); phần **Case Study TP.HCM** tập trung tại khu vực **Cụm Ngã tư Hàng Xanh – Bình Thạnh** (`OSMnx` + `Open-Meteo` lịch sử/hiện tại) nhằm trực quan hóa ứng dụng thực tiễn do tại Việt Nam chưa có bộ dữ liệu cảm biến vòng từ 24/7 công khai quy mô tương đương.
* **Giới hạn hạ tầng thực thi:** Cụm xử lý phân tán **Apache Spark (PySpark)** và thư viện **FAISS** được triển khai trên **Google Colab / Docker cục bộ đa lõi CPU**, mô phỏng cơ chế phân vùng Parquet và Catalyst Optimizer mà không đòi hỏi cụm máy chủ đám mây trả phí.
* **Giới hạn phạm vi môn học:** Đề tài tập trung chuyên sâu vào kỹ thuật **Dữ liệu lớn (5V, PySpark phân tán, Parquet, Tiền xử lý GIGO, Benchmark tài nguyên, Tìm kiếm láng giềng LSH/ANN và trực quan hóa)** đúng trọng tâm môn *Phân tích dữ liệu lớn*.

---

### 6. Đối tượng và phạm vi nghiên cứu

* **Đối tượng nghiên cứu:** Dữ liệu chuỗi thời gian không–thời gian giao thông quy mô lớn (`METR-LA`, `PEMS-BAY`), đồ thị đường bộ `OSMnx`, dữ liệu API thời tiết `Open-Meteo`, hệ sinh thái **Apache Spark / PySpark / Columnar Parquet**, quy trình **GIGO**, và các giải thuật tìm kiếm tương đồng quy mô lớn (**KNN, LSH, ANN FAISS**).
* **Phạm vi nghiên cứu:** Ứng dụng **Phân tích Dữ liệu lớn** trong bài toán **Giao thông Thông minh (ITS)**, từ khâu thu thập/làm sạch dữ liệu nhiều triệu bản ghi đến truy vấn mẫu hình ùn tắc nhanh và trực quan hóa trên Dashboard.

---

### 7. Kết quả mong đợi (Khớp 100% Rubric `YeuCau` – TS. Hà Minh Tân)

1. **Quyển Báo cáo Đồ án (`01 Quyển`):** Bộ tệp **Word (`.docx`) và PDF (`.pdf`) giống hệt nhau**, độ dài **> 40 trang** *(Chương 2 > 13 trang, Chương 3 > 13 trang)*, trình bày chuẩn học thuật UIT/CITD.
2. **Hồ sơ Trình bày (`02 Trình bày`):** Bộ **Slide thuyết trình 25–30 trang** (`.pptx`) và **Video trình bày đồ án 20–30 phút** có mặt và thuyết trình đầy đủ cả **4 thành viên Nhóm 11**.
3. **Sản phẩm Kỹ thuật (`03 Sản phẩm`):**
   * Kho mã nguồn sạch + bộ dữ liệu thực nghiệm & kết quả chạy (`PySpark GIGO`, `Benchmark Pandas vs Spark`, `KNN vs LSH vs ANN FAISS`, `Case Study TP.HCM + Open-Meteo`, `Web Dashboard Streamlit/Folium`).
   * **Video demo mã nguồn & giải thích thực nghiệm (10–15 phút)**.

---

## II. KẾ HOẠCH THỰC HIỆN CHI TIẾT (LỘ TRÌNH GỌN TUẦN 4 – TUẦN 9)

| Tuần | Thời Gian | Nội Dung Công Việc Cụ Thể | Thành Viên Phụ Trách | Sản Phẩm Bàn Giao | Mức Độ |
| :---: | :---: | :--- | :--- | :--- | :---: |
| **Tuần 4** | `11/11 – 17/11/2026` | • Chốt Đề cương chi tiết & cập nhật Hàng 33 `Final_Project.xlsx` trình TS. Hà Minh Tân.<br>• Tải & kiểm tra schema `METR-LA`, `PEMS-BAY` ($\sim 7\text{M} - 17\text{M}$ dòng).<br>• Cào đồ thị `OSMnx` TP.HCM (cụm Hàng Xanh) & API thời tiết `Open-Meteo`. | **Trần Hoàng Hôn** *(NT)*<br>**Phạm Thành Trung**<br>**Nguyễn Duy Khang**<br>**Vũ Văn Duy** | • `De_Cuong_Chi_Tiet...` & `Ke_Hoach...xlsx`<br>• Dữ liệu thô chuẩn bị sẵn sàng + File đồ thị TP.HCM | **0%** |
| **Tuần 5** | `18/11 – 24/11/2026` | • Triển khai pipeline **PySpark** chuyển đổi dữ liệu sang **Parquet** & thực thi làm sạch **GIGO** (Imputation giá trị 0/NaN, khử ngoại lai IQR, chuẩn hóa Min-Max/Z-score).<br>• Viết hoàn chỉnh **Mở đầu & Chương 1** (Tổng quan, 5V, Trích dẫn TLTK & Research Gap). | **Phạm Thành Trung**<br>**Nguyễn Duy Khang**<br>**Trần Hoàng Hôn** | • Notebook/Script PySpark GIGO & dữ liệu sạch Parquet<br>• Toàn văn Mở đầu + Chương 1 (~10–12 trang) | **0%** |
| **Tuần 6** | `25/11 – 01/12/2026` | • Hoàn thành bài thi **Giữa kỳ viết tay 90 phút** trên lớp.<br>• Chạy thực nghiệm **Benchmark tài nguyên (Pandas vs. PySpark)** qua các mốc $1\text{M} \to 5\text{M} \to 10\text{M} \to 17\text{M}$ dòng.<br>• Viết **Chương 2 (> 13 trang)** trình bày cơ sở toán học Spark, GIGO, OLS, Đồ thị & LSH/ANN. | **Vũ Văn Duy**<br>**Nguyễn Duy Khang**<br>**Trần Hoàng Hôn** | • Bộ biểu đồ & bảng số liệu Benchmark Pandas vs. Spark<br>• Toàn văn Chương 2 (> 13 trang) | **0%** |
| **Tuần 7** | `02/12 – 08/12/2026` | • Cài đặt và thực nghiệm đối chứng **KNN vét cạn vs. LSH vs. ANN (FAISS)** (`Recall@K`, `Latency ms`, `RAM`).<br>• Triển khai **Case Study TP.HCM (`OSMnx` + `Open-Meteo` OLS)**.<br>• Viết **Chương 3 (> 13 trang)** phân tích sâu các kịch bản thực nghiệm. | **Nguyễn Duy Khang**<br>**Phạm Thành Trung**<br>**Vũ Văn Duy** | • Notebook đối chứng KNN/LSH/ANN + Case Study TP.HCM<br>• Toàn văn Chương 3 (> 13 trang) | **0%** |
| **Tuần 8** | `09/12 – 15/12/2026` | • Xây dựng **Web Dashboard (`Streamlit/Folium`)** trực quan hóa bản đồ Hàng Xanh, chuỗi thời gian và kết quả Benchmark.<br>• Đóng gói mã nguồn sạch trên repo, hoàn chỉnh bản thảo **Quyển Báo cáo $\ge 40$ trang** + Kết luận + TLTK IEEE. | **Cả nhóm 4 thành viên**<br>*(Trần Hoàng Hôn điều phối)* | • Web Dashboard chạy ổn định<br>• Bản thảo Quyển Báo cáo $\ge 40$ trang hoàn chỉnh | **0%** |
| **Tuần 9** | `16/12 – 20/12/2026` | • Xuất **Word + PDF đồng nhất 100%** (> 40 trang), kiểm tra định dạng chuẩn CITD.<br>• Hoàn thiện **Slide 25–30 trang** (theo khung 26 slide).<br>• Quay **Video Demo Code (10–15p)** & **Video Trình bày đủ 4 người (20–30p)** nộp Google Drive lớp. | **Cả nhóm 4 thành viên** | • **HOÀN TẤT 100% HỒ SƠ NỘP TS. HÀ MINH TÂN** *(Word, PDF, PPTX, Code+Data, 2 Videos)* | **0%** |

---

### XÁC NHẬN CỦA CÁC BÊN

**CÁN BỘ HƯỚNG DẪN**  
*(Ký và ghi rõ họ tên)*  
<br><br><br><br>  
**TS. Hà Minh Tân**

*TP. Hồ Chí Minh, ngày 11 tháng 11 năm 2026*  
**ĐẠI DIỆN NHÓM SINH VIÊN THỰC HIỆN (NHÓM 11)**  
*(Ký và ghi rõ họ tên)*  
<br><br><br><br>  
**Trần Hoàng Hôn** *(Trưởng nhóm)*
