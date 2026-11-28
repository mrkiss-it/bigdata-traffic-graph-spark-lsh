# ĐÓNG GÓP CHƯƠNG 2: KIẾN TRÚC TÍNH TOÁN PHÂN TÁN SPARK – PARQUET & CƠ SỞ TOÁN HỌC TÌM KIẾM LÁNG GIỀNG (KNN, LSH, FAISS HNSW)
## BẢN THẢO `[K5]` PHỤC VỤ MỤC 2.1 VÀ MỤC 2.4 CỦA CHƯƠNG 2
**Môn học:** Phân tích Dữ liệu Lớn (*Big Data Analytics*) — Học kỳ 1, Năm học 2026–2027  
**Nhóm thực hiện:** Nhóm 11 — CITD / Trường Đại học Công nghệ Thông tin, ĐHQG-HCM  
**Thành viên phụ trách:** Nguyễn Duy Khang (Kỹ sư PySpark & Thuật toán LSH/ANN — MSSV: `26410055`)  
**Tài liệu tham chiếu:** [draft_mo_dau_va_chuong_1.md](./draft_mo_dau_va_chuong_1.md) | [literature_survey_notes.md](./literature_survey_notes.md) | [parquet_schema_contract.md](../02_pyspark_gigo/parquet_schema_contract.md) | [smoke_test_lsh_faiss.py](../03_benchmarks_similarity/smoke_test_lsh_faiss.py)

> **Ghi chú biên tập cho Tổng biên tập Chương 2 (`[H9b]`):** Bản thảo gồm hai khối độc lập, nạp vào vị trí **Mục 2.1** (*Kiến trúc Spark & Parquet*) và **Mục 2.4** (*Toán học LSH & FAISS HNSW*) theo cấu trúc 5 mục của Kế hoạch Tuần 6. Số hiệu tài liệu tham khảo `[1]–[5]` giữ nguyên như Chương 1; các tài liệu mới đánh số tiếp từ `[6]` (danh mục ở cuối). Số liệu thực nghiệm chỉ được nêu ở mức minh họa cho lý thuyết; phân tích đầy đủ thuộc Chương 3.

---

## 2.1. KIẾN TRÚC TÍNH TOÁN PHÂN TÁN APACHE SPARK VÀ ĐỊNH DẠNG LƯU TRỮ CỘT PARQUET

### 2.1.1. Từ mô hình MapReduce đến mô hình tính toán trong bộ nhớ

Mô hình **MapReduce** (Dean & Ghemawat [6]) biểu diễn một phép tính phân tán thành hai hàm do người dùng định nghĩa:

$$\text{map}: (k_1, v_1) \rightarrow \text{list}(k_2, v_2), \qquad \text{reduce}: (k_2, \text{list}(v_2)) \rightarrow \text{list}(v_3)$$

Giữa hai pha là bước **Shuffle & Sort**: mọi cặp trung gian có cùng khóa $k_2$ được băm về cùng một tác vụ *reduce*. Hệ thống lưu trữ đi kèm, **HDFS**, chia tệp thành các khối cố định (mặc định $128\ \text{MB}$), nhân bản mỗi khối $3$ lần trên các *DataNode* và để *NameNode* quản lý siêu dữ liệu; bộ lập lịch ưu tiên chạy tác vụ *map* ngay trên nút đang giữ khối dữ liệu (*data locality*).

Hạn chế cốt lõi của MapReduce đối với bài toán của đề tài là **mọi kết quả trung gian giữa hai job đều được ghi xuống đĩa phân tán**. Một pipeline gồm nhiều bước nối tiếp — đọc dữ liệu thô, xoay trục (*unpivot*) ma trận thời gian × cảm biến, làm sạch GIGO, tính cửa sổ trượt, băm LSH — sẽ trở thành một chuỗi job, mỗi job trả giá một lần ghi và một lần đọc lại toàn bộ dữ liệu. Với các thuật toán lặp hoặc truy vấn tương tác, chi phí I/O này áp đảo chi phí tính toán.

**Apache Spark** (Zaharia et al. [2], [7]) giải quyết hạn chế trên bằng trừu tượng **RDD** (*Resilient Distributed Dataset*): một tập hợp bất biến, được phân hoạch thành các *partition* trên nhiều nút, và có thể được giữ lại trong bộ nhớ để tái sử dụng giữa các phép tính. Một RDD không lưu bản sao dữ liệu để chịu lỗi; thay vào đó nó ghi nhớ **dòng dõi** (*lineage*) — chuỗi phép biến đổi đã tạo ra nó. Khi một partition bị mất, Spark chỉ tính lại đúng partition đó từ dòng dõi.

### 2.1.2. Đồ thị thực thi DAG, phụ thuộc hẹp/rộng và ranh giới Stage

Các phép toán Spark chia thành hai nhóm:
* **Biến đổi** (*transformations*: `select`, `where`, `groupBy`, `join`, …) chỉ được ghi nhận, không thực thi ngay — cơ chế **đánh giá lười** (*lazy evaluation*).
* **Hành động** (*actions*: `count`, `collect`, `write`, …) kích hoạt việc thực thi toàn bộ chuỗi biến đổi phía trước.

Khi một hành động được gọi, *DAGScheduler* dựng **đồ thị có hướng không chu trình** (*Directed Acyclic Graph – DAG*) của các RDD và phân loại từng cạnh phụ thuộc:
* **Phụ thuộc hẹp** (*narrow dependency*): mỗi partition cha được dùng bởi tối đa một partition con (ví dụ `map`, `filter`). Các phép này được **gộp ống** (*pipelining*) trong cùng một tác vụ, không cần trao đổi dữ liệu qua mạng.
* **Phụ thuộc rộng** (*wide dependency*): một partition con cần dữ liệu từ nhiều partition cha (ví dụ `groupByKey`, `join` không đồng phân hoạch, `Window.partitionBy`). Đây là điểm phải **shuffle**.

DAG được cắt thành các **Stage** tại mỗi ranh giới shuffle; mỗi Stage gồm $P$ tác vụ (*task*) song song, với $P$ là số partition. Nếu bỏ qua chi phí lập lịch, thời gian của một Stage được chặn bởi tác vụ chậm nhất:

$$T_{\text{stage}} \approx \max_{1 \le p \le P} \left( T_{\text{read}}^{(p)} + T_{\text{compute}}^{(p)} + T_{\text{write}}^{(p)} \right)$$

Biểu thức này giải thích hai hiện tượng quan sát được trong thực nghiệm của đề tài: (i) với dữ liệu nhỏ, chi phí khởi động JVM và lập lịch tác vụ chiếm phần lớn thời gian nên Spark chậm hơn Pandas; (ii) khi dữ liệu bị **lệch** (*data skew*) — một partition lớn hơn hẳn các partition khác — thời gian Stage tăng theo partition lớn nhất chứ không theo trung bình.

**Áp dụng trong đề tài — vectorizer cửa sổ trượt.** Phép tính cửa sổ trượt của `[K3]` dùng hàm cửa sổ Spark SQL:

```python
w = Window.partitionBy("sensor_id").orderBy("timestamp").rowsBetween(-11, 0)
F.collect_list("speed").over(w)
```

`partitionBy("sensor_id")` sinh **một** lần shuffle băm theo mã cảm biến (phụ thuộc rộng), sau đó mỗi partition được sắp xếp cục bộ theo `timestamp` và khung $12$ dòng được trượt tuần tự (phụ thuộc hẹp). Vì METR-LA có $207$ cảm biến với số mốc bằng nhau ($34.272$ mốc), dữ liệu sau shuffle phân bố đều giữa các partition, tránh được hiện tượng lệch.

### 2.1.3. Spark SQL, DataFrame và bộ tối ưu hóa Catalyst

Từ Spark 2.x, giao diện chính là **DataFrame** — tập dữ liệu phân tán có lược đồ (*schema*) — được tối ưu bởi **Catalyst** (Armbrust et al. [8]). Catalyst biểu diễn truy vấn dưới dạng cây và áp dụng các quy tắc biến đổi cây qua bốn pha:

1. **Phân tích** (*Analysis*): phân giải tên cột, kiểu dữ liệu dựa trên *catalog*, tạo kế hoạch logic đã phân giải.
2. **Tối ưu kế hoạch logic** (*Logical Optimization*): các quy tắc dựa trên luật như **đẩy vị từ xuống** (*predicate pushdown*), **cắt tỉa cột** (*column pruning*), gộp hằng số (*constant folding*), rút gọn biểu thức boolean.
3. **Lập kế hoạch vật lý** (*Physical Planning*): sinh nhiều kế hoạch vật lý và chọn theo mô hình chi phí — ví dụ chọn *Broadcast Hash Join* khi một bảng nhỏ hơn ngưỡng `spark.sql.autoBroadcastJoinThreshold`, ngược lại dùng *Sort-Merge Join*.
4. **Sinh mã** (*Code Generation*): biên dịch kế hoạch vật lý thành bytecode JVM.

Trong pipeline của đề tài, truy vấn đọc một tháng dữ liệu

```python
spark.read.parquet(".../metr_la_sample.parquet").where("year_month = '2012-05'").select("sensor_id", "speed")
```

được Catalyst chuyển thành kế hoạch vật lý có `PartitionFilters: [year_month = 2012-05]` (chỉ mở thư mục của tháng 5) và `ReadSchema` chỉ gồm hai cột `sensor_id`, `speed` (bỏ qua các cột còn lại ngay ở tầng đọc tệp). Hai tối ưu này chỉ khả thi vì dữ liệu được lưu theo định dạng cột có phân vùng (Mục 2.1.5).

### 2.1.4. Động cơ thực thi Tungsten

**Tungsten** là tầng thực thi của Spark SQL, tập trung vào hiệu quả sử dụng CPU và bộ nhớ thay vì I/O:
* **Quản lý bộ nhớ nhị phân tường minh:** các bản ghi được lưu ở định dạng nhị phân nhỏ gọn (`UnsafeRow`) trong vùng nhớ do Spark tự quản lý (có thể ngoài heap JVM), tránh chi phí đối tượng Java và áp lực bộ gom rác (*garbage collector*).
* **Tính toán thân thiện bộ nhớ đệm** (*cache-aware computation*): cấu trúc dữ liệu cho sắp xếp và băm được thiết kế để truy cập tuần tự, tận dụng bộ nhớ đệm CPU.
* **Sinh mã toàn Stage** (*Whole-Stage Code Generation*): theo mô hình *produce/consume* của Neumann [9], chuỗi toán tử liền nhau trong một Stage (ví dụ `Scan → Filter → Project → HashAggregate`) được gộp thành **một hàm Java duy nhất** với vòng lặp chặt, loại bỏ lời gọi hàm ảo giữa các toán tử của mô hình *Volcano iterator* truyền thống.
* **Đọc Parquet vector hóa:** bộ đọc Parquet trả về các lô cột (*ColumnarBatch*) thay vì từng dòng, phù hợp với cách Parquet lưu dữ liệu.

**Arrow và ranh giới Python–JVM.** Khi dữ liệu đi qua ranh giới giữa JVM và Python (`toPandas`, `createDataFrame` từ Pandas), Spark dùng định dạng cột **Apache Arrow** để chuyển theo lô, tránh tuần tự hóa từng dòng bằng `pickle`. Cấu hình môi trường của đề tài (`[K1]`) bật Arrow và **tắt cơ chế tự quay về đường chậm** (`spark.sql.execution.arrow.pyspark.fallback.enabled=false`) để mọi lỗi tương thích Arrow lộ ra ngay thay vì âm thầm làm chậm pipeline.

### 2.1.5. Định dạng lưu trữ cột Parquet và nén Snappy

**Parquet** là định dạng lưu trữ cột lai (*hybrid columnar*) kế thừa mô hình mã hóa dữ liệu lồng nhau của Dremel [10]. Một tệp Parquet có cấu trúc phân cấp:

```text
Tệp Parquet
 ├── Row Group 1  (nhóm dòng, thường 128 MB)
 │    ├── Column Chunk "timestamp" ── Page 1, Page 2, ...
 │    ├── Column Chunk "sensor_id" ── Page 1, Page 2, ...
 │    └── Column Chunk "speed"     ── Page 1, Page 2, ...
 ├── Row Group 2 ...
 └── Footer: schema + thống kê min/max/null_count của từng column chunk
```

Các đặc tính then chốt:

1. **Cắt tỉa cột:** truy vấn chỉ cần $p$ trong $C$ cột thì chỉ đọc các *column chunk* tương ứng. Với các cột có kích thước xấp xỉ nhau, khối lượng I/O giảm theo tỷ lệ $\approx p / C$ so với định dạng dòng (CSV).
2. **Bỏ qua nhóm dòng nhờ thống kê** (*row-group skipping*): vị từ `speed > 60` được so với cặp $(\min, \max)$ ở footer; nhóm dòng có $\max \le 60$ bị bỏ qua mà không cần giải nén.
3. **Phân vùng thư mục kiểu Hive:** dữ liệu được ghi thành các thư mục `year_month=2012-03/`, `year_month=2012-04/`, …; điều kiện trên cột phân vùng loại bỏ cả thư mục ở bước lập kế hoạch (*partition pruning*).
4. **Mã hóa theo cột:** các giá trị cùng kiểu, cùng miền nằm liền nhau nên mã hóa từ điển (*dictionary encoding*), mã hóa độ dài loạt (*Run-Length Encoding – RLE*) và đóng gói bit (*bit-packing*) đạt hiệu quả cao. Cột `sensor_id` của METR-LA chỉ có $207$ giá trị phân biệt nên mỗi giá trị được thay bằng chỉ số từ điển $\lceil \log_2 207 \rceil = 8$ bit.
5. **Nén Snappy:** sau mã hóa, mỗi trang được nén bằng Snappy — thuật toán thuộc họ LZ77, ưu tiên tốc độ giải nén hơn tỷ lệ nén, phù hợp cho truy vấn phân tích đọc nhiều lần.

**Liên hệ Data Contract của đề tài (`[K2]`).** Dữ liệu long-format được sắp xếp trong tệp theo thứ tự $(\text{timestamp}, \text{sensor\_id})$: mỗi mốc thời gian lặp lại thành một loạt $207$ dòng liên tiếp nên RLE nén cột `timestamp` rất hiệu quả. Kết quả đo: METR-LA giảm từ CSV $69{,}4\ \text{MB}$ xuống Parquet $9{,}8\ \text{MB}$ (**$7{,}1\times$**), PEMS-BAY từ $81{,}7\ \text{MB}$ xuống $25{,}4\ \text{MB}$ ($3{,}2\times$). Phép thử thứ tự $(\text{sensor\_id}, \text{timestamp})$ làm tệp METR-LA tăng lên $20{,}9\ \text{MB}$, xác nhận vai trò của thứ tự dòng đối với mã hóa cột. Cột thời gian được lưu kiểu `TIMESTAMP(MICROS, isAdjustedToUTC=false)` — Spark 3.5 không đọc được kiểu `TIMESTAMP(NANOS)` mặc định của `pyarrow`.

---

## 2.4. CƠ SỞ TOÁN HỌC CỦA BÀI TOÁN TÌM KIẾM LÁNG GIỀNG GẦN NHẤT: KNN VÉT CẠN, LSH VÀ FAISS HNSW

### 2.4.1. Phát biểu bài toán trên vector cửa sổ trượt

Gọi $s_i(t)$ là vận tốc trung bình 5 phút (mph) của cảm biến $v_i \in V$ tại mốc $t$, với bước lưới $\Delta = 5$ phút. **Vector cửa sổ trượt** độ dài $w$ kết thúc tại $t$ được định nghĩa:

$$\mathbf{x}_{i,t} = \big( s_i(t - (w-1)\Delta),\ \dots,\ s_i(t - \Delta),\ s_i(t) \big) \in \mathbb{R}^{w}$$

Đề tài chọn $w = 12$ (một giờ quan sát), tức $d = 12$ chiều. Một cửa sổ chỉ hợp lệ khi đủ $w$ giá trị không rỗng, trải đúng $(w-1)\Delta = 55$ phút (loại cửa sổ đi qua chỗ hở thời gian, ví dụ giờ DST bị thiếu của PEMS-BAY) và — khi dùng dữ liệu chưa làm sạch — không chứa giá trị cảm biến mất tín hiệu $0{,}0$. Cửa sổ được lấy theo bước $\text{stride}$ mốc; với $\text{stride} = w$ các cửa sổ không chồng lấn, tránh tình trạng hai cửa sổ lệch nhau $5$ phút gần như trùng nhau làm sai lệch đánh giá. Với METR-LA, $34.272$ mốc × $207$ cảm biến cho tối đa $2.856 \times 207 = 591.192$ cửa sổ; sau khi loại cửa sổ chứa $0{,}0$ còn $523.899$ vector. PEMS-BAY cho $1.410.954$ vector.

Cho tập dữ liệu $\mathcal{D} = \{\mathbf{x}_1, \dots, \mathbf{x}_N\} \subset \mathbb{R}^d$ và vector truy vấn $\mathbf{q}$ (trạng thái giao thông một giờ gần nhất), bài toán **$k$ láng giềng gần nhất** (*k-Nearest Neighbors – KNN*) tìm tập $\mathcal{N}_k(\mathbf{q}) \subset \mathcal{D}$ gồm $k$ phần tử có khoảng cách Euclid nhỏ nhất:

$$\|\mathbf{x} - \mathbf{q}\|_2 = \sqrt{\sum_{j=1}^{d} (x_j - q_j)^2}$$

Ý nghĩa ứng dụng: $k$ cửa sổ lịch sử gần nhất là $k$ "kịch bản giao thông" trong quá khứ có diễn biến vận tốc giống trạng thái hiện tại nhất, dùng để tra cứu mẫu ùn tắc tương tự hoặc làm cơ sở dự báo theo phương pháp láng giềng.

**Khoảng cách Euclid và Cosine.** Nếu chuẩn hóa mọi vector về độ dài đơn vị, $\hat{\mathbf{x}} = \mathbf{x} / \|\mathbf{x}\|$, thì

$$\|\hat{\mathbf{x}} - \hat{\mathbf{q}}\|_2^2 = 2 - 2\cos\theta(\mathbf{x}, \mathbf{q})$$

nên thứ tự láng giềng theo Euclid trên vector đã chuẩn hóa trùng với thứ tự theo độ tương đồng Cosine. Đề tài dùng Euclid trên vận tốc thô vì **mức** vận tốc (ùn tắc $20$ mph khác hẳn thông thoáng $65$ mph) mang thông tin, trong khi Cosine chỉ so sánh **hình dạng** chuỗi.

### 2.4.2. Exact KNN vét cạn: độ phức tạp và lưu ý số học

Thuật toán vét cạn tính khoảng cách từ $\mathbf{q}$ tới mọi $\mathbf{x} \in \mathcal{D}$ rồi chọn $k$ giá trị nhỏ nhất bằng hàng đợi ưu tiên:

$$T_{\text{exact}}(N, d, k) = \mathcal{O}(N \cdot d + N \log k), \qquad M_{\text{exact}} = \mathcal{O}(N \cdot d)$$

Đây là **chuẩn đối chứng tuyệt đối** (recall $= 1$) nhưng chi phí tuyến tính theo $N$ cho **mỗi** truy vấn. Trong thực thi, các thư viện như FAISS (`IndexFlatL2`) tính đồng thời nhiều khoảng cách bằng khai triển

$$\|\mathbf{x} - \mathbf{q}\|^2 = \|\mathbf{x}\|^2 + \|\mathbf{q}\|^2 - 2\,\mathbf{x}^{\top}\mathbf{q}$$

để tận dụng phép nhân ma trận BLAS. Khai triển này gây **triệt tiêu số học** (*catastrophic cancellation*) trong dấu phẩy động 32 bit: sai số tuyệt đối trên **bình phương khoảng cách** $\|\mathbf{x} - \mathbf{q}\|^2$ xấp xỉ $\delta \approx \varepsilon_{\text{fp32}} \cdot \|\mathbf{x}\|^2$ với $\varepsilon_{\text{fp32}} \approx 6 \times 10^{-8}$. Với vector vận tốc $\approx 58$ mph × $12$ chiều, $\|\mathbf{x}\|^2 \approx 4 \times 10^4$ nên $\delta \approx 2{,}4 \times 10^{-3}\ \text{mph}^2$; đặt $D = \|\mathbf{x} - \mathbf{q}\|$, sau khi lấy căn bậc hai sai số khoảng cách vào cỡ $\delta / (2D) \sim 10^{-3}$ mph khi $D \approx 1$ mph và có thể tới $\sqrt{\delta} \approx 0{,}05$ mph với các cặp láng giềng rất sát nhau ($D \to 0$) — đủ để đảo thứ tự và làm hụt recall. Đề tài khắc phục bằng ba biện pháp: (i) **ground truth** được tính riêng bằng vét cạn `float64`; (ii) trước khi lập chỉ mục FAISS, dữ liệu được **trừ vector trung bình** $\boldsymbol{\mu}$ — phép tịnh tiến không đổi khoảng cách, $\|(\mathbf{x}-\boldsymbol{\mu}) - (\mathbf{q}-\boldsymbol{\mu})\| = \|\mathbf{x}-\mathbf{q}\|$, nhưng giảm $\|\mathbf{x}\|^2$ và do đó giảm sai số; (iii) phương pháp Exact KNN lấy $4k$ ứng viên từ `IndexFlatL2` rồi **tính lại khoảng cách bằng `float64`** và giữ $k$ phần tử gần nhất. Biện pháp (iii) cần thiết với tập dày đặc: trên PEMS-BAY $1{,}41$ triệu vector, riêng `IndexFlatL2` chỉ đạt recall@10 $= 0{,}995$ so với ground truth `float64` vì nhiều láng giềng sát nhau bị sai số triệt tiêu đảo thứ tự.

### 2.4.3. Băm nhạy cảm vị trí (LSH): định nghĩa và định lý khuếch đại

**Định nghĩa (Indyk & Motwani [3]).** Một họ hàm băm $\mathcal{H} = \{h: \mathbb{R}^d \to U\}$ được gọi là **$(r_1, r_2, p_1, p_2)$-nhạy cảm** với $r_1 < r_2$ và $p_1 > p_2$ nếu với mọi $\mathbf{x}, \mathbf{y} \in \mathbb{R}^d$:

$$\|\mathbf{x} - \mathbf{y}\| \le r_1 \ \Rightarrow\ \Pr_{h \in \mathcal{H}}[h(\mathbf{x}) = h(\mathbf{y})] \ge p_1$$
$$\|\mathbf{x} - \mathbf{y}\| \ge r_2 \ \Rightarrow\ \Pr_{h \in \mathcal{H}}[h(\mathbf{x}) = h(\mathbf{y})] \le p_2$$

Nói cách khác, điểm gần có xác suất **đụng độ** (rơi vào cùng thùng băm) cao, điểm xa có xác suất đụng độ thấp. Một hàm băm đơn lẻ thường có khoảng cách $p_1 - p_2$ quá hẹp, nên LSH dùng hai phép **khuếch đại** (*amplification*, Gionis et al. [11]):

* **Khuếch đại AND:** ghép $K$ hàm băm độc lập thành một khóa $g(\mathbf{x}) = (h_1(\mathbf{x}), \dots, h_K(\mathbf{x}))$. Hai điểm đụng độ theo $g$ khi đụng độ ở cả $K$ hàm: xác suất $p^{K}$, làm **giảm mạnh** đụng độ của điểm xa.
* **Khuếch đại OR:** dùng $L$ bảng băm độc lập $g_1, \dots, g_L$; điểm $\mathbf{x}$ là **ứng viên** của $\mathbf{q}$ nếu đụng độ ở ít nhất một bảng:

$$P_{\text{cand}}(p) = 1 - \left(1 - p^{K}\right)^{L}$$

Hàm $P_{\text{cand}}$ có dạng chữ S theo $p$: chọn $K, L$ phù hợp sẽ đẩy $P_{\text{cand}}(p_1) \to 1$ (giữ được láng giềng thật) và $P_{\text{cand}}(p_2) \to 0$ (loại phần lớn điểm xa). Truy vấn chỉ tính khoảng cách thật trên tập ứng viên.

**Định lý (Indyk & Motwani [3]).** Với họ $(r, cr, p_1, p_2)$-nhạy cảm, đặt

$$\rho = \frac{\ln(1/p_1)}{\ln(1/p_2)} < 1, \qquad K = \log_{1/p_2} N, \qquad L = N^{\rho},$$

bài toán láng giềng gần đúng $(c, r)$ được giải với xác suất hằng số, bộ nhớ $\mathcal{O}(N^{1+\rho} + N d)$ và thời gian truy vấn **dưới tuyến tính** theo $N$: $\mathcal{O}(N^{\rho} \cdot (d + K))$ khi mỗi hàm băm đơn tính trong $\mathcal{O}(1)$ (như lấy mẫu bit Hamming trong [3]), hoặc $\mathcal{O}(d \cdot K \cdot N^{\rho}) = \mathcal{O}(d \cdot N^{\rho} \log_{1/p_2} N)$ với họ chiếu ngẫu nhiên $p$-ổn định dưới đây (mỗi phép chiếu tốn $\mathcal{O}(d)$) — nhỏ hơn hẳn $\mathcal{O}(N d)$ của vét cạn.

### 2.4.4. Họ hàm băm cho khoảng cách Euclid và Cosine

**(a) Chiếu ngẫu nhiên $p$-ổn định cho $L_2$ (Datar et al. [12]).** Chọn $\mathbf{a} \sim \mathcal{N}(0, I_d)$, độ rộng thùng $r > 0$ và độ lệch $b \sim \mathcal{U}[0, r)$:

$$h_{\mathbf{a}, b}(\mathbf{x}) = \left\lfloor \frac{\mathbf{a}^{\top}\mathbf{x} + b}{r} \right\rfloor$$

Do phân phối chuẩn là $2$-ổn định, $\mathbf{a}^{\top}\mathbf{x} - \mathbf{a}^{\top}\mathbf{y} \sim D \cdot Z$ với $D = \|\mathbf{x} - \mathbf{y}\|$, $Z \sim \mathcal{N}(0,1)$. Xác suất đụng độ chỉ phụ thuộc $D$:

$$p(D) = \int_0^{r} \frac{1}{D}\, f_{|Z|}\!\left(\frac{t}{D}\right)\left(1 - \frac{t}{r}\right) dt = 1 - 2\,\Phi\!\left(-\frac{r}{D}\right) - \frac{2}{\sqrt{2\pi}\,(r/D)}\left(1 - e^{-\frac{(r/D)^2}{2}}\right)$$

trong đó $\Phi$ là hàm phân phối tích lũy chuẩn tắc. $p(D)$ giảm đơn điệu theo $D$ nên họ hàm này là $(r_1, r_2, p(r_1), p(r_2))$-nhạy cảm với mọi $r_1 < r_2$; phân tích số của Datar et al. cho thấy với $r$ chọn phù hợp, $\rho$ nằm dưới $1/c$ (với $c = r_2 / r_1$ là hệ số xấp xỉ).

**(b) Siêu phẳng ngẫu nhiên cho Cosine — SimHash (Charikar [13]).** Với $\mathbf{a} \sim \mathcal{N}(0, I_d)$, $h_{\mathbf{a}}(\mathbf{x}) = \operatorname{sign}(\mathbf{a}^{\top}\mathbf{x})$ cho

$$\Pr[h_{\mathbf{a}}(\mathbf{x}) = h_{\mathbf{a}}(\mathbf{y})] = 1 - \frac{\theta(\mathbf{x}, \mathbf{y})}{\pi}$$

Spark MLlib không cung cấp SimHash; theo đẳng thức ở Mục 2.4.1, có thể dùng LSH Euclid trên vector đã chuẩn hóa độ dài để thu được thứ tự tương đương Cosine.

### 2.4.5. Cài đặt `BucketedRandomProjectionLSH` của Spark MLlib

Lớp `pyspark.ml.feature.BucketedRandomProjectionLSH` (BRP) cài đặt biến thể của họ (a) với hai khác biệt cần nêu rõ khi diễn giải kết quả:

$$h_j(\mathbf{x}) = \left\lfloor \frac{\mathbf{v}_j^{\top}\mathbf{x}}{r} \right\rfloor, \qquad \mathbf{v}_j = \frac{\mathbf{g}_j}{\|\mathbf{g}_j\|},\ \mathbf{g}_j \sim \mathcal{N}(0, I_d), \qquad j = 1, \dots, L$$

1. **Vector chiếu được chuẩn hóa về độ dài đơn vị và không có độ lệch $b$.** Hiệu hình chiếu $\mathbf{v}^{\top}(\mathbf{x} - \mathbf{y})$ có phương sai $D^2 / d$ thay vì $D^2$, nên tham số `bucketLength` $r$ của Spark tương ứng với $r\sqrt{d}$ trong công thức của Datar. Thiếu $b$ làm ranh giới thùng cố định; khi lấy trung bình theo vị trí dữ liệu, hành vi xấp xỉ họ (a).
2. **Chỉ có khuếch đại OR** ($K = 1$): tham số `numHashTables` là $L$. Một vector là ứng viên khi trùng thùng với truy vấn ở **ít nhất một** trong $L$ bảng, nên

$$P_{\text{cand}}(D) = 1 - \big(1 - p(D)\big)^{L}$$

Không có khuếch đại AND, $P_{\text{cand}}$ tăng nhanh theo $L$ cho **cả** điểm gần lẫn điểm xa. Trong không gian số chiều thấp ($d = 12$), khi chọn $r$ đủ lớn để giữ recall cao, tỷ lệ ứng viên cũng cao.

Spark cung cấp hai cách truy vấn:
* `approxNearestNeighbors(dataset, key, k)`: lọc các dòng có ít nhất một giá trị băm trùng với `key`, tính khoảng cách thật, sắp xếp và lấy $k$ dòng đầu. Mỗi truy vấn là **một Spark job quét toàn bộ** bảng đã băm (không có chỉ mục tra cứu theo khóa), nên độ trễ có cận dưới là chi phí lập lịch job, cỡ hàng chục đến hàng trăm mili-giây.
* `approxSimilarityJoin(A, B, threshold)`: "nổ" (*explode*) mỗi vector thành $L$ cặp `(chỉ số bảng, giá trị băm)` và thực hiện **equi-join phân tán** trên cặp này, sau đó lọc theo ngưỡng khoảng cách. Đây là chế độ Spark LSH được thiết kế cho xử lý **theo lô** quy mô lớn; chi phí được chia đều cho nhiều truy vấn.

**Minh họa thực nghiệm sơ bộ (`[K3]`, METR-LA, $N = 10.000$, $200$ truy vấn — riêng `approxNearestNeighbors` đo trên $50$ truy vấn đầu, $k = 10$).** Với $r = 4$, $L = 8$: Spark LSH đạt recall@10 $\approx 0{,}96$ nhưng tỷ lệ ứng viên $\approx 82\%$ tập dữ liệu, độ trễ $\approx 117$ ms/truy vấn với `approxNearestNeighbors` và $\approx 26$ ms/truy vấn (khấu hao) với `approxSimilarityJoin` (ngưỡng khoảng cách hiệu chỉnh ở phân vị $0{,}95$ từ ground truth); trong khi FAISS HNSW ($M = 32$, $ef_{\text{search}} = 64$) đạt recall@10 $= 1{,}000$ với $\approx 0{,}06$ ms/truy vấn và chỉ tính khoảng cách trên $\approx 7\%$ tập dữ liệu. Số liệu khớp với phân tích trên: chỉ khuếch đại OR trên $d = 12$ chiều không đủ loại điểm xa. Kết quả đầy đủ theo quy mô thuộc Chương 3.

### 2.4.6. Đồ thị thế giới nhỏ phân cấp HNSW và thư viện FAISS

**FAISS** (Johnson et al. [4]) là thư viện tìm kiếm tương đồng vector tối ưu cho CPU/GPU, cung cấp nhiều loại chỉ mục: vét cạn (`IndexFlatL2`), phân vùng Voronoi (`IndexIVFFlat`), lượng tử hóa tích (`IndexIVFPQ`) và đồ thị (`IndexHNSWFlat`). Đề tài dùng `IndexHNSWFlat` cài đặt thuật toán **HNSW** (*Hierarchical Navigable Small World*, Malkov & Yashunin [14]).

**Cấu trúc đa tầng.** HNSW tổ chức dữ liệu thành các đồ thị lân cận lồng nhau $G_0 \supseteq G_1 \supseteq \dots \supseteq G_{\ell_{\max}}$. Mỗi phần tử mới được gán tầng cao nhất

$$\ell = \left\lfloor -\ln(U) \cdot m_L \right\rfloor, \qquad U \sim \mathcal{U}(0, 1),\ m_L = \frac{1}{\ln M}$$

nên $\Pr[\ell \ge j] = M^{-j}$: số phần tử giảm theo cấp số nhân khi lên tầng, và số tầng kỳ vọng là $\mathcal{O}(\log_M N)$ — tương tự danh sách nhảy (*skip list*). Tầng trên thưa, cạnh dài, dùng để "nhảy xa"; tầng $0$ chứa mọi phần tử, cạnh ngắn, dùng để tinh chỉnh.

**Tìm kiếm.** Bắt đầu từ điểm vào ở tầng cao nhất, thuật toán **tham lam** di chuyển tới láng giềng gần $\mathbf{q}$ hơn cho tới khi không cải thiện được, rồi xuống tầng dưới. Tại tầng $0$, thuật toán chạy **tìm kiếm chùm** (*beam search*) giữ danh sách động $ef_{\text{search}} \ge k$ ứng viên tốt nhất và trả về $k$ phần tử gần nhất.

**Xây dựng.** Mỗi phần tử được chèn bằng chính thủ tục tìm kiếm với chùm $ef_{\text{construction}}$, sau đó nối tới tối đa $M$ láng giềng ở mỗi tầng ($M_0 = 2M$ ở tầng $0$) theo **heuristic chọn láng giềng đa dạng** — ưu tiên láng giềng không bị láng giềng đã chọn "che khuất" — giúp đồ thị giữ tính điều hướng được (*navigability*) cả khi dữ liệu phân cụm.

**Độ phức tạp và bộ nhớ.** Thực nghiệm của [14] cho thấy thời gian truy vấn tăng xấp xỉ $\mathcal{O}(\log N)$ và thời gian xây dựng $\mathcal{O}(N \log N)$; đây là kết quả thực nghiệm, HNSW không có bảo đảm lý thuyết như LSH. Bộ nhớ của `IndexHNSWFlat` gồm vector gốc và danh sách kề ở tầng $0$:

$$M_{\text{HNSW}} \approx N \cdot \left( 4d + 4 \cdot 2M \right)\ \text{byte} \quad (\text{chưa kể tầng trên, chiếm phần nhỏ})$$

Với $d = 12$, $M = 32$: $48 + 256 = 304$ byte/vector, tức $\approx 2{,}90\ \text{MiB}$ cho tầng $0$ của $10.000$ vector; kích thước chỉ mục đo được trong `[K3]` là $3{,}05\ \text{MiB}$, phần chênh lệch là các tầng trên và siêu dữ liệu. Ở số chiều thấp, **danh sách kề chiếm phần lớn bộ nhớ** (gấp $\approx 5{,}3$ lần vector gốc).

**Tham số đánh đổi.** $M$ và $ef_{\text{construction}}$ quyết định chất lượng đồ thị (đổi lấy thời gian xây dựng và bộ nhớ); $ef_{\text{search}}$ điều chỉnh đánh đổi recall – độ trễ **tại thời điểm truy vấn** mà không cần xây lại chỉ mục.

### 2.4.7. Chỉ số đánh giá thực nghiệm

Gọi $\mathcal{Q}$ là tập truy vấn (lấy ngẫu nhiên và **loại khỏi** tập chỉ mục), $R_k(\mathbf{q})$ là $k$ kết quả thuật toán trả về và $d_k^{*}(\mathbf{q})$ là khoảng cách tới láng giềng thật thứ $k$.

* **Recall@k** tính theo khoảng cách (theo quy ước của ANN-Benchmarks [15]) để không phạt các vector trùng nhau hoặc đồng khoảng cách:

$$\text{Recall@}k = \frac{1}{|\mathcal{Q}|} \sum_{\mathbf{q} \in \mathcal{Q}} \frac{\big|\{\mathbf{x} \in R_k(\mathbf{q}) : \|\mathbf{x} - \mathbf{q}\| \le d_k^{*}(\mathbf{q}) + \epsilon\}\big|}{k}, \qquad \epsilon = 10^{-3}$$

* **Độ trễ truy vấn** (*Query Latency*, ms): đo từng truy vấn riêng lẻ, báo cáo trung bình, trung vị $p_{50}$ và $p_{95}$.
* **Thời gian lập chỉ mục** (*Build Index Time*, s) và **dung lượng chỉ mục** (*Memory Footprint*, MB).
* **Tỷ lệ ứng viên** $\gamma$: tỷ lệ vector phải tính khoảng cách thật cho mỗi truy vấn — $\gamma = 1$ với vét cạn; với LSH là tỷ lệ vector trùng thùng với truy vấn ở ít nhất một bảng; với HNSW là số lần tính khoảng cách chia cho $N$. Chỉ số này độc lập với phần cứng và cho thấy trực tiếp mức tiết kiệm tính toán của thuật toán xấp xỉ.

### 2.4.8. Tổng hợp so sánh lý thuyết

| Tiêu chí | Exact KNN (vét cạn) | Spark BRP-LSH | FAISS HNSW |
| :--- | :--- | :--- | :--- |
| Thời gian truy vấn | $\mathcal{O}(N d)$ | $\mathcal{O}(\gamma N d)$ + chi phí job Spark; lý thuyết $\mathcal{O}(N^{\rho} d)$ khi có khuếch đại AND | $\approx \mathcal{O}(\log N)$ (thực nghiệm) |
| Thời gian lập chỉ mục | $\mathcal{O}(N d)$ (chỉ sao chép) | $\mathcal{O}(N d L)$, song song hóa trên cụm | $\mathcal{O}(N \log N)$, một máy |
| Bộ nhớ | $4Nd$ byte | Vector + $L$ giá trị băm/vector, phân tán | $\approx N(4d + 8M)$ byte, một máy |
| Bảo đảm chất lượng | Chính xác tuyệt đối | Xác suất, có cận lý thuyết | Không có cận lý thuyết; recall cao trong thực nghiệm |
| Tham số chính | — | `bucketLength` $r$, `numHashTables` $L$ | $M$, $ef_{\text{construction}}$, $ef_{\text{search}}$ |
| Vai trò trong đề tài | Chuẩn đối chứng (ground truth) | Tìm kiếm/kết nối tương đồng **theo lô, phân tán** | Tra cứu **trực tuyến độ trễ thấp** trên một nút |

Hai hướng tiếp cận bổ sung cho nhau: Spark LSH phù hợp với phép kết nối tương đồng hàng loạt trên dữ liệu phân tán không vừa một máy, còn FAISS HNSW phù hợp với tra cứu tương tác khi chỉ mục vừa bộ nhớ một nút. Chương 3 kiểm chứng đánh đổi này trên METR-LA và PEMS-BAY theo quy mô $10^4 \to 10^6$ vector.

---

## TÀI LIỆU THAM KHẢO BỔ SUNG (đánh số tiếp theo Chương 1)

> `[2]` Zaharia et al. (2016), `[3]` Indyk & Motwani (1998), `[4]` Johnson et al. (2021) giữ nguyên như danh mục của Chương 1 tại [literature_survey_notes.md](./literature_survey_notes.md).

[6] J. Dean and S. Ghemawat, "MapReduce: Simplified data processing on large clusters," *Communications of the ACM*, vol. 51, no. 1, pp. 107–113, Jan. 2008. DOI: `10.1145/1327452.1327492`.

[7] M. Zaharia, M. Chowdhury, T. Das, A. Dave, J. Ma, M. McCauley, M. J. Franklin, S. Shenker, and I. Stoica, "Resilient distributed datasets: A fault-tolerant abstraction for in-memory cluster computing," in *Proc. 9th USENIX Symp. Networked Systems Design and Implementation (NSDI)*, San Jose, CA, USA, 2012, pp. 15–28.

[8] M. Armbrust, R. S. Xin, C. Lian, Y. Huai, D. Liu, J. K. Bradley, X. Meng, T. Kaftan, M. J. Franklin, A. Ghodsi, and M. Zaharia, "Spark SQL: Relational data processing in Spark," in *Proc. ACM SIGMOD Int. Conf. Management of Data*, Melbourne, Australia, 2015, pp. 1383–1394. DOI: `10.1145/2723372.2742797`.

[9] T. Neumann, "Efficiently compiling efficient query plans for modern hardware," *Proceedings of the VLDB Endowment*, vol. 4, no. 9, pp. 539–550, 2011. DOI: `10.14778/2002938.2002940`.

[10] S. Melnik, A. Gubarev, J. J. Long, G. Romer, S. Shivakumar, M. Tolton, and T. Vassilakis, "Dremel: Interactive analysis of web-scale datasets," *Proceedings of the VLDB Endowment*, vol. 3, no. 1–2, pp. 330–339, 2010. DOI: `10.14778/1920841.1920886`.

[11] A. Gionis, P. Indyk, and R. Motwani, "Similarity search in high dimensions via hashing," in *Proc. 25th Int. Conf. Very Large Data Bases (VLDB)*, Edinburgh, Scotland, 1999, pp. 518–529.

[12] M. Datar, N. Immorlica, P. Indyk, and V. S. Mirrokni, "Locality-sensitive hashing scheme based on p-stable distributions," in *Proc. 20th Annu. Symp. Computational Geometry (SoCG)*, Brooklyn, NY, USA, 2004, pp. 253–262. DOI: `10.1145/997817.997857`.

[13] M. S. Charikar, "Similarity estimation techniques from rounding algorithms," in *Proc. 34th Annu. ACM Symp. Theory of Computing (STOC)*, Montréal, Canada, 2002, pp. 380–388. DOI: `10.1145/509907.509965`.

[14] Y. A. Malkov and D. A. Yashunin, "Efficient and robust approximate nearest neighbor search using hierarchical navigable small world graphs," *IEEE Transactions on Pattern Analysis and Machine Intelligence*, vol. 42, no. 4, pp. 824–836, Apr. 2020. DOI: `10.1109/TPAMI.2018.2889473`.

[15] M. Aumüller, E. Bernhardsson, and A. Faithfull, "ANN-Benchmarks: A benchmarking tool for approximate nearest neighbor algorithms," *Information Systems*, vol. 87, Art. no. 101374, Jan. 2020. DOI: `10.1016/j.is.2019.02.006`.
