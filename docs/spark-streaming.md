# Đặc tả Kỹ thuật Spark Structured Streaming (Nguyễn Tuấn Minh)

Tài liệu kỹ thuật đặc tả kiến trúc luồng xử lý dữ liệu thời gian thực **Spark Structured Streaming Engine** (nhiệm vụ S1, S2, S3, S4) thuộc hệ thống **FreewayPulse**.

---

## 1. Tổng quan Kiến trúc Luồng Dữ liệu (Streaming Architecture)

```
[Kafka Topic: traffic-events]
       │
       ▼  (S1: spark.readStream + from_json)
[Parsed Traffic DataFrame]
       │
       ├─── (Invalid Records) ───────────► [Quarantine Sink: Parquet]
       │                                     (data/runs/<run_id>/quarantine/)
       ▼  (S2: Valid Records + Watermark 10min + DropDuplicates)
[Clean Valid Stream]
       │
       ├─── (S3: 5-min Tumbling Window) ──► [5-min Metrics Parquet Sink]
       │                                     (data/runs/<run_id>/traffic_metrics/5min/)
       │                                     (checkpoint: checkpoints/window_5/)
       │
       └─── (S3: 15-min Sliding Window) ─► [15-min Metrics Parquet Sink]
                                             (data/runs/<run_id>/traffic_metrics/15min/)
                                             (checkpoint: checkpoints/window_15/)
```

---

## 2. Đặc tả Các Nhiệm vụ Triển khai (Tasks S1 - S4)

### 2.1 S1: Đọc và Phân tích Cú pháp Bản tin (`parse_kafka_stream`)
- **Nguồn dữ liệu**: Đọc stream từ Kafka broker theo cấu hình `Settings.bootstrap_servers` và topic `traffic-events`.
- **Cấu hình Spark-Kafka**: Sử dụng gói thư viện Maven `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7`.
- **Phân giải Schema**:
  - Trích xuất trường nhị phân `value` sang chuỗi JSON UTF-8.
  - Sử dụng `from_json` với schema `RAW_TRAFFIC_EVENT_SCHEMA` để ánh xạ thành các cột định kiểu tường minh.
  - Chuyển đổi chuỗi ISO 8601 `event_time` sang `TimestampType` với múi giờ Los Angeles (`America/Los_Angeles`).

### 2.2 S2: Kiểm tra Hợp lệ, Cách ly và Khử Trùng lặp (`validate_records`, `apply_watermark_and_dedup`)
- **Quy tắc Kiểm tra Tính Toàn vẹn (Validation Rules)**:
  - `station_id`: Phải khác NULL và > 0.
  - `event_time`: Phải khác NULL.
  - `event_id`: Phải khác NULL và không rỗng.
  - `speed_mph`: Cho phép NULL (giá trị thiếu) hoặc nằm trong khoảng $[0.0, 120.0]$ mph.
  - `flow_veh_5min`: Cho phép NULL hoặc $\ge 0$.
  - `occupancy_fraction`: Cho phép NULL hoặc nằm trong khoảng $[0.0, 1.0]$.
- **Xử lý Dữ liệu Lỗi (Quarantine)**: Các bản ghi vi phạm quy tắc trên được gắn nhãn `quarantine_reason` và tách riêng sang thư mục cách ly `quarantine/`.
- **Event-time Watermarking**: Áp dụng `.withWatermark("event_time", settings.watermark)` (mặc định 10 phút) nhằm cho phép Spark dọn dẹp state bộ nhớ và chấp nhận dữ liệu đến muộn trong ngưỡng cho phép.
- **Khử Trùng lặp (Deduplication)**: Áp dụng `.dropDuplicates(["event_id", "event_time"])` giúp loại bỏ hoàn toàn các bản tin phát lặp từ network retransmission.

### 2.3 S3: Tổng hợp Cửa sổ Thời gian (`compute_traffic_windows`)
- Tính toán gom nhóm theo từng trạm cảm biến (`station_id`) và cửa sổ thời gian sự kiện:
  1. **Cửa sổ 5 phút (Tumbling)**: `window(event_time, "5 minutes")`.
  2. **Cửa sổ 15 phút (Sliding)**: `window(event_time, "15 minutes", "5 minutes")` (bước trượt 5 phút cung cấp độ mịn cao cho analytics).
- **Công thức chỉ số**:
  - `avg_speed`: $\text{round}(\text{avg}(\text{speed\_mph}), 2)$
  - `total_flow`: $\sum(\text{flow\_veh\_5min})$ (thay NULL bằng 0)
  - `avg_occupancy`: $\text{round}(\text{avg}(\text{occupancy\_fraction}), 4)$
  - `valid_count`: Số lượng quan trắc hợp lệ được tính trong cửa sổ.

### 2.4 S4: Lưu trữ Parquet Sink và Streaming Checkpoint
- Toàn bộ kết quả được ghi dưới dạng Apache Parquet theo layout P1:
  - `data/runs/<run_id>/traffic_metrics/5min/`
  - `data/runs/<run_id>/traffic_metrics/15min/`
  - `data/runs/<run_id>/quarantine/`
- Checkpoint state được lưu trữ độc lập cho từng truy vấn tại:
  - `data/runs/<run_id>/checkpoints/window_5/`
  - `data/runs/<run_id>/checkpoints/window_15/`
  - `data/runs/<run_id>/checkpoints/quarantine/`
- Đảm bảo tính khả phục (fault-tolerance): Khi khởi động lại job với cùng `run_id`, Spark đọc offset từ checkpoint và tiếp tục xử lý mà không gây trùng lặp dữ liệu trong Parquet.

---

## 3. Hướng dẫn Vận hành & Lệnh Thực thi

### 3.1 Chạy qua Docker Container (Khuyến nghị)

Khởi động hạ tầng Kafka và chạy Spark Streaming Engine:

```powershell
# 1. Khởi động Kafka broker
docker compose up -d kafka

# 2. Khởi tạo cấu trúc thư mục lưu trữ cho run
.\scripts\platform.cmd init-storage

# 3. Khởi chạy Spark Streaming Engine
docker compose run --rm worker python -m freewaypulse.streaming.cli --run-id checkpoint-01
```

### 3.2 Chạy trực tiếp qua CLI trên Máy Host

```powershell
$env:PYTHONPATH = "src"
$env:KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:29092"

# Chạy continuous stream (micro-batch 5s)
python -m freewaypulse.streaming.cli --run-id checkpoint-01

# Chạy ở chế độ drain toàn bộ dữ liệu có sẵn (batch availableNow)
python -m freewaypulse.streaming.cli --run-id checkpoint-01 --trigger available_now
```

### 3.3 Đọc và Nghiệm thu Sản phẩm Parquet

Sử dụng Python / PyArrow kiểm tra dữ liệu đầu ra:

```powershell
python -c "
import pyarrow.parquet as pq
table = pq.read_table('data/runs/checkpoint-01/traffic_metrics/5min')
print(table.to_pandas().head(10))
"
```

---

## 4. Hợp đồng Giao diện Handoff (Handoff Contracts)

- **Đầu vào**: Đã kiểm chứng tương thích 100% với tệp `observations.csv.gz` của Sơn và `replay.py` của Long.
- **Đầu ra bàn giao cho Tường Minh (Analytics - A1/A2)**:
  Bảng Parquet chứa các trường: `[window_start, window_end, station_id, avg_speed, total_flow, avg_occupancy, valid_count]`.
- **Đầu ra bàn giao cho Tất Thanh (Platform - P2/P3)**:
  Lệnh khởi chạy đồng nhất `python -m freewaypulse.streaming.cli` tuân thủ đầy đủ biến môi trường và quy ước thư mục của P1.
