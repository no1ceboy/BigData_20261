# Ingestion Architecture: Apache Kafka & Replay Producer Specification

Tài liệu kỹ thuật đặc tả hạ tầng hàng đợi tin nhắn (Message Broker), cấu trúc giao thức dữ liệu (Data Contract) và quy trình vận hành thành phần **Streaming Ingestion** thuộc hệ thống phân tích dữ liệu giao thông thời gian thực **FreewayPulse**.

---

## 1. Kiến trúc Hạ tầng & Định tuyến Mạng (Network Topology)

Hệ thống hàng đợi sử dụng **Apache Kafka 3.9.1** vận hành ở chế độ **KRaft (Kafka Raft Metadata Mode)**, không sử dụng ZooKeeper. Triển khai được đóng gói qua `compose.yaml`.

### 1.1 Ma trận Kết nối Broker

| Môi trường Thực thi | Bootstrap Server Address | Listener / Security Protocol | Mục đích Sử dụng |
| :--- | :--- | :--- | :--- |
| **Host Environment** (Local OS / PowerShell) | `127.0.0.1:29092` | `EXTERNAL:PLAINTEXT` | Replay Producer CLI, Pytest test suites |
| **Container Network** (Docker Bridge) | `kafka:9092` | `INTERNAL:PLAINTEXT` | Spark Streaming Engine, Internal Workers |

> **Quy tắc tương thích mạng:**
> - Trên máy host (Windows / Linux), client bắt buộc kết nối qua `127.0.0.1:29092`. Không sử dụng alias `localhost:29092` nhằm ngăn ngừa lỗi phân giải socket sang loopback IPv6 (`[::1]`) của thư viện C-level `librdkafka`.
> - Ứng dụng chạy bên trong mạng Docker (Spark, worker container) truy cập qua hostname nội bộ `kafka:9092`.

### 1.2 Thiết kế Topic & Phân vùng (Partitioning Strategy)

* **Tên Topic:** `traffic-events`
* **Số phân vùng (Partitions):** `3` (Cung cấp mức độ song song cần thiết cho consumer group và các worker Spark Streaming).
* **Hệ số nhân bản (Replication Factor):** `1` (Chuẩn hóa cho kiến trúc single-node broker).
* **Phân bổ khóa (Partition Key):** `station_id` (Kiểu chuỗi UTF-8).
  * **Bảo toàn thứ tự (Ordering Guarantee):** Mọi sự kiện phát sinh từ cùng một trạm cảm biến (`station_id`) được định tuyến vào cùng một phân vùng duy nhất, bảo toàn tuần tự thời gian tuyệt đối theo từng trạm.

---

## 2. Quản trị Broker & Chuẩn hóa Topic (Administration)

Broker đã tắt tính năng tự động tạo topic (`KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"`). Topic phải được khởi tạo chủ động theo đúng tham số kỹ thuật.

### 2.1 Khởi động Hạ tầng Broker

Khởi động container Kafka ở chế độ detached và chờ container đạt trạng thái `healthy`:

```powershell
docker compose up -d kafka
docker compose ps
```

*Lưu ý:* Cấu hình có `start_period: 20s`, trạng thái container sẽ chuyển từ `(health: starting)` sang `(healthy)` trong khoảng 20–30 giây.

### 2.2 Khởi tạo Topic

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server localhost:9092 `
  --create --if-not-exists `
  --topic traffic-events `
  --partitions 3 `
  --replication-factor 1
```

### 2.3 Nghiệm thu Cấu hình Phân vùng

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server localhost:9092 `
  --describe `
  --topic traffic-events
```

*Tiêu chí nghiệm thu:* Lệnh in ra thông tin topic gồm đúng 3 partition (0, 1, 2) với `ReplicationFactor: 1`.

### 2.4 Đặt lại Topic (Topic Reset)

Phục vụ việc làm sạch topic trước các đợt benchmark tải hoặc chạy thử nghiệm mới:

```powershell
# Xóa topic cũ
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server localhost:9092 `
  --delete `
  --topic traffic-events

# Khởi tạo lại topic sạch
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server localhost:9092 `
  --create `
  --topic traffic-events `
  --partitions 3 `
  --replication-factor 1
```

---

## 3. Đặc tả Hợp đồng Dữ liệu (Data Contract Specification)

Mỗi bản tin (message) đẩy vào topic `traffic-events` tuân thủ định dạng JSON với cấu trúc chuẩn:

### 3.1 Cấu trúc Bản tin (JSON Schema)

```json
{
  "event_id": "760063_20261014T000000",
  "event_time": "2026-10-14T00:00:00-07:00",
  "station_id": 760063,
  "speed_mph": 65.4,
  "flow_veh_5min": 182,
  "occupancy_fraction": 0.0312,
  "observed_percent": 100.0,
  "latitude": 34.0522,
  "longitude": -118.2437,
  "freeway": 101,
  "direction": "N",
  "lane_type": "ML"
}
```

### 3.2 Quy chuẩn Kiểu dữ liệu & Giá trị Thiếu

| Khóa (Field) | Kiểu dữ liệu | Bắt buộc | Mô tả & Ràng buộc Kỹ thuật |
| :--- | :--- | :---: | :--- |
| `event_id` | `String` | Có | Định danh bản tin duy nhất theo định dạng `{station_id}_{YYYYMMDDTHHmmss}`. Sử dụng làm khóa khử trùng lặp (Deduplication Key). |
| `event_time` | `String` (ISO 8601) | Có | Thời gian quan trắc thực tế, bảo toàn offset địa phương California (`-07:00`). Không quy đổi sang UTC tại tầng producer. |
| `station_id` | `Integer` | Có | Mã định danh trạm cảm biến thu thập số liệu. |
| `speed_mph` | `Float` / `null` | Không | Vận tốc trung bình (mph). Giá trị thiếu hoặc không xác định xuất hiện dưới dạng `null` (không để chuỗi `"NaN"`). |
| `flow_veh_5min` | `Integer` / `null` | Không | Tổng lưu lượng phương tiện quan sát trong cửa sổ 5 phút. |
| `occupancy_fraction` | `Float` / `null` | Không | Tỷ lệ chiếm dụng mặt đường ($0.0 \rightarrow 1.0$). |
| `observed_percent` | `Float` / `null` | Không | Tỷ lệ % dữ liệu đo thực tế ($0 \rightarrow 100$). |

---

## 4. Replay Producer Engine (`src/freewaypulse/producer/replay.py`)

Module `replay.py` đảm nhận việc phát lại dữ liệu quan trắc chuỗi thời gian từ tập dữ liệu gốc vào Kafka broker.

### 4.1 Cơ chế Kỹ thuật Chính
- **Deterministic Replay:** Dữ liệu đầu vào được đọc và sắp xếp tuần tự theo `(event_time, station_id)` trước khi truyền phát.
- **Pacing Control:** Tốc độ gửi được điều tiết bám sát tham số `--rate` (events/giây) bằng thuật toán bù sai lệch trôi thời gian (sleep drift compensation). Tham số `--rate 0` tương ứng chế độ gửi tối đa năng lực I/O.
- **Idempotent Delivery:** Kích hoạt `enable.idempotence=True` và `acks=all` để loại trừ nguy cơ ghi lặp bản tin tại broker khi xảy ra sự cố mạng.
- **Buffer Backpressure:** Kiểm soát chống tràn hàng đợi bộ đệm local qua cơ chế bắt ngoại lệ `BufferError` và kích hoạt `poll()`.

### 4.2 Hướng dẫn Vận hành CLI

Thiết lập môi trường thực thi trên máy host (PowerShell):

```powershell
$env:PYTHONPATH = "src"
$env:KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:29092"
```

Chạy phát dữ liệu mẫu:

```powershell
# Chế độ thử nghiệm: Gửi 1.000 sự kiện với tốc độ giới hạn 500 events/giây
python -m freewaypulse.producer.replay --rate 500 --limit 1000

# Chế độ tải tối đa: Gửi toàn bộ tập dữ liệu ở tốc độ không giới hạn
python -m freewaypulse.producer.replay --rate 0
```

Kết quả thực thi xuất ra một bản tin JSON duy nhất trên dòng cuối của stdout:
```json
{"sent": 1000, "failed": 0, "seconds": 1.995, "events_per_sec": 501.25, "topic": "traffic-events"}
```

---

## 5. Quy trình Kiểm thử & Nghiệm thu (Verification Suite)

Toàn bộ logic chuyển đổi hợp đồng dữ liệu và tính tương thích với Broker được kiểm thử tự động tại `tests/test_replay.py`.

### 5.1 Danh mục Kiểm thử (Test Matrix)

1. **`test_load_keeps_contract` (Contract Test - Offline):**
   - Xác minh sự hiện diện của các khóa bắt buộc: `event_id`, `event_time`, `station_id`.
   - Kiểm tra ràng buộc không chứa giá trị `null` ở các trường định danh chính.
   - Xác minh chuỗi JSON được serialize hợp lệ (chuyển đổi hoàn toàn `NaN` $\rightarrow$ `null`).
2. **`test_smoke_e2e` (Integration Smoke Test - Online):**
   - Gửi 1.000 bản tin vào topic `traffic-events` với `rate = 0`.
   - Xác nhận callback báo cáo `sent == 1000` và `failed == 0`.
   - Truy vấn trực tiếp High Watermark Offsets từ toàn bộ các phân vùng trên broker trước và sau khi gửi; đảm bảo mức chênh lệch tuyệt đối $\Delta \text{Offset} \equiv 1000$.

### 5.2 Lệnh Thực thi Kiểm thử

Chạy kiểm thử có ghi nhận kết quả phục vụ CI/CD hoặc lưu vết nghiệm thu (tránh lỗi xung đột luồng console trên PowerShell):

```powershell
# Bước 1: Thiết lập biến môi trường
$env:PYTHONPATH = "src"
$env:KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:29092"

# Bước 2: Chạy kiểm thử xuất định dạng JUnit XML
pytest -s tests/test_replay.py --junitxml=reports/junit_replay.xml
```

Trạng thái đạt chuẩn khi terminal trả về:
```text
tests/test_replay.py ..
[K3 Smoke Test Passed] Throughput: ... events/sec over ...s
2 passed in ...s
```

---

## 6. Hướng dẫn Tích hợp dành cho Tầng Xử lý Hạ nguồn (Downstream Consumer)

Các ứng dụng tiêu thụ dữ liệu (Spark Structured Streaming / Consumer Applications) cần thiết lập kết nối theo các tham số kỹ thuật sau:

- **Format Data:** JSON chuỗi (UTF-8) nằm trong trường `value` của Kafka message.
- **Khóa phân vùng (`key`):** `station_id` (UTF-8 string).
- **Trường Watermark / Event Time:** `event_time` (Định dạng ISO 8601, Timezone `-07:00`).
- **Khóa khử trùng lặp (Deduplication):** Cột `event_id` được đảm bảo tính đơn nhất tuyệt đối cho mỗi sự kiện đo đạc tại một trạm ở một mốc thời gian.