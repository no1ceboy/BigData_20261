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

**Topic riêng của checkpoint P2/P3:** `freewaypulse.demo` tạo `<KAFKA_TOPIC>-<run-id>` với **1 partition**, replication factor 1. Producer hoàn tất replay theo thứ tự thời gian trước khi Spark `available_now` đọc các micro-batch giới hạn offset. Một partition giúp giữ thứ tự event toàn cục cho watermark trong replay hữu hạn; không thay đổi cấu hình 3 partition của topic dùng chung ở trên. Xem [runbook checkpoint](checkpoint-runbook.md) để chạy toàn chuỗi và hiểu chế độ finite replay.

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
  "flow": 182,
  "speed": 65.4,
  "occupancy": 0.0312,
  "observed_percent": 100.0,
  "latitude": 34.0522,
  "longitude": -118.2437,
  "freeway": 101,
  "direction": "N",
  "lane_type": "ML"
}
```

Producer chuẩn hóa tên cột PeMS trước khi gửi: `timestamp` → `event_time`, `flow_veh_5min` → `flow`, `speed_mph` → `speed`, và `occupancy_fraction` → `occupancy`. Đơn vị được giữ nguyên: flow là xe/5 phút, speed là mph, occupancy là tỷ lệ từ 0 đến 1. Các cột metadata như latitude, longitude và lane_type vẫn được giữ lại trong JSON.

### 3.2 Quy chuẩn Kiểu dữ liệu & Giá trị Thiếu

| Khóa (Field) | Kiểu dữ liệu | Bắt buộc | Mô tả & Ràng buộc Kỹ thuật |
| :--- | :--- | :---: | :--- |
| `event_id` | `String` | Có | Định danh bản tin duy nhất theo định dạng `{station_id}_{YYYYMMDDTHHmmss}`. Sử dụng làm khóa khử trùng lặp (Deduplication Key). |
| `event_time` | `String` (ISO 8601) | Có | Thời gian quan trắc thực tế, bảo toàn offset địa phương California (`-07:00`). Không quy đổi sang UTC tại tầng producer. |
| `station_id` | `Integer` | Có | Mã định danh trạm cảm biến thu thập số liệu. |
| `flow` | `Integer` / `null` | Không | Tổng lưu lượng phương tiện quan sát trong 5 phút (xe/5 phút). |
| `speed` | `Float` / `null` | Không | Vận tốc trung bình (mph). Giá trị thiếu hoặc không xác định xuất hiện dưới dạng `null` (không để chuỗi `"NaN"`). |
| `occupancy` | `Float` / `null` | Không | Tỷ lệ chiếm dụng mặt đường ($0.0 \rightarrow 1.0$). |
| `observed_percent` | `Float` / `null` | Không | Tỷ lệ % dữ liệu đo thực tế ($0 \rightarrow 100$). |
| `freeway` | `Integer` | Có | Mã tuyến cao tốc. |
| `direction` | `String` | Có | Hướng lưu thông (`N`, `S`, `E`, `W`). |

---

## 4. Replay Producer Engine (`src/freewaypulse/producer/replay.py`)

Module `replay.py` đảm nhận việc phát lại dữ liệu quan trắc chuỗi thời gian từ tập dữ liệu gốc vào Kafka broker.

### 4.1 Cơ chế Kỹ thuật Chính
- **Deterministic Replay:** Dữ liệu đầu vào được đọc và sắp xếp tuần tự theo `(event_time, station_id)` trước khi truyền phát.
- **Pacing Control:** Tốc độ gửi được điều tiết bám sát tham số `--rate` (events/giây) bằng thuật toán bù sai lệch trôi thời gian (sleep drift compensation). Tham số `--rate 0` tương ứng chế độ gửi tối đa năng lực I/O.
- **Idempotent Delivery:** Kích hoạt `enable.idempotence=True` và `acks=all` để loại trừ nguy cơ ghi lặp bản tin tại broker khi xảy ra sự cố mạng.
- **Buffer Backpressure:** Kiểm soát chống tràn hàng đợi bộ đệm local qua cơ chế bắt ngoại lệ `BufferError` và kích hoạt `poll()`.
- **Cấu hình P1:** `Settings.read()` đọc biến môi trường, `.env` và mặc định chung; producer dùng đúng `bootstrap_servers`, `topic`, đường dẫn nguồn, tốc độ replay, station/event limit. Đường dẫn input tương đối được tính từ gốc repository; input không tồn tại gây lỗi thay vì tự chuyển sang sample khác.
- **Workload checkpoint:** Mặc định chọn 100 station ID nhỏ nhất từ `stations.csv` cạnh nguồn; với CSV độc lập không có metadata, chọn 100 ID nhỏ nhất trong chính file. CSV được đọc theo chunk để lọc station trước khi giữ/sắp xếp event. `--limit` được áp dụng sau khi chọn station và sắp xếp; `--station-limit 0` chọn mọi trạm.
- **Delivery report:** `--report <path>` lưu JSON kèm station IDs, số event đã chọn, khoảng thời gian nguồn, số gửi thành công/thất bại và throughput. Thông điệp còn nằm trong hàng đợi sau `flush` được tính lỗi; exit code khác 0 nếu chưa xác nhận đủ số event.

Producer giữ nguyên chuỗi `event_time` có offset và `event_id` sẵn có; nếu chưa có ID thì sinh `{station_id}_{YYYYMMDDTHHMMSS}` theo đồng hồ địa phương. Việc sắp xếp so sánh thời điểm UTC tạm thời, không thay đổi thời gian lịch sử trong bản tin. Giá trị `flow` có phần thập phân bằng 0 được xuất thành JSON integer để khớp schema Spark; NaN, Inf và giá trị thiếu được xuất thành `null`.

### 4.2 Hướng dẫn Vận hành CLI

Thiết lập môi trường thực thi trên máy host (PowerShell):

```powershell
$env:PYTHONPATH = "src"
$env:KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:29092"
```

Chạy phát dữ liệu mẫu:

```powershell
# Chế độ thử nghiệm: 1.000 event của 100 trạm đã chọn
python -m freewaypulse.producer.replay --station-limit 100 --rate 500 --limit 1000 --report reports/replay-smoke.json

# Đủ 100 trạm x 7 ngày: 201.600 event, tốc độ không giới hạn
python -m freewaypulse.producer.replay --station-limit 100 --limit 0 --rate 0 --report reports/replay-full.json

# Tùy chọn: mọi trạm trong dữ liệu nguồn, không dùng cho workload checkpoint chuẩn
python -m freewaypulse.producer.replay --station-limit 0 --limit 0 --rate 0
```

Host chạy riêng producer cần cài `requirements.txt` trước. Full demo dùng thư viện trong container worker nên không cần cài chúng trên host. Topic phải được tạo trước khi producer chạy.

Kết quả thực thi xuất ra một bản tin JSON trên stdout và ghi vào đường dẫn `--report` nếu được chỉ định. Ví dụ rút gọn các trường chính:
```json
{"status": "passed", "selected_events": 1000, "station_count": 100, "sent": 1000, "failed": 0, "seconds": 1.995, "events_per_sec": 501.25, "topic": "traffic-events"}
```

Report đầy đủ còn có `input`, `station_ids`, `station_limit`, `event_limit`, `event_time_min` và `event_time_max`. `main(argv=None)` trả 0 khi mọi event đã được xác nhận gửi, trả 1 khi delivery thiếu/lỗi. Không xem ví dụ report là bằng chứng một run đã được thực hiện.

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

Các regression test bổ sung kiểm tra cấu hình P1, lựa chọn station và event cap, giữ offset/ID khi sắp xếp, flow integer/null, input thiếu và delivery chưa xác nhận phải thất bại. Test `test_smoke_e2e` dùng broker thật và được skip khi không kết nối được; cần phân biệt test offline pass với nghiệm thu Kafka thật.

### 5.2 Lệnh Thực thi Kiểm thử

Chạy kiểm thử có ghi nhận kết quả phục vụ CI/CD hoặc lưu vết nghiệm thu (tránh lỗi xung đột luồng console trên PowerShell):

```powershell
# Bước 1: Thiết lập biến môi trường
$env:PYTHONPATH = "src"
$env:KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:29092"

# Bước 2: Chạy kiểm thử xuất định dạng JUnit XML
pytest -s tests/test_replay.py --junitxml=reports/junit_replay.xml
```

Chỉ chạy regression offline khi không có broker:

```powershell
python -m pytest -q tests/test_replay.py -k "not smoke_e2e"
```

Trong full suite, kiểm tra JUnit/summary để xác nhận test Kafka đã chạy hay bị skip. Để nghiệm thu cả Kafka–Spark, dùng workflow checkpoint hoặc lệnh demo trong [runbook](checkpoint-runbook.md); smoke test producer riêng không thay cho P2/P3.

---

## 6. Hướng dẫn Tích hợp dành cho Tầng Xử lý Hạ nguồn (Downstream Consumer)

Các ứng dụng tiêu thụ dữ liệu (Spark Structured Streaming / Consumer Applications) cần thiết lập kết nối theo các tham số kỹ thuật sau:

- **Format Data:** JSON chuỗi (UTF-8) nằm trong trường `value` của Kafka message.
- **Khóa phân vùng (`key`):** `station_id` (UTF-8 string).
- **Trường Watermark / Event Time:** `event_time` (Định dạng ISO 8601, Timezone `-07:00`).
- **Khóa khử trùng lặp (Deduplication):** Cột `event_id` được đảm bảo tính đơn nhất tuyệt đối cho mỗi sự kiện đo đạc tại một trạm ở một mốc thời gian.
