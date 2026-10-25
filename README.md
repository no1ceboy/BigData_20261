# BigData_20261 - Caltrans PeMS District 7 Traffic Dataset

## FreewayPulse — P1 của Nguyễn Tất Thanh

Phần Data của Sơn đã hoàn thành theo xác nhận của nhóm. Phạm vi bàn giao hiện tại là **P1: chuẩn hóa runtime, biến môi trường, đường dẫn lưu trữ và quy ước chạy chung**. [Tài liệu P1 và danh sách file cần commit](docs/platform-p1.md).

Các lệnh P1 chỉ cần Python 3.11 và thư viện chuẩn, chạy được khi chưa có Docker/Kafka/Spark. Trên Windows, wrapper ưu tiên `.venv/Scripts/python.exe` nếu có; Linux/macOS ưu tiên `.venv/bin/python`. Có thể tạo venv bằng `uv venv --python 3.11 .venv`.

```powershell
.\scripts\platform.cmd config
.\scripts\platform.cmd check
.\scripts\platform.cmd init-storage
```

Linux/macOS dùng `bash scripts/platform.sh` với cùng action. `config` hiển thị cấu hình và layout; `check` kiểm tra runtime Python, sự hiện diện của nguồn và đường dẫn; `init-storage` tạo thư mục sample/run nếu chưa có, giữ nguyên dữ liệu hiện có. `.env.example` chứa giá trị dùng chung; sao chép thành `.env` khi cần tùy chỉnh.

Dockerfile/Compose chuẩn hóa môi trường container để nhóm dùng sau. Chỉ cần Docker engine khi build/chạy container; Kafka/Spark thật được dùng khi tích hợp P2 và demo P3. [Bằng chứng kiểm tra P1](reports/p1/validation.json).

## P2/P3 — pipeline tích hợp và demo một lệnh

Sau khi có Python 3.11 và Docker Compose với Linux containers, chạy tại gốc repository:

```powershell
.\scripts\demo.cmd run --run-id checkpoint-01
```

Linux/macOS: `bash scripts/demo.sh run --run-id checkpoint-01`. Lệnh dùng producer, Spark và analytics của nhóm để replay **100 trạm × 7 ngày**, kiểm chứng từng event/metric/nhãn từ Parquet rồi chạy lại cùng checkpoint. Host chạy orchestration chỉ dùng thư viện chuẩn; Java/Spark và thư viện producer nằm trong worker image.

Output và log mới được tạo dưới `data/runs/checkpoint-01/`; mỗi demo mới dùng ID mới. [Runbook đầy đủ](docs/checkpoint-runbook.md) mô tả thiết lập, số liệu kỳ vọng, kiểm chứng, dừng và phục hồi. CI Linux trong `.github/workflows/checkpoint.yml` chạy cùng lệnh trên clone sạch và lưu bằng chứng. Không cần tạo lại thư mục `data/sample` cho đường chạy này.

**Đã nghiệm thu P2/P3:** [CI 40213958909](https://github.com/no1ceboy/BigData_20261/actions/runs/40213958909) chạy đủ 201.600 event, 268.800 summary, đối chiếu nguồn/Parquet và restart thành công; **66 test pass**. [Báo cáo bàn giao](reports/checkpoint/README.md) ghi đúng source commit, môi trường và các giới hạn. A3 vẫn do trưởng nhóm xác nhận.

---

Dữ liệu giao thông 1 tuần thu thập từ hệ thống **Caltrans PeMS** (District 7 - Los Angeles & Ventura, California) phục vụ cho môn học Big Data.

---

## 1. Thông tin bộ dữ liệu (Dataset Overview)

- **Khu vực**: Caltrans District 7 (Los Angeles & Ventura County)
- **Khoảng thời gian**: `2026-10-14T00:00:00` đến `2026-10-20T23:55:00` (1 tuần trọn vẹn)
- **Độ phân giải thời gian**: 5 phút / chu kỳ (`--granularity 5min`)
- **Loại làn đường**: Làn chính (`--lane-type ML` - Mainline)
- **Tổng số trạm cảm biến**: 1,933 trạm

---

## 2. Cấu trúc thư mục

```text
data/d07-1week-mainline/
├── processed/
│   ├── observations.csv.gz     # Bảng dữ liệu chuỗi thời gian chính (lưu lượng, tốc độ, mật độ)
│   ├── stations.csv            # Danh sách thông tin chi tiết các trạm (tên, tọa độ, số làn)
│   ├── edges.csv               # Cấu trúc đồ thị không gian nối các trạm kế cận
│   └── detectors.geojson       # Tọa độ các điểm trạm định dạng GeoJSON
├── network/
│   └── map.html                # Bản đồ tương tác trực quan mạng lưới đường và các trạm
├── filtered/
│   └── station_5min.csv.gz     # Bảng dữ liệu gốc 5 phút đã lọc
├── metadata/
│   └── d07_text_meta_2026_08_25.txt  # File siêu dữ liệu cấu hình trạm từ PeMS
└── manifest.json               # Manifest ghi lại chi tiết quá trình tải
```

---

## 3. Từ điển dữ liệu (`observations.csv.gz`)

| Cột | Ý nghĩa | Đơn vị / Định dạng |
|---|---|---|
| `timestamp` | Thời điểm ghi nhận | ISO 8601 (Giờ địa phương California) |
| `station_id` | Mã định danh trạm cảm biến | Số nguyên (Integer) |
| `speed_mph` | Vận tốc trung bình dòng xe | mph (dặm / giờ) |
| `flow_veh_5min` | Lưu lượng phương tiện | Số xe / 5 phút |
| `occupancy_fraction` | Tỷ lệ chiếm dụng mặt đường | Giá trị từ `0.0` đến `1.0` |
| `observed_percent` | Tỷ lệ dữ liệu đo thực tế | Tỷ lệ % (`0` là nội suy) |
| `latitude`, `longitude` | Tọa độ địa lý của trạm | Độ WGS84 |
| `freeway` | Tuyến cao tốc | Số tuyến (VD: 5, 10, 105, 405...) |
| `direction` | Hướng di chuyển | `N`, `S`, `E`, `W` |
| `lane_type` | Loại làn | `ML` (Mainline) |

---

## 4. Trực quan hóa

Bạn có thể mở trực tiếp file `data/d07-1week-mainline/network/map.html` bằng trình duyệt web bất kỳ (Chrome, Edge) để xem bản đồ tương tác hiển thị vị trí các trạm cảm biến trên mạng lưới cao tốc Los Angeles.
