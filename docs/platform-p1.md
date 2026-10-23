# Bàn giao P1 — Nguyễn Tất Thanh

Phạm vi lần bàn giao này là **P1: chuẩn hóa cấu hình chạy và cấu trúc lưu trữ**. Sơn đã hoàn thành phần dữ liệu theo xác nhận của nhóm; P1 tiếp nhận bộ dữ liệu đó làm đầu vào.

## 1. P1 làm gì trong dự án?

Checklist ghi tên P1 là “Standardize runtime configuration and storage layout”, đầu ra là “Repository configuration, environment variables, common paths and execution conventions are documented”, không có task phụ thuộc. Tiêu chí hoàn thành là “All components use consistent configuration and paths”.

Thanh xây dựng quy ước để producer của Long, Spark của Tuấn Minh và analytics của Tường Minh sử dụng cùng một bộ cấu hình: lấy dữ liệu ở đâu, kết nối Kafka bằng địa chỉ/topic nào, ghi kết quả và checkpoint vào đâu, dùng phiên bản runtime nào và chạy lệnh từ repository như thế nào. Ví dụ, khi đổi tên topic trong cấu hình chung, các thành phần phải đọc lại giá trị đó thay vì giữ một tên topic riêng trong mã.

P1 cung cấp các giá trị và đường dẫn chung. Việc ghép các module, gửi event thật qua Kafka, chạy Spark và nghiệm thu output toàn chuỗi thuộc P2; script demo toàn chuỗi và runbook nghiệm thu thuộc P3.

| Thành phần của P1 | Tệp bàn giao | Mục đích |
|---|---|---|
| Cấu hình mẫu | `.env.example` | Công bố biến môi trường, giá trị mặc định và cách chọn địa chỉ Kafka |
| Đọc cấu hình và đường dẫn chung | `src/freewaypulse/config.py` | Các module dùng chung một cách đọc cấu hình và tạo đường dẫn |
| Công cụ kiểm tra P1 | `src/freewaypulse/platform.py` | In cấu hình, kiểm tra đầu vào/cấu hình và tạo thư mục chứa sản phẩm |
| Cách chạy thống nhất | `scripts/platform.cmd`, `scripts/platform.ps1`, `scripts/platform.sh` | Chạy công cụ P1 trên Windows và Linux/macOS |
| Môi trường container tham chiếu | `Dockerfile`, `compose.yaml`, `requirements*.txt` | Công bố runtime để dùng ở bước tích hợp sau |
| Quy ước repository | `.gitignore`, `.dockerignore`, `.gitattributes` | Tách dữ liệu sinh ra, cấu hình cá nhân và quy ước kết thúc dòng |
| Hướng dẫn và bằng chứng | Tài liệu này, `reports/p1/` | Cho người khác kiểm tra và tiếp nhận P1 |

## 2. Có cần Docker, Kafka hoặc Spark không?

| Phần việc | Cần để hoàn thành phần việc đó |
|---|---|
| **P1: viết và kiểm tra cấu hình, đường dẫn, quy ước chạy** | Python 3.11; các lệnh P1 chỉ dùng thư viện chuẩn, không cần `pip install` |
| **P1: cung cấp môi trường container chuẩn** | Viết Dockerfile/Compose; chỉ cần Docker engine khi muốn build/chạy kiểm chứng container |
| **P2: nghiệm thu pipeline thật** | Kafka, Spark, Java và các thư viện tương ứng; bộ Compose hiện có dùng Docker để cung cấp môi trường đó |
| **P3: nghiệm thu demo toàn chuỗi** | Môi trường đã chạy được P2 |

Docker là công cụ đóng gói và vận hành môi trường; Kafka là broker chuyển event; Spark là bộ xử lý dữ liệu. Chúng có vai trò khác nhau. P2 có thể dùng Kafka/Spark cài trực tiếp hoặc dịch vụ được cung cấp sẵn, nhưng đường chạy Compose đang chuẩn bị trong repository cần Docker. P1 chưa cần khởi động các dịch vụ này. HDFS/MongoDB, Kubernetes và hệ thống giám sát thuộc phạm vi cuối kỳ, không phải điều kiện để chạy công cụ P1.

## 3. Nguồn cấu hình chung

Thứ tự ưu tiên: **biến môi trường của tiến trình → `.env` tại gốc repository → mặc định trong mã**. `.env.example` là mẫu được commit; `.env` là cấu hình trên từng máy và được bỏ qua bởi Git. Không ghi đường dẫn tuyệt đối của máy cá nhân vào mã dùng chung.

Tệp `.env` dùng cú pháp đơn giản `KEY=value`; không nội suy biến như `${OTHER_VARIABLE}`. Giá trị đã đọc được giữ trong môi trường của tiến trình. Sau khi sửa `.env`, chạy lại lệnh hoặc khởi động lại tiến trình để nạp cấu hình mới.

Trên PowerShell, tạo `.env` một lần nếu máy chưa có tệp đó:

```powershell
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
```

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `DATASET_DIR` | `data/d07-1week-mainline` | Dữ liệu đầu vào Sơn bàn giao |
| `SAMPLE_DIR` | `data/sample` | Nơi đặt sample/canonical event dùng chung |
| `RUNS_DIR` | `data/runs` | Thư mục chứa các lần chạy và kết quả sinh ra |
| `STATION_LIMIT` | `100` | Số trạm được chọn cho workload checkpoint |
| `DEMO_EVENT_LIMIT` | `0` | `0` dùng toàn bộ sample; giá trị dương giới hạn số event cho smoke test |
| `REPLAY_EVENTS_PER_SECOND` | `5000` | Tốc độ replay đề xuất cho producer |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Địa chỉ broker khi module chạy trong mạng Compose |
| `KAFKA_TOPIC` | `traffic-events` | Topic thống nhất giữa producer và Spark |
| `SPARK_WATERMARK` | `10 minutes` | Tham số watermark để Spark owner sử dụng |

Đường dẫn tương đối được tính từ **gốc repository**, không phụ thuộc thư mục hiện hành khi module đọc cấu hình. Trong container, repository được mount tại `/workspace` nên cùng giá trị `data/runs` trỏ vào `/workspace/data/runs`.

Các thư mục nguồn/sample/run phải nằm trong repository. Sample và run phải tách nhau, không chứa lẫn nhau, không trùng hoặc bao quanh thư mục dữ liệu nguồn và không dùng các thư mục mã/công cụ như `src`, `scripts`, `tests`, `.git`, `.venv`. Công cụ từ chối cấu hình vi phạm trước khi tạo thư mục.

Nếu producer hoặc Spark chạy trực tiếp trên máy host và Kafka chạy bằng Compose, dùng `KAFKA_BOOTSTRAP_SERVERS=localhost:29092`. Nếu module chạy trong container của Compose, dùng `kafka:9092`. Cấu hình đúng phụ thuộc vị trí chạy module; P1 công bố rõ hai trường hợp để tránh nhầm địa chỉ mạng.

Các biến ngưỡng phân loại trong cấu hình tham chiếu thuộc phần analytics và cần Analytics owner xác nhận khi tích hợp.

## 4. Cấu trúc lưu trữ chung

```text
data/
  d07-1week-mainline/           # Nguồn Sơn bàn giao; module đọc, không ghi đè
    processed/
      observations.csv.gz
      stations.csv
    manifest.json
  sample/                      # Canonical sample dùng chung
  runs/
    <run-id>/                  # Một bộ artifact cho một lần chạy
      input/
      valid_events/
      quarantine/
      checkpoints/
        ingest/
        quarantine/
        window_5/
        window_15/
      traffic_metrics/
        5min/
        15min/
      traffic_summary/
      reports/
```

Một run dùng một ID thống nhất giữa các module; ví dụ `thanh-checkpoint-01`. ID gồm 1–64 ký tự chữ không dấu, chữ số, dấu gạch dưới hoặc dấu gạch ngang; không dùng đường dẫn trong ID. Run mới dùng ID mới. Khi cần kiểm tra khôi phục cùng một Spark query, dùng lại run ID và checkpoint của query đó. Không chia sẻ một thư mục checkpoint cho nhiều query độc lập.

`input/` chứa snapshot đầu vào của run; `valid_events/` chứa event đã qua kiểm tra; `quarantine/` chứa event bị loại. `traffic_metrics/5min` và `15min` là output cửa sổ; `traffic_summary/` là output analytics; `reports/` lưu cấu hình, log ngắn và kết quả kiểm chứng. Đây là quy ước cho các bước sau, không phải các artifact được P1 tự xử lý tạo ra.

Không commit `.env`, `.venv`, cache, sample lớn, output và checkpoint dưới `data/runs/`. Báo cáo nhỏ dưới `reports/p1/` được giữ làm bằng chứng. Lệnh `init-storage` của P1 chỉ tạo `SAMPLE_DIR` và `RUNS_DIR`; không tải lại, chuẩn hóa lại hoặc thay đổi dữ liệu Sơn.

## 5. Các lệnh thực hiện P1

Mở PowerShell tại gốc repository, dùng Python 3.11. Wrapper sẽ chọn Python theo môi trường được hỗ trợ; có thể dùng `.venv` sẵn có. Không cần cài thư viện Kafka/PySpark/PyArrow để chạy ba lệnh sau:

```powershell
.\scripts\platform.cmd config
.\scripts\platform.cmd check
.\scripts\platform.cmd init-storage
```

Không truyền action tương đương chạy `check`. Muốn xem trước toàn bộ đường dẫn của một run cụ thể, dùng:

```powershell
.\scripts\platform.cmd config --run-id thanh-checkpoint-01
```

Lệnh này chỉ in cấu hình JSON, không tạo run, gửi event hoặc ghi metric. Trường `configuration.example_run` liệt kê các đường dẫn đã thống nhất. Khi cấu hình hợp lệ, `status` là `passed`; lệnh kiểm tra trả exit code `0` nếu đạt và mã khác `0` nếu có lỗi.

| Lệnh | Kết quả cần đọc |
|---|---|
| `config` | Cấu hình có hiệu lực, đường dẫn nguồn/sample/run và tham số Kafka/Spark dùng chung |
| `check` | Kiểm tra đúng Python 3.11, sự có mặt của `processed/observations.csv.gz`, `processed/stations.csv` và tính hợp lệ của đường dẫn lưu trữ; không khởi động broker hoặc Spark |
| `init-storage` | Hai thư mục sample/run đã tồn tại hoặc được tạo thành công; gọi lại không xóa nội dung |

Linux/macOS dùng cùng action:

```bash
bash scripts/platform.sh config
bash scripts/platform.sh check
bash scripts/platform.sh init-storage
```

`scripts/platform.ps1` là wrapper PowerShell tương đương. Windows có thể dùng `.cmd` khi chính sách chạy script trên máy không cho gọi `.ps1` trực tiếp.

Nếu muốn chủ động kiểm tra thêm Docker, dùng:

```powershell
.\scripts\platform.cmd check --docker
```

Chế độ này yêu cầu có Docker CLI, engine và Compose hoạt động; nó không khởi động dịch vụ. Docker không sẵn sàng chỉ làm lần kiểm tra có `--docker` thất bại; không phải điều kiện của kiểm tra P1 mặc định.

Dockerfile dùng cùng lệnh kiểm tra P1 làm lệnh chạy mặc định. Nếu sau này muốn kiểm tra container P1 riêng sau khi đã build image, dùng `docker compose run --rm --no-deps worker` để không khởi động Kafka do quan hệ `depends_on`. Lần bàn giao P1 hiện tại chưa chạy build/container thật.

Các module chính thức dùng `Settings.read()` từ `freewaypulse.config` để đọc cùng bộ biến thay vì tự tạo mặc định riêng. Lấy đường dẫn run bằng `settings.for_run(run_id).as_dict()`; các hàm này không tự tạo thư mục hoặc chạy dịch vụ. Khi nhận module của đồng đội, đối chiếu cách dùng cấu hình/đường dẫn với tài liệu này; P1 không phải đợi module Kafka/Spark/analytics hoàn thiện mới cung cấp được hợp đồng đó.

## 6. Tiếp nhận từ Sơn và bàn giao cho nhóm

P1 ghi nhận phần dữ liệu của Sơn **đã hoàn thành theo xác nhận của người dùng**. Kiểm tra nguồn trong công cụ P1 chỉ xác nhận tệp cần dùng có sẵn để các module nhận đầu vào; không thực hiện lại nhiệm vụ thu thập/chuẩn hóa/phân tích chất lượng D1–D4.

| Người nhận | Cần áp dụng từ P1 |
|---|---|
| Long | Đọc cùng Kafka address/topic, đường dẫn input, tốc độ replay và vị trí báo cáo |
| Tuấn Minh | Đọc cùng Kafka address/topic, watermark, output root và checkpoint riêng cho từng query |
| Tường Minh | Đọc metric từ các đường dẫn thống nhất và ghi summary/báo cáo đúng run |
| Thanh khi làm P2 sau này | Truyền cùng cấu hình/run ID cho các module và kiểm chứng chúng hoạt động cùng nhau |

**Trạng thái bàn giao:** cấu hình, hợp đồng đường dẫn và công cụ kiểm tra P1 đã được chuẩn hóa, kiểm chứng riêng trên host. Việc các module chính thức của đồng đội cùng sử dụng chúng cần được xác nhận khi nhóm tiếp nhận; kết quả hiện tại không tự chứng minh mọi module đã áp dụng tiêu chí “All components use consistent configuration and paths”.

Báo cáo trong `reports/p1/` ghi các kiểm tra đã thực chạy: `configuration.json` chứa cấu hình có hiệu lực, `check.json` chứa kiểm tra P1 mặc định, `storage.json` chứa kết quả khởi tạo storage, `validation.json` tổng hợp kết quả và giới hạn kiểm chứng, `test-results.txt` chứa kết quả test. CLI in JSON ra stdout; các tệp bằng chứng được lưu từ output lệnh, không có tham số `--report`.

Đã chạy **13/13 test P1 đạt**, kể cả trong thư mục cô lập chỉ có mã P1 và tắt site-packages bằng `python -S`. Không cần module P2/P3 hay thư viện cài thêm. Đã so SHA-256 trước/sau trên 11 file nguồn/sample hiện có: giữ nguyên. Cấu hình Compose hợp lệ qua CLI v2.39.4; build/container thật chưa chạy.

Để chạy riêng các test P1 trên Windows tại gốc repository, không kéo theo test tích hợp của P2/P3:

```powershell
$env:PYTHONPATH = Join-Path (Get-Location) 'src'
.\.venv\Scripts\python.exe -m unittest discover -s tests -p test_config.py
.\.venv\Scripts\python.exe -m unittest discover -s tests -p test_platform.py
```

Nếu không dùng `.venv`, thay đường dẫn Python trong hai lệnh bằng Python 3.11 trên máy. Test P1 chỉ dùng thư viện chuẩn. Khi chưa build hoặc khởi động container thật, không dùng kết quả kiểm tra cấu hình để khẳng định Docker/Kafka/Spark đã chạy thành công; bằng chứng đó sẽ được bổ sung ở giai đoạn tích hợp.

## 7. Chọn đúng tệp P1 để đưa lên Git

Để tạo commit P1 có phạm vi rõ ràng, dùng danh sách tệp cụ thể dưới đây rồi xem diff. Nếu workspace có thêm công việc khác, không stage toàn bộ các thư mục `src`, `scripts`, `docs`, `tests` hay `reports`.

```powershell
git status --short
git add .env.example .gitignore .dockerignore .gitattributes compose.yaml Dockerfile requirements.txt requirements-spark.txt
git add src/freewaypulse/__init__.py src/freewaypulse/config.py src/freewaypulse/platform.py
git add scripts/platform.cmd scripts/platform.ps1 scripts/platform.sh
git add tests/test_config.py tests/test_platform.py docs/platform-p1.md README.md
git add reports/p1
git diff --cached --stat
git diff --cached
```

Sau khi tự xem diff, commit có thể dùng tên theo checklist:

```powershell
git commit -m "chore(platform): standardize runtime configuration and storage layout"
```

Branch bàn giao P1 là `thanh_p1`. Đẩy commit trên branch này bằng `git push -u origin thanh_p1`.
