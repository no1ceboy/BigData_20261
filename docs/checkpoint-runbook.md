# P2/P3 — Tích hợp và demo checkpoint của Nguyễn Tất Thanh

Tài liệu này hướng dẫn chạy các module đã được nhóm bàn giao trên `main`: dữ liệu D1–D4 của Sơn, Kafka/replay K1–K3 của Long, Spark S1–S4 của Tuấn Minh, analytics A1–A2 của Tường Minh và cấu hình P1 của Thanh. P2 nối các module và kiểm chứng đầu ra; P3 cung cấp một lệnh demo, cách chạy lại và hướng dẫn chẩn đoán.

## 1. Phụ thuộc và trạng thái nghiệm thu

Đầu vào từ đồng đội đã sẵn sàng: checklist đánh dấu D1–D4, K1–K3, S1–S4 và A1–A2 là Done. P2/P3 không còn bị chặn bởi task của thành viên khác. Trước lần triển khai này, checklist còn ghi P2/P3 chưa bắt đầu; trạng thái bàn giao sau khi chạy nghiệm thu như sau:

| Task | Trạng thái bàn giao | Bằng chứng/phạm vi |
|---|---|---|
| P1 — Cấu hình/runtime/storage | Done | Cấu hình chung và kiểm tra đã bàn giao |
| P2 — Tích hợp pipeline | Done | Kafka → Spark → analytics → Parquet đủ 100 trạm × 7 ngày, verifier pass |
| P3 — Demo/runbook | Done | Một lệnh từ checkout sạch trên Linux CI, restart cùng checkpoint giữ nguyên output |
| A3 — Acceptance của trưởng nhóm | Not Started | Tường Minh đánh giá và sign-off; Thanh không tự nghiệm thu thay |

**Bằng chứng thực chạy:** [GitHub Actions run 40213958909](https://github.com/no1ceboy/BigData_20261/actions/runs/40213958909) đã **SUCCESS**, kết thúc ngày **26/10/2026 theo Asia/Saigon**. Run kiểm tra chính xác source commit [`3a3beabcdbc9742d0ba3057708e10045554bc6db`](https://github.com/no1ceboy/BigData_20261/commit/3a3beabcdbc9742d0ba3057708e10045554bc6db), không phải hash của các commit cập nhật tài liệu sau đó. Bộ test trong Linux worker đạt **66 passed trong 21,33 giây**.

Producer xác nhận **201.600 sent, 0 failed**, trong **40,321 giây**, đạt **4.999,89 event/giây**. Spark lưu 201.600 valid events, quarantine 0; verifier độc lập đối chiếu nguồn và Parquet thành công. Chạy lại Spark với cùng checkpoint, không phát lại producer, giữ nguyên kết quả. Báo cáo tổng hợp được lưu tại [reports/checkpoint/validation.json](../reports/checkpoint/validation.json).

Workflow [checkpoint.yml](../.github/workflows/checkpoint.yml) lưu báo cáo thành artifact `checkpoint-evidence`; chi tiết mỗi run nằm trong `data/runs/<run-id>/reports/`. Máy Windows local vẫn thiếu Docker engine; bằng chứng Kafka–Spark trên là từ Linux CI chạy thật. A3 thuộc trưởng nhóm; kết quả P2/P3 không tự thay cho sign-off A3.

## 2. Chuẩn bị trên một bản checkout mới

Yêu cầu:

- Python **3.11** trên máy gọi lệnh.
- Docker engine đang chạy **Linux containers**, có Docker Compose v2 hỗ trợ `up --wait`.
- Kết nối tải image, gói Python và Kafka connector từ Maven trong lần chạy đầu.
- Hai tệp dữ liệu Sơn đã bàn giao: `data/d07-1week-mainline/processed/observations.csv.gz` và `stations.csv`.

Host chỉ cần Python thư viện chuẩn để gọi `run`, `doctor` và `down`. Java 17, Spark 3.5.7 và thư viện xử lý được cài trong image `worker`; không cần cài riêng Kafka/Spark/Java trên host khi dùng cách chạy này. HDFS, MongoDB và Kubernetes không thuộc điều kiện chạy checkpoint.

Các mặc định trong mã đã đủ để demo. Nếu cần cấu hình riêng, sao chép `.env.example` thành `.env` một lần, rồi điều chỉnh cấu hình theo [P1](platform-p1.md). Trong Compose, giữ `KAFKA_BOOTSTRAP_SERVERS=kafka:9092`; địa chỉ `127.0.0.1:29092` dành cho client chạy trực tiếp trên host.

Kiểm tra môi trường tại gốc repository:

```powershell
.\scripts\demo.cmd doctor
```

Linux/macOS:

```bash
bash scripts/demo.sh doctor
```

Nếu có `.venv`, script dùng Python trong đó; nếu không, script dùng `python` trên Windows hoặc `python3` trên Linux/macOS. Python được chọn phải là 3.11. Bước `doctor` trả exit code khác 0 khi thiếu dữ liệu, sai phiên bản Python hoặc Docker/Compose chưa sẵn sàng.

## 3. Demo toàn bộ 100 trạm × 7 ngày bằng một lệnh

Windows:

```powershell
.\scripts\demo.cmd run --run-id thanh-checkpoint-20261025 --station-limit 100 --event-limit 0
```

Linux/macOS:

```bash
bash scripts/demo.sh run --run-id thanh-checkpoint-20261025 --station-limit 100 --event-limit 0
```

`run` là action mặc định. Bỏ `--run-id` để chương trình tạo ID mới. `--event-limit 0` dùng đủ các event của các trạm đã chọn; một số dương như `1200` chỉ dùng cho smoke test và không thay cho nghiệm thu đủ tuần.

Không cần `data/sample`. Producer đọc dữ liệu gốc theo chunk và chọn 100 station ID nhỏ nhất từ metadata. Run hiện tại tạo output mới trong `data/runs/<run-id>`; không dùng lại sample hoặc output từ những lần chạy cũ đã xóa. Không sửa dữ liệu nguồn.

Lệnh tự thực hiện các bước:

1. Kiểm tra cấu hình, dữ liệu và Docker/Compose.
2. Khởi động Kafka, chờ health check và build image `worker`.
3. Tạo topic riêng `<KAFKA_TOPIC>-<run-id>` cho lần chạy.
4. Gọi producer của Long, giữ thời gian lịch sử và event ID, chờ xác nhận delivery và ghi `producer.json`.
5. Gọi Spark của Tuấn Minh bằng `--finite-demo --trigger available_now --slide-15min none`, đọc Kafka, parse/validate, watermark/dedup và lưu valid events/quarantine cùng checkpoint.
6. Chạy cửa sổ Spark streaming 5 phút và 15 phút, lưu Parquet và áp dụng analytics A1 để tạo summary.
7. Đọc lại Parquet, đối chiếu nguồn, số lượng, khóa cửa sổ, giá trị metric và nhãn analytics.
8. Chạy lại Spark bằng đúng topic/cấu hình/checkpoint, **không phát lại producer**, kiểm chứng đầu ra lần nữa và ghi `restart.json` khi kết quả giữ nguyên.

Một thành phần lỗi sẽ làm lệnh dừng với exit code khác 0; xem `failure.json` và log của bước tương ứng. Thành công toàn bộ mới in `Demo and restart passed`.

## 4. Vì sao checkpoint dùng một partition và cửa sổ complete?

Topic ingestion dùng chung của Long vẫn có thiết kế 3 partition. Topic riêng của demo dùng **1 partition** để giữ thứ tự event toàn cục khi Spark đọc các micro-batch giới hạn 5.000 offset. Với một replay lịch sử đã kết thúc, việc đọc lệch tiến độ giữa nhiều partition có thể đẩy watermark vượt qua event cũ chưa đọc ở partition khác. Topic demo không thay đổi thiết kế topic dùng chung.

Trong stream liên tục, append-mode window chỉ phát kết quả khi watermark đủ tiến để đóng cửa sổ. Replay đúng 7 ngày rồi dừng không có event tương lai để đóng các cửa sổ cuối. Chế độ `--finite-demo` xử lý điều này thành hai bước thật bằng Structured Streaming:

- Đọc Kafka qua `readStream`, validate và watermark/dedup, ghi valid events bằng sink Parquet có checkpoint.
- Sau khi ingest kết thúc, đọc valid Parquet bằng `readStream`, tổng hợp cửa sổ bằng complete mode và ghi snapshot Parquet. Summary áp dụng module analytics hiện có lên hai bộ metric đã lưu.

Cửa sổ 15 phút trong checkpoint là **tumbling**, tương ứng `--slide-15min none`; chế độ Spark liên tục hiện có vẫn hỗ trợ cửa sổ 15 phút trượt mỗi 5 phút. Không được dùng số lượng của hai cách chia cửa sổ để đối chiếu lẫn nhau.

Complete mode giữ trạng thái tổng hợp của cả replay và ghi đè snapshot. Cách này phục vụ workload demo hữu hạn, chưa phải chiến lược lưu trữ stream chạy vô hạn. Ghi snapshot của hai bộ metric và summary chưa tạo thành một transaction atomic. Kiểm tra restart ở đây xác nhận chạy lại một run hoàn tất bằng cùng checkpoint không nhân đôi output; chưa chứng minh phục hồi atomic khi tiến trình bị crash giữa lúc ghi snapshot.

## 5. Output cần kiểm tra

```text
data/runs/<run-id>/
├── valid_events/                 # event hợp lệ, dedup từ Kafka
├── quarantine/                   # event bị loại và lý do
├── checkpoints/                  # state/offset riêng từng query
├── traffic_metrics/
│   ├── 5min/                     # Parquet metric 5 phút
│   └── 15min/                    # Parquet metric 15 phút tumbling
├── traffic_summary/              # metric + traffic_state/explanation
└── reports/
    ├── runtime.json              # source/topic/tham số của lần chạy
    ├── producer.json             # selected/sent/failed/tốc độ/time range
    ├── streaming-config.json     # hợp đồng cấu hình với checkpoint
    ├── streaming*.json           # query ID, tiến độ và output Spark
    ├── verification.json         # đối chiếu Parquet với nguồn
    ├── restart.json              # xác nhận chạy lại không đổi kết quả
    └── *.log                     # producer, Spark, verify và restart
```

Kỳ vọng với bộ nguồn hiện tại, đủ 100 trạm × 7 ngày, không giới hạn event:

| Kết quả | Kỳ vọng |
|---|---:|
| Event producer chọn/gửi thành công | 201.600 |
| Khoảng thời gian nguồn | 14/10/2026 00:00 đến 20/10/2026 23:55, offset `-07:00` |
| Số event mỗi trạm | 2.016 |
| Metric 5 phút | 201.600 |
| Metric 15 phút tumbling | 67.200 |
| Traffic summary | 268.800 |
| Producer delivery lỗi | 0 |

Linux CI run `40213958909` đã xác nhận đầy đủ các số lượng trên: metric 5 phút **201.600**, metric 15 phút **67.200**, summary **268.800**; verifier và kiểm tra restart đều pass. Khi thay số trạm, cap event, nguồn hoặc quy tắc validation, cần dùng verifier để tính và đối chiếu lại; không ép số lượng bằng cách bỏ qua event lỗi.

Verifier cần xác nhận giá trị metric và nhãn, ngoài số lượng file/dòng. `avg_speed` dùng mph, `total_flow` là tổng xe/5 phút của những event trong cửa sổ và `avg_occupancy` giữ tỷ lệ 0–1. Summary giữ `window_minutes` để phân biệt hai bộ cửa sổ.

P2 sử dụng luật A1/A2 hiện có với các ngưỡng mặc định 20/40/55 mph và ghi cấu hình của run trong `runtime.json`. Các biên 20, 40 và 55 lần lượt vào `CONGESTED`, `MODERATE` và `FREE`; tốc độ thấp hơn 20 vào `SEVERE`. [Tài liệu analytics](analytics-checkpoint.md) giải thích giới hạn diễn giải nghiệp vụ của các ngưỡng này. Tích hợp pass xác nhận code áp dụng nhất quán, không thay cho xác nhận ngưỡng ngoài thực tế.

Chất lượng nguồn được giữ nguyên: không đổi `observed_percent=0` thành dữ liệu quan sát thật, không tự nâng chất lượng đo và không xóa event chỉ để số liệu đẹp hơn. `observed_percent=0` là thông tin chất lượng cần nêu khi demo; nó không tự chứng minh event lỗi schema hoặc pipeline bị stall.

## 6. Kiểm chứng lại và dừng dịch vụ

Host đã cài `requirements.txt` có thể kiểm tra lại một run:

```powershell
.\scripts\demo.cmd verify --run-id thanh-checkpoint-20261025
```

Nếu chưa cài thư viện xử lý trên host, dùng image worker đã build:

```powershell
docker compose run --rm -T worker python -m freewaypulse.verification --run-dir /workspace/data/runs/thanh-checkpoint-20261025 --source /workspace/data/d07-1week-mainline/processed/observations.csv.gz
```

Không gọi `run` lần nữa với ID cũ: orchestration từ chối ghi đè run đã có. Dùng `verify` để đọc kết quả hiện có hoặc chọn ID mới để chạy demo mới. Việc thử restart bằng cùng checkpoint đã nằm trong lệnh `run` thành công.

Dừng Kafka sau khi demo:

```powershell
.\scripts\demo.cmd down
```

`down` giữ volume broker và output run. File checkpoint/output là artifact sinh ra, không đưa vào Git. Giữ các báo cáo JSON/log nhỏ cần thiết cho review; workflow CI lưu chúng thành artifact để tải về.

## 7. Chẩn đoán lỗi

| Dấu hiệu | Cách xử lý |
|---|---|
| `doctor` báo thiếu Docker CLI/engine/Compose | Khởi động/cài môi trường Docker Linux containers có Compose v2; chạy lại `doctor`. Cài riêng file Compose CLI chưa cung cấp Docker engine. |
| Python không phải 3.11 | Chọn Python 3.11 cho `python` hoặc tạo `.venv` bằng Python 3.11. Script ưu tiên `.venv` nếu có. |
| Không tìm thấy observations/stations | Kiểm tra hai file nguồn và `DATASET_DIR`; không cần tạo lại `data/sample`. |
| Broker không healthy hoặc cổng 29092 đã dùng | Xem `docker compose ps` và `docker compose logs kafka`; kiểm tra ứng dụng đang dùng cổng trước khi đổi cấu hình. |
| Producer host không kết nối được | Host dùng `127.0.0.1:29092`; worker dùng `kafka:9092`. Chạy demo Compose với địa chỉ container. |
| `Run already exists` hoặc checkpoint contract khác | Dùng `verify` với run cũ hoặc một `--run-id` mới; không đổi topic/window/watermark rồi tái sử dụng checkpoint. |
| Không tải được Spark Kafka connector/Ivy/Maven | Kiểm tra Internet/proxy và xem `spark.log`. Lần đầu cần tải connector; Compose giữ cache Ivy riêng. |
| Java gateway lỗi khi chạy trực tiếp trên host | Dùng `worker` có Java 17 và Spark đã pin. Chạy Spark trực tiếp trên Windows còn cần Hadoop native phù hợp; runbook dùng container Linux. |
| Lệnh Spark timeout hoặc out of memory | Xem `spark.log`, kiểm tra tài nguyên cấp cho Docker. Dùng run ID mới với `--event-limit 1200` để chẩn đoán nhanh rồi nghiệm thu lại workload đầy đủ. |
| Số metric 15 phút khác 67.200 | Xem `window_15_slide`, cap event, số trạm và validation; checkpoint chuẩn dùng tumbling `none`. Xem verifier thay vì chỉ đếm file. |
| Dừng ở verify/restart | Đọc `verify.log`, `verify-restart.log`, `restart.log`, `failure.json`; giữ artifact để sửa lỗi rồi chạy bằng ID mới. |

## 8. Bàn giao để review

Các phần chính của Thanh là `src/freewaypulse/demo.py`, `src/freewaypulse/verification.py`, `scripts/demo.*`, workflow checkpoint, tài liệu này và các sửa tương thích tại producer/Spark. Module nghiệp vụ tiếp tục thuộc owner ban đầu.

P2/P3 đã có đủ bằng chứng trong [CI thành công](https://github.com/no1ceboy/BigData_20261/actions/runs/40213958909) và [báo cáo bàn giao](../reports/checkpoint/validation.json): source commit, tham số, `runtime.json`, `producer.json`, báo cáo Spark, `verification.json`, `restart.json` và 66 test pass. Có thể review và bàn giao cho trưởng nhóm thực hiện A3. Giữ nguyên giới hạn chế độ finite replay và snapshot không atomic đã nêu ở mục 4 khi trình bày kết quả.
