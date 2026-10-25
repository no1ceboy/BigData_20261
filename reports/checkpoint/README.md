# Bằng chứng nghiệm thu P2/P3

Ngày bàn giao theo giờ Việt Nam: **26/10/2026**. [CI 40213958909 thành công](https://github.com/no1ceboy/BigData_20261/actions/runs/40213958909) kiểm thử source commit `3a3beabcdbc9742d0ba3057708e10045554bc6db` trên checkout sạch của Ubuntu, với Kafka/Spark thật trong Docker Linux containers.

| Kiểm chứng | Kết quả |
|---|---:|
| Trạm × ngày | 100 × 7 |
| Event producer gửi / lỗi | 201.600 / 0 |
| Event hợp lệ / quarantine | 201.600 / 0 |
| Metric 5 phút / 15 phút tumbling | 201.600 / 67.200 |
| Summary | 268.800 |
| Event bị watermark loại | 0 |
| Test Linux | 66 pass, 0 fail, 0 skip |

Verifier đọc nguồn CSV/gzip độc lập, so tất cả event, khóa cửa sổ, metric, nhãn và explanation với Parquet. Chạy lại cùng checkpoint giữ nguyên bốn query ID, không phát lại producer, không nhận thêm event và không thay đổi kết quả.

- `validation.json`: trạng thái task, môi trường và phạm vi kiểm chứng.
- `producer.json`, `runtime.json`: delivery và cấu hình đã dùng.
- `verification.json`: kết quả đối chiếu đầy đủ, coverage và phân bố trạng thái.
- `restart.json`, `streaming-summary.json`: bằng chứng restart, query và watermark.
- `test-results.txt`: trích kết quả từ log CI.

Toàn bộ log/progress có trong artifact **checkpoint-evidence** của run CI. Bộ file ở đây là bằng chứng nhỏ để review; không chứa output Parquet lớn. P2/P3 đủ bằng chứng bàn giao; A3 vẫn do trưởng nhóm nghiệm thu. Chế độ hữu hạn giữ toàn bộ aggregate state và ghi các snapshot riêng, chưa phải giao dịch atomic cho mọi tình huống crash.
