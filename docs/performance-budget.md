# Performance budget

Yêu cầu từ người dùng (2026-10-31): **app nhẹ, mở lên không lag, không tốn bộ nhớ.** Mọi con số dưới đây là **ngân sách đề xuất** (BA), đo trên máy dev, build release; chốt lại sau khi có số đo thật từ spike T04 và bản chạy đầu tiên. Vượt ngân sách = MAJOR khi review.

## Ngân sách

| # | Chỉ số | Ngân sách | Cách đo |
|---|---|---|---|
| P1 | Khởi động nguội đến khi cửa sổ có thể tương tác | ≤ 0.5 s | `time` từ lúc `open` đến khi vẽ xong cửa sổ đầu; Instruments (App Launch) |
| P2 | RAM khi mở app, chưa kết nối cluster nào | ≤ 40 MB **phys_footprint** | `footprint -p <pid>` (cột Memory của Activity Monitor) sau 30 s idle |
| P3 | RAM khi kết nối 1 cluster, xem danh sách topic (5000 topic) | ≤ 60 MB phys_footprint | như trên |
| P4 | RAM khi hiển thị 500 message (mỗi cái ≤ 10 KB) | ≤ 100 MB phys_footprint; không tăng khi để mở 10 phút | như trên + Instruments (Allocations/Leaks) |
| P5 | CPU khi idle (không thao tác, không tail) | ≈ 0% (không có timer/poll nền) | Activity Monitor, Instruments (Time Profiler) |
| P6 | Mở danh sách 5000 topic, cuộn, tìm kiếm | không rớt khung hình rõ rệt; lọc ≤ 100 ms | Instruments (SwiftUI), thủ công |
| P7 | Kích thước bản `.app` | ≤ 40 MB (kể cả thư viện Kafka) | `du -sh` |

## Đơn vị đo bộ nhớ (chốt 2026-10-31, sau T13b)

Dùng **phys_footprint** (`footprint -p`), không dùng RSS. RSS tính cả các framework hệ thống dùng chung nên một cửa sổ SwiftUI trống đã ~85 MB RSS nhưng chỉ ~21 MB phys_footprint (đo ở T13b, build release, macOS 26) — chỉ footprint phản ánh phần app thật sự tốn. Số P2–P4 ở trên là đề xuất dựa trên baseline này, sẽ chốt lại với số đo thật; spike T04 đã ghi cả hai (librdkafka đã giảm buffer thêm ~5 MB footprint).

## Nguyên tắc thiết kế để đạt ngân sách

1. **Không làm gì lúc khởi động.** Không kết nối cluster, không đọc Keychain, không tải topic. Chỉ đọc `connections.json` (nhỏ), vẽ sidebar. Mọi thứ khác lười (lazy), kích hoạt khi người dùng chọn.
2. **Kết nối theo yêu cầu, ngắt khi rảnh.** Một client cho mỗi cluster đang mở; đóng sau một thời gian không dùng (mặc định 5 phút) hoặc khi đóng cửa sổ/cluster.
3. **Consumer tạm thời.** Mỗi lần xem message tạo consumer riêng, đọc đủ `limit` rồi đóng ngay. Không giữ consumer nền.
4. **Giới hạn buffer của thư viện Kafka**: tắt/prefetch nhỏ (thư viện thường mặc định hàng chục MB mỗi consumer); đặt kích thước fetch tối đa vừa đủ cho `limit`.
5. **Dữ liệu bị chặn trần**: số message tối đa (mặc định 500, tối đa 10 000), value bị cắt ở bảng, bản đầy đủ chỉ tải khi chọn dòng; không lưu lịch sử message.
6. **Metadata gọn**: danh sách topic chỉ lấy tên/số partition; offset/số message tính khi mở chi tiết, không cho cả 5000 topic.
7. **UI ảo hoá** (`List`/`Table`), cập nhật theo batch ~100 ms; không timer nền; tự refresh mặc định tắt.
8. **Không tải thư viện nặng khi không cần**: không thêm framework/dependency lớn; ưu tiên API hệ thống.
9. **Không rò bộ nhớ**: Task được huỷ, stream dọn tài nguyên (`onTermination`), không giữ tham chiếu vòng giữa view model và closure.

## Ảnh hưởng tới lựa chọn Kafka client (spike T04)

Tiêu chí đo bắt buộc cho mỗi ứng viên (đo, không ước đoán):
- Thêm bao nhiêu MB vào bản `.app` (thư viện + phụ thuộc như OpenSSL).
- RSS của process PoC: sau khi tạo client (chưa kết nối), sau khi kết nối + list topic, sau khi đọc 500 message, sau khi đóng consumer (có trả bộ nhớ không).
- Thời gian tạo client + kết nối + list topic trên cluster local.
- Có cấu hình giới hạn buffer/prefetch không, và hiệu quả thực tế.
- Có thể khởi tạo lười hoàn toàn (không tốn gì cho đến lần dùng đầu).

## Cách kiểm soát

- Mỗi task UI/stream báo cáo số đo liên quan (P1–P6) hoặc ghi "không ảnh hưởng".
- Trước khi phát hành MVP: đo toàn bộ P1–P7 bằng build release, ghi vào `docs/` như baseline.
