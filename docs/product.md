# Product

## Mục tiêu

Một công cụ desktop nhanh, native cho dev/ops làm việc với Kafka hằng ngày, thay cho CLI (`kafka-topics.sh`, `kafka-console-consumer.sh`) và các UI web nặng.

## Người dùng

Backend dev và SRE cần xem topic, đọc message, kiểm tra lag của consumer group, và thao tác nhanh trên nhiều cluster (local, staging, prod).

Hai việc dùng nhiều nhất: **xem message của một topic** và **xem lag của một consumer group**. Làm tốt hai việc này thì app đã có giá trị trước khi có phần còn lại.

## Tổng thể feature

Giai đoạn: **MVP** = bản dùng được đầu tiên, **P2** = ngay sau MVP, **P3** = về sau.

### 1. Kết nối và bảo mật

| Feature | Giai đoạn |
|---|---|
| Nhiều connection profile: tên, bootstrap servers, màu và nhãn môi trường (local/staging/prod) | MVP |
| Xác thực PLAINTEXT, SASL (PLAIN, SCRAM), SSL | MVP |
| Secret lưu Keychain, không ghi vào JSON/UserDefaults | MVP |
| "Test connection" kèm thông báo lỗi dễ hiểu (lỗi Kafka gốc thường khó đọc) | MVP |
| SASL OAUTHBEARER, mTLS (client cert), cluster cloud (Confluent, MSK, Aiven) | P2 |

### 2. Topic

| Feature | Giai đoạn |
|---|---|
| Danh sách topic, tìm kiếm, sắp xếp theo kích thước/số message | MVP |
| Chi tiết: partition, leader, replica, ISR, offset đầu/cuối, config | MVP |
| Tạo topic, xoá topic (có xác nhận) | MVP |
| Sửa config topic | P2 |
| Thêm partition | P3 |

### 3. Xem message

| Feature | Giai đoạn |
|---|---|
| Chọn partition, offset, thời điểm, hoặc "latest N message" | MVP |
| Hiển thị key, value, headers, timestamp, partition, offset | MVP |
| Tự format JSON; xem raw/hex khi không giải mã được | MVP |
| Giới hạn số message và buffer để không tràn RAM; message lớn không làm đơ UI | MVP |
| Stream live (tail) | P2 |
| Lọc theo key hoặc nội dung | P2 |
| Copy, export (JSON/CSV) | P2 |

### 4. Consumer group

| Feature | Giai đoạn |
|---|---|
| Danh sách group và trạng thái (Stable, Rebalancing, Empty, Dead) | MVP |
| Member, offset, **lag theo từng partition**, tổng lag | MVP |
| Reset offset (về đầu, cuối, theo thời điểm, theo số) có preview trước khi áp dụng | P2 |
| Xoá group | P2 |
| Biểu đồ lag theo thời gian | P3 |

### 5. Produce message

| Feature | Giai đoạn |
|---|---|
| Gửi một message: key, value, headers, chọn partition | P2 |
| Gửi lại một message đang xem | P2 |
| Template/lịch sử message đã gửi | P3 |

### 6. Schema và giải mã

| Feature | Giai đoạn |
|---|---|
| Kết nối Schema Registry, tự giải mã Avro | P2 |
| Protobuf, JSON Schema | P3 |

Không có phần này thì nhiều cluster thật chỉ hiện dữ liệu nhị phân.

### 7. Broker và cluster

| Feature | Giai đoạn |
|---|---|
| Danh sách broker, controller, version | P2 |
| Config broker (chỉ đọc) | P2 |
| ACL, quota | P3 |

### 8. An toàn khi thao tác

| Feature | Giai đoạn |
|---|---|
| Cluster production mặc định chỉ đọc | MVP |
| Thao tác phá huỷ (xoá topic, reset offset) phải gõ lại tên để xác nhận | MVP |
| Nhật ký các thao tác đã thực hiện | P2 |

### 9. Trải nghiệm native

| Feature | Giai đoạn |
|---|---|
| Sidebar cluster → topic/group, dark mode | MVP |
| Tìm kiếm nhanh ⌘K, phím tắt | P2 |
| Nhiều cửa sổ, khôi phục tab/trạng thái khi mở lại | P2 |

## Ngoài phạm vi (hiện tại)

Kafka Connect, ksqlDB, quản lý Schema Registry (chỉ đọc để giải mã), monitoring dài hạn và alert.

## Nguyên tắc

- Mặc định an toàn: thao tác phá huỷ cần xác nhận rõ, nhất là với cluster gắn nhãn production.
- Không bao giờ ghi credential ra file JSON hay UserDefaults.
- Message lớn phải stream/phân trang, không nạp hết vào bộ nhớ.

## Thứ tự làm

1. Chọn Kafka client (xem `architecture.md`; cần cả admin API lẫn consume), kết nối được tới broker local.
2. Connection profile + Keychain (mục 1).
3. Topic list + topic detail (mục 2).
4. Xem message (mục 3).
5. Consumer group + lag (mục 4).
6. Các mục P2 theo nhu cầu: produce, Schema Registry, reset offset, stream live.
7. Đóng gói, ký, notarize `.app`.
