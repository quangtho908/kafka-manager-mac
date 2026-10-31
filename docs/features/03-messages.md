# 03 · Xem message

## Mục tiêu

Đọc nhanh message của một topic mà không phải dùng CLI. Đây là tính năng dùng nhiều nhất.

## Hành vi / UI

**Thanh truy vấn** (trên cùng tab Messages)
- **Partition**: All / một partition.
- **Bắt đầu từ**: Latest N (mặc định, N=100) · Earliest · Offset cụ thể · Thời điểm (date picker).
- **Giới hạn**: số message tối đa (mặc định 500, tối đa 10 000).
- Nút **Fetch**; **Stop** khi đang chạy.

**Bảng kết quả**
- Cột: Partition, Offset, Timestamp, Key, Value (rút gọn), Size.
- Sắp xếp mặc định theo timestamp giảm dần; sắp xếp được theo cột.
- Chọn dòng → panel chi tiết bên phải: Key, Value, Headers, metadata (timestamp type, size, partition, offset).
- Value: tab **Auto / JSON / Text / Hex**. Auto thử JSON (pretty print), rồi UTF-8, cuối cùng hex.
- Copy value / key / cả message (JSON).

**Lọc phía client** trên kết quả đã lấy: ô tìm chuỗi trong key hoặc value.

## Core API

```swift
struct MessageQuery { topic: String; partition: Int?; start: StartPosition; limit: Int }
enum StartPosition { case latest(Int), earliest, offset(Int64), timestamp(Date) }
struct KafkaMessage { partition: Int; offset: Int64; timestamp: Date; key: Data?; value: Data?; headers: [Header] }
```

`fetchMessages` trả `AsyncThrowingStream`, kết thúc khi đủ limit hoặc hết dữ liệu; huỷ Task thì dừng consumer và giải phóng tài nguyên. Dùng consumer riêng, không `group.id` commit offset (**không được làm lệch offset của group thật**).

## Hiệu năng

- Giới hạn buffer; cắt value hiển thị ở ~10 KB trong bảng, giữ nguyên bản đầy đủ cho panel chi tiết (tải khi chọn).
- Message >1 MB: không pretty print đồng bộ trên main thread.
- Cập nhật UI theo batch (vd. mỗi 100 ms), không từng message.

## Giới hạn

- Bộ nhớ: tổng key+value các message giữ lại tối đa 64 MB; chạm ngưỡng thì dừng fetch và báo "size limit reached" (số message có thể ít hơn limit).
- Cập nhật bảng: mặc định mỗi 100 ms, khoảng cách tăng dần theo số dòng đã hiển thị (thêm 100 ms mỗi 500 dòng) vì mỗi lần cập nhật SwiftUI phải diff cả bảng; luôn cập nhật nốt khi kết thúc, dừng hoặc lỗi.

## Edge case

- Topic rỗng → trạng thái "No messages", không phải spinner mãi.
- Offset ngoài khoảng → kẹp về khoảng hợp lệ và báo.
- Message null value (tombstone) → hiển thị rõ "null (tombstone)".
- Dữ liệu nhị phân/UTF-8 lỗi → rơi về Hex, không crash.
- Mất kết nối giữa chừng → giữ message đã lấy, báo lỗi, cho Retry.

## Tiêu chí hoàn thành

- Lấy 500 message gần nhất của topic local trong <2s.
- Fetch topic 1 triệu message với limit 500 không tăng bộ nhớ đáng kể.
- Fetch không tạo/ảnh hưởng consumer group nào (kiểm tra bằng `kafka-consumer-groups.sh --list`).

## Test

- Decode Auto: JSON, text, hex, null, UTF-8 lỗi.
- Lọc client; sắp xếp.
- View model: huỷ giữa chừng dừng stream (dùng Fake có độ trễ).

## Gửi message (Produce, T71)

Gửi **một** message vào topic từ app: nút **Produce…** ở header chi tiết topic, và **Produce Again…** ở panel chi tiết message / menu chuột phải trên dòng bảng Messages (điền sẵn từ message đang chọn).

**Core:** `KafkaClient.produce(_ request: ProduceRequest) async throws -> ProduceResult` là lệnh GHI. `GuardedKafkaClient` chặn khi cluster read-only (có trong bảng phân loại ghi của test). `ProduceRequest` gồm topic, partition (`nil` = partitioner chọn), key, value (`nil` = tombstone, khác với value rỗng), headers, timestamp (`nil` = bây giờ). `ProduceResult` trả partition, offset, timestamp thật do broker ghi. `ProduceValidator` kiểm tra thuần: topic không rỗng, partition ≥ 0, tên header không rỗng và không chứa NUL, timestamp ≥ 1970, tổng kích thước (key + value + header) ≤ 1 MiB. Broker từ chối quá cỡ (MSG_SIZE_TOO_LARGE) được ánh xạ thành thông báo thân thiện.

**Mã hoá nhập liệu (`ProduceEncoder`):** Text (UTF-8 đúng như gõ), JSON (kiểm tra hợp lệ, gửi nguyên văn; nút *Format* thụt dòng lại), Hex (chấp nhận khoảng trắng, `: - ,`, tiền tố `0x`; số chữ số lẻ hoặc ký tự lạ báo lỗi). Key chỉ có Text/Hex; để trống = không có key. Không có Avro / Schema Registry.

**Live:** một producer librdkafka tạo lười cho mỗi client (không phải mỗi message), cấu hình nhỏ (`acks=all`, `enable.idempotence=true`, `linger.ms=0`, `queue.buffering.max.messages=100`, `queue.buffering.max.kbytes=4096`, `allow.auto.create.topics=false`, `message.timeout.ms` = timeout của client). Đóng sau 30 giây không dùng và khi disconnect. Chạy trên executor riêng của client, chờ delivery report bằng `rd_kafka_poll` (không dùng cooperative pool), quá hạn thì purge rồi báo `.timeout`; lỗi hạ tầng (không tới được broker) được giải thích bằng lỗi mà thread của client đã ghi lại. Trước khi gửi, app kiểm tra topic tồn tại và partition hợp lệ bằng metadata, nên gửi vào topic không có sẽ báo "Topic … doesn't exist" ngay và **không** tạo topic. Lưu ý: `enable.idempotence=true` cần broker/ACL cho phép ghi idempotent; cluster cũ có ACL chặt có thể báo lỗi quyền.

**UI:** sheet có Partition (Auto / 0…n−1), Key, Value (soạn thảo monospaced, bật *Null (tombstone)* thì khoá ô soạn), Headers (Text/Hex/Null cho từng giá trị), Timestamp tuỳ chọn, và *Repeat* 1…1000 lần có tiến độ + Stop. Báo lỗi tại chỗ (icon + chữ), hiển thị kích thước "12 KB of 1 MB", gửi bằng ⌘Return. Thành công hiện "Sent to partition P at offset O" với *Send Another* (giữ nguyên các trường). Cluster **Production** hỏi gõ lại tên topic trước khi gửi; Local/Staging gửi luôn; read-only thì nút bị tắt kèm tooltip. Sau khi gửi, nếu tab Messages đã có dữ liệu thì hiện gợi ý "New message available — Fetch again" (không tự fetch, không polling).

**Test:** validation, encoding (hex lẻ/tiền tố/ký tự lạ, JSON), Guarded, Fake (hiện ra ở lần fetch sau), view model (gửi/lỗi/double-submit/read-only/production/prefill/repeat), và itest broker thật chỉ với topic `km-itest-*` (helper `requireTestOwnedTopic` từ chối topic khác).
