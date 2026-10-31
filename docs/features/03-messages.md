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
