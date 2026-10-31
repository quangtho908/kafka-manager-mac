# 02 · Topics

## Mục tiêu

Xem và quản lý topic của cluster đang chọn.

## Hành vi / UI

**Danh sách**
- Cột: Name, Partitions, Replication factor, (tuỳ chọn) Messages ≈ tổng (end − start offset), Size nếu lấy được.
- Ô tìm kiếm lọc theo tên (không phân biệt hoa thường); sắp xếp theo cột.
- Ẩn topic nội bộ (`__consumer_offsets`, `_schemas`…) theo toggle "Show internal".
- Nút refresh; tự refresh khi quay lại tab sau >30s.

**Chi tiết** (chọn một topic)
- Tab **Partitions**: id, leader, replicas, ISR, start offset, end offset, số message. Đánh dấu partition under-replicated.
- Tab **Config**: bảng key/value, đánh dấu giá trị khác default; chỉ đọc ở MVP.
- Tab **Messages**: xem spec 03.

**Tạo topic**: form Name, Partitions (mặc định 1), Replication factor (mặc định 1), config tuỳ chọn (`retention.ms`, `cleanup.policy`). Validate tên theo luật Kafka (`[a-zA-Z0-9._-]`, ≤249 ký tự, không phải `.`/`..`).

**Xoá topic**: theo spec 05 (gõ lại tên để xác nhận; bị chặn ở cluster read-only).

## Core API

`listTopics`, `describeTopic`, `createTopic`, `deleteTopic` (spec 00). Danh sách topic lớn (hàng nghìn) → lấy metadata một lần, tính offset khi mở chi tiết/theo yêu cầu, không tính cho tất cả.

## Edge case

- Cluster có >5000 topic: danh sách dùng `List`/`Table` ảo hoá, tìm kiếm không giật.
- Không có quyền describe → hiện topic với trạng thái "no access", không làm hỏng cả danh sách.
- Topic bị xoá bởi người khác khi đang xem → báo `unknownTopic`, quay về danh sách.
- Tạo topic với replication factor > số broker → hiện lỗi gốc đã dịch.

## Tiêu chí hoàn thành

- Xem, tìm kiếm, sắp xếp 5000 topic mượt (Fake data).
- Tạo và xoá topic trên Kafka local.
- Partitions tab khớp với `kafka-topics.sh --describe`.

## Test

- Lọc/sắp xếp (logic ở view model, test thuần).
- Validate tên topic.
- Lỗi một phần (no access) không phá danh sách.
