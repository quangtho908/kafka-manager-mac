# 04 · Consumer groups

## Mục tiêu

Xem trạng thái và **lag** của consumer group.

## Hành vi / UI

**Danh sách**: Group ID, State (Stable / PreparingRebalance / CompletingRebalance / Empty / Dead), số member, tổng lag. Tìm kiếm theo tên. Tổng lag tính khi mở/hiển thị, không chặn danh sách.

**Chi tiết**
- Header: state, coordinator broker, partition assignor, tổng lag.
- Tab **Lag**: bảng theo (topic, partition): committed offset, end offset, **lag = end − committed**, member đang giữ partition. Tô màu theo ngưỡng (0 xanh, >0 vàng, >10 000 đỏ; ngưỡng cấu hình sau). Nhóm theo topic, có tổng lag mỗi topic.
- Tab **Members**: member id, client id, host, partition được gán.
- Refresh thủ công + tự động (mặc định tắt; bật thì 5s/10s/30s).

**Từ topic sang group**: trong chi tiết topic, danh sách các group đang consume topic đó, bấm để nhảy sang chi tiết group.

## Core API

`listGroups`, `describeGroup` (spec 00). `GroupDetail` chứa `state`, `coordinator`, `members`, `lags: [PartitionLag]`.

`PartitionLag` (chốt 2026-10-31, sau review T01):

```swift
struct PartitionLag { topic: String; partition: Int
                      committedOffset: Int64?   // nil = chưa từng commit
                      startOffset: Int64; endOffset: Int64 }
var hasCommittedOffset: Bool        // committedOffset != nil
var lag: Int64                      // luôn có giá trị, không âm:
                                    //   có commit  -> max(0, end - committed)
                                    //   chưa commit -> max(0, end - start)
var isCommittedAheadOfEnd: Bool     // committed > end (topic bị tạo lại) -> UI cảnh báo
```

UI hiển thị chữ "no offset" khi `!hasCommittedOffset` (kèm lag = end − start, ghi chú rõ), và cảnh báo khi `isCommittedAheadOfEnd`.

## Edge case

- Group `Empty` nhưng còn offset commit → vẫn hiện lag.
- Group chỉ commit một số partition → các partition còn lại "no offset".
- Offset commit > end offset (topic bị xoá/tạo lại) → lag âm bị kẹp về 0 và cảnh báo.
- Không có quyền describe group → trạng thái "no access".
- Cluster có hàng trăm group → lấy lag theo yêu cầu (lazy), có giới hạn đồng thời.

## Tiêu chí hoàn thành

- Lag khớp `kafka-consumer-groups.sh --describe` trên Kafka local với một consumer đang chạy chậm.
- Danh sách 500 group không chặn UI.

## Test

- Tính lag (kể cả no offset, âm).
- Màu theo ngưỡng.
- Nhóm theo topic và tổng.

## Làm sau (P2)

Biểu đồ lag theo thời gian. (Reset offset đã làm ở T70, xem bên dưới; xoá group: chưa làm, theo quyết định của người dùng.)

## Reset offset (T70)

Đổi offset đã commit của một group. Chỉ làm được khi group **không còn thành viên** (`Empty` hoặc `Dead`); group đang chạy bị từ chối với thông báo "Stop the group's consumers first; offsets can only be changed while the group has no active members."

**Lối vào:** nút/menu "Reset Offsets…" ở header chi tiết group và menu chuột phải trong danh sách, với các preset *Skip to Latest (clear lag)*, *Rewind to Earliest*, *To Time…*, *To Offset…*. Tắt (có tooltip lý do) khi cluster read-only, group đang có thành viên, hoặc chưa tải xong.

**Đích (`OffsetResetTarget`):** latest = log end; earliest = log start; timestamp = offset đầu tiên có thời gian ≥ mốc (không có message nào → log end); offset = số cụ thể, bị kẹp vào `[start, end]` và đánh dấu *Clamped*. **Phạm vi (`OffsetResetScope`):** cả group, một số topic, hoặc một topic + partition.

**Preview:** bảng Topic · Partition · Current · New · Lag trước→sau · Changed/Unchanged/Clamped, tổng "Lag X → Y across N partitions". Tính bằng `OffsetResetPlanner` (hàm thuần); chỉ đọc, không ghi gì lên broker. Partition chưa từng commit coi như đổi (sẽ có commit mới).

**Áp dụng:** qua `DestructiveConfirmation` (hiện tên cluster + môi trường, gõ lại group id). Core: `KafkaClient.resetOffsets(group:plan:) -> OffsetResetResult` là lệnh ghi nên `GuardedKafkaClient` chặn khi read-only (có trong bảng phân loại ghi). Live dùng `rd_kafka_AlterConsumerGroupOffsets`; lỗi từng partition được trả về trong `failures` (lỗi một phần), `isComplete` chỉ đúng khi không có lỗi. Broker từ chối vì group đang hoạt động (NON_EMPTY_GROUP, UNKNOWN_MEMBER_ID, ILLEGAL_GENERATION, GROUP_SUBSCRIBED_TO_TOPIC, REBALANCE_IN_PROGRESS) → `.clusterError(code: 68)` / `isNonEmptyGroup`. Sau khi áp dụng thành công, chi tiết group và danh sách được tải lại.

**Test:** planner (mọi đích, kẹp, chưa commit, lọc phạm vi, tính lag, bão hoà), Guarded, Fake, view model, ánh xạ lỗi, và itest trên broker thật chỉ với topic `km-itest-*` / group `km-itest-g-*` do test tự tạo (helper `requireTestOwnedGroup` từ chối mọi group khác).
