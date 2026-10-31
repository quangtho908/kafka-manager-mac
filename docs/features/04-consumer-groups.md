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

Reset offset (earliest/latest/timestamp/offset) có preview và xác nhận; xoá group; biểu đồ lag theo thời gian.
