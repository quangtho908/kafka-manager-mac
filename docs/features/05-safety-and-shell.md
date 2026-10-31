# 05 · An toàn khi thao tác + khung UX

## Mục tiêu

Không làm hỏng cluster production vì lỡ tay; app có khung điều hướng rõ ràng.

## An toàn

- `Environment` của cluster quyết định chế độ: **Production mặc định read-only**; Local/Staging ghi được. Người dùng đổi được trong profile.
- Read-only: các thao tác ghi bị `GuardedKafkaClient` từ chối bằng `KafkaError.readOnly(clusterName:)` (đã có, T03); các nút tạo/xoá bị vô hiệu và có tooltip lý do; `KafkaClient` wrapper (`GuardedKafkaClient`) từ chối thao tác ghi ở tầng Core để UI lỗi cũng không lọt.
- **Thao tác phá huỷ** (xoá topic, về sau reset offset/xoá group): hộp thoại nêu rõ cluster + tên đối tượng, bắt gõ lại tên, nút hành động màu đỏ. Cluster production hiện banner đỏ trong hộp thoại.
- Banner màu cố định trên cửa sổ theo environment (xanh Local, vàng Staging, đỏ Production) để luôn biết đang ở cluster nào.

## Khung ứng dụng

- `NavigationSplitView` 3 cột: Clusters → (Topics | Consumer Groups) → chi tiết.
- Dark mode theo hệ thống; hỗ trợ VoiceOver cho các điều khiển chính.
- Menu: File › New Connection (⌘N), Refresh (⌘R), Find (⌘F).
- Trạng thái rỗng, lỗi, đang tải thống nhất (component dùng chung `StateView`).
- Lỗi hiển thị bằng `KafkaError.userMessage`, có nút Retry khi hợp lý; chi tiết kỹ thuật nằm trong phần "Details" thu gọn.

## Edge case

- Đổi environment sang Production khi đang mở → áp read-only ngay.
- Mất kết nối → banner "Disconnected" kèm Retry, không mất state đang xem.

## Tiêu chí hoàn thành

- Với cluster Production, mọi thao tác ghi bị chặn cả ở UI lẫn Core.
- Không thể xoá topic mà không gõ đúng tên.

## Test

- `GuardedKafkaClient` chặn ghi khi read-only (từng method ghi).
- Validate chuỗi xác nhận (khớp chính xác, phân biệt hoa thường).

## Làm sau (P2)

Nhật ký thao tác, ⌘K, nhiều cửa sổ, khôi phục tab.
