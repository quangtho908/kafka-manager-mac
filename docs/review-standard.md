# Review standard

Tiêu chuẩn **bắt buộc** cho mọi task. Coder tự kiểm theo đây trước khi báo `review`; BA review theo đúng danh sách này, không thêm tiêu chí ngoài (muốn thêm → sửa file này trước, rồi áp dụng cho các task sau).

Nguồn sự thật theo thứ tự ưu tiên: **spec task** (`docs/features/`) > file này > `architecture.md` > `AGENTS.md`. Mâu thuẫn → dừng và hỏi BA.

## 1. Mức độ lỗi

| Mức | Nghĩa | Xử lý |
|---|---|---|
| **BLOCKER** | Sai spec, mất/lộ dữ liệu, lộ secret, vi phạm kiến trúc, crash, build/test đỏ, thiếu test cho logic | Không nghiệm thu. Sửa xong review lại toàn bộ task |
| **MAJOR** | Sai quy ước, xử lý lỗi thiếu, hiệu năng kém rõ rệt, test yếu, race tiềm ẩn | Phải sửa trước khi nghiệm thu |
| **MINOR** | Đặt tên, comment, tối ưu nhỏ | Sửa nếu tiện; không chặn |
| **NIT** | Sở thích cá nhân | Không bắt buộc, không tính |

Nghiệm thu khi: **0 BLOCKER, 0 MAJOR**.

## 2. Cổng tự động (coder chạy, đính kèm kết quả)

1. `scripts/dev.sh build` — 0 error, **0 warning** (kể cả concurrency).
2. `scripts/dev.sh test` — toàn bộ pass.
3. Không còn `TODO`/`FIXME`/`print(`/`dump(` không có ticket trong code mới.
4. `git diff --stat` chỉ gồm file thuộc phạm vi task.

Cổng đỏ → không được chuyển `review`.

## 3. Checklist review

### A. Đúng spec (BLOCKER nếu sai)
- [ ] Mọi hành vi trong mục "Hành vi / UI" của spec đều có.
- [ ] Từng "Edge case" của spec được xử lý và có test hoặc ghi chú vì sao không.
- [ ] Từng "Tiêu chí hoàn thành" đạt, coder nêu cách đã verify.
- [ ] Không làm thêm tính năng ngoài task (scope creep). Phát hiện việc thiếu → báo BA, không tự thêm.

### B. Kiến trúc (BLOCKER nếu sai)
- [ ] `KafkaManagerCore` **không** import SwiftUI/AppKit/Kafka library cụ thể.
- [ ] View không gọi `KafkaClient`/store trực tiếp; đi qua view model.
- [ ] View model chỉ phụ thuộc protocol, nhận dependency qua init (không singleton, không `static shared`).
- [ ] Kiểu của thư viện ngoài không rò ra ngoài adapter.
- [ ] Thao tác ghi đi qua `GuardedKafkaClient`; không có đường vòng.

### C. Concurrency (BLOCKER/MAJOR)
- [ ] Swift 6 strict, không `@unchecked Sendable` / `nonisolated(unsafe)` trừ khi có comment giải thích và BA đồng ý.
- [ ] Model `Sendable`; state mutable chia sẻ nằm trong actor hoặc `@MainActor`.
- [ ] Không chặn main thread; không `DispatchQueue.main.sync`, không `Thread.sleep`, không `Task.sleep` để "chờ cho đúng".
- [ ] `Task` sở hữu bởi view model và **được huỷ** khi view biến mất / Stop; `AsyncStream` có `onTermination` dọn tài nguyên.
- [ ] Mọi `await` I/O có timeout.
- [ ] Không `Task.detached` / `DispatchQueue.global` nếu không có lý do ghi trong comment.

### D. Lỗi và độ bền
- [ ] Không `try!`, `as!`, `!` (force unwrap), `fatalError` ngoài test/preview.
- [ ] Mọi lỗi từ adapter map sang `KafkaError`; UI chỉ hiện `userMessage`.
- [ ] Không `catch {}` nuốt lỗi; không `try?` làm mất thông tin quan trọng (ghi rõ nếu cố ý).
- [ ] Lỗi một phần không phá toàn bộ danh sách.
- [ ] Input ngoài (file JSON, message, config) được validate, dữ liệu hỏng không làm crash.

### E. Bảo mật (BLOCKER nếu sai)
- [ ] Password/secret chỉ trong Keychain; **không** có trong JSON, UserDefaults, log, error message, preview, test fixture thật.
- [ ] Không log nội dung message hoặc key (dữ liệu khách hàng) ở mức mặc định.
- [ ] Thao tác phá huỷ có xác nhận gõ lại tên; production read-only ở cả UI và Core.
- [ ] Không thêm dependency mới khi chưa được BA duyệt.

### F. Hiệu năng
- [ ] Danh sách lớn dùng `List`/`Table` ảo hoá, không `VStack` + `ForEach` toàn bộ.
- [ ] Stream message có giới hạn buffer và cập nhật UI theo batch.
- [ ] Không tính toán nặng / decode / pretty print đồng bộ trong `body` hoặc trên main thread.
- [ ] Observation hợp lý: không để một thay đổi nhỏ làm render lại cả cây (xem skill `swiftui-macos`).

### G. Test (BLOCKER nếu logic không có test)
- [ ] Mỗi hàm logic (lọc, sắp xếp, tính lag, decode, validate, map lỗi) có test, gồm trường hợp biên.
- [ ] View model có test với `FakeKafkaClient`: thành công, lỗi, huỷ giữa chừng.
- [ ] Dùng Swift Testing (`@Test`, `#expect`), tên test mô tả hành vi.
- [ ] Test xác định, không phụ thuộc thời gian thực / mạng / thứ tự chạy; không `sleep`.
- [ ] Test thật sự fail khi code sai (không `#expect(true)`, không assert tautology).

### H. Code style
- [ ] Theo Swift API Design Guidelines; tên rõ nghĩa, không viết tắt khó hiểu.
- [ ] Kiểu: `UpperCamelCase`; hàm/biến: `lowerCamelCase`; bool đọc như câu (`isLoading`, `hasMore`).
- [ ] Access control tối thiểu: mặc định `internal`/`private`; `public` chỉ API thật sự của Core cần cho App.
- [ ] `struct`/`enum` trước `class`; `final` cho class; `let` trước `var`.
- [ ] Một kiểu chính mỗi file, tên file = tên kiểu; file > 400 dòng hoặc hàm > 50 dòng → tách.
- [ ] Không comment lặp lại code; chỉ comment **vì sao** khi không hiển nhiên. Không code bị comment sẵn.
- [ ] Không magic number/string lặp lại → hằng số có tên.
- [ ] Không có code chết, import thừa, biến không dùng.
- [ ] Giữ đúng phong cách các file xung quanh (xem file lân cận trước khi viết).
- [ ] Chuỗi hiển thị cho người dùng: dùng `String(localized:)`; mặc định tiếng Anh, tiếng Việt thêm sau (chốt với BA).

### I. SwiftUI
- [ ] `@Observable` + `@MainActor` cho view model; `@State` sở hữu ở view gốc tính năng.
- [ ] View nhỏ, ghép được; `body` không chứa logic nghiệp vụ.
- [ ] Mọi màn tải dữ liệu dùng `StateView` (idle/loading/loaded/failed + Retry).
- [ ] Có `#Preview` dùng `FakeKafkaClient` cho view chính.
- [ ] Accessibility: control có label, điều hướng được bằng bàn phím, không dựa vào màu duy nhất để truyền thông tin.
- [ ] Dark mode hiển thị đúng; không hard-code màu (dùng semantic color / asset).

### J. Docs và repo
- [ ] `AGENTS.md`/`docs/` cập nhật khi cấu trúc, lệnh hay quyết định đổi.
- [ ] Commit nhỏ, đúng task, message dạng `T13: add connection form view model`.
- [ ] Không commit file sinh ra (`.build/`, `xcuserdata/`), secret, file lớn.

## 4. Báo cáo của coder (bắt buộc, dán vào khi chuyển `review`)

```md
## T<ID> — <tên task>
**Trạng thái:** review
**Nhánh/commit:** task/T13-... @ <sha>

### Đã làm
- ...

### Cổng tự động
- build: 0 error / 0 warning
- test: <N> pass, <M> test mới
- diff: <số file>, đều trong phạm vi

### Đối chiếu spec
| Tiêu chí hoàn thành | Cách verify | Kết quả |
|---|---|---|

### Edge case đã xử lý
- ...

### Không làm / để lại
- ... (kèm lý do)

### Câu hỏi cho BA
- ...
```

Thiếu báo cáo hoặc thiếu kết quả cổng tự động → BA trả lại, chưa review.

## 5. Kết quả review của BA

```md
## Review T<ID> — <APPROVED | CHANGES REQUESTED>
Cổng tự động: đạt/không đạt (BA chạy lại độc lập)

| # | Mức | File:dòng | Vấn đề | Cách sửa |
|---|---|---|---|---|

Tổng: B BLOCKER · M MAJOR · m MINOR
```

- Mỗi lỗi phải trích **`file:dòng`** và tham chiếu mục checklist (vd. "C: Task không được huỷ").
- BA chạy lại `build` và `test` độc lập, không tin kết quả báo cáo.
- Sửa xong, coder trả lời từng dòng (fixed / không đồng ý + lý do); BA review lại các dòng đó và vùng bị ảnh hưởng.
- Tranh chấp tiêu chí → BA quyết định; nếu là chỗ tiêu chuẩn mơ hồ, BA sửa file này.

## 6. Quy tắc thép

1. Không nghiệm thu khi cổng tự động đỏ.
2. Không nghiệm thu logic không có test.
3. Không tự mở rộng phạm vi; không tự đổi spec.
4. Không thêm dependency, không nới `Sendable`/force unwrap để "cho chạy được".
5. Không để secret xuất hiện ở bất kỳ đâu ngoài Keychain.
