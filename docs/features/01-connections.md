# 01 · Connection profiles

## Mục tiêu

Lưu nhiều cluster, kết nối an toàn, secret nằm trong Keychain.

## Hành vi / UI

- Sidebar liệt kê cluster; nút `+` mở form thêm/sửa.
- Form: **Name**, **Bootstrap servers** (`host:port,host:port`), **Environment** (Local / Staging / Production → màu + nhãn), **Security** (Plaintext, SSL, SASL_PLAINTEXT, SASL_SSL), **SASL mechanism** (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512), username/password, tuỳ chọn CA cert.
- Nút **Test connection**: hiện spinner, rồi báo "OK, N brokers" hoặc `KafkaError.userMessage`. Timeout cố định (vd. 10s).
- Chọn cluster → kết nối lười (khi mở topic/group đầu tiên), hiện trạng thái (connecting / connected / error + retry) ở sidebar.
- Sửa, nhân bản, xoá profile (xoá có xác nhận; xoá luôn secret trong Keychain).

## Lưu trữ

- Profile (không chứa secret): JSON trong `Application Support/KafkaManager/connections.json`, qua `ConnectionStore` (actor, ghi atomic).
- Secret: Keychain (`kSecClassGenericPassword`, service `KafkaManager`, account = profile id). `SecretStore` protocol để test bằng bản in-memory.
- `ClusterConnection` mở rộng: `environment`, `security`, `saslMechanism`, `username` (không có password).

## Core API

```swift
protocol ConnectionStore { func load() async throws -> [ClusterConnection]; func save(_:) async throws }
protocol SecretStore { func password(for id: UUID) throws -> String?; func setPassword(_:for:) throws; func delete(for:) throws }
```

## Edge case

- Bootstrap servers sai định dạng → validate ngay trong form, không cho Save.
- Tên trùng → cho phép nhưng cảnh báo.
- File JSON hỏng → không crash; báo lỗi, giữ bản backup `.bak`.
- Keychain bị từ chối → báo rõ, không lưu password.
- Cert không hợp lệ/hết hạn → lỗi `authenticationFailed` kèm gợi ý.

## Tiêu chí hoàn thành

- Thêm, sửa, xoá profile; khởi động lại app vẫn còn.
- Password không xuất hiện trong `connections.json`, log hay UserDefaults.
- Test connection cho 3 kết quả: OK, unreachable, auth failed (với Fake và với broker local thật).

## Test

- `ConnectionStore` round-trip, file hỏng, ghi atomic.
- Validate bootstrap servers (bảng input hợp lệ/không).
- `SecretStore` in-memory; xoá profile xoá secret.
