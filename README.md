# Kafka Manager

Ứng dụng macOS native (Swift/SwiftUI) để quản lý Apache Kafka: cluster, topic, partition, consumer group, produce/consume message và config.

> Trạng thái: skeleton. Chưa có Kafka client; UI mới là khung `NavigationSplitView`.

## Yêu cầu

- macOS 14+ (xem `docs/development.md` về việc nâng lên macOS 26)
- Xcode / Swift 6 toolchain

## Chạy nhanh

```sh
scripts/dev.sh build   # biên dịch
scripts/dev.sh test    # chạy test
scripts/dev.sh run     # mở app
```

## Tài liệu

| File | Nội dung |
|---|---|
| [docs/product.md](docs/product.md) | Tính năng, phạm vi MVP, roadmap |
| [docs/features/](docs/features/README.md) | Spec chi tiết từng tính năng MVP |
| [docs/architecture.md](docs/architecture.md) | Các tầng, quyết định kiến trúc còn mở |
| [docs/development.md](docs/development.md) | Setup, cấu trúc code, quy ước, test, Kafka local |
| [docs/performance-budget.md](docs/performance-budget.md) | Ngân sách khởi động / RAM / CPU |
| [docs/review-standard.md](docs/review-standard.md) | Tiêu chuẩn review bắt buộc cho mọi task |
| [AGENTS.md](AGENTS.md) | Bản đồ repo cho coding agent |
