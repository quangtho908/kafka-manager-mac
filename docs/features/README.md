# Feature specs

Spec chi tiết để làm từng tính năng. Danh sách và giai đoạn: [../product.md](../product.md).

Mỗi spec gồm: **Mục tiêu · Hành vi / UI · Core API · Edge case · Tiêu chí hoàn thành · Test**.

| # | Spec | Giai đoạn | Phụ thuộc |
|---|---|---|---|
| 00 | [Core client protocol](00-core-client.md) | MVP | — |
| 01 | [Connection profiles](01-connections.md) | MVP | 00 |
| 02 | [Topics](02-topics.md) | MVP | 00, 01 |
| 03 | [Xem message](03-messages.md) | MVP | 00, 01, 02 |
| 04 | [Consumer groups](04-consumer-groups.md) | MVP | 00, 01 |
| 05 | [An toàn khi thao tác + UX nền](05-safety-and-shell.md) | MVP | 01 |

Thứ tự làm: 00 → 01 → 02 → 03 → 04; 05 làm song song từ 01.

Quy ước chung: Core không import UI; mọi lỗi từ Kafka được map sang `KafkaError` (xem 00); view model `@Observable @MainActor`; test bằng Swift Testing.
