# AGENTS.md

Native macOS Kafka manager (Swift 6 / SwiftUI, SwiftPM). Every entry answers: "Would an agent miss this without reading multiple files?"

## Layout

- `Sources/KafkaManagerCore/` — UI-free domain models + Kafka client layer. Never import SwiftUI/AppKit here.
- `Sources/KafkaManagerApp/` — SwiftUI `@main` app; depends on Core only.
- `Tests/KafkaManagerCoreTests/` — Swift Testing (`import Testing`), not XCTest.
- `docs/` — `product.md` (tính năng/MVP), `architecture.md` (tầng + quyết định mở), `development.md` (setup/quy ước).

## Review standard (mandatory)

Every task must satisfy `docs/review-standard.md` before it moves to `review`: run the automated gates (0 warnings, tests pass), self-check sections A–J, and submit the report template in section 4. Spec > review standard > architecture > this file; on conflict, stop and ask the BA.

## Commands

```sh
scripts/dev.sh build | test | run
```

## Conventions

- Swift 6 strict concurrency: models are `Sendable`; all Kafka I/O goes through actors, never on the main actor.
- View models are `@Observable` + `@MainActor`; they call Core services only.
- macOS 14+. SwiftUI first; AppKit (`NSViewRepresentable`) only for things SwiftUI can't do, e.g. large message tables.
- Credentials (SASL/SSL) go in the Keychain — never in saved connection JSON or UserDefaults.

## Known gaps

- No Kafka client yet (see open decisions in `docs/architecture.md`).
- SwiftPM executable cannot carry entitlements/signing; an Xcode project or XcodeGen is needed before distribution.
