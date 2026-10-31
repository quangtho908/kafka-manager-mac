# AGENTS.md

Native macOS Kafka manager (Swift 6 / SwiftUI, SwiftPM). Every entry answers: "Would an agent miss this without reading multiple files?"

## Layout

- `Sources/KafkaManagerCore/` — UI-free domain models + Kafka client layer. Never import SwiftUI/AppKit here.
- `Sources/KafkaManagerUI/` — `@Observable` view models and shared UI state (`LoadState`); depends on Core.
- `Sources/KafkaManagerApp/` — SwiftUI `@main` app; depends on UI and Core.
- `Tests/KafkaManagerCoreTests/`, `Tests/KafkaManagerUITests/` — Swift Testing (`import Testing`), not XCTest.
- `docs/` — `product.md` (tính năng/MVP), `architecture.md` (tầng + quyết định mở), `development.md` (setup/quy ước).

## Review standard (mandatory)

Every task must satisfy `docs/review-standard.md` before it moves to `review`: run the automated gates (0 warnings, tests pass), self-check sections A–J, and submit the report template in section 4. Spec > review standard > architecture > this file; on conflict, stop and ask the BA.

## UI work

Any task touching SwiftUI/view models MUST follow the `swiftui-macos` skill (`.claude/skills/swiftui-macos/`): load it before coding, read the references matching the task (table in review-standard section I), and list them in the report. The BA reviews UI against the same references.

## Commands

```sh
scripts/dev.sh build | test | run
```

## Conventions

- Swift 6 strict concurrency: models are `Sendable`; all Kafka I/O goes through actors, never on the main actor.
- View models are `@Observable` + `@MainActor`; they call Core services only.
- **Deployment target macOS 14** (support older Macs). No API newer than 14 without `#available` + a working fallback; see `docs/development.md`. SwiftUI first; AppKit (`NSViewRepresentable`) only for things SwiftUI can't do, e.g. large message tables.
- Credentials (SASL/SSL) go in the Keychain — never in saved connection JSON or UserDefaults.

## Known gaps

- No Kafka client yet (see open decisions in `docs/architecture.md`).
- SwiftPM executable cannot carry entitlements/signing; an Xcode project or XcodeGen is needed before distribution.
