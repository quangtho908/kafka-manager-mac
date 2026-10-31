# Kafka Manager

A native macOS app (Swift, SwiftUI) for working with Apache Kafka: browse clusters, topics, messages and consumer
groups, produce messages, and reset consumer group offsets. It talks to brokers through a statically linked
librdkafka, so there is nothing to install next to the app.

The app icon source is `resources/AppIcon-1024.png`.

## Features

- **Connections.** Saved cluster profiles (plaintext, TLS and SASL) with an environment badge (local, staging,
  production). Passwords are stored in the macOS Keychain.
- **Topics.** List, search and sort; topic detail (partitions with offsets, configuration); create and delete topics.
- **Messages.** Browse a topic from the latest N messages, the earliest, an offset or a timestamp, for one partition or
  all. View the key, value and headers as JSON, text or hex; filter and sort the loaded rows; copy a key, a value or the
  whole message.
- **Produce.** Send a message with a key, value (text, JSON or hex), headers, a tombstone (null value), an explicit
  partition and timestamp, optionally repeated N times. "Produce Again" prefills the form from an existing message.
- **Consumer groups.** State, lag per partition and members; reset offsets to earliest, latest, a
  timestamp or an offset, with a preview before anything is written.
- **Safety.** Production clusters open read-only and turning that off needs a confirmation. Deleting a topic,
  resetting offsets and producing on a production cluster ask you to retype the object name.

## Requirements

- To run: macOS 14 or later.
- To build: Xcode or a Swift 6 toolchain.

## Quick start

```sh
scripts/dev.sh deps    # build the vendored static librdkafka + OpenSSL once (git-ignored vendor/)
scripts/dev.sh build   # compile
scripts/dev.sh test    # unit tests
scripts/dev.sh run     # launch the app
scripts/dev.sh itest   # integration tests against a real broker (see below)
```

Without `vendor/`, `build`, `test` and `run` still work but without the live Kafka client (demo data only).

`scripts/dev.sh deps` runs `scripts/build-deps.sh`, which builds arm64 by default; set `ARCHS="arm64 x86_64"` for a
universal build (the x86_64 slice needs Rosetta already installed) and `FORCE=1` to rebuild. All commands pass
`--manifest-cache none` to SwiftPM, because the package manifest depends on whether `vendor/` exists.

### Environment variables

| Variable | Effect |
|---|---|
| `KAFKAMANAGER_DEMO=1` | Every profile talks to a built-in fake cluster instead of a broker. |
| `KAFKAMANAGER_DEMO_TOPICS=N` | Number of demo topics (default 200, up to 20,000). |
| `KAFKAMANAGER_DEMO_MESSAGES=N` | Demo messages per partition (default 30, up to 5,000). |
| `KAFKAMANAGER_DATABASE=/path/file.sqlite` | Use another profile database, so manual testing never touches real profiles. |

### Integration tests

`scripts/dev.sh itest` runs `KafkaManagerKafkaTests` with `KM_ITEST=1` against `localhost:9092` (plaintext);
`KM_ITEST_BOOTSTRAP=host:port` points it elsewhere. The tests only create and delete topics named `km-itest-*` and
groups named `km-itest-g-*`, and refuse to produce to any other topic.

## Packaging

```sh
ARCHS=arm64 scripts/package.sh   # builds dist/KafkaManager.app, ad-hoc signed with the hardened runtime
```

`ARCHS="arm64 x86_64"` makes a universal binary (needs `vendor/` built for both). Optional `BUNDLE_ID` (default
`com.thtsolution.kafkamanager`) and `ICON_PNG` override the bundle identifier and the 1024 px icon. The version comes from `VERSION`. An ad-hoc signature is rejected by
Gatekeeper on other Macs. `scripts/sign-and-notarize.sh` does the Developer ID signing and notarization; it needs the
owner's Apple account (`DEVELOPER_ID_APPLICATION`, `NOTARY_PROFILE`) and is never run by the development
tooling. `DRY_RUN=1` prints its commands without running them.

## Security

- Passwords live only in the macOS Keychain, never in the profile database, logs or error messages.
- Profiles are stored in SQLite at `~/Library/Application Support/KafkaManager/kafkamanager.sqlite`.
- Production clusters are read-only by default; writes need an explicit switch, and destructive actions a retyped name.
- Fetching messages uses a consumer that only assigns partitions and never commits, so browsing cannot change a
  group's offsets.

## Architecture

- `KafkaManagerCore`: domain models, the `KafkaClient` protocol, a fake client, storage and payload decoding. No UI.
- `KafkaManagerKafka`: the live client over librdkafka (static, vendored). Built only when `vendor/` exists.
- `KafkaManagerUI`: SwiftUI views and observable view models.
- `KafkaManagerApp`: the executable that wires the pieces together.

## Documentation

The documents below are in Vietnamese.

| File | Contents |
|---|---|
| [docs/product.md](docs/product.md) | Features, MVP scope, roadmap (Vietnamese) |
| [docs/features/](docs/features/README.md) | Detailed spec per feature (Vietnamese) |
| [docs/architecture.md](docs/architecture.md) | Layers and architecture decisions (Vietnamese) |
| [docs/development.md](docs/development.md) | Setup, code layout, conventions, tests, local Kafka (Vietnamese) |
| [docs/performance-budget.md](docs/performance-budget.md) | Startup, memory and CPU budget (Vietnamese) |
| [docs/review-standard.md](docs/review-standard.md) | Review standard for every task (Vietnamese) |
| [AGENTS.md](AGENTS.md) | Repo map for coding agents |
