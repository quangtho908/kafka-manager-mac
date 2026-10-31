import Foundation
import KafkaManagerCore
#if LIVE_KAFKA
import KafkaManagerKafka
#endif

/// Opt-in checks of what a packaged build depends on, run instead of the UI: `KAFKAMANAGER_SELFTEST=keychain` or
/// `=kafka`. They print one line per step to stdout and exit with 0 (all passed) or 1. Nothing else in the app reads
/// this variable. The Keychain check uses a throwaway service name, so it can never see or touch a real profile's item;
/// the Kafka check only connects and reads metadata.
enum SelfTest {
    static var requested: String? {
        let value = ProcessInfo.processInfo.environment["KAFKAMANAGER_SELFTEST"] ?? ""
        return value.isEmpty ? nil : value
    }

    static func run(_ name: String) async -> Int32 {
        switch name {
        case "keychain": return await keychain()
        case "kafka": return await kafka()
        default:
            say("unknown self-test '\(name)'; use keychain or kafka")
            return 1
        }
    }

    private static func say(_ line: String) {
        FileHandle.standardOutput.write(Data((line + "\n").utf8))
    }

    private static func keychain() async -> Int32 {
        let service = "km-selftest-\(UUID().uuidString)"
        let store = KeychainSecretStore(service: service)
        let id = UUID()
        let secret = "selftest-\(UUID().uuidString)"
        var failed = false
        func step(_ title: String, _ passed: Bool) {
            say("\(passed ? "PASS" : "FAIL") \(title)")
            if !passed { failed = true }
        }
        say("service \(service)")
        do {
            try await store.setPassword(secret, for: id)
            step("save", true)
            step("read back", try await store.password(for: id) == secret)
            try await store.setPassword(secret + "-2", for: id)
            step("replace", try await store.password(for: id) == secret + "-2")
        } catch {
            step("save/read/replace (\(error))", false)
        }
        do {
            try await store.deletePassword(for: id)
            step("delete", try await store.password(for: id) == nil)
        } catch {
            step("delete (\(error))", false)
        }
        return failed ? 1 : 0
    }

    private static func kafka() async -> Int32 {
        #if LIVE_KAFKA
        let bootstrap = ProcessInfo.processInfo.environment["KAFKAMANAGER_SELFTEST_BOOTSTRAP"] ?? "localhost:9092"
        let client = LiveKafkaClient()
        defer { Task { await client.disconnect() } }
        do {
            try await client.connect(ClusterConnection(name: "selftest", bootstrapServers: bootstrap), credentials: nil)
            let brokers = try await client.describeCluster().brokerCount
            let topics = try await client.listTopics().count
            say("PASS connect \(bootstrap): \(brokers) broker(s), \(topics) topic(s)")
            await client.disconnect()
            return 0
        } catch let error as KafkaError {
            say("FAIL connect \(bootstrap): \(error.userMessage)")
        } catch {
            say("FAIL connect \(bootstrap): \(error)")
        }
        return 1
        #else
        say("FAIL this build has no live Kafka client")
        return 1
        #endif
    }
}
