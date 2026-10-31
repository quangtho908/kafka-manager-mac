import Testing
import Foundation
@testable import KafkaManagerCore

private func label(_ count: Int) -> String { String(repeating: "a", count: count) }

private struct ValidCase: CustomTestStringConvertible, Sendable {
    let input: String
    let normalized: String
    var testDescription: String { "\(input.debugDescription) → \(normalized)" }
}

private struct InvalidCase: CustomTestStringConvertible, Sendable {
    let input: String
    let fragment: String
    var testDescription: String { input.isEmpty ? "(empty)" : String(input.prefix(40)).debugDescription }
}

private func failure(_ text: String) -> [ValidationIssue]? {
    if case .failure(let error) = BootstrapServers.parse(text) { return error.issues }
    return nil
}

private func success(_ text: String) -> BootstrapServers? {
    if case .success(let servers) = BootstrapServers.parse(text) { return servers }
    return nil
}

// MARK: BootstrapServers — valid

private let validCases: [ValidCase] = [
    ValidCase(input: "localhost:9092", normalized: "localhost:9092"),
    ValidCase(input: "localhost", normalized: "localhost:9092"),
    ValidCase(input: "  localhost:9092  ", normalized: "localhost:9092"),
    ValidCase(input: " a:1 , b:2 ,c ", normalized: "a:1,b:2,c:9092"),
    ValidCase(input: "a:9092,b:9093,c", normalized: "a:9092,b:9093,c:9092"),
    ValidCase(input: "a:9092,\n b:9093", normalized: "a:9092,b:9093"),
    ValidCase(input: "10.0.0.1", normalized: "10.0.0.1:9092"),
    ValidCase(input: "192.168.1.10:9093", normalized: "192.168.1.10:9093"),
    ValidCase(input: "0.0.0.0:1", normalized: "0.0.0.0:1"),
    ValidCase(input: "255.255.255.255:65535", normalized: "255.255.255.255:65535"),
    ValidCase(input: "[::1]:9092", normalized: "[::1]:9092"),
    ValidCase(input: "[::1]", normalized: "[::1]:9092"),
    ValidCase(input: "[2001:db8::1]:19092", normalized: "[2001:db8::1]:19092"),
    ValidCase(input: "[::ffff:192.0.2.1]:9092", normalized: "[::ffff:192.0.2.1]:9092"),
    ValidCase(input: "kafka_1.my-corp.example.com:9094", normalized: "kafka_1.my-corp.example.com:9094"),
    ValidCase(input: "broker-01", normalized: "broker-01:9092"),
    ValidCase(input: "1abc.example", normalized: "1abc.example:9092"),
    ValidCase(input: "Broker.Example.COM:9092", normalized: "Broker.Example.COM:9092"),
    ValidCase(input: "h:1", normalized: "h:1"),
    ValidCase(input: "h:65535", normalized: "h:65535"),
    ValidCase(input: "\(label(63)).example", normalized: "\(label(63)).example:9092"),
    ValidCase(input: [label(63), label(63), label(63), label(61)].joined(separator: "."), normalized: "\([label(63), label(63), label(63), label(61)].joined(separator: ".")):9092"),
    ValidCase(input: (1...20).map { "h\($0)" }.joined(separator: ","), normalized: (1...20).map { "h\($0):9092" }.joined(separator: ",")),
]

@Test(arguments: validCases)
private func validBootstrapListsNormalize(_ testCase: ValidCase) {
    let servers = success(testCase.input)
    #expect(servers?.normalized == testCase.normalized)
    #expect(servers?.warnings.isEmpty == true)
}

@Test func entriesExposeHostAndPort() throws {
    let servers = try #require(success("a.example:1234, [::1]:5678, 10.0.0.1"))
    #expect(servers.entries.map(\.host) == ["a.example", "::1", "10.0.0.1"])
    #expect(servers.entries.map(\.port) == [1234, 5678, 9092])
}

// MARK: BootstrapServers — invalid

private let invalidCases: [InvalidCase] = [
    InvalidCase(input: "", fragment: "at least one"),
    InvalidCase(input: "   \n\t ", fragment: "at least one"),
    InvalidCase(input: ",,", fragment: "is empty"),
    InvalidCase(input: "a:9092,", fragment: "is empty"),
    InvalidCase(input: "a:b", fragment: "port"),
    InvalidCase(input: "host:0", fragment: "port"),
    InvalidCase(input: "host:65536", fragment: "port"),
    InvalidCase(input: "host:-1", fragment: "port"),
    InvalidCase(input: "host:+9092", fragment: "port"),
    InvalidCase(input: "host:12x", fragment: "port"),
    InvalidCase(input: "host:", fragment: "port"),
    InvalidCase(input: "host:٩٠٩٢", fragment: "port"),
    InvalidCase(input: "host:99999999999999999999", fragment: "port"),
    InvalidCase(input: "[::1]:", fragment: "port"),
    InvalidCase(input: ":9092", fragment: "missing a host"),
    InvalidCase(input: "PLAINTEXT://host:9092", fragment: "protocol"),
    InvalidCase(input: "http://h", fragment: "protocol"),
    InvalidCase(input: "://", fragment: "protocol"),
    InvalidCase(input: "user@host:9092", fragment: "@"),
    InvalidCase(input: "host/path", fragment: "path"),
    InvalidCase(input: "host:9092/", fragment: "path"),
    InvalidCase(input: "host?x=1", fragment: "path"),
    InvalidCase(input: "a b:9092", fragment: "space"),
    InvalidCase(input: "a:9092 b:9093", fragment: "space"),
    InvalidCase(input: "[::1", fragment: "closing"),
    InvalidCase(input: "::1:9092", fragment: "brackets"),
    InvalidCase(input: "::1", fragment: "brackets"),
    InvalidCase(input: "[::1]x", fragment: "unexpected"),
    InvalidCase(input: "[zzz]:9092", fragment: "IPv6"),
    InvalidCase(input: "[]:9092", fragment: "IPv6"),
    InvalidCase(input: "[1.2.3.4]:9092", fragment: "IPv6"),
    InvalidCase(input: "999.1.1.1:9092", fragment: "IPv4"),
    InvalidCase(input: "256.0.0.1", fragment: "IPv4"),
    InvalidCase(input: "1.2.3", fragment: "IPv4"),
    InvalidCase(input: "1.2.3.4.5", fragment: "IPv4"),
    InvalidCase(input: "12345", fragment: "IPv4"),
    InvalidCase(input: "my.host.123", fragment: "IPv4"),
    InvalidCase(input: "\(label(64)).example", fragment: "longer than 63"),
    InvalidCase(input: [label(63), label(63), label(63), label(62)].joined(separator: "."), fragment: "longer than 253"),
    InvalidCase(input: "-a.example", fragment: "start or end"),
    InvalidCase(input: "a-.example", fragment: "start or end"),
    InvalidCase(input: "a.-b", fragment: "start or end"),
    InvalidCase(input: "a..b", fragment: "empty part"),
    InvalidCase(input: "a.b.", fragment: "empty part"),
    InvalidCase(input: ".a", fragment: "empty part"),
    InvalidCase(input: "ho$t", fragment: "invalid character"),
    InvalidCase(input: "hôst", fragment: "invalid character"),
    InvalidCase(input: (1...21).map { "h\($0)" }.joined(separator: ","), fragment: "Too many"),
]

@Test(arguments: invalidCases)
private func invalidBootstrapListsAreRejectedWithTheRightReason(_ testCase: InvalidCase) throws {
    let issues = try #require(failure(testCase.input), "expected rejection")
    #expect(issues.contains { $0.severity == .error })
    #expect(issues.allSatisfy { $0.field == .bootstrapServers })
    #expect(issues.contains { $0.message.contains(testCase.fragment) }, "\(issues.map(\.message))")
}

@Test func issuesNameTheOffendingEntry() throws {
    let issues = try #require(failure("good:9092, bad host:9092, other:99999"))
    #expect(issues.count == 2)
    #expect(issues[0].message.contains("bad host:9092"))
    #expect(issues[1].message.contains("other:99999"))
}

@Test func credentialsAreNeverEchoedInMessages() throws {
    let issues = try #require(failure("admin:hunter2@host:9092, http://admin:hunter2@h"))
    #expect(issues.count == 2)
    for issue in issues {
        #expect(!issue.message.contains("hunter2"))
        #expect(!issue.message.contains("admin"))
    }
}

// MARK: BootstrapServers — duplicates

@Test func duplicatesAreDroppedCaseInsensitivelyWithAWarning() throws {
    let servers = try #require(success("Broker:9092,broker:9092,other:9092"))
    #expect(servers.normalized == "Broker:9092,other:9092")
    #expect(servers.warnings.count == 1)
    #expect(servers.warnings[0].severity == .warning)
    #expect(servers.warnings[0].field == .bootstrapServers)
    #expect(servers.warnings[0].message.contains("broker:9092"))
}

@Test func defaultPortCountsAsTheSamePort() throws {
    let servers = try #require(success("a, a:9092, a:9093"))
    #expect(servers.normalized == "a:9092,a:9093")
    #expect(servers.warnings.count == 1)
}

@Test func sameHostDifferentPortIsNotADuplicate() throws {
    let servers = try #require(success("a:1,a:2"))
    #expect(servers.entries.count == 2)
    #expect(servers.warnings.isEmpty)
}

@Test func warningsAndErrorsComeBackTogetherInEntryOrder() throws {
    let issues = try #require(failure("a,a,,b"))
    #expect(issues.map(\.severity) == [.warning, .error])
    #expect(issues[0].message.contains("\"a\""))
    #expect(issues[1].message.contains("Address 3"))
}

// MARK: ConnectionValidator — name

private func validate(
    name: String = "prod",
    servers: String = "localhost:9092",
    security: ClusterConnection.Security = .plaintext,
    mechanism: ClusterConnection.SASLMechanism? = nil,
    username: String? = nil,
    existing: [String] = []
) -> [ValidationIssue] {
    ConnectionValidator.validate(name: name, bootstrapServers: servers, security: security, saslMechanism: mechanism, username: username, existingNames: existing)
}

@Test func aValidProfileHasNoIssues() {
    #expect(validate().isEmpty)
}

@Test(arguments: ["", "   ", "\n\t"])
func blankNameIsAnError(name: String) {
    let issues = validate(name: name)
    #expect(issues.map(\.field) == [.name])
    #expect(issues.map(\.severity) == [.error])
}

@Test func nameLengthLimitIs64AfterTrimming() {
    #expect(validate(name: String(repeating: "n", count: 64)).isEmpty)
    #expect(validate(name: "  " + String(repeating: "n", count: 64) + "  ").isEmpty)
    let tooLong = validate(name: String(repeating: "n", count: 65))
    #expect(tooLong.map(\.severity) == [.error])
    #expect(tooLong.map(\.field) == [.name])
}

@Test func duplicateNameIsAWarningComparedCaseInsensitively() {
    for existing in ["prod", "PROD", " Prod "] {
        let issues = validate(name: "prod", existing: ["other", existing])
        #expect(issues.map(\.severity) == [.warning])
        #expect(issues.map(\.field) == [.name])
    }
    #expect(validate(name: "  PrOd ", existing: ["prod"]).map(\.severity) == [.warning])
    #expect(validate(name: "prod", existing: ["production"]).isEmpty)
}

// MARK: ConnectionValidator — SASL

@Test(arguments: [ClusterConnection.Security.plaintext, .ssl])
func nonSASLSecurityIgnoresMechanismAndUsername(security: ClusterConnection.Security) {
    #expect(validate(security: security, mechanism: nil, username: nil).isEmpty)
    #expect(validate(security: security, mechanism: nil, username: "").isEmpty)
    #expect(validate(security: security, mechanism: .plain, username: "alice").isEmpty)
}

@Test(arguments: [ClusterConnection.Security.saslPlaintext, .saslSsl])
func saslSecurityRequiresMechanismAndUsername(security: ClusterConnection.Security) {
    let both = validate(security: security, mechanism: nil, username: nil)
    #expect(both.map(\.field) == [.username, .saslMechanism])
    #expect(both.allSatisfy { $0.severity == .error })

    #expect(validate(security: security, mechanism: .plain, username: nil).map(\.field) == [.username])
    #expect(validate(security: security, mechanism: .plain, username: "   ").map(\.field) == [.username])
    #expect(validate(security: security, mechanism: nil, username: "alice").map(\.field) == [.saslMechanism])
    #expect(validate(security: security, mechanism: .scramSha512, username: " alice ").isEmpty)
}

// MARK: ConnectionValidator — combined

@Test func allFieldsAreReportedTogetherInFieldOrder() {
    let issues = validate(name: "", servers: "x:0", security: .saslSsl, mechanism: nil, username: nil)
    #expect(issues.map(\.field) == [.name, .bootstrapServers, .username, .saslMechanism])
    #expect(issues.allSatisfy { $0.severity == .error })
}

@Test func bootstrapIssuesKeepEntryOrderWithinTheField() {
    let issues = validate(name: "dup", servers: "bad host, ok, ok, x:0", existing: ["dup"])
    #expect(issues.map(\.field) == [.name, .bootstrapServers, .bootstrapServers, .bootstrapServers])
    #expect(issues.map(\.severity) == [.warning, .error, .warning, .error])
    #expect(issues[1].message.contains("bad host"))
    #expect(issues[3].message.contains("x:0"))
}

@Test func warningsOnlyStillAllowSaving() {
    let issues = validate(name: "prod", servers: "a,a", existing: ["Prod"])
    #expect(issues.count == 2)
    #expect(issues.allSatisfy { $0.severity == .warning })
}

@Test func validationIsDeterministic() {
    let first = validate(name: "", servers: "a b,,x:0", security: .saslPlaintext)
    #expect((0..<20).allSatisfy { _ in validate(name: "", servers: "a b,,x:0", security: .saslPlaintext) == first })
}
