import CRdKafka
import KafkaManagerCore
import Testing
@testable import KafkaManagerKafka

struct LiveErrorMappingTests {
    private static func map(_ code: rd_kafka_resp_err_t, detail: String? = nil, secrets: [String] = []) -> KafkaError {
        LiveErrorMapping.map(code: code.rawValue, host: "broker-1", resource: "orders", detail: detail, secrets: secrets)
    }

    @Test(arguments: [
        (RD_KAFKA_RESP_ERR__TRANSPORT, KafkaError.unreachable(host: "broker-1")),
        (RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN, .unreachable(host: "broker-1")),
        (RD_KAFKA_RESP_ERR__RESOLVE, .unreachable(host: "broker-1")),
        (RD_KAFKA_RESP_ERR__TIMED_OUT, .timeout),
        (RD_KAFKA_RESP_ERR__TIMED_OUT_QUEUE, .timeout),
        (RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT, .timeout),
        (RD_KAFKA_RESP_ERR__AUTHENTICATION, .authenticationFailed),
        (RD_KAFKA_RESP_ERR_SASL_AUTHENTICATION_FAILED, .authenticationFailed),
        (RD_KAFKA_RESP_ERR_UNSUPPORTED_SASL_MECHANISM, .authenticationFailed),
        (RD_KAFKA_RESP_ERR_ILLEGAL_SASL_STATE, .authenticationFailed),
        (RD_KAFKA_RESP_ERR__SSL, .authenticationFailed),
        (RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED, .authorizationFailed(resource: "orders")),
        (RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED, .authorizationFailed(resource: "orders")),
        (RD_KAFKA_RESP_ERR_CLUSTER_AUTHORIZATION_FAILED, .authorizationFailed(resource: "orders")),
        (RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART, .unknownTopic("orders")),
        (RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC, .unknownTopic("orders")),
        (RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION, .unknownTopic("orders")),
        (RD_KAFKA_RESP_ERR_TOPIC_ALREADY_EXISTS, .topicAlreadyExists("orders")),
    ])
    func mapsKnownCodes(code: rd_kafka_resp_err_t, expected: KafkaError) {
        #expect(Self.map(code) == expected)
    }

    @Test func validationCodesKeepTheKafkaProtocolNumbers() {
        for (code, expected) in [
            (RD_KAFKA_RESP_ERR_TOPIC_EXCEPTION, KafkaError.invalidTopicCode),
            (RD_KAFKA_RESP_ERR_INVALID_PARTITIONS, KafkaError.invalidPartitionsCode),
            (RD_KAFKA_RESP_ERR_INVALID_REPLICATION_FACTOR, KafkaError.invalidReplicationFactorCode),
        ] {
            guard case .clusterError(let mapped, _) = Self.map(code) else {
                Issue.record("expected clusterError for \(code.rawValue)")
                continue
            }
            #expect(mapped == expected)
        }
    }

    @Test func aGroupTheBrokerDoesNotKnowIsRecognisedAsUnknown() {
        let error = Self.map(RD_KAFKA_RESP_ERR_GROUP_ID_NOT_FOUND)
        #expect(error.isUnknownGroup)
        #expect(error == .clusterError(code: KafkaError.groupIdNotFoundCode, message: error.diagnosticText))
        #expect(!Self.map(RD_KAFKA_RESP_ERR_OFFSET_OUT_OF_RANGE).isUnknownGroup)
    }

    @Test(arguments: [
        RD_KAFKA_RESP_ERR_NON_EMPTY_GROUP, RD_KAFKA_RESP_ERR_UNKNOWN_MEMBER_ID, RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION,
        RD_KAFKA_RESP_ERR_GROUP_SUBSCRIBED_TO_TOPIC, RD_KAFKA_RESP_ERR_REBALANCE_IN_PROGRESS,
    ])
    func activeGroupRejectionsAreRecognisedAsNonEmpty(code: rd_kafka_resp_err_t) {
        let error = Self.map(code)
        #expect(error.isNonEmptyGroup)
        #expect(error == .clusterError(code: KafkaError.nonEmptyGroupCode, message: error.diagnosticText))
        #expect(error.userMessage == "Stop the group's consumers first; offsets can only be changed while the group has no active members.")
        #expect(!Self.map(RD_KAFKA_RESP_ERR_OFFSET_OUT_OF_RANGE).isNonEmptyGroup)
    }

    @Test func produceFailuresMapToFriendlyErrors() {
        #expect(Self.map(RD_KAFKA_RESP_ERR__MSG_TIMED_OUT) == .timeout)
        let tooLarge = Self.map(RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE)
        #expect(tooLarge == .clusterError(code: KafkaError.messageTooLargeCode, message: tooLarge.diagnosticText))
        #expect(tooLarge.userMessage.contains("larger than the cluster accepts"))
        #expect(Self.map(RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC) == .unknownTopic("orders"))
        #expect(Self.map(RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED) == .authorizationFailed(resource: "orders"))
        #expect(Self.map(RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN) == .unreachable(host: "broker-1"))
    }

    @Test func unknownCodesBecomeClusterErrorsWithTheirCode() {
        let error = Self.map(RD_KAFKA_RESP_ERR_OFFSET_OUT_OF_RANGE)
        guard case .clusterError(let code, let message) = error else {
            Issue.record("expected clusterError")
            return
        }
        #expect(code == Int(RD_KAFKA_RESP_ERR_OFFSET_OUT_OF_RANGE.rawValue))
        #expect(!message.isEmpty)
    }

    /// Every code the live client maps, with a hostile detail text: nothing raw may reach `userMessage`.
    @Test(arguments: [
        RD_KAFKA_RESP_ERR__TRANSPORT, RD_KAFKA_RESP_ERR__RESOLVE, RD_KAFKA_RESP_ERR__TIMED_OUT, RD_KAFKA_RESP_ERR__AUTHENTICATION,
        RD_KAFKA_RESP_ERR__SSL, RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED, RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART,
        RD_KAFKA_RESP_ERR_GROUP_ID_NOT_FOUND, RD_KAFKA_RESP_ERR_OFFSET_OUT_OF_RANGE, RD_KAFKA_RESP_ERR_TOPIC_EXCEPTION,
    ])
    func noUserMessageCarriesRawTextOrSecrets(code: rd_kafka_resp_err_t) {
        let raw = "Local: sasl_plaintext://10.0.0.1:9092 getaddrinfo failed password=hunter2-secret"
        let message = Self.map(code, detail: raw, secrets: ["hunter2-secret"]).userMessage
        for word in ["Local:", "sasl_", "getaddrinfo", "hunter2-secret", "10.0.0.1", "rdkafka"] { #expect(!message.contains(word), "\(message)") }
        #expect(!message.isEmpty)
    }

    @Test func userMessageNeverShowsTheRawText() {
        let error = Self.map(RD_KAFKA_RESP_ERR_OFFSET_OUT_OF_RANGE, detail: "raw broker text 12345")
        #expect(!error.userMessage.contains("raw broker text"))
    }

    @Test func secretsAreRemovedFromDiagnostics() {
        let error = Self.map(RD_KAFKA_RESP_ERR_UNKNOWN, detail: "login alice with hunter2 failed", secrets: ["hunter2", "alice"])
        guard case .clusterError(_, let message) = error else {
            Issue.record("expected clusterError")
            return
        }
        #expect(!message.contains("hunter2"))
        #expect(!message.contains("alice"))
        #expect(message.contains("<redacted>"))
    }

    @Test func emptySecretsDoNotEraseEverything() {
        #expect(LiveErrorMapping.sanitize("text", code: -1, secrets: [""]) == "text")
    }
}

private extension KafkaError {
    var diagnosticText: String {
        if case .clusterError(_, let message) = self { return message }
        return ""
    }
}
