import CRdKafka
import Foundation
import KafkaManagerCore

/// Translates librdkafka error codes (`rd_kafka_resp_err_t` raw values) into `KafkaError`. Pure, so it is tested
/// without a broker. The raw librdkafka text only travels in `clusterError.message` for diagnostics, with secrets
/// removed; `userMessage` never shows it.
public enum LiveErrorMapping {
    /// - Parameters:
    ///   - host: shown when the cluster can't be reached.
    ///   - resource: topic or group name, for unknown-topic and authorization errors.
    ///   - secrets: strings that must not appear in the diagnostic text (passwords, usernames).
    public static func map(code: Int32, host: String, resource: String, detail: String?, secrets: [String] = []) -> KafkaError {
        switch code {
        case code32(RD_KAFKA_RESP_ERR__TRANSPORT), code32(RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN),
             code32(RD_KAFKA_RESP_ERR__RESOLVE), code32(RD_KAFKA_RESP_ERR__DESTROY_BROKER):
            return .unreachable(host: host)
        case code32(RD_KAFKA_RESP_ERR__TIMED_OUT), code32(RD_KAFKA_RESP_ERR__TIMED_OUT_QUEUE), code32(RD_KAFKA_RESP_ERR__MSG_TIMED_OUT),
             code32(RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT):
            return .timeout
        case code32(RD_KAFKA_RESP_ERR__AUTHENTICATION), code32(RD_KAFKA_RESP_ERR_SASL_AUTHENTICATION_FAILED),
             code32(RD_KAFKA_RESP_ERR_UNSUPPORTED_SASL_MECHANISM), code32(RD_KAFKA_RESP_ERR_ILLEGAL_SASL_STATE),
             code32(RD_KAFKA_RESP_ERR__SSL):
            return .authenticationFailed
        case code32(RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED), code32(RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED),
             code32(RD_KAFKA_RESP_ERR_CLUSTER_AUTHORIZATION_FAILED):
            return .authorizationFailed(resource: resource)
        case code32(RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART), code32(RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC),
             code32(RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION):
            return .unknownTopic(resource)
        // Every way the broker says "this group has members / is rebalancing": a reset or delete cannot proceed.
        case code32(RD_KAFKA_RESP_ERR_NON_EMPTY_GROUP), code32(RD_KAFKA_RESP_ERR_UNKNOWN_MEMBER_ID),
             code32(RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION), code32(RD_KAFKA_RESP_ERR_GROUP_SUBSCRIBED_TO_TOPIC),
             code32(RD_KAFKA_RESP_ERR_REBALANCE_IN_PROGRESS):
            return .clusterError(code: KafkaError.nonEmptyGroupCode, message: sanitize(detail, code: code, secrets: secrets))
        case code32(RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE):
            return .clusterError(code: KafkaError.messageTooLargeCode, message: sanitize(detail, code: code, secrets: secrets))
        case code32(RD_KAFKA_RESP_ERR_TOPIC_ALREADY_EXISTS):
            return .topicAlreadyExists(resource)
        case code32(RD_KAFKA_RESP_ERR_TOPIC_EXCEPTION):
            return .clusterError(code: KafkaError.invalidTopicCode, message: sanitize(detail, code: code, secrets: secrets))
        case code32(RD_KAFKA_RESP_ERR_INVALID_PARTITIONS):
            return .clusterError(code: KafkaError.invalidPartitionsCode, message: sanitize(detail, code: code, secrets: secrets))
        case code32(RD_KAFKA_RESP_ERR_INVALID_REPLICATION_FACTOR):
            return .clusterError(code: KafkaError.invalidReplicationFactorCode, message: sanitize(detail, code: code, secrets: secrets))
        default:
            return .clusterError(code: Int(code), message: sanitize(detail, code: code, secrets: secrets))
        }
    }

    /// Diagnostic text for a code: librdkafka's own description, with every secret replaced.
    public static func sanitize(_ detail: String?, code: Int32, secrets: [String]) -> String {
        var text = detail ?? String(cString: rd_kafka_err2str(rd_kafka_resp_err_t(rawValue: code)))
        for secret in secrets where !secret.isEmpty {
            text = text.replacingOccurrences(of: secret, with: "<redacted>")
        }
        return text
    }

    private static func code32(_ error: rd_kafka_resp_err_t) -> Int32 { error.rawValue }
}
