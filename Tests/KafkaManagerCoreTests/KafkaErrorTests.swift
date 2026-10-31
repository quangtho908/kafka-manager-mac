import Testing
@testable import KafkaManagerCore

private let allErrors: [KafkaError] = [
    .unreachable(host: "broker:9092"),
    .authenticationFailed,
    .authorizationFailed(resource: "topic orders"),
    .timeout,
    .unknownTopic("orders"),
    .topicAlreadyExists("orders"),
    .clusterError(code: 42, message: "Local: Broker transport failure"),
]

@Test(arguments: allErrors)
func everyKafkaErrorHasUserMessage(error: KafkaError) {
    #expect(!error.userMessage.isEmpty)
}

@Test func userMessageMentionsTheSubject() {
    #expect(KafkaError.unreachable(host: "broker:9092").userMessage.contains("broker:9092"))
    #expect(KafkaError.unknownTopic("orders").userMessage.contains("orders"))
    #expect(KafkaError.topicAlreadyExists("orders").userMessage.contains("orders"))
    #expect(KafkaError.authorizationFailed(resource: "topic orders").userMessage.contains("topic orders"))
}

@Test func clusterErrorUserMessageDoesNotLeakRawMessage() {
    let message = KafkaError.clusterError(code: 42, message: "Local: Broker transport failure").userMessage
    #expect(!message.contains("Local:"))
    #expect(message.contains("42"))
}

@Test func kafkaErrorEquality() {
    #expect(KafkaError.unknownTopic("a") == .unknownTopic("a"))
    #expect(KafkaError.unknownTopic("a") != .unknownTopic("b"))
    #expect(KafkaError.timeout != .authenticationFailed)
}
