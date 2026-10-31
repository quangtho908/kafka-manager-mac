import Testing
@testable import KafkaManagerCore

private let secret = "s3cret-pass"
private let credentials = Credentials(username: "alice", password: secret)

@Test func credentialsDescriptionsHidePassword() {
    #expect(!String(describing: credentials).contains(secret))
    #expect(!String(reflecting: credentials).contains(secret))
    #expect(!"\(credentials)".contains(secret))
    #expect(String(describing: credentials).contains("alice"))
}

@Test func credentialsDumpHidesPassword() {
    var output = ""
    dump(credentials, to: &output)
    #expect(!output.contains(secret))
    #expect(output.contains("alice"))
}

@Test func credentialsStillExposePasswordToCallers() {
    #expect(credentials.password == secret)
}
