import Testing
@testable import KafkaManagerCore

@Suite("TopicNameValidator")
struct TopicNameValidatorTests {
    @Test(arguments: ["orders", "a", "Orders.v1", "my_topic", "my-topic", "a.b_c-d", "0", "-x", "_internal", "...", "a..b", "ünı".filter(\.isASCII) + "x"])
    func validNames(name: String) {
        #expect(TopicNameValidator.problem(in: name) == nil)
    }

    @Test func emptyNameIsRejected() {
        #expect(TopicNameValidator.problem(in: "") == .empty)
    }

    @Test(arguments: [".", ".."])
    func dotNamesAreReserved(name: String) {
        #expect(TopicNameValidator.problem(in: name) == .reserved(name))
    }

    @Test func lengthLimitIs249() {
        #expect(TopicNameValidator.problem(in: String(repeating: "a", count: 249)) == nil)
        #expect(TopicNameValidator.problem(in: String(repeating: "a", count: 250)) == .tooLong(count: 250))
    }

    @Test func spacesAreInvalidAndNotTrimmed() {
        #expect(TopicNameValidator.problem(in: "a b") == .invalidCharacters([" "]))
        #expect(TopicNameValidator.problem(in: " a") == .invalidCharacters([" "]))
        #expect(TopicNameValidator.problem(in: "a\n") == .invalidCharacters(["\n"]))
    }

    @Test(arguments: ["café", "トピック", "orders🙂", "a/b", "a:b", "a*b", "a@b", "a,b", "a+b"])
    func nonAllowedCharactersAreInvalid(name: String) {
        guard case .invalidCharacters(let characters)? = TopicNameValidator.problem(in: name) else {
            Issue.record("\(name) should be invalid")
            return
        }
        #expect(!characters.isEmpty)
    }

    @Test func offendingCharactersAreDistinctInOrder() {
        #expect(TopicNameValidator.problem(in: "a/b:c/d") == .invalidCharacters(["/", ":"]))
    }

    @Test func invalidCharactersAreReportedBeforeLength() {
        #expect(TopicNameValidator.problem(in: String(repeating: "a", count: 300) + "!") == .invalidCharacters(["!"]))
    }

    @Test func everyProblemHasAMessage() {
        let problems: [TopicNameProblem] = [.empty, .reserved(".."), .invalidCharacters(["/"]), .tooLong(count: 250)]
        #expect(problems.allSatisfy { !$0.message.isEmpty })
        #expect(TopicNameProblem.invalidCharacters(["/"]).message.contains("/"))
    }

    @Test func collisionRiskNeedsBothDotAndUnderscore() {
        #expect(TopicNameValidator.hasMetricNameCollisionRisk("a.b_c"))
        #expect(!TopicNameValidator.hasMetricNameCollisionRisk("a.b"))
        #expect(!TopicNameValidator.hasMetricNameCollisionRisk("a_b"))
    }
}
