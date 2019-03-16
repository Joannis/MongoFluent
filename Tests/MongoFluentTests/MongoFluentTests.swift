import XCTest
@testable import MongoFluent

final class MongoFluentTests: XCTestCase {
    func testExample() {
        // This is an example of a functional test case.
        // Use XCTAssert and related functions to verify your tests produce the correct
        // results.
        XCTAssertEqual(MongoFluent().text, "Hello, World!")
    }

    static var allTests = [
        ("testExample", testExample),
    ]
}
