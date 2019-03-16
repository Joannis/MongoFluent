import XCTest

import MongoFluentTests

var tests = [XCTestCaseEntry]()
tests += MongoFluentTests.allTests()
XCTMain(tests)