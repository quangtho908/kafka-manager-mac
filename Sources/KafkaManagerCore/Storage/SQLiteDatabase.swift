import Foundation
import SQLite3

struct SQLiteError: Error, Equatable {
    let code: Int32
    let message: String

    /// Primary result code (extended codes carry detail in the upper bits).
    var primaryCode: Int32 { code & 0xFF }
}

enum SQLiteValue: Equatable {
    case text(String)
    case integer(Int64)
    case real(Double)
    case null
}

struct SQLiteRow {
    fileprivate let statement: OpaquePointer

    func text(_ column: Int32) -> String? {
        sqlite3_column_text(statement, column).map { String(cString: $0) }
    }

    func integer(_ column: Int32) -> Int64 { sqlite3_column_int64(statement, column) }
    func real(_ column: Int32) -> Double { sqlite3_column_double(statement, column) }
}

/// Thin synchronous wrapper over one sqlite3 connection. Not thread-safe: confine it to one actor.
final class SQLiteDatabase {
    private static let busyTimeoutMilliseconds: Int32 = 2_000
    private static let transient = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

    private let handle: OpaquePointer

    init(path: String) throws {
        var opened: OpaquePointer?
        let code = sqlite3_open_v2(path, &opened, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nil)
        guard code == SQLITE_OK, let opened else {
            let message = opened.map { String(cString: sqlite3_errmsg($0)) } ?? "Unable to open database"
            sqlite3_close_v2(opened)
            throw SQLiteError(code: code, message: message)
        }
        handle = opened
        sqlite3_busy_timeout(opened, Self.busyTimeoutMilliseconds)
    }

    deinit {
        sqlite3_close_v2(handle)
    }

    /// For statements without bound values (pragmas, DDL, transaction control).
    func execute(_ sql: String) throws {
        var message: UnsafeMutablePointer<CChar>?
        let code = sqlite3_exec(handle, sql, nil, nil, &message)
        defer { sqlite3_free(message) }
        guard code == SQLITE_OK else {
            throw SQLiteError(code: code, message: message.map { String(cString: $0) } ?? "sqlite3_exec failed")
        }
    }

    func run(_ sql: String, _ bindings: [SQLiteValue] = []) throws {
        try withStatement(sql, bindings) { statement in
            let code = sqlite3_step(statement)
            guard code == SQLITE_DONE else { throw lastError(code) }
        }
    }

    /// Rows for which `row` returns nil are skipped.
    func query<Value>(_ sql: String, _ bindings: [SQLiteValue] = [], row: (SQLiteRow) -> Value?) throws -> [Value] {
        try withStatement(sql, bindings) { statement in
            var result: [Value] = []
            while true {
                let code = sqlite3_step(statement)
                if code == SQLITE_DONE { return result }
                guard code == SQLITE_ROW else { throw lastError(code) }
                if let value = row(SQLiteRow(statement: statement)) { result.append(value) }
            }
        }
    }

    func transaction<Result>(_ body: () throws -> Result) throws -> Result {
        try execute("BEGIN IMMEDIATE")
        do {
            let result = try body()
            try execute("COMMIT")
            return result
        } catch {
            try? execute("ROLLBACK")  // The original error is what matters; a failed rollback leaves nothing to report.
            throw error
        }
    }

    var userVersion: Int {
        get throws {
            try query("PRAGMA user_version") { Int($0.integer(0)) }.first ?? 0
        }
    }

    func setUserVersion(_ version: Int) throws {
        try execute("PRAGMA user_version = \(version)")
    }

    private func withStatement<Result>(
        _ sql: String,
        _ bindings: [SQLiteValue],
        _ body: (OpaquePointer) throws -> Result
    ) throws -> Result {
        var prepared: OpaquePointer?
        let prepareCode = sqlite3_prepare_v2(handle, sql, -1, &prepared, nil)
        guard prepareCode == SQLITE_OK, let statement = prepared else { throw lastError(prepareCode) }
        defer { sqlite3_finalize(statement) }
        for (offset, value) in bindings.enumerated() {
            let index = Int32(offset + 1)
            let code = switch value {
            case .text(let text): sqlite3_bind_text(statement, index, text, -1, Self.transient)
            case .integer(let number): sqlite3_bind_int64(statement, index, number)
            case .real(let number): sqlite3_bind_double(statement, index, number)
            case .null: sqlite3_bind_null(statement, index)
            }
            guard code == SQLITE_OK else { throw lastError(code) }
        }
        return try body(statement)
    }

    private func lastError(_ code: Int32) -> SQLiteError {
        SQLiteError(code: code, message: String(cString: sqlite3_errmsg(handle)))
    }
}
