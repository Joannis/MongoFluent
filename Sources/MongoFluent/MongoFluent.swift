import MongoKitten
import FluentKit

extension MongoKitten.Database: FluentKit.Database {
    public func transaction<T>(_ closure: @escaping (FluentKit.Database) -> EventLoopFuture<T>) -> EventLoopFuture<T> {
        do {
            let db = try startTransaction(with: .init())
            return closure(db)
        } catch {
            return eventLoop.makeFailedFuture(error)
        }
    }
    
    public func execute(_ schema: DatabaseSchema) -> EventLoopFuture<Void> {
        return eventLoop.makeSucceededFuture(())
    }
    
    public func execute(_ query: DatabaseQuery, _ onOutput: @escaping (DatabaseOutput) throws -> ()) -> EventLoopFuture<Void> {
        do {
            let collection = self[query.entity]
            
            switch query.action {
            case .create:
                return try collection.insert(query.makeInputDocument()).map { _ in }
            case .read:
                return try collection.find(query.filters.mkQuery()).forEach(handler: onOutput)
            case .update:
                return try collection.update(where: query.filters.mkQuery(), to: query.makeInputDocument()).map { _ in }
            case .delete:
                return try collection.deleteAll(where: query.filters.mkQuery()).map { _ in }
            case .custom(let operation):
                return eventLoop.makeFailedFuture(MongoFluentError.unknownOperation(operation))
            }
        } catch {
            return eventLoop.makeFailedFuture(error)
        }
    }
    
    public func close() -> EventLoopFuture<Void> {
        return cluster.closeConnections()
    }
}

extension DatabaseQuery {
    func makeInputDocument() throws -> Document {
        var document = Document()
        let inputs = input[0]
        
        for i in 0..<fields.count {
            try fields[i].apply(inputs[i], to: &document)
        }
        return document
    }
}

extension Document: DatabaseOutput {
    public var description: String {
        return self.debugDescription
    }
    
    public func decode<T>(field: String, as type: T.Type) throws -> T where T : Decodable {
        let value = self[field]
        
        switch value {
        case let value as T:
            return value
            // More conversion
        default:
            throw MongoFluentError.valueNotFound(needed: type, found: value)
        }
    }
}

enum MongoFluentError: Error {
    case unknownOperation(Any)
    case unknownField(Any)
    case unknownFilter(Any)
    case unknownRelation(Any)
    case unknownQueryMethod(Any)
    case valueNotFound(needed: Any.Type, found: Primitive?)
}

extension Array where Element == DatabaseQuery.Filter {
    func mkQuery() throws -> Query {
        var query = Query()
        
        for filter in self {
            try filter.apply(to: &query)
        }
        
        return query
    }
}

extension DatabaseQuery.Field {
    func apply(_ value: PrimitiveConvertible, to doc: inout Document) throws {
        switch self {
        case .aggregate(let aggregate):
            fatalError()
        case .field(let path, entity: let entity, let alias):
            var iterator = path.makeIterator()
            
            func next(key: String, to document: inout Document) {
                if let nextKey = iterator.next() {
                    var subDoc = Document()
                    next(key: nextKey, to: &subDoc)
                    document[key] = subDoc
                } else {
                    document[key] = value.makePrimitive()
                }
            }
            
            if let nextKey = iterator.next() {
                next(key: nextKey, to: &doc)
            }
        case .custom(let field):
            throw MongoFluentError.unknownFilter(field)
        }
    }
}

extension DatabaseQuery.Filter {
    func apply(to query: inout Query) throws {
        switch self {
        case .basic(let field, let method, let value):
            var doc = Document()
            let subQuery: Document
            
            switch method {
            case .equality(let inverse):
                if inverse {
                    subQuery = [
                        "$ne": value
                    ]
                } else {
                    subQuery = [
                        "$eq": value
                    ]
                }
            default:
                fatalError()
            }
            
            try field.apply(subQuery, to: &doc)
            
            query = query && Query.custom(doc)
        case .group(let filters, let relation):
            let queries = try filters.map { filter -> Query in
                var query = Query()
                try filter.apply(to: &query)
                return query
            }
            
            switch relation {
            case .and:
                query = query && queries.reduce(Query(), &&)
            case .or:
                query = query && queries.reduce(Query(), ||)
            case .custom(let relation):
                throw MongoFluentError.unknownRelation(relation)
            }
        case .custom(let any):
            throw MongoFluentError.unknownFilter(any)
        }
    }
}

extension DatabaseQuery.Value: PrimitiveConvertible {
    public func makePrimitive() -> Primitive? {
        switch self {
        case .array(let values):
            return Document(array: values.compactMap { $0.makePrimitive() })
        case .null:
            return Null()
        case .bind(let encodable):
            let encoder = BSONEncoder()
            
            do {
                return try encoder.encodePrimitive(encodable)
            } catch {
                return nil
            }
        case .dictionary(let dictionary):
            return dictionary.makePrimitive()
        case .custom(_):
            return nil
        }
    }
}
