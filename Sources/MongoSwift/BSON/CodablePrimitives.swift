import Foundation

/// A protocol for types that are not BSON types but require encoding/decoding support. 
internal protocol Primitive {
	/// Attempts to initialize this type from an analogous BsonValue. Returns nil
	/// the `from` value cannot be accurately represented as this type.
    init?(from value: BsonValue)

    /// when we rewrite encoder, will add a `asBsonValue` method here that handles the 
    /// Primitive -> BsonValue conversion

    /// Initializer for creating from Int, Int32, Int64
    init?<T>(exactly source: T) where T: BinaryInteger

    /// Initializer for creating from a Double
    init?(exactly source: Double)
}

extension Primitive {
    init?(from value: BsonValue) {
        print("value: \(value)")
        switch value {
        case let v as Int:
            let v2 = Float(exactly: v)
            print("v2: \(v2)")
            if let exact = Self(exactly: v) { self = exact; return }
        case let v as Int32:
            if let exact = Self(exactly: v) { self = exact; return }
        case let v as Int64:
            if let exact = Self(exactly: v) { self = exact; return }
        case let v as Double:
            if let exact = Self(exactly: v) { self = exact; return }
        default:
            break
        }
        return nil
    }
}

extension Int8: Primitive {}
extension Int16: Primitive {}
extension UInt8: Primitive {}
extension UInt16: Primitive {}
extension UInt32: Primitive {}
extension UInt64: Primitive {}
extension UInt: Primitive {}
extension Float: Primitive {}
