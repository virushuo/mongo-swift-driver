import libmongoc

/// An enumeration of possible ReadConcern levels.
public enum ReadConcernLevel: String {
    case local
    case available
    case majority
    case linearizable
    case snapshot
}

/// A class to represent a MongoDB read concern.
public class ReadConcern: Equatable, CustomStringConvertible {

    /// A pointer to a mongoc_read_concern_t
    internal var _readConcern: OpaquePointer?

    /// The level of this readConcern, or nil if the level is not set.
    public var level: String? {
        guard let level = mongoc_read_concern_get_level(self._readConcern) else {
            return nil
        }
        return String(cString: level)
    }

    private var asDocument: Document {
        let doc = Document()
        try? self.append(to: doc)
        return doc
    }

    public var description: String {
        return self.asDocument.description
    }

    /// Initialize a new ReadConcern from a ReadConcernLevel.
    public convenience init(_ level: ReadConcernLevel) throws {
        try self.init(level.rawValue)
    }

    /// Initialize a new ReadConcern from a String corresponding to a read concern level.
    public init(_ level: String) throws {
        self._readConcern = mongoc_read_concern_new()
        if !mongoc_read_concern_set_level(self._readConcern, level) {
            throw MongoError.readConcernError(message: "Failed to set read concern level to '\(level)'")
        }
    }

    /// Initialize a new empty ReadConcern.
    public init() {
        self._readConcern = mongoc_read_concern_new()
    }

    /// Initialize a new ReadConcern from a Document.
    public convenience init(_ doc: Document) throws {
        if let level = doc["level"] as? String {
            try self.init(level)
        } else {
            self.init()
        }
    }

    /// Initializes a new ReadConcern by copying a mongoc_read_concern_t.
    /// The caller is responsible for freeing the original mongoc_read_concern_t.
    internal init(_ readConcern: OpaquePointer?) {
        self._readConcern = mongoc_read_concern_copy(readConcern)
    }

    /// Appends this readConcern to a Document.
    internal func append(to doc: Document) throws {
        if !mongoc_read_concern_append(self._readConcern, doc.data) {
            throw MongoError.readConcernError(message: "Error appending readconcern to document \(doc)")
        }
    }

    /// Since we have to follow certain rules about whether to include or omit a ReadConcern,
    /// this function handles obeying those, factoring in the ReadConcern, if any, for
    /// whatever object is calling this function. It returns a final options Document for the
    /// calling function to use, or nil if the Document ends up being empty.
    internal static func append(_ readConcern: ReadConcern?, to opts: Document?, callerRC: ReadConcern) throws -> Document? {
        // if the user didn't specify a readConcern, then we just want to use
        // whatever the default is for the caller. 
        guard let rc = readConcern else { return opts }

        // the caller is using the server's default RC and we are also using default, don't append anything
        if callerRC.level == nil && rc.level == nil { return opts }

        // otherwise either us or the caller is using a non-default, so we need to append it
        let output = opts ?? Document() // create base opts if they don't exist
        try rc.append(to: output)
        return output
    }

    public static func == (lhs: ReadConcern, rhs: ReadConcern) -> Bool {
        return lhs.level == rhs.level
    }

    deinit {
        guard let readConcern = self._readConcern else { return }
        mongoc_read_concern_destroy(readConcern)
        self._readConcern = nil
    }
}

/// A class to represent a MongoDB write concern.
public class WriteConcern: Equatable, CustomStringConvertible {

    /// A pointer to a mongoc_write_concern_t
    internal var _writeConcern: OpaquePointer?

    // Indicates whether to wait for the write operation to get committed to the journal. 
    public var journal: Bool? {
        return mongoc_write_concern_get_journal(self._writeConcern)
    }

    // Specifies the number of nodes that should acknowledge the write. 
    // MUST be greater than or equal to 0. Only one of this and wTags may be set.
    public var w: Int32? {
        return mongoc_write_concern_get_w(self._writeConcern)
    }

    // Indicates a tag for nodes that should acknowledge the write. Only one of this and w may be set.
    public var wTag: String? {
        guard let wTag = mongoc_write_concern_get_wtag(self._writeConcern) else { return nil }
        return String(cString: wTag)
    }

    /// If the write concern is not satisfied within this timeout (in milliseconds),
    /// the operation will return an error. The value MUST be greater than or equal to 0.
    public var wtimeoutMS: Int32? {
        return mongoc_write_concern_get_wtimeout(self._writeConcern)
    }

    /// Indicates whether this is an acknowledged write concern.
    public var isAcknowledged: Bool {
        return mongoc_write_concern_is_acknowledged(self._writeConcern)
    }

    /// Indicates whether this is the default write concern.
    public var isDefault: Bool {
        return mongoc_write_concern_is_default(self._writeConcern)
    }

    private var asDocument: Document {
        let doc = Document()
        try? self.append(to: doc)
        return doc
    }

    public var description: String {
        return self.asDocument.description
    }

    /// Initializes a new, empty WriteConcern
    public init() {
        self._writeConcern = mongoc_write_concern_new()
    }

    /// Initialize a WriteConcern using an Int32 `w` value
    public init(journal: Bool? = nil, w: Int32? = nil, wtimeoutMS: Int32? = nil) throws {
        self._writeConcern = mongoc_write_concern_new()
        self.setJournal(journal)
        self.setW(w)
        self.setWTimeoutMS(wtimeoutMS)
        try self.checkIsValid()
    }

    /// Initialize a WriteConcern using a String `w` value (`wTag`)
    public init(journal: Bool? = nil, wTag: String? = nil, wtimeoutMS: Int32? = nil) throws {
        self._writeConcern = mongoc_write_concern_new()
        self.setJournal(journal)
        self.setWTag(wTag)
        self.setWTimeoutMS(wtimeoutMS)
        try self.checkIsValid()
    }

    /// Initializes a new WriteConcern by copying a mongoc_write_concern_t.
    /// The caller is responsible for freeing the original mongoc_write_concern_t.
    internal init(_ writeConcern: OpaquePointer?) {
        self._writeConcern = mongoc_write_concern_copy(writeConcern)
    }

    /// Sets the journal value on this writeConcern
    private func setJournal(_ journal: Bool?) {
        if let journal = journal { mongoc_write_concern_set_journal(self._writeConcern, journal) }
    }

    /// Sets the wTag value on this writeConcern
    private func setWTag(_ wTag: String?) {
        if let wTag = wTag { mongoc_write_concern_set_wtag(self._writeConcern, wTag) }
    }

    /// Sets the wtimeoutMS value on this writeConcern
    private func setWTimeoutMS(_ wtimeoutMS: Int32?) {
        if let wtimeoutMS = wtimeoutMS { mongoc_write_concern_set_wtimeout(self._writeConcern, wtimeoutMS) }
    }

    /// Sets the w value on this writeConcern
    private func setW(_ w: Int32?) {
        if let w = w { mongoc_write_concern_set_w(self._writeConcern, w) }
    }

    /// Checks if this writeConcern has an invalid combination of options
    private func checkIsValid() throws {
        if !mongoc_write_concern_is_valid(self._writeConcern) {
            throw MongoError.writeConcernError(message: "Invalid combination of WriteConcern options")
        }
    }

    /// Appends this writeConcern to a Document.
    private func append(to doc: Document) throws {
        if !mongoc_write_concern_append(self._writeConcern, doc.data) {
            throw MongoError.writeConcernError(message: "Error appending WriteConcern to document \(doc)")
        }
    }

    /// Since we have to follow certain rules about whether to include or omit a WriteConcern,
    /// this function handles obeying those, factoring in the WriteConcern, if any, for
    /// whatever object is calling this function. It returns a final options Document for the
    /// calling function to use, or nil if the Document ends up being empty.
    internal static func append(_ writeConcern: WriteConcern?, to opts: Document?, callerWC: WriteConcern) throws -> Document? {
        // if the user didn't specify a writeConcern, then we just want to use
        // whatever the default is for the caller. 
        guard let wc = writeConcern else { return opts }

        // the caller is using the server's default WC and we are also using default, don't append anything
        if callerWC.isDefault && wc.isDefault { return opts }

        // otherwise either us or the caller is using a non-default, so we need to append it
        let output = opts ?? Document() // create base opts if they don't exist
        try wc.append(to: output)
        return output
    }

    public static func == (lhs: WriteConcern, rhs: WriteConcern) -> Bool {
        return lhs.asDocument == rhs.asDocument
    }

    deinit {
        guard let writeConcern = self._writeConcern else { return }
        mongoc_write_concern_destroy(writeConcern)
        self._writeConcern = nil
    }
}
