@testable import MongoSwift
import Nimble
import XCTest

import libmongoc

extension WriteConcern {
    /// Initialize a new ReadConcern from a Document.
    fileprivate convenience init(_ doc: Document) throws {
        let wtag = doc["w"] as? String
        let w = doc["w"] as? Int

        // can be stored under either "j" or "journal"
        var jToUse: Bool? = nil
        if let j = doc["journal"] as? Bool {
            jToUse = j
        } else if let j = doc["j"] as? Bool {
            jToUse = j
        }

        // can be stored under either "wtimeout" or "wtimeoutMS"
        var wtToUse: Int32? = nil
        if let wt = doc["wtimeoutMS"] as? Int {
            wtToUse = Int32(wt)
        } else if let wt = doc["wtimeout"] as? Int {
            wtToUse = Int32(wt)
        }

        if wtag != nil {
            try self.init(journal: jToUse, wTag: wtag, wtimeoutMS: wtToUse)
        } else {
            try self.init(journal: jToUse, w: w != nil ? Int32(w!) : nil, wtimeoutMS: wtToUse)
        }
    }
}

final class ReadWriteConcernTests: XCTestCase {
	static var allTests: [(String, (ReadWriteConcernTests) -> () throws -> Void)] {
        return [
        	("testReadConcernType", testReadConcernType),
            ("testWriteConcernType", testWriteConcernType),
            ("testClientReadConcern", testClientReadConcern),
            ("testClientWriteConcern", testClientWriteConcern),
            ("testDatabaseReadConcern", testDatabaseReadConcern),
            ("testDatabaseWriteConcern", testDatabaseWriteConcern),
            ("testOperationReadConcerns", testOperationReadConcerns),
            ("testOperationWriteConcerns", testOperationReadConcerns),
            ("testConnectionStrings", testConnectionStrings),
            ("testDocuments", testDocuments)
        ]
    }

    override func setUp() {
    	self.continueAfterFailure = false
    }

    func testReadConcernType() throws {
    	// check that setting readConcern level to all valid inputs doesn't throw
    	expect(try ReadConcern(.local)).toNot(throwError())
    	expect(try ReadConcern(.majority)).toNot(throwError())
    	expect(try ReadConcern(.available)).toNot(throwError())
    	expect(try ReadConcern(.linearizable)).toNot(throwError())
    	expect(try ReadConcern(.snapshot)).toNot(throwError())

    	expect(try ReadConcern("local")).toNot(throwError())
    	expect(try ReadConcern("majority")).toNot(throwError())
    	expect(try ReadConcern("available")).toNot(throwError())
    	expect(try ReadConcern("linearizable")).toNot(throwError())
    	expect(try ReadConcern("snapshot")).toNot(throwError())

    	// check level var works as expected
    	let rc = try ReadConcern(.majority)
    	expect(rc.level).to(equal("majority"))

    }

    func testWriteConcernType() throws {
        // try creating write concerns with various valid options
        expect(try WriteConcern(w: 0)).toNot(throwError())
        expect(try WriteConcern(w: 3)).toNot(throwError())
        expect(try WriteConcern(journal: true, w: 1)).toNot(throwError())
        expect(try WriteConcern(w: 0, wtimeoutMS: 1000)).toNot(throwError())

        // verify that we throw on invalid option combination
        expect(try WriteConcern(journal: true, w: 0)).to(throwError())
    }

    func testClientReadConcern() throws {
    	// create a client with no options and check its RC
    	let client1 = try MongoClient()
    	// expect the readConcern property to exist with a nil level
    	expect(client1.readConcern.level).to(beNil())

    	// expect that a DB created from this client inherits its unset RC 
    	let db1 = try client1.db("test")
    	expect(db1.readConcern.level).to(beNil())

    	// expect that a DB created from this client can override the client's unset RC
    	let db2 = try client1.db("test", options: DatabaseOptions(readConcern: try ReadConcern(.majority)))
    	expect(db2.readConcern.level).to(equal("majority"))

    	client1.close()

    	// create a client with local read concern and check its RC
    	let client2 = try MongoClient(options: ClientOptions(readConcern: try ReadConcern(.local)))
    	// although local is default, if it is explicitly provided it should be set
    	expect(client2.readConcern.level).to(equal("local"))

    	// expect that a DB created from this client inherits its local RC 
    	let db3 = try client2.db("test")
    	expect(db3.readConcern.level).to(equal("local"))

    	// expect that a DB created from this client can override the client's local RC
    	let db4 = try client2.db("test", options: DatabaseOptions(readConcern: try ReadConcern(.majority)))
    	expect(db4.readConcern.level).to(equal("majority"))

    	client2.close()

    	// create a client with majority read concern and check its RC
    	let client3 = try MongoClient(options: ClientOptions(readConcern: try ReadConcern(.majority)))
    	expect(client3.readConcern.level).to(equal("majority"))

    	// expect that a DB created from this client can override the client's majority RC with an unset one
    	let db5 = try client3.db("test", options: DatabaseOptions(readConcern: ReadConcern()))
    	expect(db5.readConcern.level).to(beNil())

    	client3.close()
    }

    func testClientWriteConcern() throws {
        // create a client with no options and check its RC
        let client1 = try MongoClient()
        // expect the readConcern property to exist and be default
        expect(client1.writeConcern.isDefault).to(beTrue())

        // expect that a DB created from this client inherits its default WC
        let db1 = try client1.db("test")
        expect(db1.writeConcern.isDefault).to(beTrue())

        // expect that a DB created from this client can override the client's default WC
        let db2 = try client1.db("test", options: DatabaseOptions(writeConcern: try WriteConcern(w: 2)))
        expect(db2.writeConcern.w).to(equal(2))

        client1.close()

        // create a client with w: 1 and check its WC
        let client2 = try MongoClient(options: ClientOptions(writeConcern: try WriteConcern(w: 1)))
        // although w:1 is default, if it is explicitly provided it should be set
        expect(client2.writeConcern.w).to(equal(1))

        // expect that a DB created from this client inherits its WC
        let db3 = try client2.db("test")
        expect(db3.writeConcern.w).to(equal(1))

        // expect that a DB created from this client can override the client's WC
        let db4 = try client2.db("test", options: DatabaseOptions(writeConcern: try WriteConcern(w: 2)))
        expect(db4.writeConcern.w).to(equal(2))

        client2.close()

        // create a client with w:2 and check its WC
        let client3 = try MongoClient(options: ClientOptions(writeConcern: try WriteConcern(w: 2)))
        expect(client3.writeConcern.w).to(equal(2))

        // expect that a DB created from this client can override the client's WC with an unset one
        let db5 = try client3.db("test", options: DatabaseOptions(writeConcern: WriteConcern()))
        expect(db5.writeConcern.isDefault).to(beTrue())

        client3.close()
    }

    func testDatabaseReadConcern() throws {
    	let client = try MongoClient()

    	let db1 = try client.db("test")
    	defer { do { try db1.drop() } catch {} }

    	// expect that a collection created from a DB with unset RC also has unset RC
    	var coll1 = try db1.createCollection("coll1")
    	expect(coll1.readConcern.level).to(beNil())

    	// expect that a collection retrieved from a DB with unset RC also has unset RC
    	coll1 = try db1.collection("coll1")
    	expect(coll1.readConcern.level).to(beNil())

    	// expect that a collection created from a DB with unset RC can override the DB's RC
    	var coll2 = try db1.createCollection("coll2", options: CreateCollectionOptions(readConcern: try ReadConcern(.local)))
    	expect(coll2.readConcern.level).to(equal("local"))

    	// expect that a collection retrieved from a DB with unset RC can override the DB's RC
    	coll2 = try db1.collection("coll2", options: CollectionOptions(readConcern: try ReadConcern(.local)))
    	expect(coll2.readConcern.level).to(equal("local"))

    	try db1.drop()

    	let db2 = try client.db("test", options: DatabaseOptions(readConcern: try ReadConcern(.local)))
    	defer { do { try db2.drop() } catch {} }

    	// expect that a collection created from a DB with local RC also has local RC
    	var coll3 = try db2.createCollection("coll3")
    	expect(coll3.readConcern.level).to(equal("local"))

    	// expect that a collection retrieved from a DB with local RC also has local RC
    	coll3 = try db2.collection("coll3")
    	expect(coll3.readConcern.level).to(equal("local"))

    	// expect that a collection created from a DB with local RC can override the DB's RC
    	var coll4 = try db2.createCollection("coll4", options: CreateCollectionOptions(readConcern: try ReadConcern(.majority)))
    	expect(coll4.readConcern.level).to(equal("majority"))

 		// expect that a collection retrieved from a DB with local RC can override the DB's RC
 		coll4 = try db2.collection("coll4", options: CollectionOptions(readConcern: try ReadConcern(.majority)))
    	expect(coll4.readConcern.level).to(equal("majority"))
    }

    func testDatabaseWriteConcern() throws {
        let client = try MongoClient()

        let db1 = try client.db("test")
        defer { do { try db1.drop() } catch {} }

        // expect that a collection created from a DB with default WC also has default WC
        var coll1 = try db1.createCollection("coll1")
        expect(coll1.writeConcern.isDefault).to(beTrue())

        // expect that a collection retrieved from a DB with default WC also has default WC
        coll1 = try db1.collection("coll1")
        expect(coll1.writeConcern.isDefault).to(beTrue())

        // expect that a collection created from a DB with default WC can override the DB's WC
        var coll2 = try db1.createCollection("coll2", options: CreateCollectionOptions(writeConcern: try WriteConcern(w: 1)))
        expect(coll2.writeConcern.w).to(equal(1))

        // expect that a collection retrieved from a DB with default WC can override the DB's WC
        coll2 = try db1.collection("coll2", options: CollectionOptions(writeConcern: try WriteConcern(w: 1)))
        expect(coll2.writeConcern.w).to(equal(1))

        try db1.drop()

        let db2 = try client.db("test", options: DatabaseOptions(writeConcern: try WriteConcern(w: 1)))
        defer { do { try db2.drop() } catch {} }

        // expect that a collection created from a DB with w:1 also has w:1
        var coll3 = try db2.createCollection("coll3")
        expect(coll3.writeConcern.w).to(equal(1))

        // expect that a collection retrieved from a DB with w:1 also has w:1
        coll3 = try db2.collection("coll3")
        expect(coll3.writeConcern.w).to(equal(1))

        // expect that a collection created from a DB with w:1 can override the DB's WC
        var coll4 = try db2.createCollection("coll4", options: CreateCollectionOptions(writeConcern: try WriteConcern(w: 2)))
        expect(coll4.writeConcern.w).to(equal(2))

        // expect that a collection retrieved from a DB with w:1 can override the DB's WC
        coll4 = try db2.collection("coll4", options: CollectionOptions(writeConcern: try WriteConcern(w: 2)))
        expect(coll4.writeConcern.w).to(equal(2))
    }

    func testOperationReadConcerns() throws {
    	// setup a collection 
    	let client = try MongoClient()
    	let db = try client.db("test")
    	defer { do { try db.drop() } catch {} }
    	let coll = try db.createCollection("coll1")

    	let command: Document = ["count": "coll1"]

    	// run command with a valid readConcern
    	let options1 = RunCommandOptions(readConcern: try ReadConcern(.local))
    	let res1 = try db.runCommand(command, options: options1)
        expect(res1["ok"] as? Double).to(equal(1.0))

        // run command with an empty readConcern
        let options2 = RunCommandOptions(readConcern: ReadConcern())
        let res2 = try db.runCommand(command, options: options2)
        expect(res2["ok"] as? Double).to(equal(1.0))

        // running command with an invalid RC level should throw
        let options3 = RunCommandOptions(readConcern: try ReadConcern("blah"))
        expect(try db.runCommand(command, options: options3)).to(throwError())

        // try various command + read concern pairs to make sure they work
        expect(try coll.find(options: FindOptions(readConcern: try ReadConcern(.local)))).toNot(throwError())

        expect(try coll.aggregate([["$project": ["a": 1] as Document]],
        	options: AggregateOptions(readConcern: try ReadConcern(.majority)))).toNot(throwError())

        expect(try coll.count(options: CountOptions(readConcern: try ReadConcern(.majority)))).toNot(throwError())

        expect(try coll.distinct(fieldName: "a",
        	options: DistinctOptions(readConcern: try ReadConcern(.local)))).toNot(throwError())
    }

    func testOperationWriteConcerns() throws {

    }

    func testConnectionStrings() throws {
    	let csPath = "\(self.getSpecsPath())/read-write-concern/tests/connection-string"
    	let testFiles = try FileManager.default.contentsOfDirectory(atPath: csPath).filter { $0.hasSuffix(".json") }
    	for filename in testFiles {
    		let testFilePath = URL(fileURLWithPath: "\(csPath)/\(filename)")
            let asDocument = try Document(fromJSONFile: testFilePath)
            let tests: [Document] = try asDocument.get("tests")
            for test in tests {
	            let description: String = try test.get("description")
                // skipping because C driver does not comply with these; see CDRIVER-2621
                if description.lowercased().contains("wtimeoutms") { continue }
	            let uri: String = try test.get("uri")
	            let valid: Bool = try test.get("valid")
	            if valid {
                    let client = try MongoClient(connectionString: uri)
                    if let readConcern = test["readConcern"] as? Document {
                        let rc = try ReadConcern(readConcern)
                        expect(client.readConcern).to(equal(rc))
                    } else if let writeConcern = test["writeConcern"] as? Document {
                        let wc = try WriteConcern(writeConcern)
                        expect(client.writeConcern).to(equal(wc))
                    }
	            } else {
	            	expect(try MongoClient(connectionString: uri)).to(throwError())
	            }
            }
    	}
    }

    func testDocuments() throws {
        let docsPath = "\(self.getSpecsPath())/read-write-concern/tests/document"
        let testFiles = try FileManager.default.contentsOfDirectory(atPath: docsPath).filter { $0.hasSuffix(".json") }
        for filename in testFiles {
            let testFilePath = URL(fileURLWithPath: "\(docsPath)/\(filename)")
            let asDocument = try Document(fromJSONFile: testFilePath)
            let tests: [Document] = try asDocument.get("tests")
            for test in tests {
                let description: String = try test.get("description")
                // skipping because C driver does not comply with these; see CDRIVER-2621
                if ["WTimeoutMS as an invalid number", "W as an invalid number"].contains(description) { continue }
                let valid: Bool = try test.get("valid")
                if let rcToUse = test["readConcern"] as? Document {
                    let rc = try ReadConcern(rcToUse)
                    let rcToSend = try ReadConcern(test["readConcernDocument"] as! Document)
                    expect(rcToSend).to(equal(rc))
                } else if let wcToUse = test["writeConcern"] as? Document {
                    if valid {
                        let wc = try WriteConcern(wcToUse)
                        let wcToSend = try WriteConcern(test["writeConcernDocument"] as! Document)
                        expect(wcToSend).to(equal(wc))
                    } else {
                        expect(try WriteConcern(wcToUse)).to(throwError())
                    }
                }
            }
        }
    }
}
