
0.2.7 / 2011-07-11
==================

  * Added fromURI Method to MongoConnection which will connect based on a Mongo URI
  * Added parsing of URI Options, but not fully tested yet.
  * Hosts now return a tuple of (String, Int) for URIs always
  * MongoURI parsing support
  * Didnt update unit test for change of findAndModify to Option[T]

0.2.6 / 2011-07-09 
==================

  * Fixes #34 - FindAndModify / FindAndRemove now Require Option[], and have their own wrapper type - Some stub work around a custom error type for not found which   proved... less useful than desired but hooked for figuring out None
  * Cleanup some logging spam
  * Scalariform cleanups..  Unfortunately it made a mess of my pretty specs2 class formatting :/
  * Wire FindAndModify to use a custom wrapper object, only decoding user defined type class objects for 'value'
  * Re-enabled Scalariform plugin
  * Beginning of code correction for proper testing of Binary data
  * Correct boolean test for whether or not we're connected.
  * Cleaning up connection handling so that we defer any operations until after isMaster succeeds.
  * Merge pull request #30 from havocp/support-immutable-checkID
  * Make SerializableBSONObject.checkID return a new doc, so it can work with immutable doc types

0.2.5 / 2011-07-03 
==================

  * Was using same collection name to test batch inserts in both concurrency and normal test which caused test failures.  Fixed.
  * Change CompletableWriteRequest to throw an exception in same cases Java driver would [havocp]
  * Convert null on the wire to an absent document field rather than None [havocp]
  * In boolCmdResult, do get("ok") not getAs[Double] [havocp]
  * Make BSONDocument.checkedCast handle AnyVal types [havocp]
  * In BSONDocument.getAs and friends, do a runtime type check [havocp]
  * add fields parameter to findOneByID() [havocp]
  * Fix tests to reflect that count() now has parameters [havocp] 
  * Add query, fields, skip, limit parameters to count() [havocp]
  * Handle DeleteMessage and UpdateMessage replies in addition to InsertMessage [havocp]

0.2.0 / 2011-06-29 
==================

  * Sorted out remaining issues w/ Type Class move.
  * Programming is hard, let's go shopping!
  * Code weirdly hanging & dropping in 4th decode  of message.  ARGH
  * Closer to sane on some of the "Working" parts here.  Figuring out the type class flow from the read side has been painful mentally and compile wise
  * Verify time to tieout on concurrent inserts
  * Added a test to validate that concurrent inserts succeed.
  * New encoding system in place with type classes
  * Broken checkpoint; almost done a refactor into Type Class based serialization
  * Tweak deps
  * Fixed project defs.
  * Made all tests run concurrently on a single connection.
  * Decided using chained ThrownExpectations is a bad approach as noted by several people.  Will build up shared examples etc instead.
  * Was testing a value after the callback was defined, NOT after the call. ThrownExpectations exposed this brokenness.
  * Specs2 doesn't fail unless the LAST matcher does; In some cases I need more discreet multi matching for concurrency testing.  Mixed in ThrownExpectations trait to change this.

0.1.0 / 2011-06-01 
==================

  * Fixes #28 - FindAndModify tests out, as does findAndRemove.  Also added Db/Coll level interfaces
  * In pretty good shape; need to iron out a bit more in findAndRemove but my battery has about 2 minutes left and next compile will nuke it.
  * Last few sets had some weird logical confusions from me being half alseep.  All works now although the rewriteID stuff is wonky and disabled.  Does it matter if _id isn't first?!?!?
  * Corrections to index creation behavior; still need an ensureindex cache
  * Count command
  * Implemented a batch insert, with insert now taking only a single doc per for sanity.  Batch Insert needs to also be tweaked to return a Seq[_id]
  * Fixes #27 - Some inverted logic and duplicate code loops was causing ObjectID insertion to skip and/or break
  * Now validating that writes succeeded by actually looking for the data int he database (novel idea, I know)
  * First layout of findAndModify
  * Migrated Specs to acceptance spec
  * CursorCleaner wasn't able to create consistent WeakRefs from call to call; switching to a WeakHashMap was the proper solution
  * Added an implicit conversion for "NOOP" Writes, (aka blind inserts) and a test of the behavior.
  * Refs #15, Fixes #7: Wired remaining code for writes     
    - Generate ID when it isn't there     
    - Validate keys     
    - Ensure getLastError semantics as a 'batch'
    - Callback to write if no safe mode    
    - Adjustments to MongoClientWriteMessage to have helper attributes
  * Added support for 'distinct'
  * Finished laying out find, createIndex, insert, update remove methods
  * Moved the Docs queue on Cursors to the new ConcurrentQueue implementation to ensure thread safety and avoid any blocking.
  * Refs #20 - Finished first pass at Cursor cleanup, with a custom ConcurrentQueue Scala API sitting on top of ConcurrentLinkedQueue.
  * Finished instrumenting remaining wire protocol messages with apply() objects
  * Refs #20 Laying the groundwork for Cursor Cleanup...    
    - Built out a timer infrastructure which reference counts registered ConnectionHandlers    
    - Unclean cursors which are cleaned or finalized are added to their handlers' dead cursors list     
    - When the scheduled callback clean runs, each handler is asked to clean its own dead cursors
  * Protected foreach method.
  * Fixed a stupid scoping and thus timing bug on callbacks.
  * Migrate Document / BSONDocument tree into an "org.bson.collection" area, likely to become Mutable / Immutable delineated at a later date.
  * Migrated base MongoDB package to com.mongodb.async
  * Added a getAsOrElse Method for castable orElse-ing
  * Switched to Either[Throwable, A] for results, provided a "SimpleRequestFutures" version which swallows exceptions.
  * Fixes #13 Defaults wired in, chain upwards to prior level using Option[WriteConcern]
  * Implemented Authentication.
  * Attempt to detect an empty queue condition WITHOUT an exception for a potential (perceived?)  performance boost.
  * Added an internal only 'basicIter' helper which is protected[mongodb] for default Iteration with Batch fetch behavior.
  * Iteratee Pattern now implemented for Cursors.  It made my brain hurt but it's a far cleaner methodology.
  * Store the request message with the future in a "CompletableRequest" in the dispatcher map
  * Settable Batch Size 
  * Added Twitter util-core for Future and Channels
  * Refs #11 
    - A few more utility methods on BSON Lists.  We need a constructor that takes an existing list, and implicits.
    - Saner implementation of BSONList, with an asList method to convert to a normal list
    - List Representation and cleanup of some of the base traits for Documents
  * Added in some of Casbah's Syntactic Sugar for BSONDocument
  * Allocate default dynamic buffer size on write to maxBSON if it's set, otherwise default of <1.8's 4 mb
  * Cleaned up BSON Size / isMaster check to use foreach
  * Netty's default assumes that the length is NOT inclusive of the header, while in BSON it is.  Needed to adjust frame length to -4.
  * For now, removing deserializer from SerializableBSONObject trait as it makes little sense until i lay that side of the API out
  * Deserializer structuring, similar PartialFunction syntax to the other
  * Swapped the writes of wire protocol to use the new BSONSerializer instead
  * Decoding framework with support for most Scala types baked in, use of partialfunction for decoding so you can customise in a subclass
  * Framework for basic connections via Netty
  * Wire Protocol messages laid out 
