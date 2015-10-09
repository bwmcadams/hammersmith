
package com.mongodb

import com.mongodb.{DefaultDBDecoder, DefaultDBEncoder, DBObject, OutMessage}
import org.bson.BasicBSONEncoder
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import codes.bytes.hammersmith.collection.{BSONDocument}
import codes.bytes.hammersmith.wire.QueryMessage

// old syntax but easiest for this patching
object `package` {

  val encoder = DefaultDBEncoder.FACTORY.create()
  val decoder = DefaultDBDecoder.FACTORY.create()

  /**
   * This should maintain a reset requestID for testing.
   * @return
   */
  def legacyConn = new Mongo

  def legacyDelete(id: Any) = {
    val q = new BasicDBObject
    q.put("_id", "1234")
    val om = OutMessage.remove(legacyConn.getDB("test").getCollection("deletion"), encoder, q)
    _out(om)
  }


  def parseLegacyResponse(data: Array[Byte]) =
    new Response(legacyConn.getAddress, legacyConn.getDB("test").getCollection("deletion"),
                                 new ByteArrayInputStream(data), decoder)

  def legacyGetMore(numReturn: Int, cursorID: Long) = {
    val om = OutMessage.getMore(legacyConn.getDB("test").getCollection("getMore"), cursorID, numReturn)
    _out(om)
  }

  def _out(om: OutMessage) = {
    om.prepare()
    val out = new ByteArrayOutputStream
    om.pipe(out)
    out.toByteArray
  }

  /* can't do right now */
  /*def legacyInsert(docs: BSONDocument*) = {
    val om = OutMessage.insert(legacyConn.getDB("test").getCollection("insert"), false, docs)
    om.prepare()
    val out = new ByteArrayOutputStream
    om.pipe(out)
    out.toByteArray
  }*/

  def legacyKillCursors(ids: Seq[Long]) = {
    val om = OutMessage.killCursors(legacyConn, ids.length)
    ids foreach { om.writeLong }
    _out(om)
  }

  def legacyQuery(ns: String, numSkip: Int, numReturn: Int, q: DBObject,
            fields: Option[DBObject] = None, tailable: Boolean = false,
            slaveOkay: Boolean = false, disableCursorTimeout: Boolean = false, await: Boolean = false,
            exhaust: Boolean = false, partial: Boolean = false) = {
    val dbColl = ns.split('.')
    // todo - flags for options
    val om = OutMessage.query(legacyConn.getDB(dbColl(0)).getCollection(dbColl(1)), 0, numSkip, numReturn, q, fields.getOrElse(null))
    _out(om)
  }

  def legacyUpdate(ns: String, query: DBObject, update: DBObject, upsert: Boolean, multi: Boolean) = {
    val dbColl = ns.split('.')
    // todo - flags for options
    val om = OutMessage.update(legacyConn.getDB(dbColl(0)).getCollection(dbColl(1)), encoder, upsert, multi, query, update)
    _out(om)
  }

}
