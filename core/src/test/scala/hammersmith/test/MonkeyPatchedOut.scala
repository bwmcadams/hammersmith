
package com.mongodb

import org.bson.BasicBSONEncoder
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import hammersmith.collection.{BSONDocument}

object `package` {

  val encoder = DefaultDBEncoder.FACTORY.create()
  val decoder = DefaultDBDecoder.FACTORY.create()

  val legacyConn = new Mongo

  def legacyDelete(id: Any) = {
    val q = new BasicDBObject
    q.put("_id", "1234")
    val om = OutMessage.remove(legacyConn.getDB("test").getCollection("deletion"), encoder, q)
    om.prepare()
    val out = new ByteArrayOutputStream
    om.pipe(out)
    out.toByteArray

  }


  def parseLegacyResponse(data: Array[Byte]) =
    new Response(legacyConn.getAddress, legacyConn.getDB("test").getCollection("deletion"),
                                 new ByteArrayInputStream(data), decoder)

  def legacyGetMore(numReturn: Int, cursorID: Long) = {
    val om = OutMessage.getMore(legacyConn.getDB("test").getCollection("getMore"), cursorID, numReturn)
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

}
