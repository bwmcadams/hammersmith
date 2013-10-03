
package com.mongodb

import org.bson.BasicBSONEncoder
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

object `package` {

  val encoder = DefaultDBEncoder.FACTORY.create()
  val decoder = DefaultDBDecoder.FACTORY.create()

  val legacyConn = new Mongo

  lazy val legacyDelete = {
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
}
