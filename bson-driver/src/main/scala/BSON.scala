/**
 * Copyright (c) 2010, 2011 10gen, Inc. <http://10gen.com>

 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.bson.scala

import scala.io.{Source, UTF8Codec}

import java.io.{IOException, InputStream, ByteArrayInputStream}

/** 
 * Core Object for handling BSON, provides extractors and 
 * apply methods for parsing
 * 
 * @author Brendan W. McAdams <brendan@10gen.com>
 * @version 1.0, 12/10/10
 * @since 1.0
 */
object BSON extends util.Logging {
  
  implicit val codec = UTF8Codec 

  def unapply(bytes: Array[Byte]) = 
    unapply()(new ByteArrayInputStream(bytes))

  def unapply(implicit stream: InputStream) = try {
    log.debug("Beginning decode of BSON from InputStream (%s)", stream)
    // Grab the header bytes
    val len = stream.read()
    log.debug("Expecting BSON document of %d length.", len)
    
    for (i <- 0 until len) {


    }

  } 
  catch {
    case e => throw new InvalidBSONException("Decoding failed with " + e.getMessage(), e)
    
  }


}

// vim: set ts=2 sw=2 sts=2 et:
