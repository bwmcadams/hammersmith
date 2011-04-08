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

import com.mongodb.MongoConnection
import org.bson.util.{ Logger, Logging }
import org.specs2.mutable._
import org.specs2.runner._

class DirectConnectionSpec extends SpecificationWithJUnit with Logging {
  //  println(org.apache.commons.logging.Log)

  "The MongoDB Direct Connection" should {
    "Connect correctly and grab isMaster" in {
      val conn = MongoConnection("localhost")
      conn must not beNull
    }
  }
}