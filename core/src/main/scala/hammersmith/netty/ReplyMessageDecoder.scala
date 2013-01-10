/**
 * Copyright (c) 2011-2013 Brendan W. McAdams <http://evilmonkeylabs.com>
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

package hammersmith
package netty

import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder
import hammersmith.bson.util.Logging
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.channel.Channel
import org.jboss.netty.buffer.ChannelBufferInputStream
import hammersmith.wire.MongoMessage
import hammersmith.wire.ReplyMessage

/**
 * Decoder capable of safely decoding fragmented frames from BSON
 *
 * @TODO - Toggleable setting of maxFrameLength based on server BSON Size
 * (Currently defaults to a max of 4MB)
 */
protected[hammersmith] class ReplyMessageDecoder extends LengthFieldBasedFrameDecoder(1024 * 1024 * 4, 0, 4, -4, 0) with Logging {
  protected override def decode(ctx: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer): AnyRef = {
    val frame = super.decode(ctx, channel, buffer).asInstanceOf[ChannelBuffer]
    if (frame == null) {
      // don't have the whole message yet; netty will retry later
      null
    } else {
      // we have one message (and nothing else) in the "frame" buffer
      MongoMessage.unapply(new ChannelBufferInputStream(frame)) match {
        case reply: ReplyMessage ⇒
          reply
        case default ⇒
          // this should not happen;
          throw new Exception("Unknown message type '%s' incoming from MongoDB; ignoring.".format(default))
      }
    }
  }

  // Because we return a new object rather than a buffer from decode(),
  // we can use slice() here according to the docs (the slice won't escape
  // the decode() method so it will be valid while we're using it)
  protected override def extractFrame(buffer: ChannelBuffer, index: Int, length: Int): ChannelBuffer = {
    return buffer.slice(index, length);
  }
}