/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Changes for TIBCO Project SnappyData data platform.
 *
 * Portions Copyright (c) 2017-2020 TIBCO Software Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package org.apache.spark.util

import java.io.{EOFException, IOException, ObjectInputStream, ObjectOutputStream}
import java.nio.ByteBuffer
import java.nio.channels.Channels

import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.esotericsoftware.kryo.io.{Input, Output}

/**
 * A wrapper around a java.nio.ByteBuffer that is serializable through Java serialization, to make
 * it easier to pass ByteBuffers in case class messages.
 */
private[spark]
class SerializableBuffer(@transient var buffer: ByteBuffer)
    extends Serializable with KryoSerializable {

  def value: ByteBuffer = buffer

  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    val length = in.readInt()
    buffer = ByteBuffer.allocate(length)
    var amountRead = 0
    val channel = Channels.newChannel(in)
    while (amountRead < length) {
      val ret = channel.read(buffer)
      if (ret == -1) {
        throw new EOFException("End of file before fully reading buffer")
      }
      amountRead += ret
    }
    buffer.rewind() // Allow us to read it later
  }

  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    out.writeInt(buffer.limit())
    if (Channels.newChannel(out).write(buffer) != buffer.limit()) {
      throw new IOException("Could not fully write buffer to output stream")
    }
    buffer.rewind() // Allow us to write it again later
  }

  override def write(kryo: Kryo, output: Output) {
    if (buffer.position() != 0) {
      throw new IOException(s"Unexpected buffer position ${buffer.position()}")
    }
    output.writeInt(buffer.limit())
    output.writeBytes(buffer.array(), buffer.arrayOffset(), buffer.limit())
  }

  override def read(kryo: Kryo, input: Input) {
    val length = input.readInt()
    val b = new Array[Byte](length)
    input.readBytes(b)
    buffer = ByteBuffer.wrap(b)
    buffer.rewind() // Allow us to read it later
  }
}
