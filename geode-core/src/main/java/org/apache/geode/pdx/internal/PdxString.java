/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.pdx.internal;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.Sendable;
import org.apache.geode.internal.serialization.DSCODE;
import org.apache.geode.internal.tcp.ByteBufferInputStream;

/**
 * A class that references the String offset in PdxInstance Used as Index keys for PdxInstances and
 * query evaluation for PdxInstances
 *
 * @since GemFire 7.0
 */
public class PdxString implements Comparable<PdxString>, Sendable {
  private final byte[] bytes;
  private final int offset;
  private final byte header;
  // private int hash; // optimization: cache the hashcode

  public PdxString(byte[] bytes, int offset) {
    this.bytes = bytes;
    header = bytes[offset];
    this.offset = calcOffset(header, offset);
  }

  public PdxString(String s) {
    var bos = new ByteArrayOutputStream(s.length());
    try {
      DataSerializer.writeString(s, new DataOutputStream(bos));
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    bytes = bos.toByteArray();
    header = bytes[0];
    offset = calcOffset(header, 0);
  }

  private int calcOffset(int header, int offset) {
    offset++; // increment offset for the header byte
    // length is stored as short for small strings
    if (header == DSCODE.STRING_BYTES.toByte() || header == DSCODE.STRING.toByte()) {
      offset += 2; // position the offset to the start of the String
                   // (skipping header and length bytes)
    }
    // length is stored as int for huge strings
    else if (header == DSCODE.HUGE_STRING_BYTES.toByte() || header == DSCODE.HUGE_STRING.toByte()) {
      offset += 4; // position the offset to the start of the String
                   // (skipping header and length bytes)
    }
    return offset;
  }

  private int getLength() {
    var length = 0;
    var lenOffset = offset;
    if (header == DSCODE.STRING_BYTES.toByte() || header == DSCODE.STRING.toByte()) {
      lenOffset -= 2;
      var a = bytes[lenOffset];
      var b = bytes[lenOffset + 1];
      length = ((a & 0xff) << 8) | (b & 0xff);
    }
    // length is stored as int for huge strings
    else if (header == DSCODE.HUGE_STRING_BYTES.toByte() || header == DSCODE.HUGE_STRING.toByte()) {
      lenOffset -= 4;
      var a = bytes[lenOffset];
      var b = bytes[lenOffset + 1];
      var c = bytes[lenOffset + 2];
      var d = bytes[lenOffset + 3];
      length = (((a & 0xff) << 24) | ((b & 0xff) << 16) | ((c & 0xff) << 8) | (d & 0xff));
    }
    return length;
  }

  @Override
  public int compareTo(PdxString o) {
    // not handling strings with different headers
    if (header != o.header) {
      var diff = toString().compareTo(o.toString());
      return diff;
    }
    var len1 = getLength();
    var len2 = o.getLength();
    var n = Math.min(len1, len2);

    var i = offset;
    var j = o.offset;

    if (i == j) {
      var k = i;
      var lim = n + i;
      while (k < lim) {
        var c1 = bytes[k];
        var c2 = o.bytes[k];
        if (c1 != c2) {
          return c1 - c2;
        }
        k++;
      }
    } else {
      while (n-- != 0) {
        var c1 = bytes[i++];
        var c2 = o.bytes[j++];
        if (c1 != c2) {
          return c1 - c2;
        }
      }
    }
    return len1 - len2;
  }

  public int hashCode() {
    var h = 0;
    var len = getLength();
    if (len > 0) {
      var off = offset;
      for (var i = 0; i < len; i++) {
        h = 31 * h + bytes[off++];
      }
    }
    return h;
  }

  public boolean equals(Object anObject) {
    if (this == anObject) {
      return true;
    }
    if (anObject instanceof PdxString) {
      var o = (PdxString) anObject;
      if (header != o.header) { // header needs to be same for Pdxstrings to be equal
        return false;
      }
      var n = getLength();
      if (n == o.getLength()) {
        var i = offset;
        var j = o.offset;
        while (n-- != 0) {
          if (bytes[i++] != o.bytes[j++]) {
            return false;
          }
        }
        return true;
      }
    }
    return false;
  }


  public String toString() {
    String s = null;
    var headerOffset = offset;
    try {
      --headerOffset; // for header byte
      if (header == DSCODE.STRING_BYTES.toByte() || header == DSCODE.STRING.toByte()) {
        headerOffset -= 2; // position the offset to the start of the String (skipping
        // header and length bytes)
      }
      // length is stored as int for huge strings
      else if (header == DSCODE.HUGE_STRING_BYTES.toByte()
          || header == DSCODE.HUGE_STRING.toByte()) {
        headerOffset -= 4;
      }
      var stringByteBuffer =
          ByteBuffer.wrap(bytes, headerOffset, bytes.length - headerOffset); // Wrapping more bytes
                                                                             // than the actual
                                                                             // String bytes in
      // array. Counting on the readString() to read only String
      // bytes
      try (var byteBufferInputStream =
          new ByteBufferInputStream(stringByteBuffer)) {
        s = DataSerializer.readString(byteBufferInputStream);
      }
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    return s;
  }

  @Override
  public void sendTo(DataOutput out) throws IOException {
    var offset = this.offset;
    var len = getLength();
    --offset; // for header byte
    len++;
    if (header == DSCODE.STRING_BYTES.toByte() || header == DSCODE.STRING.toByte()) {
      len += 2;
      offset -= 2;
    } else if (header == DSCODE.HUGE_STRING_BYTES.toByte()
        || header == DSCODE.HUGE_STRING.toByte()) {
      len += 4;
      offset -= 4;
    }
    out.write(bytes, offset, len);
  }

}
