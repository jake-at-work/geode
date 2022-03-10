/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.geode.internal.net;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class SslUtils {
  static final int SSL_CONTENT_TYPE_CHANGE_CIPHER_SPEC = 20;

  static final int SSL_CONTENT_TYPE_ALERT = 21;

  static final int SSL_CONTENT_TYPE_HANDSHAKE = 22;

  static final int SSL_CONTENT_TYPE_APPLICATION_DATA = 23;

  static final int SSL_CONTENT_TYPE_EXTENSION_HEARTBEAT = 24;

  static final int SSL_RECORD_HEADER_LENGTH = 5;

  static final int GMSSL_PROTOCOL_VERSION = 0x101;

  static final int NOT_ENOUGH_DATA = -1;

  static final int NOT_ENCRYPTED = -2;

  static int getEncryptedPacketLength(ByteBuffer buffer) {
    int packetLength = 0;
    int pos = buffer.position();
    // SSLv3 or TLS - Check ContentType
    boolean tls;
    switch (unsignedByte(buffer.get(pos))) {
      case SSL_CONTENT_TYPE_CHANGE_CIPHER_SPEC:
      case SSL_CONTENT_TYPE_ALERT:
      case SSL_CONTENT_TYPE_HANDSHAKE:
      case SSL_CONTENT_TYPE_APPLICATION_DATA:
      case SSL_CONTENT_TYPE_EXTENSION_HEARTBEAT:
        tls = true;
        break;
      default:
        // SSLv2 or bad data
        tls = false;
    }

    if (tls) {
      // SSLv3 or TLS or GMSSLv1.0 or GMSSLv1.1 - Check ProtocolVersion
      int majorVersion = unsignedByte(buffer.get(pos + 1));
      if (majorVersion == 3 || buffer.getShort(pos + 1) == GMSSL_PROTOCOL_VERSION) {
        // SSLv3 or TLS or GMSSLv1.0 or GMSSLv1.1
        packetLength = unsignedShortBE(buffer, pos + 3) + SSL_RECORD_HEADER_LENGTH;
        if (packetLength <= SSL_RECORD_HEADER_LENGTH) {
          // Neither SSLv3 or TLSv1 (i.e. SSLv2 or bad data)
          tls = false;
        }
      } else {
        // Neither SSLv3 or TLSv1 (i.e. SSLv2 or bad data)
        tls = false;
      }
    }

    if (!tls) {
      // SSLv2 or bad data - Check the version
      int headerLength = (unsignedByte(buffer.get(pos)) & 0x80) != 0 ? 2 : 3;
      int majorVersion = unsignedByte(buffer.get(pos + headerLength + 1));
      if (majorVersion == 2 || majorVersion == 3) {
        // SSLv2
        packetLength = headerLength == 2 ?
            (shortBE(buffer, pos) & 0x7FFF) + 2 : (shortBE(buffer, pos) & 0x3FFF) + 3;
        if (packetLength <= headerLength) {
          return NOT_ENOUGH_DATA;
        }
      } else {
        return NOT_ENCRYPTED;
      }
    }
    return packetLength;
  }

  private static short unsignedByte(byte b) {
    return (short) (b & 0xFF);
  }

  private static int unsignedShortBE(ByteBuffer buffer, int offset) {
    return shortBE(buffer, offset) & 0xFFFF;
  }

  private static short shortBE(ByteBuffer buffer, int offset) {
    return buffer.order() == ByteOrder.BIG_ENDIAN ?
        buffer.getShort(offset) : Short.reverseBytes(buffer.getShort(offset));
  }

}

