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
package org.apache.geode.internal.memcached.commands;

import java.nio.ByteBuffer;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.internal.memcached.Reply;
import org.apache.geode.internal.memcached.RequestReader;
import org.apache.geode.internal.memcached.ResponseStatus;
import org.apache.geode.memcached.GemFireMemcachedServer.Protocol;

/**
 * The command "delete" allows for explicit deletion of items: delete <key> [noreply]\r\n
 *
 *
 */
public class DeleteCommand extends AbstractCommand {

  @Override
  public ByteBuffer processCommand(RequestReader request, Protocol protocol, Cache cache) {
    if (protocol == Protocol.ASCII) {
      return processAsciiCommand(request.getRequest(), cache);
    }
    return processBinaryCommand(request, cache);
  }

  private ByteBuffer processAsciiCommand(ByteBuffer buffer, Cache cache) {
    var flb = getFirstLineBuffer();
    getAsciiDecoder().reset();
    getAsciiDecoder().decode(buffer, flb, false);
    flb.flip();
    var firstLine = getFirstLine();
    var firstLineElements = firstLine.split(" ");

    assert "delete".equals(firstLineElements[0]);
    var key = stripNewline(firstLineElements[1]);
    var noReply = firstLineElements.length > 2;
    var r = getMemcachedRegion(cache);
    String reply = null;
    try {
      r.destroy(key);
      reply = Reply.DELETED.toString();
    } catch (EntryNotFoundException e) {
      reply = Reply.NOT_FOUND.toString();
    }
    return noReply ? null : asciiCharset.encode(reply);
  }

  private ByteBuffer processBinaryCommand(RequestReader request, Cache cache) {
    var buffer = request.getRequest();
    var response = request.getResponse();

    var key = getKey(buffer, HEADER_LENGTH);

    var r = getMemcachedRegion(cache);
    try {
      r.destroy(key);
      if (isQuiet()) {
        return null;
      }
      response.putShort(POSITION_RESPONSE_STATUS, ResponseStatus.NO_ERROR.asShort());
    } catch (EntryNotFoundException e) {
      response.putShort(POSITION_RESPONSE_STATUS, ResponseStatus.KEY_NOT_FOUND.asShort());
    } catch (Exception e) {
      response = handleBinaryException(key, request, response, "delete", e);
    }
    if (getLogger().fineEnabled()) {
      getLogger().fine("delete:key:" + key);
    }
    return response;
  }

  /**
   * Overridden by Q command
   */
  protected boolean isQuiet() {
    return false;
  }
}
