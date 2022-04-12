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
 *
 */
package org.apache.geode.redis.internal.commands.executor.key;

import static org.apache.geode.redis.internal.RedisConstants.ERROR_CURSOR;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;
import static org.apache.geode.redis.internal.netty.Coder.bytesToLong;
import static org.apache.geode.redis.internal.netty.Coder.equalsIgnoreCaseBytes;
import static org.apache.geode.redis.internal.netty.Coder.narrowLongToInt;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.COUNT;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.MATCH;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import org.apache.geode.redis.internal.commands.Command;
import org.apache.geode.redis.internal.commands.executor.GlobPattern;
import org.apache.geode.redis.internal.commands.executor.RedisResponse;
import org.apache.geode.redis.internal.data.RedisDataType;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class ScanExecutor extends AbstractScanExecutor {
  private static final BigInteger UNSIGNED_LONG_CAPACITY = new BigInteger("18446744073709551615");

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    var commandElems = command.getProcessedCommand();

    var cursorString = command.getStringKey();
    BigInteger cursor;
    byte[] globPattern = null;
    var count = DEFAULT_COUNT;

    try {
      cursor = new BigInteger(cursorString).abs();
    } catch (NumberFormatException e) {
      return RedisResponse.error(ERROR_CURSOR);
    }

    if (cursor.compareTo(UNSIGNED_LONG_CAPACITY) > 0) {
      return RedisResponse.error(ERROR_CURSOR);
    }

    if (!cursor.equals(context.getScanCursor())) {
      cursor = new BigInteger("0");
    }

    for (var i = 2; i < commandElems.size(); i = i + 2) {
      var commandElemBytes = commandElems.get(i);
      if (equalsIgnoreCaseBytes(commandElemBytes, MATCH)) {
        commandElemBytes = commandElems.get(i + 1);
        globPattern = commandElemBytes;

      } else if (equalsIgnoreCaseBytes(commandElemBytes, COUNT)) {
        commandElemBytes = commandElems.get(i + 1);
        try {
          count = narrowLongToInt(bytesToLong(commandElemBytes));
        } catch (NumberFormatException e) {
          return RedisResponse.error(ERROR_NOT_INTEGER);
        }

        if (count < 1) {
          return RedisResponse.error(ERROR_SYNTAX);
        }

      } else {
        return RedisResponse.error(ERROR_SYNTAX);
      }
    }

    var scanResult =
        scan(context.getRegion().keySet(), convertGlobToRegex(globPattern), count, cursor);
    context.setScanCursor(scanResult.getLeft());

    return RedisResponse.scan(scanResult.getLeft().intValue(), scanResult.getRight());
  }

  private Pair<BigInteger, List<Object>> scan(Collection<RedisKey> list,
      GlobPattern matchPattern,
      int count, BigInteger cursor) {
    List<Object> returnList = new ArrayList<>();
    var size = list.size();
    var beforeCursor = new BigInteger("0");
    var numElements = 0;
    var i = -1;
    for (var key : list) {
      i++;
      if (beforeCursor.compareTo(cursor) < 0) {
        beforeCursor = beforeCursor.add(new BigInteger("1"));
        continue;
      }

      if (matchPattern != null) {
        if (matchPattern.matches(key.toBytes())) {
          returnList.add(key);
          numElements++;
        }
      } else {
        returnList.add(key);
        numElements++;
      }
      if (numElements == count) {
        break;
      }
    }

    Pair<BigInteger, List<Object>> scanResult;
    if (i >= size - 1) {
      scanResult = new ImmutablePair<>(new BigInteger("0"), returnList);
    } else {
      scanResult = new ImmutablePair<>(new BigInteger(String.valueOf(i + 1)), returnList);
    }
    return scanResult;
  }

  // TODO: When SCAN is supported, refactor to use these methods and not override executeCommand()
  @Override
  protected Pair<Integer, List<byte[]>> executeScan(ExecutionHandlerContext context, RedisKey key,
      GlobPattern pattern, int count, int cursor) {
    return null;
  }

  @Override
  protected RedisDataType getDataType() {
    return null;
  }
}
