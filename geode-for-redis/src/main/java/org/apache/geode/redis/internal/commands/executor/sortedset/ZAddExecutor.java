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
package org.apache.geode.redis.internal.commands.executor.sortedset;

import static org.apache.geode.redis.internal.RedisConstants.ERROR_INVALID_ZADD_OPTION_NX_XX;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_A_VALID_FLOAT;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_ZADD_OPTION_TOO_MANY_INCR_PAIR;
import static org.apache.geode.redis.internal.netty.Coder.isInfinity;
import static org.apache.geode.redis.internal.netty.Coder.toUpperCaseBytes;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.CH;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.INCR;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.NX;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.XX;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.geode.redis.internal.commands.Command;
import org.apache.geode.redis.internal.commands.executor.CommandExecutor;
import org.apache.geode.redis.internal.commands.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class ZAddExecutor implements CommandExecutor {

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    var zAddExecutorState = new ZAddExecutorState();
    var commandElements = command.getProcessedCommand();
    var region = context.getRegion();
    var key = command.getKey();

    var commandIterator = commandElements.iterator();
    skipCommandAndKey(commandIterator);

    var optionsFoundCount = findAndValidateZAddOptions(command, commandIterator, zAddExecutorState);
    if (zAddExecutorState.exceptionMessage != null) {
      return RedisResponse.error(zAddExecutorState.exceptionMessage);
    }

    var size = (commandElements.size() - optionsFoundCount - 2) / 2;
    List<byte[]> members = new ArrayList<>(size);
    var scores = new double[size];
    var n = 0;
    for (var i = optionsFoundCount + 2; i < commandElements.size(); i += 2, n++) {
      try {
        scores[n] = Coder.bytesToDouble(commandElements.get(i));
      } catch (NumberFormatException e) {
        throw new NumberFormatException(ERROR_NOT_A_VALID_FLOAT);
      }
      members.add(commandElements.get(i + 1));
    }
    var options = makeOptions(zAddExecutorState);
    var retVal = context.sortedSetLockedExecute(key, false,
        zset -> zset.zadd(region, key, members, scores, options));

    if (zAddExecutorState.incrFound) {
      if (retVal == null) {
        return RedisResponse.nil();
      }
      return RedisResponse.bulkString(retVal);
    }

    return RedisResponse.integer((int) retVal);
  }

  private void skipCommandAndKey(Iterator<byte[]> commandIterator) {
    commandIterator.next();
    commandIterator.next();
  }

  private int findAndValidateZAddOptions(Command command, Iterator<byte[]> commandIterator,
      ZAddExecutorState executorState) {
    var scoreFound = false;
    var optionsFoundCount = 0;

    while (commandIterator.hasNext() && !scoreFound) {
      var subCommand = toUpperCaseBytes(commandIterator.next());
      if (Arrays.equals(subCommand, NX)) {
        executorState.nxFound = true;
        optionsFoundCount++;
      } else if (Arrays.equals(subCommand, XX)) {
        executorState.xxFound = true;
        optionsFoundCount++;
      } else if (Arrays.equals(subCommand, CH)) {
        executorState.chFound = true;
        optionsFoundCount++;
      } else if (Arrays.equals(subCommand, INCR)) {
        executorState.incrFound = true;
        optionsFoundCount++;
      } else if (isInfinity(subCommand)) {
        scoreFound = true;
      } else {
        // assume it's a double; if not, this will throw exception later
        scoreFound = true;
      }
    }
    if ((command.getProcessedCommand().size() - optionsFoundCount - 2) % 2 != 0) {
      executorState.exceptionMessage = ERROR_SYNTAX;
    } else if (executorState.nxFound && executorState.xxFound) {
      executorState.exceptionMessage = ERROR_INVALID_ZADD_OPTION_NX_XX;
    } else if (executorState.incrFound
        && command.getProcessedCommand().size() - optionsFoundCount - 2 > 2) {
      executorState.exceptionMessage = ERROR_ZADD_OPTION_TOO_MANY_INCR_PAIR;
    }
    return optionsFoundCount;
  }

  private ZAddOptions makeOptions(ZAddExecutorState executorState) {
    var existsOption = ZAddOptions.Exists.NONE;

    if (executorState.nxFound) {
      existsOption = ZAddOptions.Exists.NX;
    }
    if (executorState.xxFound) {
      existsOption = ZAddOptions.Exists.XX;
    }
    return new ZAddOptions(existsOption, executorState.chFound, executorState.incrFound);
  }

  private static class ZAddExecutorState {
    private boolean nxFound = false;
    private boolean xxFound = false;
    private boolean chFound = false;
    private boolean incrFound = false;
    private String exceptionMessage = null;
  }
}
