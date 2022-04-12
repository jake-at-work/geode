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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;

import org.apache.geode.redis.internal.commands.Command;
import org.apache.geode.redis.internal.commands.executor.CommandExecutor;
import org.apache.geode.redis.internal.commands.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class ExpireExecutor implements CommandExecutor {

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    var commandElems = command.getProcessedCommand();
    var SECONDS_INDEX = 2;

    var key = command.getKey();
    var delayByteArray = commandElems.get(SECONDS_INDEX);
    long delay;
    try {
      delay = Coder.bytesToLong(delayByteArray);
    } catch (NumberFormatException e) {
      return RedisResponse.error(ERROR_NOT_INTEGER);
    }

    if (!timeUnitMillis()) {
      delay = SECONDS.toMillis(delay);
    }

    var timestamp = System.currentTimeMillis() + delay;

    int result =
        context.dataLockedExecute(key, false,
            data -> data.pexpireat(context.getRegionProvider(), key, timestamp));

    return RedisResponse.integer(result);
  }

  /*
   * Overridden by PExpire
   */
  protected boolean timeUnitMillis() {
    return false;
  }

}
