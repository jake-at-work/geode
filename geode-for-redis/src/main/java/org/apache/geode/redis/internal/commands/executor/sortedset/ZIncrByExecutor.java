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



import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_A_VALID_FLOAT;

import org.apache.geode.redis.internal.commands.Command;
import org.apache.geode.redis.internal.commands.executor.CommandExecutor;
import org.apache.geode.redis.internal.commands.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class ZIncrByExecutor implements CommandExecutor {
  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    var commandElements = command.getProcessedCommand();
    var region = context.getRegion();
    var key = command.getKey();

    double increment;
    try {
      increment = Coder.bytesToDouble(commandElements.get(2));
    } catch (NumberFormatException e) {
      return RedisResponse.error(ERROR_NOT_A_VALID_FLOAT);
    }
    var member = commandElements.get(3);

    var retVal = context.sortedSetLockedExecute(key, false,
        zset -> zset.zincrby(region, key, increment, member));

    return RedisResponse.bulkString(retVal);
  }
}
