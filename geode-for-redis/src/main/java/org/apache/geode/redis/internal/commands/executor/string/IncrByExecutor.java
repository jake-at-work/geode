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
package org.apache.geode.redis.internal.commands.executor.string;



import org.apache.geode.redis.internal.commands.Command;
import org.apache.geode.redis.internal.commands.executor.CommandExecutor;
import org.apache.geode.redis.internal.commands.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class IncrByExecutor implements CommandExecutor {

  private static final String ERROR_INCREMENT_NOT_USABLE =
      "The increment on this key must be numeric";

  private static final int INCREMENT_INDEX = 2;

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    var commandElems = command.getProcessedCommand();
    var region = context.getRegion();
    var key = command.getKey();

    var incrArray = commandElems.get(INCREMENT_INDEX);
    long increment;

    try {
      increment = Coder.bytesToLong(incrArray);
    } catch (NumberFormatException e) {
      return RedisResponse.error(ERROR_INCREMENT_NOT_USABLE);
    }

    var value = context.stringLockedExecute(key, false,
        string -> string.incrby(region, key, increment));

    return RedisResponse.integer(value);
  }
}
