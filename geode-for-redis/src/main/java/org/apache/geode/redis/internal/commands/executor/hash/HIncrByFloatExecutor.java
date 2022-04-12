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
package org.apache.geode.redis.internal.commands.executor.hash;



import org.apache.geode.redis.internal.commands.Command;
import org.apache.geode.redis.internal.commands.executor.CommandExecutor;
import org.apache.geode.redis.internal.commands.executor.RedisResponse;
import org.apache.geode.redis.internal.commands.executor.string.IncrByFloatExecutor;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

/**
 * <pre>
 * Implementation of HINCRBYFLOAT Redis command.
 * The purpose is to increment the specified field of a hash for a given key.
 *  The value is floating number (represented as a double), by the specified increment.
 *
 * Examples:
 *
 * redis> HSET mykey field 10.50
 * (integer) 1
 * redis> HINCRBYFLOAT mykey field 0.1
 * "10.6"
 * redis> HINCRBYFLOAT mykey field -5
 * "5.6"
 * redis> HSET mykey field 5.0e3
 * (integer) 0
 * redis> HINCRBYFLOAT mykey field 2.0e2
 * "5200"
 *
 *
 * </pre>
 */
public class HIncrByFloatExecutor implements CommandExecutor {

  private static final int INCREMENT_INDEX = 3;

  @Override
  public RedisResponse executeCommand(Command command,
      ExecutionHandlerContext context) {
    var commandElems = command.getProcessedCommand();

    var validated =
        IncrByFloatExecutor.validateIncrByFloatArgument(commandElems.get(INCREMENT_INDEX));
    if (validated.getRight() != null) {
      return validated.getRight();
    }

    var region = context.getRegion();
    var key = command.getKey();
    var field = commandElems.get(2);

    var value = context.hashLockedExecute(key, false,
        hash -> hash.hincrbyfloat(region, key, field, validated.getLeft()));

    return RedisResponse.bigDecimal(value);
  }

}
