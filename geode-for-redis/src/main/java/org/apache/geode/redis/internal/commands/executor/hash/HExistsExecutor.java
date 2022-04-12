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
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

/**
 * <pre>
 *
 * Implements the Redis HEXISTS command to determine if a hash field exists for a given key.
 *
 * Examples:
 *
 * redis> HSET myhash field1 "foo"
 * (integer) 1
 * redis> HEXISTS myhash field1
 * (integer) 1
 * redis> HEXISTS myhash field2
 * (integer) 0
 *
 * </pre>
 */
public class HExistsExecutor implements CommandExecutor {

  @Override
  public RedisResponse executeCommand(Command command,
      ExecutionHandlerContext context) {
    var commandElems = command.getProcessedCommand();

    var field = commandElems.get(2);
    var key = command.getKey();
    int result = context.hashLockedExecute(key, true, hash -> hash.hexists(field));

    return RedisResponse.integer(result);
  }

}
