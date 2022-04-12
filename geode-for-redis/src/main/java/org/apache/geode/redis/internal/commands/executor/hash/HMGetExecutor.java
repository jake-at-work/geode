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
 * Implementation of the HMGET command.
 * Returns values associated with the specified fields in the hash stored for a given key.
 *
 * Examples:
 *
 * redis> HSET myhash field1 "Hello"
 * (integer) 1
 * redis> HSET myhash field2 "World"
 * (integer) 1
 * redis> HMGET myhash field1 field2 nofield
 * 1) "Hello"
 * 2) "World"
 * 3) (nil)
 *
 * </pre>
 */
public class HMGetExecutor implements CommandExecutor {

  @Override
  public RedisResponse executeCommand(Command command,
      ExecutionHandlerContext context) {

    var key = command.getKey();
    var commandElements = command.getProcessedCommand();
    var fields = commandElements.subList(2, commandElements.size());

    var values = context.hashLockedExecute(key, true, hash -> hash.hmget(fields));

    return RedisResponse.array(values, true);
  }
}
