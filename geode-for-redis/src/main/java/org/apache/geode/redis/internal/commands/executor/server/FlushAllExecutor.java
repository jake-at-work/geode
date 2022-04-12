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
package org.apache.geode.redis.internal.commands.executor.server;

import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.internal.cache.LocalDataSet;
import org.apache.geode.redis.internal.commands.Command;
import org.apache.geode.redis.internal.commands.executor.CommandExecutor;
import org.apache.geode.redis.internal.commands.executor.RedisResponse;
import org.apache.geode.redis.internal.commands.executor.key.DelExecutor;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class FlushAllExecutor implements CommandExecutor {

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    var local = (LocalDataSet) PartitionRegionHelper
        .getLocalPrimaryData(context.getRegionProvider().getLocalDataRegion());

    for (var skey : local.keySet()) {
      DelExecutor.del(context, (RedisKey) skey);
    }

    return RedisResponse.ok();
  }

}
