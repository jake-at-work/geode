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
package org.apache.geode.management.internal.cli.functions;

import java.util.Map;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.ConfigSource;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;

public class AlterRuntimeConfigFunction implements InternalFunction<Map<String, String>> {
  private static final long serialVersionUID = 1L;
  private static final Logger logger = LogService.getLogger();
  private static final String ID =
      "org.apache.geode.management.internal.cli.functions.AlterRuntimeConfigFunction";

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public void execute(FunctionContext<Map<String, String>> context) {
    var memberId = "";

    try {
      var cache = (InternalCache) context.getCache();
      var config = cache.getInternalDistributedSystem().getConfig();
      memberId = cache.getDistributedSystem().getDistributedMember().getId();

      var runtimeAttributes = context.getArguments();
      var entries = runtimeAttributes.entrySet();

      for (var entry : entries) {
        var attributeName = entry.getKey();
        var attributeValue = entry.getValue();

        switch (attributeName) {
          case CliStrings.ALTER_RUNTIME_CONFIG__COPY__ON__READ:
            cache.setCopyOnRead(Boolean.parseBoolean(attributeValue));
            break;
          case CliStrings.ALTER_RUNTIME_CONFIG__LOCK__LEASE:
            cache.setLockLease(Integer.parseInt(attributeValue));
            break;
          case CliStrings.ALTER_RUNTIME_CONFIG__LOCK__TIMEOUT:
            var lockTimeout = Integer.parseInt(attributeValue);
            cache.setLockTimeout(lockTimeout);
            break;
          case CliStrings.ALTER_RUNTIME_CONFIG__SEARCH__TIMEOUT:
            cache.setSearchTimeout(Integer.parseInt(attributeValue));
            break;
          case CliStrings.ALTER_RUNTIME_CONFIG__MESSAGE__SYNC__INTERVAL:
            cache.setMessageSyncInterval(Integer.parseInt(attributeValue));
            break;
          default:
            config.setAttribute(attributeName, attributeValue, ConfigSource.runtime());
            break;
        }
      }

      var cliFuncResult = new CliFunctionResult(memberId, true, null);
      context.getResultSender().lastResult(cliFuncResult);

    } catch (CacheClosedException cce) {
      var result = new CliFunctionResult(memberId, false, null);
      context.getResultSender().lastResult(result);

    } catch (Exception e) {
      logger.error("Exception happened on : " + memberId, e);
      var cliFuncResult =
          new CliFunctionResult(memberId, e, ExceptionUtils.getStackTrace(e));
      context.getResultSender().lastResult(cliFuncResult);
    }
  }
}
