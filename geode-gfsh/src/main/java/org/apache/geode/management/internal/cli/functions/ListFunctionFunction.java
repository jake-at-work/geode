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

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.functions.CliFunctionResult;

public class ListFunctionFunction implements InternalFunction<Object[]> {
  private static final Logger logger = LogService.getLogger();

  private static final long serialVersionUID = 1L;

  private static final String ID =
      "org.apache.geode.management.internal.cli.functions.ListFunctionFunction";

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public void execute(FunctionContext<Object[]> context) {
    // Declared here so that it's available when returning a Throwable
    var memberId = "";

    try {
      final var args = context.getArguments();
      final var stringPattern = (String) args[0];

      var cache = context.getCache();
      var member = cache.getDistributedSystem().getDistributedMember();

      memberId = member.getId();
      // If they set a name use it instead
      if (!member.getName().equals("")) {
        memberId = member.getName();
      }

      @SuppressWarnings("rawtypes")
      final var functions = FunctionService.getRegisteredFunctions();
      CliFunctionResult result;
      if (stringPattern == null || stringPattern.isEmpty()) {
        result = new CliFunctionResult(memberId, new HashSet<>(functions.keySet()), null);
      } else {
        var pattern = Pattern.compile(stringPattern);
        Set<String> resultSet = new HashSet<>();
        for (var functionId : functions.keySet()) {
          var matcher = pattern.matcher(functionId);
          if (matcher.matches()) {
            resultSet.add(functionId);
          }
        }
        result = new CliFunctionResult(memberId, resultSet, null);
      }
      context.getResultSender().lastResult(result);

    } catch (Exception cce) {
      logger.error(cce.getMessage(), cce);
      var result = new CliFunctionResult(memberId, false, cce.getMessage());
      context.getResultSender().lastResult(result);
    }
  }

  @Override
  public boolean hasResult() {
    return true;
  }

  @Override
  public boolean optimizeForWrite() {
    return false;
  }

  @Override
  public boolean isHA() {
    return false;
  }
}
