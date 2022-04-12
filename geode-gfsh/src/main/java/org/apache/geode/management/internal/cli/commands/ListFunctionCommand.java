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

package org.apache.geode.management.internal.cli.commands;

import java.util.Arrays;
import java.util.Set;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.functions.ListFunctionFunction;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class ListFunctionCommand extends GfshCommand {
  private final ListFunctionFunction listFunctionFunction = new ListFunctionFunction();

  @CliCommand(value = CliStrings.LIST_FUNCTION, help = CliStrings.LIST_FUNCTION__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_FUNCTION})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  public ResultModel listFunction(
      @CliOption(key = CliStrings.LIST_FUNCTION__MATCHES,
          help = CliStrings.LIST_FUNCTION__MATCHES__HELP) String matches,
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.LIST_FUNCTION__GROUP__HELP) String[] groups,
      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          optionContext = ConverterHint.MEMBERIDNAME,
          help = CliStrings.LIST_FUNCTION__MEMBER__HELP) String[] members) {
    var targetMembers = findMembers(groups, members);

    if (targetMembers.isEmpty()) {
      return ResultModel.createInfo(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    }

    var results = executeAndGetFunctionResult(listFunctionFunction,
        new Object[] {matches}, targetMembers);

    var result = new ResultModel();
    var tabularData = result.addTable("functions");
    for (var cliResult : results) {
      @SuppressWarnings("unchecked")
      var resultObject = (Set<String>) cliResult.getResultObject();
      if (resultObject == null) {
        continue;
      }
      var strings = resultObject.toArray(new String[0]);
      Arrays.sort(strings);
      for (var string : strings) {
        tabularData.accumulate("Member", cliResult.getMemberIdOrName());
        tabularData.accumulate("Function", string);
      }
    }

    if (tabularData.getRowSize() == 0) {
      return ResultModel.createInfo(CliStrings.LIST_FUNCTION__NO_FUNCTIONS_FOUND_ERROR_MESSAGE);
    }
    return result;

  }
}
