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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.logging.internal.executors.LoggingExecutors;
import org.apache.geode.management.GatewaySenderMXBean;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class StartGatewaySenderCommand extends GfshCommand {

  @CliCommand(value = CliStrings.START_GATEWAYSENDER, help = CliStrings.START_GATEWAYSENDER__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_WAN)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.GATEWAY)
  public ResultModel startGatewaySender(@CliOption(key = CliStrings.START_GATEWAYSENDER__ID,
      mandatory = true, optionContext = ConverterHint.GATEWAY_SENDER_ID,
      help = CliStrings.START_GATEWAYSENDER__ID__HELP) String senderId,

      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.START_GATEWAYSENDER__GROUP__HELP) String[] onGroup,

      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          optionContext = ConverterHint.MEMBERIDNAME,
          help = CliStrings.START_GATEWAYSENDER__MEMBER__HELP) String[] onMember,

      @CliOption(key = CliStrings.START_GATEWAYSENDER__CLEAN_QUEUE,
          unspecifiedDefaultValue = "false",
          specifiedDefaultValue = "true",
          help = CliStrings.START_GATEWAYSENDER__CLEAN_QUEUE__HELP) final Boolean cleanQueues) {

    final var id = senderId.trim();

    final var cache = getCache();
    final SystemManagementService service = getManagementService();

    var dsMembers = findMembers(onGroup, onMember);

    if (dsMembers.isEmpty()) {
      return ResultModel.createError(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    }

    var execService =
        LoggingExecutors.newCachedThreadPool("Start Sender Command Thread ", true);

    List<Callable<List<String>>> callables = new ArrayList<>();

    for (final var member : dsMembers) {

      callables.add(() -> {

        GatewaySenderMXBean bean;
        var statusList = new ArrayList<String>();
        if (cache.getDistributedSystem().getDistributedMember().getId().equals(member.getId())) {
          bean = service.getLocalGatewaySenderMXBean(id);
        } else {
          var objectName = service.getGatewaySenderMBeanName(member, id);
          bean = service.getMBeanProxy(objectName, GatewaySenderMXBean.class);
        }
        if (bean != null) {
          if (bean.isRunning()) {
            statusList.add(member.getId());
            statusList.add(CliStrings.GATEWAY_ERROR);
            statusList.add(CliStrings.format(
                CliStrings.GATEWAY_SENDER_0_IS_ALREADY_STARTED_ON_MEMBER_1, id, member.getId()));
          } else {
            if (cleanQueues) {
              bean.startWithCleanQueue();
            } else {
              bean.start();
            }
            statusList.add(member.getId());
            statusList.add(CliStrings.GATEWAY_OK);
            statusList.add(CliStrings.format(CliStrings.GATEWAY_SENDER_0_IS_STARTED_ON_MEMBER_1, id,
                member.getId()));
          }
        } else {
          statusList.add(member.getId());
          statusList.add(CliStrings.GATEWAY_ERROR);
          statusList.add(CliStrings.format(CliStrings.GATEWAY_SENDER_0_IS_NOT_AVAILABLE_ON_MEMBER_1,
              id, member.getId()));
        }
        return statusList;

      });
    }

    var memberIterator = dsMembers.iterator();
    List<Future<List<String>>> futures;

    try {
      futures = execService.invokeAll(callables);
    } catch (InterruptedException ite) {
      return ResultModel.createError(
          CliStrings.format(CliStrings.GATEWAY_SENDER_START_0_COULD_NOT_BE_INVOKED_DUE_TO_1, id,
              ite.getMessage()));
    } finally {
      execService.shutdown();
    }

    var resultModel = new ResultModel();
    var resultData = resultModel.addTable(CliStrings.START_GATEWAYSENDER);
    for (var future : futures) {
      var member = memberIterator.next();
      List<String> memberStatus;
      try {
        memberStatus = future.get();
        resultData.addMemberStatusResultRow(memberStatus.get(0),
            memberStatus.get(1), memberStatus.get(2));
      } catch (InterruptedException | ExecutionException ite) {
        resultData.addMemberStatusResultRow(member.getId(),
            CliStrings.GATEWAY_ERROR,
            CliStrings.format(CliStrings.GATEWAY_SENDER_0_COULD_NOT_BE_STARTED_ON_MEMBER_DUE_TO_1,
                id, ite.getMessage()));
      }
    }
    execService.shutdown();

    return resultModel;
  }
}
