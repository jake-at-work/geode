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
import java.util.Arrays;
import java.util.List;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.management.CacheServerMXBean;
import org.apache.geode.management.ClientHealthStatus;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.functions.ContinuousQueryFunction;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.management.internal.util.ManagementUtils;
import org.apache.geode.security.ResourcePermission;

public class DescribeClientCommand extends GfshCommand {
  @CliCommand(value = CliStrings.DESCRIBE_CLIENT, help = CliStrings.DESCRIBE_CLIENT__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_CLIENT})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  public ResultModel describeClient(@CliOption(key = CliStrings.DESCRIBE_CLIENT__ID,
      mandatory = true, help = CliStrings.DESCRIBE_CLIENT__ID__HELP) String clientId)
      throws Exception {
    var result = new ResultModel();

    if (clientId.startsWith("\"")) {
      clientId = clientId.substring(1);
    }

    if (clientId.endsWith("\"")) {
      clientId = clientId.substring(0, clientId.length() - 2);
    }

    if (clientId.endsWith("\";")) {
      clientId = clientId.substring(0, clientId.length() - 2);
    }

    var service = getManagementService();
    var cacheServers = service.getDistributedSystemMXBean().listCacheServerObjectNames();
    if (cacheServers.length == 0) {
      return ResultModel.createError(
          CliStrings.format(CliStrings.DESCRIBE_CLIENT_COULD_NOT_RETRIEVE_SERVER_LIST));
    }

    ClientHealthStatus clientHealthStatus = null;

    for (var objName : cacheServers) {
      var serverMbean = service.getMBeanInstance(objName, CacheServerMXBean.class);
      List<String> listOfClient =
          new ArrayList<>(Arrays.asList(serverMbean.getClientIds()));
      if (listOfClient.contains(clientId)) {
        if (clientHealthStatus == null) {
          try {
            clientHealthStatus = serverMbean.showClientStats(clientId);
            if (clientHealthStatus == null) {
              return ResultModel.createError(CliStrings.format(
                  CliStrings.DESCRIBE_CLIENT_COULD_NOT_RETRIEVE_STATS_FOR_CLIENT_0, clientId));
            }
          } catch (Exception eee) {
            return ResultModel.createError(CliStrings.format(
                CliStrings.DESCRIBE_CLIENT_COULD_NOT_RETRIEVE_STATS_FOR_CLIENT_0_REASON_1, clientId,
                eee.getMessage()));
          }
        }
      }
    }

    if (clientHealthStatus == null) {
      return ResultModel.createError(
          CliStrings.format(CliStrings.DESCRIBE_CLIENT__CLIENT__ID__NOT__FOUND__0, clientId));
    }

    var dsMembers = getAllMembers();
    String isDurable = null;
    List<String> primaryServers = new ArrayList<>();
    List<String> secondaryServers = new ArrayList<>();

    if (dsMembers.size() > 0) {
      var continuousQueryFunction = new ContinuousQueryFunction();
      FunctionService.registerFunction(continuousQueryFunction);
      var resultList = (List<?>) ManagementUtils
          .executeFunction(continuousQueryFunction, clientId, dsMembers).getResult();
      for (Object aResultList : resultList) {
        var object = aResultList;
        if (object instanceof Throwable) {
          LogWrapper.getInstance(getCache()).warning(
              "Exception in Describe Client " + ((Throwable) object).getMessage(),
              ((Throwable) object));
          continue;
        }

        if (object != null) {
          var objectResult =
              (ContinuousQueryFunction.ClientInfo) object;
          isDurable = objectResult.isDurable;

          if (objectResult.primaryServer != null && objectResult.primaryServer.length() > 0) {
            if (primaryServers.size() == 0) {
              primaryServers.add(objectResult.primaryServer);
            } else {
              primaryServers.add(" ,");
              primaryServers.add(objectResult.primaryServer);
            }
          }

          if (objectResult.secondaryServer != null && objectResult.secondaryServer.length() > 0) {
            if (secondaryServers.size() == 0) {
              secondaryServers.add(objectResult.secondaryServer);
            } else {
              secondaryServers.add(" ,");
              secondaryServers.add(objectResult.secondaryServer);
            }
          }
        }
      }

      buildTableResult(result, clientHealthStatus, isDurable, primaryServers, secondaryServers);
    } else {
      result = ResultModel.createError(CliStrings.DESCRIBE_CLIENT_NO_MEMBERS);
    }

    LogWrapper.getInstance(getCache()).info("describe client result " + result);
    return result;
  }

  private void buildTableResult(ResultModel result, ClientHealthStatus clientHealthStatus,
      String isDurable, List<String> primaryServers, List<String> secondaryServers) {

    var primServers = new StringBuilder();
    for (var primaryServer : primaryServers) {
      primServers.append(primaryServer);
    }

    var secondServers = new StringBuilder();
    for (var secondServer : secondaryServers) {
      secondServers.append(secondServer);
    }

    var dataSection = result.addData("infoSection");
    if (clientHealthStatus != null) {
      dataSection.addData(CliStrings.DESCRIBE_CLIENT_COLUMN_PRIMARY_SERVERS, primServers);
      dataSection.addData(CliStrings.DESCRIBE_CLIENT_COLUMN_SECONDARY_SERVERS, secondServers);
      dataSection.addData(CliStrings.DESCRIBE_CLIENT_COLUMN_CPU, clientHealthStatus.getCpus());
      dataSection.addData(CliStrings.DESCRIBE_CLIENT_COLUMN_LISTENER_CALLS,
          clientHealthStatus.getNumOfCacheListenerCalls());
      dataSection.addData(CliStrings.DESCRIBE_CLIENT_COLUMN_GETS,
          clientHealthStatus.getNumOfGets());
      dataSection.addData(CliStrings.DESCRIBE_CLIENT_COLUMN_MISSES,
          clientHealthStatus.getNumOfMisses());
      dataSection.addData(CliStrings.DESCRIBE_CLIENT_COLUMN_PUTS,
          clientHealthStatus.getNumOfPuts());
      dataSection.addData(CliStrings.DESCRIBE_CLIENT_COLUMN_THREADS,
          clientHealthStatus.getNumOfThreads());
      dataSection.addData(CliStrings.DESCRIBE_CLIENT_COLUMN_PROCESS_CPU_TIME,
          clientHealthStatus.getProcessCpuTime());
      dataSection.addData(CliStrings.DESCRIBE_CLIENT_COLUMN_QUEUE_SIZE,
          clientHealthStatus.getQueueSize());
      dataSection.addData(CliStrings.DESCRIBE_CLIENT_COLUMN_UP_TIME,
          clientHealthStatus.getUpTime());
      dataSection.addData(CliStrings.DESCRIBE_CLIENT_COLUMN_DURABLE, isDurable);

      var poolStats = clientHealthStatus.getPoolStats();

      if (poolStats.size() > 0) {
        for (var entry : poolStats.entrySet()) {
          var poolStatsResultTable = result.addTable(entry.getKey());
          poolStatsResultTable.setHeader("Pool Stats For Pool Name = " + entry.getKey());
          var poolStatsStr = entry.getValue();
          var str = poolStatsStr.split(";");

          var logWrapper = LogWrapper.getInstance(getCache());
          logWrapper.info("describe client clientHealthStatus min conn="
              + str[0].substring(str[0].indexOf("=") + 1));
          logWrapper.info("describe client clientHealthStatus max conn ="
              + str[1].substring(str[1].indexOf("=") + 1));
          logWrapper.info("describe client clientHealthStatus redundancy ="
              + str[2].substring(str[2].indexOf("=") + 1));
          logWrapper.info("describe client clientHealthStatus CQs ="
              + str[3].substring(str[3].indexOf("=") + 1));

          poolStatsResultTable.accumulate(CliStrings.DESCRIBE_CLIENT_MIN_CONN,
              str[0].substring(str[0].indexOf("=") + 1));
          poolStatsResultTable.accumulate(CliStrings.DESCRIBE_CLIENT_MAX_CONN,
              str[1].substring(str[1].indexOf("=") + 1));
          poolStatsResultTable.accumulate(CliStrings.DESCRIBE_CLIENT_REDUNDANCY,
              str[2].substring(str[2].indexOf("=") + 1));
          poolStatsResultTable.accumulate(CliStrings.DESCRIBE_CLIENT_CQs,
              str[3].substring(str[3].indexOf("=") + 1));
        }
      }
    }
  }
}
