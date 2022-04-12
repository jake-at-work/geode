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
package org.apache.geode.cache.query.cq.internal.command;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.jetbrains.annotations.NotNull;

import org.apache.geode.cache.query.CqException;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.AuthenticationExpiredException;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

public class CloseCQ extends BaseCQCommand {

  private static final CloseCQ singleton = new CloseCQ();

  public static Command getCommand() {
    return singleton;
  }

  private CloseCQ() {
    // nothing
  }

  @Override
  public void cmdExecute(final @NotNull Message clientMessage,
      final @NotNull ServerConnection serverConnection,
      final @NotNull SecurityService securityService, long start) throws IOException {
    var crHelper = serverConnection.getCachedRegionHelper();
    var id = serverConnection.getProxyID();
    var stats = serverConnection.getCacheServerStats();

    serverConnection.setAsTrue(REQUIRES_RESPONSE);
    serverConnection.setAsTrue(REQUIRES_CHUNKED_RESPONSE);

    start = DistributionStats.getStatTime();
    // Retrieve the data from the message parts
    var cqName = clientMessage.getPart(0).getString();

    if (logger.isDebugEnabled()) {
      logger.debug("{}: Received close CQ request from {} cqName: {}", serverConnection.getName(),
          serverConnection.getSocketString(), cqName);
    }

    // Process the query request
    if (cqName == null) {
      var err =
          "The cqName for the cq close request is null";
      sendCqResponse(MessageType.CQDATAERROR_MSG_TYPE, err, clientMessage.getTransactionId(), null,
          serverConnection);
      return;
    }

    // Process CQ close request
    try {
      // Append Client ID to CQ name
      var cqService = crHelper.getCache().getCqService();
      cqService.start();

      var serverCqName = cqName;
      if (id != null) {
        serverCqName = cqService.constructServerCqName(cqName, id);
      }
      var cqQuery = cqService.getCq(serverCqName);

      if (cqQuery != null) {
        securityService.authorize(Resource.DATA, Operation.READ, cqQuery.getRegionName());

        var authzRequest = serverConnection.getAuthzRequest();
        if (authzRequest != null) {

          var queryStr = cqQuery.getQueryString();
          Set<String> cqRegionNames = new HashSet<>();
          cqRegionNames.add(cqQuery.getRegionName());
          authzRequest.closeCQAuthorize(cqName, queryStr, cqRegionNames);

        }

        cqService.closeCq(cqName, id);
      }
      if (cqQuery != null) {
        serverConnection.removeCq(cqName, cqQuery.isDurable());
      }
    } catch (CqException cqe) {
      sendCqResponse(MessageType.CQ_EXCEPTION_TYPE, "", clientMessage.getTransactionId(), cqe,
          serverConnection);
      return;
    } catch (AuthenticationExpiredException e) {
      writeChunkedException(clientMessage, e, serverConnection);
      return;
    } catch (Exception e) {
      var err =
          String.format("Exception while closing CQ CqName :%s", cqName);
      sendCqResponse(MessageType.CQ_EXCEPTION_TYPE, err, clientMessage.getTransactionId(), e,
          serverConnection);
      return;
    }

    // Send OK to client
    sendCqResponse(MessageType.REPLY,
        "cq closed successfully.",
        clientMessage.getTransactionId(), null, serverConnection);
    serverConnection.setAsTrue(RESPONDED);

    var oldStart = start;
    start = DistributionStats.getStatTime();
    stats.incProcessCloseCqTime(start - oldStart);
  }

}
