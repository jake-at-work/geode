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
package org.apache.geode.internal.cache.tier.sockets.command;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.jetbrains.annotations.NotNull;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

public class ClearRegion extends BaseCommand {

  @Immutable
  private static final ClearRegion singleton = new ClearRegion();

  private ClearRegion() {}

  public static Command getCommand() {
    return singleton;
  }

  @Override
  public void cmdExecute(final @NotNull Message clientMessage,
      final @NotNull ServerConnection serverConnection,
      final @NotNull SecurityService securityService, long start)
      throws IOException, InterruptedException {
    Part regionNamePart = null, callbackArgPart = null;
    String regionName = null;
    Object callbackArg = null;
    Part eventPart = null;
    var crHelper = serverConnection.getCachedRegionHelper();
    var stats = serverConnection.getCacheServerStats();
    serverConnection.setAsTrue(REQUIRES_RESPONSE);

    {
      var oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incReadClearRegionRequestTime(start - oldStart);
    }
    // Retrieve the data from the message parts
    regionNamePart = clientMessage.getPart(0);
    eventPart = clientMessage.getPart(1);
    // callbackArgPart = null; (redundant assignment)
    if (clientMessage.getNumberOfParts() > 2) {
      callbackArgPart = clientMessage.getPart(2);
      try {
        callbackArg = callbackArgPart.getObject();
      } catch (Exception e) {
        writeException(clientMessage, e, false, serverConnection);
        serverConnection.setAsTrue(RESPONDED);
        return;
      }
    }
    regionName = regionNamePart.getCachedString();
    if (logger.isDebugEnabled()) {
      logger.debug(serverConnection.getName() + ": Received clear region request ("
          + clientMessage.getPayloadLength() + " bytes) from " + serverConnection.getSocketString()
          + " for region " + regionName);
    }

    // Process the clear region request
    if (regionName == null) {
      logger.warn("{}: The input region name for the clear region request is null",
          serverConnection.getName());
      var errMessage =
          "The input region name for the clear region request is null";

      writeErrorResponse(clientMessage, MessageType.CLEAR_REGION_DATA_ERROR, errMessage,
          serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    var region = (LocalRegion) crHelper.getRegion(regionName);
    if (region == null) {
      var reason = "was not found during clear region request";
      writeRegionDestroyedEx(clientMessage, regionName, reason, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    var eventIdPartsBuffer = ByteBuffer.wrap(eventPart.getSerializedForm());
    var threadId = EventID.readEventIdPartsFromOptimizedByteArray(eventIdPartsBuffer);
    var sequenceId = EventID.readEventIdPartsFromOptimizedByteArray(eventIdPartsBuffer);
    var eventId =
        new EventID(serverConnection.getEventMemberIDByteArray(), threadId, sequenceId);

    try {
      // Clear the region
      securityService.authorize(Resource.DATA, Operation.WRITE, regionName);

      var authzRequest = serverConnection.getAuthzRequest();
      if (authzRequest != null) {
        var clearContext =
            authzRequest.clearAuthorize(regionName, callbackArg);
        callbackArg = clearContext.getCallbackArg();
      }
      region.basicBridgeClear(callbackArg, serverConnection.getProxyID(),
          true /* boolean from cache Client */, eventId);
    } catch (Exception e) {
      // If an interrupted exception is thrown , rethrow it
      checkForInterrupt(serverConnection, e);

      // If an exception occurs during the clear, preserve the connection
      writeException(clientMessage, e, false, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    // Update the statistics and write the reply
    {
      var oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incProcessClearRegionTime(start - oldStart);
    }
    writeReply(clientMessage, serverConnection);
    serverConnection.setAsTrue(RESPONDED);
    if (logger.isDebugEnabled()) {
      logger.debug(
          serverConnection.getName() + ": Sent clear region response for region " + regionName);
    }
    stats.incWriteClearRegionResponseTime(DistributionStats.getStatTime() - start);
  }


}
