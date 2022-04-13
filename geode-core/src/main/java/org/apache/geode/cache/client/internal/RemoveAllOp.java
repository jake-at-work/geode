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
package org.apache.geode.cache.client.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PutAllPartialResultException;
import org.apache.geode.internal.cache.PutAllPartialResultException.PutAllPartialResult;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.ChunkedMessage;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.VersionedObjectList;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Does a region removeAll on a server
 *
 * @since GemFire 8.1
 */
public class RemoveAllOp {

  public static final Logger logger = LogService.getLogger();

  public static final int FLAG_EMPTY = 0x01;
  public static final int FLAG_CONCURRENCY_CHECKS = 0x02;


  /**
   * Does a region removeAll on a server using connections from the given pool to communicate with
   * the server.
   *
   * @param pool the pool to use to communicate with the server.
   * @param region the name of the region to do the removeAll on
   * @param keys Collection of keys to remove
   * @param eventId the event id for this op
   */
  public static VersionedObjectList execute(ExecutablePool pool, Region region,
      Collection<Object> keys, EventID eventId, boolean isRetry, Object callbackArg) {
    var op = new RemoveAllOpImpl(region, keys, eventId,
        ((PoolImpl) pool).getPRSingleHopEnabled(), callbackArg);
    op.initMessagePart();
    if (isRetry) {
      op.getMessage().setIsRetry();
    }
    return (VersionedObjectList) pool.execute(op);
  }

  /**
   * Does a region put on a server using connections from the given pool to communicate with the
   * server.
   *
   * @param pool the pool to use to communicate with the server.
   * @param region the name of the region to do the removeAll on
   * @param keys the Collection of keys to remove
   * @param eventId the event id for this removeAll
   */
  public static VersionedObjectList execute(ExecutablePool pool, Region region,
      Collection<Object> keys, EventID eventId, int retryAttempts, Object callbackArg) {
    final var isDebugEnabled = logger.isDebugEnabled();
    var cms = ((InternalRegion) region).getCache().getClientMetadataService();

    var serverToFilterMap = cms.getServerToFilterMap(keys, region, true);

    if (serverToFilterMap == null || serverToFilterMap.isEmpty()) {
      AbstractOp op = new RemoveAllOpImpl(region, keys, eventId,
          ((PoolImpl) pool).getPRSingleHopEnabled(), callbackArg);
      op.initMessagePart();
      return (VersionedObjectList) pool.execute(op);
    }

    var callableTasks = constructAndGetRemoveAllTasks(region, eventId, serverToFilterMap,
        (PoolImpl) pool, callbackArg);

    if (isDebugEnabled) {
      logger.debug("RemoveAllOp#execute : Number of removeAll tasks is :{}", callableTasks.size());
    }
    var failedServers =
        new HashMap<ServerLocation, RuntimeException>();
    var result = new PutAllPartialResult(keys.size());
    try {
      var results = SingleHopClientExecutor
          .submitBulkOp(callableTasks, cms,
              (LocalRegion) region, failedServers);
      for (var entry : results.entrySet()) {
        var value = entry.getValue();
        if (value instanceof PutAllPartialResultException) {
          var pap = (PutAllPartialResultException) value;
          if (isDebugEnabled) {
            logger.debug(
                "RemoveAll SingleHop encountered BulkOpPartialResultException exception: {}, failedServers are {}",
                pap, failedServers.keySet());
          }
          result.consolidate(pap.getResult());
        } else {
          if (value != null) {
            var list = (VersionedObjectList) value;
            result.addKeysAndVersions(list);
          }
        }
      }
    } catch (RuntimeException ex) {
      logger.debug("single-hop removeAll encountered unexpected exception: {}", ex);
      throw ex;
    }

    if (!failedServers.isEmpty()) {
      if (retryAttempts == 0) {
        throw failedServers.values().iterator().next();
      }

      // if the partial result set doesn't already have keys (for tracking version tags)
      // then we need to gather up the keys that we know have succeeded so far and
      // add them to the partial result set
      if (result.getSucceededKeysAndVersions().size() == 0) {
        // if there're failed servers, we need to save the succeed keys in submitRemoveAll
        // if retry succeeded, everything is ok, otherwise, the saved "succeeded
        // keys" should be consolidated into PutAllPartialResultException
        // succeedKeySet is used to send back to client in PartialResult case
        // so it's not a must to use LinkedHashSet
        Set succeedKeySet = new LinkedHashSet();
        var serverSet = serverToFilterMap.keySet();
        for (var server : serverSet) {
          if (!failedServers.containsKey(server)) {
            succeedKeySet.addAll(serverToFilterMap.get(server));
          }
        }

        // save succeedKeys, but if retries all succeeded, discard the PutAllPartialResult
        result.addKeys(succeedKeySet);
      }

      // send maps for the failed servers one by one instead of merging
      // them into one big map. The reason is, we have to keep the same event
      // ids for each sub map. There is a unit test in PutAllCSDUnitTest for
      // the otherwise case.
      var oneSubMapRetryFailed = false;
      var failedServerSet = failedServers.keySet();
      for (var failedServer : failedServerSet) {
        var savedRTE = failedServers.get(failedServer);
        if (savedRTE instanceof PutAllPartialResultException) {
          // will not retry for BulkOpPartialResultException
          // but it means at least one sub map ever failed
          oneSubMapRetryFailed = true;
          continue;
        }
        Collection<Object> newKeys = serverToFilterMap.get(failedServer);
        try {
          var v =
              RemoveAllOp.execute(pool, region, newKeys, eventId, true, callbackArg);
          if (v == null) {
            result.addKeys(newKeys);
          } else {
            result.addKeysAndVersions(v);
          }
        } catch (PutAllPartialResultException pre) {
          oneSubMapRetryFailed = true;
          logger.debug("Retry failed with BulkOpPartialResultException: {} Before retry: {}", pre,
              result.getKeyListString());
          result.consolidate(pre.getResult());
        } catch (Exception rte) {
          oneSubMapRetryFailed = true;
          var firstKey = newKeys.iterator().next();
          result.saveFailedKey(firstKey, rte);
        }
      } // for failedServer

      // If all retries succeeded, the PRE in first tries can be ignored
      if (oneSubMapRetryFailed && result.hasFailure()) {
        var pre = new PutAllPartialResultException(result);
        throw pre;
      }
    } // failedServers!=null

    return result.getSucceededKeysAndVersions();
  }

  private RemoveAllOp() {
    // no instances allowed
  }


  static List constructAndGetRemoveAllTasks(Region region, final EventID eventId,
      final Map<ServerLocation, Set> serverToFilterMap, final PoolImpl pool,
      Object callbackArg) {
    final List<SingleHopOperationCallable> tasks = new ArrayList<>();
    var servers = new ArrayList<ServerLocation>(serverToFilterMap.keySet());

    if (logger.isDebugEnabled()) {
      logger.debug("Constructing tasks for the servers{}", servers);
    }
    for (var server : servers) {
      AbstractOp RemoveAllOp =
          new RemoveAllOpImpl(region, serverToFilterMap.get(server), eventId, true, callbackArg);

      var task =
          new SingleHopOperationCallable(new ServerLocation(server.getHostName(), server.getPort()),
              pool, RemoveAllOp, UserAttributes.userAttributes.get());
      tasks.add(task);
    }
    return tasks;
  }

  private static class RemoveAllOpImpl extends AbstractOp {

    private boolean prSingleHopEnabled = false;

    private LocalRegion region = null;

    private Collection<Object> keys = null;
    private final Object callbackArg;

    /**
     * @throws org.apache.geode.SerializationException if serialization fails
     */
    public RemoveAllOpImpl(Region region, Collection<Object> keys, EventID eventId,
        boolean prSingleHopEnabled, Object callbackArg) {
      super(MessageType.REMOVE_ALL, 5 + keys.size());
      this.prSingleHopEnabled = prSingleHopEnabled;
      this.region = (LocalRegion) region;
      getMessage().addStringPart(region.getFullPath(), true);
      getMessage().addBytesPart(eventId.calcBytes());
      this.keys = keys;
      this.callbackArg = callbackArg;
    }

    @Override
    protected void initMessagePart() {
      var size = keys.size();
      var flags = 0;
      if (region.getDataPolicy() == DataPolicy.EMPTY) {
        flags |= FLAG_EMPTY;
      }
      if (region.getConcurrencyChecksEnabled()) {
        flags |= FLAG_CONCURRENCY_CHECKS;
      }
      getMessage().addIntPart(flags);
      getMessage().addObjPart(callbackArg);
      getMessage().addIntPart(size);

      for (var key : keys) {
        getMessage().addStringOrObjPart(key);
      }
    }

    @Override
    protected @NotNull Message createResponseMessage() {
      return new ChunkedMessage(2, KnownVersion.CURRENT);
    }

    @Override
    protected Object processResponse(final @NotNull Message msg) throws Exception {
      throw new UnsupportedOperationException();
    }

    @Override
    protected Object processResponse(final @NotNull Message msg, final @NotNull Connection con)
        throws Exception {
      final var result = new VersionedObjectList();
      final var exceptionRef = new Exception[1];
      final var isDebugEnabled = logger.isDebugEnabled();
      try {
        processChunkedResponse((ChunkedMessage) msg, "removeAll", cm -> {
          var numParts = msg.getNumberOfParts();
          if (isDebugEnabled) {
            logger.debug("RemoveAllOp.processChunkedResponse processing message with {} parts",
                numParts);
          }
          for (var partNo = 0; partNo < numParts; partNo++) {
            var part = cm.getPart(partNo);
            try {
              var o = part.getObject();
              if (isDebugEnabled) {
                logger.debug("part({}) contained {}", partNo, o);
              }
              if (o == null) {
                // no response is an okay response
              } else if (o instanceof byte[]) {
                if (prSingleHopEnabled) {
                  var bytesReceived = part.getSerializedForm();
                  if (bytesReceived[0] != ClientMetadataService.INITIAL_VERSION) {
                    if (region != null) {
                      try {
                        var cms = region.getCache().getClientMetadataService();
                        cms.scheduleGetPRMetaData(region, false, bytesReceived[1]);
                      } catch (CacheClosedException ignored) {
                      }
                    }
                  }
                }
              } else if (o instanceof Throwable) {
                var s = "While performing a remote removeAll";
                exceptionRef[0] = new ServerOperationException(s, (Throwable) o);
              } else {
                var chunk = (VersionedObjectList) o;
                chunk.replaceNullIDs(con.getEndpoint().getMemberId());
                result.addAll(chunk);
              }
            } catch (Exception e) {
              exceptionRef[0] = new ServerOperationException("Unable to deserialize value", e);
            }
          }
        });
      } catch (ServerOperationException e) {
        if (e.getCause() instanceof PutAllPartialResultException) {
          var cause = (PutAllPartialResultException) e.getCause();
          cause.getSucceededKeysAndVersions().replaceNullIDs(con.getEndpoint().getMemberId());
          throw cause;
        } else {
          throw e;
        }
      }
      if (exceptionRef[0] != null) {
        throw exceptionRef[0];
      } else {
        // v7.0.1: fill in the keys
        if (result.hasVersions() && result.getKeys().isEmpty()) {
          if (logger.isTraceEnabled()) {
            logger.trace("setting keys of response to {}", keys);
          }
          ArrayList<Object> tmpKeys;
          if (keys instanceof ArrayList) {
            tmpKeys = (ArrayList<Object>) keys;
          } else {
            tmpKeys = new ArrayList<>(keys);
          }
          result.setKeys(tmpKeys);
        }
      }
      return result;
    }

    @Override
    protected boolean isErrorResponse(int msgType) {
      return msgType == MessageType.PUT_DATA_ERROR;
    }

    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startRemoveAll();
    }

    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endRemoveAllSend(start, hasFailed());
    }

    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endRemoveAll(start, hasTimedOut(), hasFailed());
    }
  }

}
