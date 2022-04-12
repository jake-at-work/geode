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
package org.apache.geode.internal.cache.wan.parallel;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.internal.cache.LocalRegion.InitializationLevel.BEFORE_INITIAL_IMAGE;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.wan.GatewayQueueEvent;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.PooledDistributionMessage;
import org.apache.geode.internal.cache.AbstractBucketRegionQueue;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Removes a batch of events from the remote secondary queues
 *
 * @since GemFire 8.0
 */
public class ParallelQueueRemovalMessage extends PooledDistributionMessage {

  private static final Logger logger = LogService.getLogger();

  private Map<String, Map<Integer, List<Object>>> regionToDispatchedKeysMap;

  public ParallelQueueRemovalMessage() {}

  public ParallelQueueRemovalMessage(
      Map<String, Map<Integer, List<Object>>> rgnToDispatchedKeysMap) {
    this.regionToDispatchedKeysMap = rgnToDispatchedKeysMap;
  }

  @Override
  public int getDSFID() {
    return PARALLEL_QUEUE_REMOVAL_MESSAGE;
  }

  @Override
  public String toString() {
    var cname = getShortClassName();
    return cname + "regionToDispatchedKeysMap=" + regionToDispatchedKeysMap
        + " sender=" + getSender();
  }

  @Override
  protected void process(ClusterDistributionManager dm) {
    final var isDebugEnabled = logger.isDebugEnabled();
    final var cache = dm.getCache();
    if (cache != null) {
      final var oldLevel =
          LocalRegion.setThreadInitLevelRequirement(BEFORE_INITIAL_IMAGE);
      try {
        for (Object name : regionToDispatchedKeysMap.keySet()) {
          final var regionName = (String) name;
          final var region = (PartitionedRegion) cache.getRegion(regionName);
          if (region == null) {
            continue;
          } else {
            var abstractSender = region.getParallelGatewaySender();
            // Find the map: bucketId to dispatchedKeys
            // Find the bucket
            // Destroy the keys
            var bucketIdToDispatchedKeys = (Map) regionToDispatchedKeysMap.get(regionName);
            for (var bId : bucketIdToDispatchedKeys.keySet()) {
              final var bucketFullPath =
                  SEPARATOR + PartitionedRegionHelper.PR_ROOT_REGION_NAME + SEPARATOR
                      + region.getBucketName((Integer) bId);
              var brq =
                  (AbstractBucketRegionQueue) cache.getInternalRegionByPath(bucketFullPath);
              if (isDebugEnabled) {
                logger.debug(
                    "ParallelQueueRemovalMessage : The bucket in the cache is bucketRegionName : {} bucket: {}",
                    bucketFullPath, brq);
              }

              var dispatchedKeys = (List) bucketIdToDispatchedKeys.get(bId);
              if (dispatchedKeys != null) {
                for (var key : dispatchedKeys) {
                  // First, clear the Event from tempQueueEvents at AbstractGatewaySender level, if
                  // exists
                  // synchronize on AbstractGatewaySender.queuedEventsSync while doing so
                  abstractSender.removeFromTempQueueEvents(key);

                  if (brq != null) {
                    if (brq.isInitialized()) {
                      if (isDebugEnabled) {
                        logger.debug(
                            "ParallelQueueRemovalMessage : The bucket {} is initialized. Destroying the key {} from BucketRegionQueue.",
                            bucketFullPath, key);
                      }
                      // fix for #48082
                      afterAckForSecondary_EventInBucket(abstractSender, brq, key);
                      destroyKeyFromBucketQueue(brq, key, region);
                    } else {
                      // if bucket is not initialized, the event should either be in bucket or
                      // tempQueue
                      var isDestroyed = false;
                      if (isDebugEnabled) {
                        logger.debug(
                            "ParallelQueueRemovalMessage : The bucket {} is not yet initialized.",
                            bucketFullPath);
                      }
                      brq.getInitializationLock().readLock().lock();
                      try {
                        if (brq.containsKey(key)) {
                          // fix for #48082
                          afterAckForSecondary_EventInBucket(abstractSender, brq, key);
                          destroyKeyFromBucketQueue(brq, key, region);
                          isDestroyed = true;
                        }

                        // Even if BucketRegionQueue does not have the key, it could be in the
                        // tempQueue
                        // remove it from there..defect #49196
                        destroyFromTempQueue(brq.getPartitionedRegion(), (Integer) bId, key);

                        // Finally, add the key to the failed batch removal keys so that it is
                        // definitely removed from the bucket region queue
                        brq.addToFailedBatchRemovalMessageKeys(key);
                      } finally {
                        brq.getInitializationLock().readLock().unlock();
                      }
                    }
                  } else {// brq is null. Destroy the event from tempQueue. Defect #49196
                    destroyFromTempQueue(region, (Integer) bId, key);
                  }
                }
              }
            }
          }
        } // for loop regionToDispatchedKeysMap.keySet()
      } finally {
        LocalRegion.setThreadInitLevelRequirement(oldLevel);
      }
    } // cache != null
  }

  // fix for #48082
  private void afterAckForSecondary_EventInBucket(AbstractGatewaySender abstractSender,
      AbstractBucketRegionQueue brq, Object key) {
    for (var filter : abstractSender.getGatewayEventFilters()) {
      var eventForFilter = (GatewayQueueEvent) brq.get(key);
      try {
        if (eventForFilter != null) {
          filter.afterAcknowledgement(eventForFilter);
        }
      } catch (Exception e) {
        logger.fatal(String.format(
            "Exception occurred while handling call to %s.afterAcknowledgement for event %s:",
            filter.toString(), eventForFilter),
            e);
      }
    }
  }

  void destroyKeyFromBucketQueue(AbstractBucketRegionQueue brq, Object key,
      PartitionedRegion prQ) {
    final var isDebugEnabled = logger.isDebugEnabled();
    try {
      brq.destroyKey(key);
      if (!brq.getBucketAdvisor().isPrimary()) {
        prQ.getParallelGatewaySender().getStatistics().decSecondaryQueueSize();
        prQ.getParallelGatewaySender().getStatistics().incEventsProcessedByPQRM(1);
      }
      if (isDebugEnabled) {
        logger.debug("Destroyed the key {} for shadowPR {} for bucket {}", key, prQ.getName(),
            brq.getId());
      }
    } catch (EntryNotFoundException e) {
      if (isDebugEnabled) {
        logger.debug("Got EntryNotFoundException while destroying the key {} for bucket {}", key,
            brq.getId());
      }
      // add the key to failedBatchRemovalMessageQueue.
      // This is to handle the last scenario in #49196
      // But if GII is already completed and FailedBatchRemovalMessageKeys
      // are already cleared then no keys should be added to it as they will
      // never be cleared and increase the memory footprint.
      if (!brq.isFailedBatchRemovalMessageKeysClearedFlag()) {
        brq.addToFailedBatchRemovalMessageKeys(key);
      }
    } catch (ForceReattemptException fe) {
      if (isDebugEnabled) {
        logger.debug(
            "Got ForceReattemptException while getting bucket {} to destroyLocally the keys.",
            brq.getId());
      }
    } catch (CancelException e) {
      return; // cache or DS is closing
    } catch (CacheException e) {
      logger.error(String.format(
          "ParallelQueueRemovalMessage::process:Exception in processing the last disptached key for a ParallelGatewaySenderQueue's shadowPR. The problem is with key,%s for shadowPR with name=%s",
          key, prQ.getName()),
          e);
    }
  }

  private boolean destroyFromTempQueue(PartitionedRegion qPR, int bId, Object key) {
    var isDestroyed = false;
    Set queues = qPR.getParallelGatewaySender().getQueues();
    if (queues != null) {
      var prq =
          (ConcurrentParallelGatewaySenderQueue) queues.toArray()[0];
      var tempQueue = prq.getBucketTmpQueue(bId);
      if (tempQueue != null) {
        var itr = tempQueue.iterator();
        while (itr.hasNext()) {
          var eventForFilter = itr.next();
          // fix for #48082
          afterAckForSecondary_EventInTempQueue(qPR.getParallelGatewaySender(), eventForFilter);
          if (eventForFilter.getShadowKey().equals(key)) {
            itr.remove();
            eventForFilter.release(); // GEODE-1282
            isDestroyed = true;
          }
        }
      }
    }
    return isDestroyed;
  }

  // fix for #48082
  private void afterAckForSecondary_EventInTempQueue(
      AbstractGatewaySender parallelGatewaySenderImpl, GatewaySenderEventImpl eventForFilter) {
    for (var filter : parallelGatewaySenderImpl.getGatewayEventFilters()) {
      try {
        if (eventForFilter != null) {
          filter.afterAcknowledgement(eventForFilter);
        }
      } catch (Exception e) {
        logger.fatal(String.format(
            "Exception occurred while handling call to %s.afterAcknowledgement for event %s:",
            filter.toString(), eventForFilter),
            e);
      }
    }
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    DataSerializer.writeHashMap(regionToDispatchedKeysMap, out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    regionToDispatchedKeysMap = DataSerializer.readHashMap(in);
  }
}
