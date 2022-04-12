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
import static org.apache.geode.internal.cache.wan.GatewaySenderEventImpl.TransactionMetadataDisposition.EXCLUDE;
import static org.apache.geode.internal.statistics.StatisticsClockFactory.disabledClock;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;

import org.mockito.stubbing.Answer;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.internal.cache.BucketAdvisor;
import org.apache.geode.internal.cache.BucketRegionQueue;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EnumListenerEvent;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.EvictionAttributesImpl;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalRegionArguments;
import org.apache.geode.internal.cache.KeyInfo;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.internal.cache.PartitionedRegionStats;
import org.apache.geode.internal.cache.ProxyBucketRegion;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.internal.cache.eviction.AbstractEvictionController;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;

public class ParallelGatewaySenderHelper {

  public static ParallelGatewaySenderEventProcessor createParallelGatewaySenderEventProcessor(
      AbstractGatewaySender sender) {
    var processor =
        new ParallelGatewaySenderEventProcessor(sender, null, false);
    var queue = new ConcurrentParallelGatewaySenderQueue(sender,
        new ParallelGatewaySenderEventProcessor[] {processor});
    Set<RegionQueue> queues = new HashSet<>();
    queues.add(queue);
    when(sender.getQueues()).thenReturn(queues);
    return processor;
  }

  public static AbstractGatewaySender createGatewaySender(GemFireCacheImpl cache) {
    // Mock gateway sender
    var sender = mock(AbstractGatewaySender.class);
    when(sender.getCache()).thenReturn(cache);
    var cancelCriterion = mock(CancelCriterion.class);
    when(sender.getCancelCriterion()).thenReturn(cancelCriterion);
    when(sender.getId()).thenReturn("");
    return sender;
  }

  public static GatewaySenderEventImpl createGatewaySenderEvent(LocalRegion lr, Operation operation,
      Object key, Object value, long sequenceId, long shadowKey) throws Exception {
    return createGatewaySenderEvent(lr, operation, key, value, 1l, sequenceId, 0, shadowKey);
  }

  public static GatewaySenderEventImpl createGatewaySenderEvent(LocalRegion lr, Operation operation,
      Object key, Object value, long threadId, long sequenceId, int bucketId, long shadowKey)
      throws Exception {
    when(lr.getKeyInfo(key, value, null)).thenReturn(new KeyInfo(key, null, null));
    var eei = EntryEventImpl.create(lr, operation, key, value, null, false, null);
    eei.setEventId(new EventID(new byte[16], threadId, sequenceId, bucketId));
    var gsei =
        new GatewaySenderEventImpl(getEnumListenerEvent(operation), eei, null, true, bucketId,
            EXCLUDE);
    gsei.setShadowKey(shadowKey);
    return gsei;
  }

  public static PartitionedRegion createMockQueueRegion(GemFireCacheImpl cache, String regionName) {
    // Mock queue region
    var queueRegion = mock(PartitionedRegion.class);
    when(queueRegion.getFullPath()).thenReturn(regionName);
    when(queueRegion.getPrStats()).thenReturn(mock(PartitionedRegionStats.class));
    when(queueRegion.getDataStore()).thenReturn(mock(PartitionedRegionDataStore.class));
    when(queueRegion.getCache()).thenReturn(cache);
    var ea = (EvictionAttributesImpl) EvictionAttributes
        .createLRUMemoryAttributes(100, null, EvictionAction.OVERFLOW_TO_DISK);
    var eviction = AbstractEvictionController.create(ea, false,
        cache.getDistributedSystem(), "queueRegion");
    when(queueRegion.getEvictionController()).thenReturn(eviction);
    return queueRegion;
  }

  public static BucketRegionQueue createBucketRegionQueue(GemFireCacheImpl cache,
      PartitionedRegion parentRegion, PartitionedRegion queueRegion, int bucketId) {
    // Create InternalRegionArguments
    var ira = new InternalRegionArguments();
    ira.setPartitionedRegion(queueRegion);
    ira.setPartitionedRegionBucketRedundancy(1);
    var ba = mock(BucketAdvisor.class);
    ira.setBucketAdvisor(ba);
    var pbrIra = new InternalRegionArguments();
    var ra = mock(RegionAdvisor.class);
    when(ra.getPartitionedRegion()).thenReturn(queueRegion);
    pbrIra.setPartitionedRegionAdvisor(ra);
    var pa = mock(PartitionAttributes.class);
    when(queueRegion.getPartitionAttributes()).thenReturn(pa);

    when(queueRegion.getBucketName(eq(bucketId))).thenAnswer(
        (Answer<String>) invocation -> PartitionedRegionHelper
            .getBucketName(queueRegion.getFullPath(), bucketId));

    when(queueRegion.getDataPolicy()).thenReturn(DataPolicy.PARTITION);

    when(pa.getColocatedWith()).thenReturn(null);

    when(ba.getProxyBucketRegion()).thenReturn(mock(ProxyBucketRegion.class));

    // Create RegionAttributes
    var factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    factory.setEvictionAttributes(
        EvictionAttributes.createLRUMemoryAttributes(100, null, EvictionAction.OVERFLOW_TO_DISK));
    var attributes = factory.create();

    // Create BucketRegionQueue
    return new BucketRegionQueue(
        queueRegion.getBucketName(bucketId), attributes, parentRegion, cache, ira, disabledClock());
  }

  public static String getRegionQueueName(String gatewaySenderId) {
    return SEPARATOR + gatewaySenderId + ParallelGatewaySenderQueue.QSTRING;
  }

  private static EnumListenerEvent getEnumListenerEvent(Operation operation) {
    EnumListenerEvent ele = null;
    if (operation.isCreate()) {
      ele = EnumListenerEvent.AFTER_CREATE;
    } else if (operation.isUpdate()) {
      ele = EnumListenerEvent.AFTER_UPDATE;
    } else if (operation.isDestroy()) {
      ele = EnumListenerEvent.AFTER_DESTROY;
    }
    return ele;
  }
}
