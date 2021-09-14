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
package org.apache.geode.internal.cache;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.internal.statistics.StatisticsClockFactory.disabledClock;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.junit.Before;

import org.apache.geode.cache.Operation;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.cache.wan.GatewaySenderStats;
import org.apache.geode.internal.cache.wan.parallel.ParallelGatewaySenderHelper;
import org.apache.geode.internal.statistics.DummyStatisticsFactory;
import org.apache.geode.test.fake.Fakes;

public class AbstractBucketRegionQueueTest {

  private static final String GATEWAY_SENDER_ID = "ny";
  private static final int BUCKET_ID = 85;
  protected static final long KEY = 198;

  protected GemFireCacheImpl cache;
  protected PartitionedRegion queueRegion;
  protected AbstractGatewaySender sender;
  private PartitionedRegion rootRegion;
  protected BucketRegionQueue bucketRegionQueue;
  protected GatewaySenderStats stats;

  @Before
  public void setUpGemFire() {
    createCache();
    createQueueRegion();
    createGatewaySender();
    createRootRegion();
    createBucketRegionQueue();
  }

  protected void createCache() {
    // Mock cache
    cache = Fakes.cache();
  }

  protected void createQueueRegion() {
    // Mock queue region
    queueRegion =
        ParallelGatewaySenderHelper.createMockQueueRegion(cache,
            ParallelGatewaySenderHelper.getRegionQueueName(GATEWAY_SENDER_ID));
  }

  protected void createGatewaySender() {
    // Mock gateway sender
    sender = ParallelGatewaySenderHelper.createGatewaySender(cache);
    when(queueRegion.getParallelGatewaySender()).thenReturn(sender);
    stats = new GatewaySenderStats(new DummyStatisticsFactory(), "gatewaySenderStats-", "ln",
        disabledClock());
    when(sender.getStatistics()).thenReturn(stats);
  }

  protected void createRootRegion() {
    // Mock root region
    rootRegion = mock(PartitionedRegion.class);
    when(rootRegion.getFullPath())
        .thenReturn(SEPARATOR + PartitionedRegionHelper.PR_ROOT_REGION_NAME);
  }

  protected void createBucketRegionQueue() {
    BucketRegionQueue realBucketRegionQueue = ParallelGatewaySenderHelper
        .createBucketRegionQueue(cache, rootRegion, queueRegion, BUCKET_ID);
    bucketRegionQueue = spy(realBucketRegionQueue);
    bucketRegionQueue.getEventTracker().setInitialized();
  }

  protected GatewaySenderEventImpl createGatewaySenderEvent(LocalRegion lr, Operation operation,
      Object key, Object value, long threadId,
      long sequenceId)
      throws Exception {
    when(lr.getKeyInfo(key, value, null)).thenReturn(new KeyInfo(key, null, null));
    when(lr.getTXId()).thenReturn(null);

    EntryEventImpl eei = EntryEventImpl.create(lr, operation, key, value, null, false, null);
    eei.setEventId(new EventID(new byte[16], threadId, sequenceId));

    return new GatewaySenderEventImpl(getEnumListenerEvent(operation), eei, null, true, false);
  }

  private EnumListenerEvent getEnumListenerEvent(Operation operation) {
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

  protected static void cleanUpDestroyedTokensAndMarkGIIComplete(
      final BucketRegionQueue bucketRegionQueue,
      final InitialImageOperation.GIIStatus giiStatus) {
    bucketRegionQueue.cleanUpDestroyedTokensAndMarkGIIComplete(giiStatus);
  }

}
