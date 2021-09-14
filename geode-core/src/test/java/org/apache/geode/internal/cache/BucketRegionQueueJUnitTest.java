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
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Operation;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.cache.wan.GatewaySenderStats;
import org.apache.geode.internal.cache.wan.parallel.ParallelGatewaySenderHelper;

public class BucketRegionQueueJUnitTest extends AbstractBucketRegionQueueTest {

  @Test
  public void testBasicDestroyConflationEnabledAndValueInRegionAndIndex() {
    // Create the event
    EntryEventImpl event = EntryEventImpl.create(bucketRegionQueue, Operation.DESTROY,
        KEY, "value", null, false, mock(DistributedMember.class));

    // Don't allow hasSeenEvent to be invoked
    doReturn(false).when(bucketRegionQueue).hasSeenEvent(event);

    // Set conflation enabled and the appropriate return values for containsKey and removeIndex
    when(queueRegion.isConflationEnabled()).thenReturn(true);
    when(bucketRegionQueue.containsKey(KEY)).thenReturn(true);
    doReturn(true).when(bucketRegionQueue).removeIndex(KEY);

    // Invoke basicDestroy
    bucketRegionQueue.basicDestroy(event, true, null, false);

    // Verify mapDestroy is invoked
    verify(bucketRegionQueue).mapDestroy(event, true, false, null);
  }

  @Test(expected = EntryNotFoundException.class)
  public void testBasicDestroyConflationEnabledAndValueNotInRegion() {
    // Create the event
    EntryEventImpl event = EntryEventImpl.create(bucketRegionQueue, Operation.DESTROY,
        KEY, "value", null, false, mock(DistributedMember.class));

    // Don't allow hasSeenEvent to be invoked
    doReturn(false).when(bucketRegionQueue).hasSeenEvent(event);

    // Set conflation enabled and the appropriate return values for containsKey and removeIndex
    when(queueRegion.isConflationEnabled()).thenReturn(true);
    when(bucketRegionQueue.containsKey(KEY)).thenReturn(false);

    // Invoke basicDestroy
    bucketRegionQueue.basicDestroy(event, true, null, false);
  }

  @Test
  public void testPeekedElementsArePossibleDuplicate()
      throws Exception {
    ParallelGatewaySenderHelper.createParallelGatewaySenderEventProcessor(sender);

    LocalRegion lr = mock(LocalRegion.class);
    when(lr.getFullPath()).thenReturn(SEPARATOR + "dataStoreRegion");
    when(lr.getCache()).thenReturn(cache);

    // Configure conflation
    when(sender.isBatchConflationEnabled()).thenReturn(true);
    when(sender.getStatistics()).thenReturn(mock(GatewaySenderStats.class));

    bucketRegionQueue
        .cleanUpDestroyedTokensAndMarkGIIComplete(InitialImageOperation.GIIStatus.NO_GII);

    // Create a batch of conflatable events with duplicate update events
    Object lastUpdateValue = "Object_13968_5";
    long lastUpdateSequenceId = 104;
    GatewaySenderEventImpl event1 = createGatewaySenderEvent(lr, Operation.CREATE,
        "Object_13964", "Object_13964_1", 1, 100);
    GatewaySenderEventImpl event2 = createGatewaySenderEvent(lr, Operation.CREATE,
        "Object_13965", "Object_13965_2", 1, 101);
    GatewaySenderEventImpl event3 = createGatewaySenderEvent(lr, Operation.CREATE,
        "Object_13966", "Object_13966_3", 1, 102);
    GatewaySenderEventImpl event4 = createGatewaySenderEvent(lr, Operation.CREATE,
        "Object_13967", "Object_13967_4", 1, 103);
    GatewaySenderEventImpl event5 = createGatewaySenderEvent(lr, Operation.CREATE,
        "Object_13968", lastUpdateValue, 1, lastUpdateSequenceId);

    bucketRegionQueue.addToQueue(1L, event1);
    bucketRegionQueue.addToQueue(2L, event2);
    bucketRegionQueue.addToQueue(3L, event3);
    bucketRegionQueue.addToQueue(4L, event4);
    bucketRegionQueue.addToQueue(5L, event5);

    bucketRegionQueue.beforeAcquiringPrimaryState();

    List<Object> objects = bucketRegionQueue.getHelperQueueList();

    assertThat(objects.size()).isEqualTo(5);

    for (Object o : objects) {
      assertThat(((GatewaySenderEventImpl) o).getPossibleDuplicate()).isFalse();
    }

    Object peekObj = bucketRegionQueue.peek();

    while (peekObj != null) {
      assertThat(((GatewaySenderEventImpl) peekObj).getPossibleDuplicate()).isTrue();
      peekObj = bucketRegionQueue.peek();
    }

  }

}
