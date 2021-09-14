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

package org.apache.geode.cache.wan.internal.txbatch.parallel;

import static java.util.Collections.emptySet;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import org.junit.Test;

import org.apache.geode.cache.TransactionId;
import org.apache.geode.cache.wan.internal.txbatch.TxBatchMetadata;
import org.apache.geode.internal.cache.AbstractBucketRegionQueueTest;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.InitialImageOperation;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.internal.cache.TXId;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.cache.wan.parallel.ConcurrentParallelGatewaySenderQueue;
import org.apache.geode.internal.cache.wan.parallel.ParallelGatewaySenderEventProcessor;

public class TxBatchingParallelGatewaySenderQueueTest extends AbstractBucketRegionQueueTest {

  @Test
  public void testGetElementsMatchingWithParallelGatewaySenderQueuePredicatesAndSomeEventsNotInTransactions()
      throws ForceReattemptException {
    createParallelGatewaySenderEventProcessor(sender);

    TransactionId tx1 = new TXId(null, 1);
    TransactionId tx2 = new TXId(null, 2);
    TransactionId tx3 = new TXId(null, 3);

    GatewaySenderEventImpl event1 = createMockGatewaySenderEvent(1, tx1, false);
    GatewaySenderEventImpl eventNotInTransaction1 = createMockGatewaySenderEvent(2, null, false);
    GatewaySenderEventImpl event2 = createMockGatewaySenderEvent(3, tx2, false);
    GatewaySenderEventImpl event3 = createMockGatewaySenderEvent(4, tx1, true);
    GatewaySenderEventImpl event4 = createMockGatewaySenderEvent(5, tx2, true);
    GatewaySenderEventImpl event5 = createMockGatewaySenderEvent(6, tx3, false);
    GatewaySenderEventImpl event6 = createMockGatewaySenderEvent(7, tx3, false);
    GatewaySenderEventImpl event7 = createMockGatewaySenderEvent(8, tx1, true);

    cleanUpDestroyedTokensAndMarkGIIComplete(bucketRegionQueue,
        InitialImageOperation.GIIStatus.NO_GII);

    bucketRegionQueue.addToQueue(1L, event1);
    bucketRegionQueue.addToQueue(2L, eventNotInTransaction1);
    bucketRegionQueue.addToQueue(3L, event2);
    bucketRegionQueue.addToQueue(4L, event3);
    bucketRegionQueue.addToQueue(5L, event4);
    bucketRegionQueue.addToQueue(6L, event5);
    bucketRegionQueue.addToQueue(7L, event6);
    bucketRegionQueue.addToQueue(8L, event7);

    Predicate<GatewaySenderEventImpl> hasTransactionIdPredicate =
        TxBatchingParallelGatewaySenderQueue.getHasTransactionIdPredicate(tx1);
    Predicate<GatewaySenderEventImpl> isLastEventInTransactionPredicate =
        TxBatchingParallelGatewaySenderQueue.getIsLastEventInTransactionPredicate();
    List<Object> objects = bucketRegionQueue.getElementsMatching(hasTransactionIdPredicate,
        isLastEventInTransactionPredicate);

    assertEquals(2, objects.size());
    assertEquals(objects, Arrays.asList(event1, event3));

    objects = bucketRegionQueue.getElementsMatching(hasTransactionIdPredicate,
        isLastEventInTransactionPredicate);
    assertEquals(1, objects.size());
    assertEquals(objects, Collections.singletonList(event7));

    hasTransactionIdPredicate =
        TxBatchingParallelGatewaySenderQueue.getHasTransactionIdPredicate(tx2);
    objects = bucketRegionQueue.getElementsMatching(hasTransactionIdPredicate,
        isLastEventInTransactionPredicate);
    assertEquals(2, objects.size());
    assertEquals(objects, Arrays.asList(event2, event4));
  }

  private GatewaySenderEventImpl createMockGatewaySenderEvent(Object key, TransactionId tId,
      boolean isLastEventInTx) {
    GatewaySenderEventImpl event = mock(GatewaySenderEventImpl.class);
    when(event.getMetadata())
        .thenReturn(tId == null ? null : new TxBatchMetadata(tId, isLastEventInTx));
    when(event.getKey()).thenReturn(key);
    return event;
  }

  private static void createParallelGatewaySenderEventProcessor(
      AbstractGatewaySender sender) {
    ParallelGatewaySenderEventProcessor processor =
        new TxBatchingRemoteParallelGatewaySenderEventProcessor(sender, emptySet(), 0, 1, null,
            false);
    ConcurrentParallelGatewaySenderQueue queue = new ConcurrentParallelGatewaySenderQueue(sender,
        new ParallelGatewaySenderEventProcessor[] {processor});
    Set<RegionQueue> queues = new HashSet<>();
    queues.add(queue);
    when(sender.getQueues()).thenReturn(queues);
  }

}
