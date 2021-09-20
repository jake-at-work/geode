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

import static org.apache.geode.cache.wan.internal.txbatch.SystemProperties.GET_TRANSACTION_EVENTS_FROM_QUEUE_RETRIES;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import org.jetbrains.annotations.NotNull;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.cache.wan.internal.txbatch.TxBatchMetadata;
import org.apache.geode.internal.cache.BucketRegionQueue;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.cache.wan.parallel.BucketRegionQueueUnavailableException;
import org.apache.geode.internal.cache.wan.parallel.ParallelGatewaySenderQueue;

public class TxBatchParallelGatewaySenderQueue extends ParallelGatewaySenderQueue {

  TxBatchParallelGatewaySenderQueue(final @NotNull AbstractGatewaySender sender,
                                    final @NotNull Set<Region<?, ?>> userRegions, final int idx,
                                    final int nDispatcher,
                                    final boolean cleanQueues) {
    super(sender, userRegions, idx, nDispatcher, cleanQueues);
  }

  @VisibleForTesting
  TxBatchParallelGatewaySenderQueue(final @NotNull AbstractGatewaySender sender,
                                    final @NotNull Set<Region<?, ?>> userRegions, final int idx,
                                    final int nDispatcher, final @NotNull MetaRegionFactory metaRegionFactory,
                                    final boolean cleanQueues) {
    super(sender, userRegions, idx, nDispatcher, metaRegionFactory, cleanQueues);
  }

  @Override
  protected void postProcessBatch(final @NotNull List<GatewaySenderEventImpl> batch,
      final @NotNull PartitionedRegion prQ) {

    if (batch.isEmpty()) {
      return;
    }

    final Map<TransactionId, Integer> incompleteTransactionIdsInBatch =
        getIncompleteTransactionsInBatch(batch);
    if (incompleteTransactionIdsInBatch.isEmpty()) {
      return;
    }

    int retries = 0;
    do {
      for (Iterator<Map.Entry<TransactionId, Integer>> iter =
          incompleteTransactionIdsInBatch.entrySet().iterator(); iter.hasNext();) {
        Map.Entry<TransactionId, Integer> pendingTransaction = iter.next();
        TransactionId transactionId = pendingTransaction.getKey();
        int bucketId = pendingTransaction.getValue();
        List<Object> events = peekEventsWithTransactionId(prQ, bucketId, transactionId);
        for (Object object : events) {
          GatewaySenderEventImpl event = (GatewaySenderEventImpl) object;
          batch.add(event);
          peekedEvents.add(event);
          final TxBatchMetadata metadata = event.getMetadata();
          if (null != metadata) {
            if (logger.isDebugEnabled()) {
              logger.debug(
                  "Peeking extra event: {}, bucketId: {}, isLastEventInTransaction: {}, batch size: {}",
                  event.getKey(), bucketId, metadata.isLastEvent(), batch.size());
            }
            if (metadata.isLastEvent()) {
              iter.remove();
            }
          }
        }
      }
    } while (!incompleteTransactionIdsInBatch.isEmpty() &&
        retries++ != GET_TRANSACTION_EVENTS_FROM_QUEUE_RETRIES);

    if (!incompleteTransactionIdsInBatch.isEmpty()) {
      logger.warn("Not able to retrieve all events for transactions: {} after {} retries",
          incompleteTransactionIdsInBatch, retries);
      stats.incBatchesWithIncompleteTransactions();
    }
  }

  @VisibleForTesting
  static @NotNull Predicate<GatewaySenderEventImpl> getIsLastEventInTransactionPredicate() {
    return x -> {
      final TxBatchMetadata metadata = x.getMetadata();
      return metadata == null || metadata.isLastEvent();
    };
  }

  @VisibleForTesting
  static @NotNull Predicate<GatewaySenderEventImpl> getHasTransactionIdPredicate(
      final @NotNull TransactionId transactionId) {
    return x -> {
      final TxBatchMetadata metadata = x.getMetadata();
      return metadata != null && transactionId.equals(metadata.getTransactionId());
    };
  }

  private @NotNull List<Object> peekEventsWithTransactionId(final @NotNull PartitionedRegion prQ,
      final int bucketId,
      final @NotNull TransactionId transactionId) throws CacheException {
    List<Object> objects;
    BucketRegionQueue brq = getBucketRegionQueueByBucketId(prQ, bucketId);

    try {
      Predicate<GatewaySenderEventImpl> hasTransactionIdPredicate =
          getHasTransactionIdPredicate(transactionId);
      Predicate<GatewaySenderEventImpl> isLastEventInTransactionPredicate =
          getIsLastEventInTransactionPredicate();
      objects =
          brq.getElementsMatching(hasTransactionIdPredicate, isLastEventInTransactionPredicate);
    } catch (BucketRegionQueueUnavailableException e) {
      // BucketRegionQueue unavailable. Can be due to the BucketRegionQueue being destroyed.
      return Collections.emptyList();
    }

    return objects; // OFFHEAP: ok since callers are careful to do destroys on region queue after
    // finished with peeked objects.
  }

  private @NotNull Map<TransactionId, Integer> getIncompleteTransactionsInBatch(
      @NotNull List<GatewaySenderEventImpl> batch) {
    Map<TransactionId, Integer> incompleteTransactionsInBatch = new HashMap<>();
    for (GatewaySenderEventImpl event : batch) {
      final TxBatchMetadata metadata = event.getMetadata();
      if (metadata != null) {
        if (metadata.isLastEvent()) {
          incompleteTransactionsInBatch.remove(metadata.getTransactionId());
        } else {
          incompleteTransactionsInBatch.put(metadata.getTransactionId(), event.getBucketId());
        }
      }
    }
    return incompleteTransactionsInBatch;
  }

  @Override
  protected void addPreviouslyPeekedEvents(final @NotNull List<GatewaySenderEventImpl> batch,
      final int batchSize) {
    final @NotNull Set<TransactionId> incompleteTransactionsInBatch = new HashSet<>();
    for (int i = 0; i < batchSize || !incompleteTransactionsInBatch.isEmpty(); i++) {
      GatewaySenderEventImpl event = peekedEventsProcessing.remove();
      batch.add(event);
      final TxBatchMetadata metadata = event.getMetadata();
      if (metadata != null) {
        if (metadata.isLastEvent()) {
          incompleteTransactionsInBatch.remove(metadata.getTransactionId());
        } else {
          incompleteTransactionsInBatch.add(metadata.getTransactionId());
        }
      }
      if (peekedEventsProcessing.isEmpty()) {
        resetLastPeeked = false;
        peekedEventsProcessingInProgress = false;
        break;
      }
    }
  }

}
