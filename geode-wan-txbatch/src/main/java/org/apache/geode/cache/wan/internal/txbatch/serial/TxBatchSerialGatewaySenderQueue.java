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

package org.apache.geode.cache.wan.internal.txbatch.serial;

import static org.apache.geode.cache.wan.internal.txbatch.SystemProperties.GET_TRANSACTION_EVENTS_FROM_QUEUE_RETRIES;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.wan.internal.txbatch.TxBatchMetadata;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.cache.wan.serial.SerialGatewaySenderQueue;

public class TxBatchSerialGatewaySenderQueue extends SerialGatewaySenderQueue {

  public TxBatchSerialGatewaySenderQueue(final @NotNull AbstractGatewaySender sender,
                                         final @NotNull String regionName,
                                         final @Nullable CacheListener<?, ?> listener,
                                         final boolean cleanQueues) {
    super(sender, regionName, listener, cleanQueues);
  }

  public TxBatchSerialGatewaySenderQueue(final @NotNull AbstractGatewaySender sender,
                                         final @NotNull String regionName,
                                         final @Nullable CacheListener<?, ?> listener, final boolean cleanQueues,
                                         final @NotNull MetaRegionFactory metaRegionFactory) {
    super(sender, regionName, listener, cleanQueues, metaRegionFactory);
  }

  @Override
  protected void postProcessBatch(final @NotNull List<AsyncEvent<?, ?>> batch,
      final long lastKey) {
    if (batch.isEmpty()) {
      return;
    }

    final Set<TransactionId> incompleteTransactionIdsInBatch =
        getIncompleteTransactionsInBatch(batch);
    if (incompleteTransactionIdsInBatch.isEmpty()) {
      return;
    }

    int retries = 0;
    do {
      for (final Iterator<TransactionId> iter = incompleteTransactionIdsInBatch.iterator(); iter
          .hasNext();) {
        final TransactionId transactionId = iter.next();
        final List<KeyAndEventPair> keyAndEventPairs =
            peekEventsWithTransactionId(transactionId, lastKey);
        if (!keyAndEventPairs.isEmpty() && hasLastEvent(keyAndEventPairs)) {
          for (KeyAndEventPair object : keyAndEventPairs) {
            final GatewaySenderEventImpl event = (GatewaySenderEventImpl) object.event;
            batch.add(event);
            peekedIds.add(object.key);
            extraPeekedIds.add(object.key);
            if (logger.isDebugEnabled()) {
              final TxBatchMetadata metadata = event.getMetadata();
              logger.debug(
                  "Peeking extra event: {}, isLastEventInTransaction: {}, batch size: {}",
                  event.getKey(), metadata != null && metadata.isLastEvent(), batch.size());
            }
          }
          iter.remove();
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

  private boolean hasLastEvent(final @NotNull List<KeyAndEventPair> keyAndEventPairs) {
    final GatewaySenderEventImpl event =
        (GatewaySenderEventImpl) (keyAndEventPairs.get(keyAndEventPairs.size() - 1)).event;
    final TxBatchMetadata metadata = event.getMetadata();
    return metadata != null && metadata.isLastEvent();
  }

  private List<KeyAndEventPair> peekEventsWithTransactionId(
      final @NotNull TransactionId transactionId,
      long lastKey) {
    final Predicate<GatewaySenderEventImpl> hasTransactionIdPredicate =
        x -> {
          final TxBatchMetadata metadata = x.getMetadata();
          return metadata != null && transactionId.equals(metadata.getTransactionId());
        };
    final Predicate<GatewaySenderEventImpl> isLastEventInTransactionPredicate =
        x -> {
          final TxBatchMetadata metadata = x.getMetadata();
          return metadata == null || metadata.isLastEvent();
        };

    return getElementsMatching(hasTransactionIdPredicate, isLastEventInTransactionPredicate,
        lastKey);
  }

  private @NotNull Set<TransactionId> getIncompleteTransactionsInBatch(
      final @NotNull List<AsyncEvent<?, ?>> batch) {
    final Set<TransactionId> incompleteTransactionsInBatch = new HashSet<>();
    for (final Object object : batch) {
      if (object instanceof GatewaySenderEventImpl) {
        final GatewaySenderEventImpl event = (GatewaySenderEventImpl) object;
        final TxBatchMetadata metadata = event.getMetadata();
        if (metadata != null) {
          final TransactionId transactionId = metadata.getTransactionId();
          if (metadata.isLastEvent()) {
            incompleteTransactionsInBatch.remove(transactionId);
          } else {
            incompleteTransactionsInBatch.add(transactionId);
          }
        }
      }
    }
    return incompleteTransactionsInBatch;
  }

  @Override
  protected boolean shouldIncEventsNotQueuedConflated() {
    // When mustGroupTransactionEvents is true, conflation cannot be enabled.
    // Therefore, if we reach here, it would not be due to a conflated event
    // but rather to an extra peeked event already sent.
    return false;
  }
}
