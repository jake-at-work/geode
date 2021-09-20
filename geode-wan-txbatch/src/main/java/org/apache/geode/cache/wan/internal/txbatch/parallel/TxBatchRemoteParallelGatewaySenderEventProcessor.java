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

import java.io.IOException;
import java.util.Set;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.cache.wan.internal.parallel.RemoteParallelGatewaySenderEventProcessor;
import org.apache.geode.cache.wan.internal.txbatch.TxBatchMetadata;
import org.apache.geode.internal.cache.EnumListenerEvent;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.cache.wan.parallel.ParallelGatewaySenderQueue;
import org.apache.geode.internal.monitoring.ThreadsMonitoring;

public class TxBatchRemoteParallelGatewaySenderEventProcessor extends
    RemoteParallelGatewaySenderEventProcessor {


  protected TxBatchRemoteParallelGatewaySenderEventProcessor(
      final @NotNull AbstractGatewaySender sender, final @NotNull Set<Region<?, ?>> userRegions,
      final int id,
      final int nDispatcher,
      final @Nullable ThreadsMonitoring tMonitoring, final boolean cleanQueues) {
    super(sender, userRegions, id, nDispatcher, tMonitoring, cleanQueues);
  }

  @Override
  protected @NotNull GatewaySenderEventImpl createGatewaySenderEvent(
      final @NotNull EnumListenerEvent operation,
      final @NotNull EntryEvent<?, ?> event,
      final @Nullable Object substituteValue,
      final boolean isLastEventInTransaction,
      final @NotNull EventID eventID)
      throws IOException {
    final TransactionId transactionId = event.getTransactionId();
    if (null == transactionId) {
      return super.createGatewaySenderEvent(operation, event, substituteValue,
          isLastEventInTransaction, eventID);
    }

    return new GatewaySenderEventImpl(operation, event, substituteValue, true,
        eventID.getBucketID(), new TxBatchMetadata(transactionId, isLastEventInTransaction));
  }

  @Override
  protected @NotNull ParallelGatewaySenderQueue createParallelGatewaySenderQueue(
      final @NotNull AbstractGatewaySender sender, final @NotNull Set<Region<?, ?>> targetRegions,
      final int index, final int nDispatcher, final boolean cleanQueues) {
    return new TxBatchParallelGatewaySenderQueue(sender, targetRegions, index, nDispatcher,
        cleanQueues);
  }

}
