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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import org.apache.geode.cache.CacheListener;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.serial.SerialGatewaySenderEventProcessor;
import org.apache.geode.internal.cache.wan.serial.SerialGatewaySenderQueue;
import org.apache.geode.internal.monitoring.ThreadsMonitoring;

public class TxBatchingRemoteSerialGatewaySenderEventProcessor
    extends SerialGatewaySenderEventProcessor {

  public TxBatchingRemoteSerialGatewaySenderEventProcessor(
      final @NotNull AbstractGatewaySender sender,
      final @NotNull String id,
      final @Nullable ThreadsMonitoring threadsMonitoring,
      final boolean cleanQueues) {
    super(sender, id, threadsMonitoring, cleanQueues);
  }

  @Override
  protected @NotNull SerialGatewaySenderQueue createQueue(
      final @NotNull AbstractGatewaySender sender, final @NotNull String regionName,
      final @Nullable CacheListener<?, ?> listener, final boolean cleanQueues) {
    return new TxBatchingSerialGatewaySenderQueue(sender, regionName, listener, cleanQueues);
  }
}
