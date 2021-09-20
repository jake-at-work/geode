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

import static org.apache.geode.cache.wan.internal.txbatch.parallel.TxBatchParallelGatewaySenderImpl.TYPE;

import org.jetbrains.annotations.NotNull;

import org.apache.geode.cache.wan.internal.spi.GatewaySender;
import org.apache.geode.cache.wan.internal.spi.GatewaySenderTypeFactory;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.wan.GatewaySenderAttributes;
import org.apache.geode.internal.statistics.StatisticsClock;

public class TxBatchParallelGatewaySenderTypeFactory implements GatewaySenderTypeFactory {
  @Override
  public @NotNull String getName() {
    return TYPE;
  }

  @Override
  public void check() {

  }

  @Override
  public @NotNull GatewaySender createInstance(final @NotNull InternalCache cache,
      final @NotNull StatisticsClock statisticsClock,
      final @NotNull GatewaySenderAttributes gatewaySenderAttributes) {
    return new TxBatchParallelGatewaySenderImpl(cache, statisticsClock, gatewaySenderAttributes);
  }

  @Override
  public @NotNull GatewaySender createCreation(final @NotNull InternalCache cache,
      final @NotNull GatewaySenderAttributes gatewaySenderAttributes) {
    return new TxBatchParallelGatewaySenderCreation(cache, gatewaySenderAttributes);
  }
}
