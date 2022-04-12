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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;

import org.junit.Test;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.query.internal.cq.CqService;
import org.apache.geode.cache.query.internal.cq.ServerCQ;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.tier.MessageType;


public class DistributedRemoveAllOperationTest {

  @Test
  public void shouldBeMockable() throws Exception {
    var mockDistributedRemoveAllOperation =
        mock(DistributedRemoveAllOperation.class);
    var mockEntryEventImpl = mock(EntryEventImpl.class);

    when(mockDistributedRemoveAllOperation.getBaseEvent()).thenReturn(mockEntryEventImpl);

    assertThat(mockDistributedRemoveAllOperation.getBaseEvent()).isSameAs(mockEntryEventImpl);
  }

  @Test
  public void testDoRemoveDestroyTokensFromCqResultKeys() {
    var baseEvent = mock(EntryEventImpl.class);
    var entryEvent = mock(EntryEventImpl.class);
    var bucketRegion = mock(BucketRegion.class);
    var internalCache = mock(InternalCache.class);
    var regionAttributes = mock(RegionAttributes.class);
    var internalDistributedSystem = mock(InternalDistributedSystem.class);
    var filterInfo = mock(FilterRoutingInfo.FilterInfo.class);
    var cqService = mock(CqService.class);
    var partitionedRegion = mock(PartitionedRegion.class);
    var serverCQ = mock(ServerCQ.class);
    var removeAllPRDataSize = 1;
    var distributedRemoveAllOperation =
        new DistributedRemoveAllOperation(baseEvent, removeAllPRDataSize, false);
    var key = new Object();
    when(entryEvent.getKey()).thenReturn(key);
    distributedRemoveAllOperation.addEntry(entryEvent);
    var hashMap = new HashMap();
    hashMap.put(1L, MessageType.LOCAL_DESTROY);
    when(filterInfo.getCQs()).thenReturn(hashMap);
    when(baseEvent.getRegion()).thenReturn(bucketRegion);
    when(bucketRegion.getAttributes()).thenReturn(regionAttributes);
    when(bucketRegion.getPartitionedRegion()).thenReturn(partitionedRegion);
    when(bucketRegion.getCache()).thenReturn(internalCache);
    when(bucketRegion.getKeyInfo(any(), any(), any())).thenReturn(new KeyInfo(key, null, null));
    when(regionAttributes.getDataPolicy()).thenReturn(DataPolicy.DEFAULT);
    when(internalCache.getDistributedSystem()).thenReturn(internalDistributedSystem);
    when(internalCache.getCqService()).thenReturn(cqService);
    when(serverCQ.getFilterID()).thenReturn(1L);
    doNothing().when(serverCQ).removeFromCqResultKeys(isA(Object.class), isA(Boolean.class));

    distributedRemoveAllOperation.doRemoveDestroyTokensFromCqResultKeys(filterInfo, serverCQ);

    verify(serverCQ, times(1)).removeFromCqResultKeys(key, true);
  }
}
