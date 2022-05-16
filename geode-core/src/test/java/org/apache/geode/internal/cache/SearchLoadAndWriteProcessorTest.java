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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.versions.VersionTag;

public class SearchLoadAndWriteProcessorTest {


  InternalDistributedMember departedMember;

  @Test
  public void verifyNoProcessingReplyFromADepartedMember() {
    SearchLoadAndWriteProcessor processor = SearchLoadAndWriteProcessor.getProcessor();
    DistributedRegion lr = mock(DistributedRegion.class);
    RegionAttributes attrs = mock(RegionAttributes.class);
    GemFireCacheImpl cache = mock(GemFireCacheImpl.class);
    InternalDistributedSystem ds = mock(InternalDistributedSystem.class);
    DistributionManager dm = mock(DistributionManager.class);
    CacheDistributionAdvisor advisor = mock(CacheDistributionAdvisor.class);
    CachePerfStats stats = mock(CachePerfStats.class);
    ExpirationAttributes expirationAttrs = mock(ExpirationAttributes.class);
    InternalDistributedMember m1 = mock(InternalDistributedMember.class);
    InternalDistributedMember m2 = mock(InternalDistributedMember.class);
    Set<InternalDistributedMember> replicates = new HashSet<>();
    replicates.add(m1);
    replicates.add(m2);

    when(lr.getAttributes()).thenReturn(attrs);
    when(lr.getSystem()).thenReturn(ds);
    when(lr.getCache()).thenReturn(cache);
    when(lr.getCacheDistributionAdvisor()).thenReturn(advisor);
    when(lr.getDistributionManager()).thenReturn(dm);
    when(lr.getCachePerfStats()).thenReturn(stats);
    when(lr.getScope()).thenReturn(Scope.DISTRIBUTED_ACK);
    when(lr.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));
    when(cache.getDistributedSystem()).thenReturn(ds);
    when(cache.getInternalDistributedSystem()).thenReturn(ds);
    when(cache.getSearchTimeout()).thenReturn(30);
    when(attrs.getScope()).thenReturn(Scope.DISTRIBUTED_ACK);
    when(attrs.getDataPolicy()).thenReturn(DataPolicy.EMPTY);
    when(attrs.getEntryTimeToLive()).thenReturn(expirationAttrs);
    when(attrs.getEntryIdleTimeout()).thenReturn(expirationAttrs);
    when(advisor.adviseInitializedReplicates()).thenReturn(replicates);

    Object key = "k1";
    byte[] v1 = "v1".getBytes();
    byte[] v2 = "v2".getBytes();
    EntryEventImpl event = EntryEventImpl.create(lr, Operation.GET, key, null, null, false, null);


    Thread t1 = new Thread(() -> {
      await()
          .until(() -> processor.getSelectedNode() != null);
      departedMember = processor.getSelectedNode();
      // Simulate member departed event
      processor.memberDeparted(dm, departedMember, true);
    });
    t1.start();

    Thread t2 = new Thread(() -> {
      await()
          .until(() -> departedMember != null && processor.getSelectedNode() != null
              && departedMember != processor.getSelectedNode());

      // Handle search result from the departed member
      processor.incomingNetSearchReply(v1, System.currentTimeMillis(), false, false, true,
          mock(VersionTag.class), departedMember);
    });
    t2.start();

    Thread t3 = new Thread(() -> {
      await()
          .until(() -> departedMember != null && processor.getSelectedNode() != null
              && departedMember != processor.getSelectedNode());
      // Handle search result from a new member
      processor.incomingNetSearchReply(v2, System.currentTimeMillis(), false, false, true,
          mock(VersionTag.class), processor.getSelectedNode());
    });
    t3.start();

    processor.initialize(lr, key, null);
    processor.doSearchAndLoad(event, null, null, false);

    assertTrue(Arrays.equals((byte[]) event.getNewValue(), v2));
  }

}
