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
 *
 */

package org.apache.geode.management.internal.operation;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheForClientAccess;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.management.DistributedRegionMXBean;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.internal.BaseManagementService;
import org.apache.geode.management.operation.RebalanceOperation;

public class RebalanceOperationPerformerTest {
  private RebalanceOperationPerformer performer;

  @Before
  public void before() throws Exception {
    performer = new RebalanceOperationPerformer();
  }

  @Test
  public void executeRebalanceOnDSWithNoRegionsReturnsSuccessAndNoRegionMessage() {
    var managementService = mock(ManagementService.class);
    var distributedSystemMXBean = mock(DistributedSystemMXBean.class);
    when(distributedSystemMXBean.listAllRegionPaths()).thenReturn(new String[] {});
    when(managementService.getDistributedSystemMXBean()).thenReturn(distributedSystemMXBean);
    var internalDistributedSystem = mock(InternalDistributedSystem.class);
    var cache = mock(InternalCache.class);
    when(cache.getInternalDistributedSystem()).thenReturn(internalDistributedSystem);
    when(internalDistributedSystem.getDistributionManager())
        .thenReturn(mock(DistributionManager.class));
    var functionExecutor =
        mock(RebalanceOperationPerformer.FunctionExecutor.class);

    var result = performer.executeRebalanceOnDS(managementService,
        cache, "true", null, functionExecutor);

    assertThat(result.getSuccess()).isTrue();
    assertThat(result.getStatusMessage())
        .isEqualTo("Distributed system has no regions that can be rebalanced.");
  }

  @Test
  public void executeRebalanceOnDSWithOneRegionReturnsSuccessAndNoRegionMessage() {
    var managementService = mock(ManagementService.class);
    var regionMXBean = mock(DistributedRegionMXBean.class);
    when(regionMXBean.getRegionType()).thenReturn("PARTITION");
    when(regionMXBean.getMembers()).thenReturn(new String[] {"member1"});
    when(managementService.getDistributedRegionMXBean(SEPARATOR + "region1"))
        .thenReturn(regionMXBean);
    var distributedSystemMXBean = mock(DistributedSystemMXBean.class);
    when(distributedSystemMXBean.listAllRegionPaths()).thenReturn(new String[] {"region1"});
    when(managementService.getDistributedSystemMXBean()).thenReturn(distributedSystemMXBean);
    var internalDistributedSystem = mock(InternalDistributedSystem.class);
    var cache = mock(InternalCache.class);
    when(cache.getInternalDistributedSystem()).thenReturn(internalDistributedSystem);
    var distributionManager = mock(DistributionManager.class);
    var distributedMember = mock(InternalDistributedMember.class);
    when(distributedMember.getUniqueId()).thenReturn("member1");
    var members = Collections.singleton(distributedMember);
    when(distributionManager.getDistributionManagerIds()).thenReturn(members);
    when(internalDistributedSystem.getDistributionManager()).thenReturn(distributionManager);
    var functionExecutor =
        mock(RebalanceOperationPerformer.FunctionExecutor.class);

    var result =
        performer.executeRebalanceOnDS(managementService, cache, "true",
            Collections.emptyList(), functionExecutor);

    assertThat(result.getSuccess()).isTrue();
    assertThat(result.getStatusMessage())
        .isEqualTo("Distributed system has no regions that can be rebalanced.");
  }

  @Test
  public void executeRebalanceOnDSWithOneRegionOnTwoMembersReturnsSuccessAndRebalanceRegionResult() {
    var managementService = mock(ManagementService.class);
    var regionMXBean = mock(DistributedRegionMXBean.class);
    when(regionMXBean.getRegionType()).thenReturn("PARTITION");
    when(regionMXBean.getMembers()).thenReturn(new String[] {"member1", "member2"});
    when(managementService.getDistributedRegionMXBean(SEPARATOR + "region1"))
        .thenReturn(regionMXBean);
    var distributedSystemMXBean = mock(DistributedSystemMXBean.class);
    when(distributedSystemMXBean.listAllRegionPaths()).thenReturn(new String[] {"region1"});
    when(managementService.getDistributedSystemMXBean()).thenReturn(distributedSystemMXBean);
    var internalDistributedSystem = mock(InternalDistributedSystem.class);
    var cache = mock(InternalCache.class);
    when(cache.getInternalDistributedSystem()).thenReturn(internalDistributedSystem);
    var distributionManager = mock(DistributionManager.class);
    when(cache.getDistributionManager()).thenReturn(distributionManager);
    var distributedMember1 = mock(InternalDistributedMember.class);
    when(distributedMember1.getUniqueId()).thenReturn("member1");
    var distributedMember2 = mock(InternalDistributedMember.class);
    when(distributedMember2.getUniqueId()).thenReturn("member2");
    Set<InternalDistributedMember> members = new HashSet<>();
    members.add(distributedMember1);
    members.add(distributedMember2);
    when(distributionManager.getDistributionManagerIds()).thenReturn(members);
    when(distributionManager.getNormalDistributionManagerIds()).thenReturn(members);
    when(internalDistributedSystem.getDistributionManager()).thenReturn(distributionManager);
    var functionExecutor =
        mock(RebalanceOperationPerformer.FunctionExecutor.class);
    List<Object> resultList = new ArrayList<>();
    resultList.add("0,1,2,3,4,5,6,7,8,9," + SEPARATOR + "region1");
    when(functionExecutor.execute(any(), any(), any())).thenReturn(resultList);
    when(distributedMember1.getVersion()).thenReturn(KnownVersion.getCurrentVersion());
    when(distributedMember2.getVersion()).thenReturn(KnownVersion.getCurrentVersion());
    var result =
        performer.executeRebalanceOnDS(managementService, cache, "true",
            Collections.emptyList(), functionExecutor);

    assertThat(result.getSuccess()).isTrue();
    assertThat(result.getRebalanceRegionResults()).isNotNull();
    assertThat(result.getRebalanceRegionResults()).hasSize(1);
    var regionResult = result.getRebalanceRegionResults().get(0);
    assertThat(regionResult.getRegionName()).isEqualTo(SEPARATOR + "region1");
    assertThat(regionResult.getBucketCreateBytes()).isEqualTo(0);
    assertThat(regionResult.getBucketCreateTimeInMilliseconds()).isEqualTo(1);
    assertThat(regionResult.getBucketCreatesCompleted()).isEqualTo(2);
    assertThat(regionResult.getBucketTransferBytes()).isEqualTo(3);
    assertThat(regionResult.getBucketTransferTimeInMilliseconds()).isEqualTo(4);
    assertThat(regionResult.getBucketTransfersCompleted()).isEqualTo(5);
    assertThat(regionResult.getPrimaryTransferTimeInMilliseconds()).isEqualTo(6);
    assertThat(regionResult.getPrimaryTransfersCompleted()).isEqualTo(7);
    assertThat(regionResult.getTimeInMilliseconds()).isEqualTo(8);
  }


  @Test
  public void performWithIncludeRegionsWhenRegionOnNoMembersReturnsFalseWithCorrectMessage() {
    var rebalanceOperation = mock(RebalanceOperation.class);
    when(rebalanceOperation.getIncludeRegions()).thenReturn(Collections.singletonList("region1"));
    var cache = mock(InternalCacheForClientAccess.class);
    when(cache.getCacheForProcessingClientRequests()).thenReturn(cache);
    var managementService = mock(BaseManagementService.class);
    BaseManagementService.setManagementService(cache, managementService);

    var result = performer.perform(cache, rebalanceOperation);

    assertThat(result.getSuccess()).isFalse();
    assertThat(result.getStatusMessage())
        .isEqualTo("For the region " + SEPARATOR + "region1, no member was found.");
  }

}
