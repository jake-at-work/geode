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

package org.apache.geode.management.internal.rest;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.management.api.ClusterManagementOperationResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.cluster.client.ClusterManagementServiceBuilder;
import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.configuration.RegionType;
import org.apache.geode.management.operation.RebalanceOperation;
import org.apache.geode.management.runtime.RebalanceResult;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.junit.rules.MemberStarterRule;

public class RebalanceManagementDunitTest {

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  private static ClusterManagementService client1, client2;

  @BeforeClass
  public static void beforeClass() {
    var locator1 = cluster.startLocatorVM(0, MemberStarterRule::withHttpService);
    var locator1Port = locator1.getPort();
    var locator2 =
        cluster.startLocatorVM(1, l -> l.withHttpService().withConnectionToLocator(locator1Port));
    cluster.startServerVM(2, "group1", locator1Port);
    cluster.startServerVM(3, "group2", locator1Port);

    client1 = new ClusterManagementServiceBuilder()
        .setHost("localhost")
        .setPort(locator1.getHttpPort())
        .build();
    client2 = new ClusterManagementServiceBuilder()
        .setHost("localhost")
        .setPort(locator2.getHttpPort())
        .build();

    // create regions
    var regionConfig = new Region();
    regionConfig.setName("customers1");
    regionConfig.setType(RegionType.PARTITION);
    client1.create(regionConfig);
    locator1.waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + "customers1", 2);

    regionConfig = new Region();
    regionConfig.setName("customers2");
    regionConfig.setType(RegionType.PARTITION);
    client1.create(regionConfig);
    locator1.waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + "customers2", 2);
  }

  @Test
  public void rebalance() throws Exception {
    var startResult =
        client1.start(new RebalanceOperation());
    assertThat(startResult.isSuccessful()).isTrue();
    var now = System.currentTimeMillis();
    assertThat(startResult.getOperationStart().getTime()).isBetween(now - 60000, now);

    var endResult =
        client1.getFuture(new RebalanceOperation(), startResult.getOperationId()).get();
    var end = endResult.getOperationEnd().getTime();
    now = System.currentTimeMillis();
    assertThat(end).isBetween(now - 60000, now)
        .isGreaterThanOrEqualTo(endResult.getOperationStart().getTime());
    var result = endResult.getOperationResult();
    assertThat(result.getRebalanceRegionResults().size()).isEqualTo(2);
    var firstRegionSummary = result.getRebalanceRegionResults().get(0);
    assertThat(firstRegionSummary.getRegionName()).isIn(SEPARATOR + "customers1",
        SEPARATOR + "customers2");
  }

  @Test
  public void rebalanceExistRegion() throws Exception {
    List<String> includeRegions = new ArrayList<>();
    includeRegions.add("customers2");
    var op = new RebalanceOperation();
    op.setIncludeRegions(includeRegions);
    var initialSize = client1.list(op).getResult().size();
    var cmr = client1.start(op);
    assertThat(cmr.isSuccessful()).isTrue();

    var result = client1.getFuture(new RebalanceOperation(), cmr.getOperationId()).get()
        .getOperationResult();
    assertThat(result.getRebalanceRegionResults().size()).isEqualTo(1);
    var firstRegionSummary = result.getRebalanceRegionResults().get(0);
    assertThat(firstRegionSummary.getRegionName()).isEqualTo(SEPARATOR + "customers2");
    assertThat(firstRegionSummary.getBucketCreateBytes()).isEqualTo(0);
    assertThat(firstRegionSummary.getTimeInMilliseconds()).isGreaterThanOrEqualTo(0);

    assertThat(client1.list(op).getResult()).hasSize(initialSize + 1);
    assertThat(client2.list(op).getResult()).hasSize(initialSize + 1);
  }

  @Test
  public void rebalanceExcludedRegion() throws Exception {
    var op = new RebalanceOperation();
    op.setExcludeRegions(Collections.singletonList("customers1"));
    var cmr = client1.start(op);
    assertThat(cmr.isSuccessful()).isTrue();

    var result =
        client1.getFuture(new RebalanceOperation(), cmr.getOperationId()).get()
            .getOperationResult();
    assertThat(result.getRebalanceRegionResults().size()).isEqualTo(1);
    var firstRegionSummary = result.getRebalanceRegionResults().get(0);
    assertThat(firstRegionSummary.getRegionName()).isEqualTo(SEPARATOR + "customers2");
    assertThat(firstRegionSummary.getBucketCreateBytes()).isEqualTo(0);
    assertThat(firstRegionSummary.getTimeInMilliseconds()).isGreaterThanOrEqualTo(0);
  }

  @Test
  public void rebalanceNonExistRegion() {
    IgnoredException.addIgnoredException(ExecutionException.class);
    IgnoredException.addIgnoredException(RuntimeException.class);
    var op = new RebalanceOperation();
    op.setIncludeRegions(Collections.singletonList("nonexisting_region"));
    var cmr = client1.start(op);
    assertThat(cmr.isSuccessful()).isTrue();
    var id = cmr.getOperationId();

    List<ClusterManagementOperationResult<RebalanceOperation, RebalanceResult>> resultArrayList =
        new ArrayList<>();
    GeodeAwaitility.await().untilAsserted(() -> {
      var rebalanceResult =
          getRebalanceResult(op, id);
      if (rebalanceResult != null) {
        resultArrayList.add(rebalanceResult);
      }
      assertThat(rebalanceResult).isNotNull();
    });

    assertThat(resultArrayList.get(0).isSuccessful()).isFalse();
    assertThat(resultArrayList.get(0).getStatusMessage())
        .contains("For the region " + SEPARATOR + "nonexisting_region, no member was found");

  }

  private ClusterManagementOperationResult<RebalanceOperation, RebalanceResult> getRebalanceResult(
      RebalanceOperation op, String id) {
    var listOperationsResult =
        client1.list(op);
    var rebalanceResult =
        listOperationsResult.getResult()
            .stream()
            .filter(rbalresult -> rbalresult.getOperationId().equals(id)
                && rbalresult.getOperationEnd() != null)
            .findFirst();
    return rebalanceResult.orElse(null);
  }

  @Test
  public void rebalanceOneExistingOneNonExistingRegion() throws Exception {
    IgnoredException.addIgnoredException(ExecutionException.class);
    IgnoredException.addIgnoredException(RuntimeException.class);
    var op = new RebalanceOperation();
    op.setIncludeRegions(Arrays.asList("nonexisting_region", "customers1"));
    var cmr = client1.start(op);
    assertThat(cmr.isSuccessful()).isTrue();

    var result = client1.getFuture(new RebalanceOperation(), cmr.getOperationId()).get()
        .getOperationResult();
    assertThat(result.getRebalanceRegionResults().size()).isEqualTo(1);
    var firstRegionSummary = result.getRebalanceRegionResults().get(0);
    assertThat(firstRegionSummary.getRegionName()).isEqualTo(SEPARATOR + "customers1");
    assertThat(firstRegionSummary.getBucketCreateBytes()).isEqualTo(0);
    assertThat(firstRegionSummary.getTimeInMilliseconds()).isGreaterThanOrEqualTo(0);
  }
}
