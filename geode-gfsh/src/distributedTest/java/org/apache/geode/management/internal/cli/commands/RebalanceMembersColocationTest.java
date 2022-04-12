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

package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Objects;
import java.util.stream.IntStream;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.cache.PartitionAttributesImpl;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.junit.categories.PersistenceTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;


@Category(PersistenceTest.class)
@RunWith(GeodeParamsRunner.class)
public class RebalanceMembersColocationTest {

  public static final String PARENT_REGION_NAME = "parentRegion";
  public static final String CHILD_REGION_NAME = "childRegion";
  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Test
  public void testRebalanceResultOutputMemberCountWithColocatedRegions() throws Exception {
    var locator = cluster.startLocatorVM(0);

    var server1 = cluster.startServerVM(1, locator.getPort());
    var server2 = cluster.startServerVM(2, locator.getPort());

    server1.invoke(() -> {
      var parentRegion = Objects.requireNonNull(ClusterStartupRule.getCache())
          .createRegionFactory(RegionShortcut.PARTITION).create(PARENT_REGION_NAME);

      IntStream.range(0, 500).forEach(i -> parentRegion.put("key" + i, "value" + 1));

      var attributes = new PartitionAttributesImpl();
      attributes.setColocatedWith(PARENT_REGION_NAME);

      var childRegion = Objects.requireNonNull(ClusterStartupRule.getCache())
          .createRegionFactory(RegionShortcut.PARTITION).setPartitionAttributes(attributes)
          .create(CHILD_REGION_NAME);

      IntStream.range(0, 500).forEach(i -> childRegion.put("key" + i, "value" + 1));
    });

    server2.invoke(() -> {
      Objects.requireNonNull(ClusterStartupRule.getCache())
          .createRegionFactory(RegionShortcut.PARTITION).create(PARENT_REGION_NAME);

      var attributes = new PartitionAttributesImpl();
      attributes.setColocatedWith(PARENT_REGION_NAME);

      Objects.requireNonNull(ClusterStartupRule.getCache())
          .createRegionFactory(RegionShortcut.PARTITION).setPartitionAttributes(attributes)
          .create(CHILD_REGION_NAME);
    });

    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + PARENT_REGION_NAME, 2);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + CHILD_REGION_NAME, 2);

    gfsh.connectAndVerify(locator);

    var rebalanceResult =
        gfsh.executeAndAssertThat("rebalance --include-region=" + SEPARATOR + PARENT_REGION_NAME)
            .statusIsSuccess().hasTableSection().getActual().getContent();

    assertThat(rebalanceResult.get("Value").get(9)).isEqualTo("2");
  }
}
