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
package org.apache.geode.cache.wan;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.wan.parallel.ParallelGatewaySenderQueue;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;

public class WANRollingUpgradeEventProcessingMixedSiteOneOldSiteTwo
    extends WANRollingUpgradeDUnitTest {

  @Test
  public void EventProcessingMixedSiteOneOldSiteTwo() {
    final var host = Host.getHost(0);

    // Get mixed site members
    var site1Locator = host.getVM(oldVersion, 0);
    var site1Server1 = host.getVM(oldVersion, 1);
    var site1Server2 = host.getVM(oldVersion, 2);
    var site1Client = host.getVM(oldVersion, 3);

    // Get old site members
    var site2Locator = host.getVM(oldVersion, 4);
    var site2Server1 = host.getVM(oldVersion, 5);
    var site2Server2 = host.getVM(oldVersion, 6);

    // Get mixed site locator properties
    var hostName = NetworkUtils.getServerHostName(host);
    final var availablePorts = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    final var site1LocatorPort = availablePorts[0];
    site1Locator.invoke(() -> DistributedTestUtils.deleteLocatorStateFile(site1LocatorPort));
    final var site1Locators = hostName + "[" + site1LocatorPort + "]";
    final var site1DistributedSystemId = 0;

    // Get old site locator properties
    final var site2LocatorPort = availablePorts[1];
    site2Locator.invoke(() -> DistributedTestUtils.deleteLocatorStateFile(site2LocatorPort));
    final var site2Locators = hostName + "[" + site2LocatorPort + "]";
    final var site2DistributedSystemId = 1;

    // Start mixed site locator
    site1Locator.invoke(() -> startLocator(site1LocatorPort, site1DistributedSystemId,
        site1Locators, site2Locators));

    // Start old site locator
    site2Locator.invoke(() -> startLocator(site2LocatorPort, site2DistributedSystemId,
        site2Locators, site1Locators));

    // Locators before 1.4 handled configuration asynchronously.
    // We must wait for configuration configuration to be ready, or confirm that it is disabled.
    site1Locator.invoke(
        () -> await()
            .untilAsserted(() -> assertTrue(
                !InternalLocator.getLocator().getConfig().getEnableClusterConfiguration()
                    || InternalLocator.getLocator().isSharedConfigurationRunning())));
    site2Locator.invoke(
        () -> await()
            .untilAsserted(() -> assertTrue(
                !InternalLocator.getLocator().getConfig().getEnableClusterConfiguration()
                    || InternalLocator.getLocator().isSharedConfigurationRunning())));

    // Start and configure mixed site servers
    var regionName = getName() + "_region";
    var site1SenderId = getName() + "_gatewaysender_" + site2DistributedSystemId;
    startAndConfigureServers(site1Server1, site1Server2, site1Locators, site2DistributedSystemId,
        regionName, site1SenderId, ParallelGatewaySenderQueue.DEFAULT_MESSAGE_SYNC_INTERVAL);

    // Roll mixed site locator to current
    rollLocatorToCurrent(site1Locator, site1LocatorPort, site1DistributedSystemId, site1Locators,
        site2Locators);

    // Roll one mixed site server to current
    rollStartAndConfigureServerToCurrent(site1Server2, site1Locators, site2DistributedSystemId,
        regionName, site1SenderId, ParallelGatewaySenderQueue.DEFAULT_MESSAGE_SYNC_INTERVAL);

    // Start and configure old site servers
    var site2SenderId = getName() + "_gatewaysender_" + site1DistributedSystemId;
    startAndConfigureServers(site2Server1, site2Server2, site2Locators, site1DistributedSystemId,
        regionName, site2SenderId, ParallelGatewaySenderQueue.DEFAULT_MESSAGE_SYNC_INTERVAL);

    // Do puts from mixed site client and verify events on old site
    var numPuts = 100;
    doClientPutsAndVerifyEvents(site1Client, site1Server1, site1Server2, site2Server1, site2Server2,
        hostName, site1LocatorPort, regionName, numPuts, site1SenderId, false);
  }
}
