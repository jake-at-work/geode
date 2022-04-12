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
package org.apache.geode.internal.cache.rollingupgrade;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.version.VersionManager;

public class RollingUpgradeTracePRQuery extends RollingUpgrade2DUnitTestBase {

  @Test
  public void testTracePRQuery() throws Exception {
    final var host = Host.getHost(0);
    var currentServer1 = host.getVM(VersionManager.CURRENT_VERSION, 0);
    var oldServer = host.getVM(oldVersion, 1);
    var currentServer2 = host.getVM(VersionManager.CURRENT_VERSION, 2);
    var oldServerAndLocator = host.getVM(oldVersion, 3);

    var regionName = "cqs";

    var shortcut = RegionShortcut.REPLICATE;
    shortcut = RegionShortcut.PARTITION;

    var serverHostName = NetworkUtils.getServerHostName();
    var port = AvailablePortHelper.getRandomAvailableTCPPort();
    try {
      var props = getSystemProperties();
      props.remove(DistributionConfig.LOCATORS_NAME);
      invokeRunnableInVMs(invokeStartLocatorAndServer(serverHostName, port, props),
          oldServerAndLocator);

      // Locators before 1.4 handled configuration asynchronously.
      // We must wait for configuration configuration to be ready, or confirm that it is disabled.
      oldServerAndLocator.invoke(
          () -> await()
              .untilAsserted(() -> assertTrue(
                  !InternalLocator.getLocator().getConfig().getEnableClusterConfiguration()
                      || InternalLocator.getLocator().isSharedConfigurationRunning())));

      props.put(DistributionConfig.LOCATORS_NAME, serverHostName + "[" + port + "]");
      invokeRunnableInVMs(invokeCreateCache(props), currentServer1, currentServer2, oldServer);

      currentServer1
          .invoke(invokeAssertVersion(VersionManager.getInstance().getCurrentVersionOrdinal()));
      currentServer2
          .invoke(invokeAssertVersion(VersionManager.getInstance().getCurrentVersionOrdinal()));

      // create region
      invokeRunnableInVMs(invokeCreateRegion(regionName, shortcut), currentServer1, currentServer2,
          oldServer, oldServerAndLocator);

      // Locators before 1.4 handled configuration asynchronously.
      // We must wait for configuration configuration to be ready, or confirm that it is disabled.
      oldServerAndLocator.invoke(
          () -> await()
              .untilAsserted(() -> assertTrue(
                  !InternalLocator.getLocator().getConfig().getEnableClusterConfiguration()
                      || InternalLocator.getLocator().isSharedConfigurationRunning())));

      putDataSerializableAndVerify(currentServer1, regionName, 0, 100, currentServer2, oldServer,
          oldServerAndLocator);
      query("<trace> Select * from " + SEPARATOR + regionName + " p where p.timeout > 0L", 99,
          currentServer1,
          currentServer2, oldServer, oldServerAndLocator);

    } finally {
      invokeRunnableInVMs(invokeCloseCache(), currentServer1, currentServer2, oldServer,
          oldServerAndLocator);
    }
  }

}
