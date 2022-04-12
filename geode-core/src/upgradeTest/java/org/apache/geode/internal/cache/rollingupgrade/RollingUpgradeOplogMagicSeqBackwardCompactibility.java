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

import java.io.File;

import org.junit.Ignore;
import org.junit.Test;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;

public class RollingUpgradeOplogMagicSeqBackwardCompactibility
    extends RollingUpgrade2DUnitTestBase {


  @Ignore("GEODE-2355: test fails consistently")
  @Test
  public void testOplogMagicSeqBackwardCompactibility() throws Exception {
    var objectType = "strings";
    var regionType = "persistentReplicate";


    final var host = Host.getHost(0);
    var server1 = host.getVM(oldVersion, 0);
    var server2 = host.getVM(oldVersion, 1);
    var server3 = host.getVM(oldVersion, 2);
    var locator = host.getVM(oldVersion, 3);

    var regionName = "aRegion";
    var shortcut = RegionShortcut.REPLICATE_PERSISTENT;
    for (var i = 0; i < testingDirs.length; i++) {
      testingDirs[i] = new File(diskDir, "diskStoreVM_" + host.getVM(i).getId())
          .getAbsoluteFile();
      if (!testingDirs[i].exists()) {
        System.out.println(" Creating diskdir for server: " + i);
        testingDirs[i].mkdirs();
      }
    }

    var locatorPorts = AvailablePortHelper.getRandomAvailableTCPPorts(1);
    var hostName = NetworkUtils.getServerHostName();
    var locatorsString = getLocatorString(locatorPorts);

    locator.invoke(() -> DistributedTestUtils.deleteLocatorStateFile(locatorPorts));

    try {
      locator.invoke(invokeStartLocator(hostName, locatorPorts[0], getTestMethodName(),
          getLocatorProperties(locatorsString), true));

      invokeRunnableInVMs(invokeCreateCache(getSystemProperties(locatorPorts)), server1, server2,
          server3);
      // invokeRunnableInVMs(invokeAssertVersion(oldOrdinal), server1, server2, server3);
      // create region
      for (var i = 0; i < testingDirs.length; i++) {
        var runnable =
            invokeCreatePersistentReplicateRegion(regionName, testingDirs[i]);
        invokeRunnableInVMs(runnable, host.getVM(i));
      }

      putAndVerify("strings", server1, regionName, 0, 10, server2, server3);
      // before upgrade headers will be absent
      var oldFormatFiles = verifyOplogHeader(testingDirs[0], null);
      locator = rollLocatorToCurrent(locator, hostName, locatorPorts[0], getTestMethodName(),
          locatorsString);
      server1 = rollServerToCurrentAndCreateRegion(server1, regionType, testingDirs[0], shortcut,
          regionName, locatorPorts);
      System.out.println(verifyOplogHeader(testingDirs[0], oldFormatFiles));
      verifyValues(objectType, regionName, 0, 10, server1);
      putAndVerify(objectType, server1, regionName, 5, 15, server2, server3);
      putAndVerify(objectType, server2, regionName, 10, 20, server1, server3);
      System.out.println(verifyOplogHeader(testingDirs[0], oldFormatFiles));
      System.out.println(verifyOplogHeader(testingDirs[1], null));
    } finally {
      invokeRunnableInVMs(true, invokeStopLocator(), locator);
      invokeRunnableInVMs(true, invokeCloseCache(), server1, server2, server3);
      deleteDiskStores();
    }
  }

}
