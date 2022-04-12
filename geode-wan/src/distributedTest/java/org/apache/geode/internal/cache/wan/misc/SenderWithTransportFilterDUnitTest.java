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
package org.apache.geode.internal.cache.wan.misc;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.dunit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.zip.Adler32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.wan.InternalGatewaySenderFactory;
import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.WanTest;

@Category({WanTest.class})
public class SenderWithTransportFilterDUnitTest extends WANTestBase {

  @Test
  public void testSerialSenderWithTransportFilter() {
    var lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));

    var nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    vm2.invoke(() -> SenderWithTransportFilterDUnitTest.createReceiverWithTransportFilters(nyPort));
    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));

    vm3.invoke(() -> WANTestBase.createCache(lnPort));

    vm3.invoke(() -> SenderWithTransportFilterDUnitTest.createSenderWithTransportFilter("ln", 2,
        false, 100, 1, false, false, true));

    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));

    vm3.invoke(() -> WANTestBase.startSender("ln"));

    vm3.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_RR", 100));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 100));
  }

  @Test
  public void testParallelSenderWithTransportFilter() {
    var lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));

    var nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    vm2.invoke(() -> SenderWithTransportFilterDUnitTest.createReceiverWithTransportFilters(nyPort));
    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 0, 10,
        isOffHeap()));

    vm3.invoke(() -> WANTestBase.createCache(lnPort));

    vm3.invoke(() -> SenderWithTransportFilterDUnitTest.createSenderWithTransportFilter("ln", 2,
        true, 100, 1, false, false, true));

    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 0, 10,
        isOffHeap()));

    vm3.invoke(() -> WANTestBase.startSender("ln"));

    vm3.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 100));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 100));
  }

  public static int createReceiverWithTransportFilters(int locPort) {
    var test = new WANTestBase();
    var props = test.getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + locPort + "]");

    var ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
    var fact = cache.createGatewayReceiverFactory();
    var port = AvailablePortHelper.getRandomAvailableTCPPort();
    fact.setStartPort(port);
    fact.setEndPort(port);
    var transportFilters = new ArrayList<GatewayTransportFilter>();
    transportFilters.add(new CheckSumTransportFilter("CheckSumTransportFilter"));
    if (!transportFilters.isEmpty()) {
      for (var filter : transportFilters) {
        fact.addGatewayTransportFilter(filter);
      }
    }
    var receiver = fact.create();
    try {
      receiver.start();
    } catch (IOException e) {
      fail("Test " + test.getName() + " failed to start GatewayReceiver on port " + port, e);
    }
    return port;
  }

  public static void createSenderWithTransportFilter(String dsName, int remoteDsId,
      boolean isParallel, Integer maxMemory, Integer batchSize, boolean isConflation,
      boolean isPersistent, boolean isManualStart) {
    var persistentDirectory =
        new File(dsName + "_disk_" + System.currentTimeMillis() + "_" + VM.getCurrentVMNum());
    persistentDirectory.mkdir();
    var dsf = cache.createDiskStoreFactory();
    var dirs1 = new File[] {persistentDirectory};

    if (isParallel) {
      var gateway = cache.createGatewaySenderFactory();
      gateway.setParallel(true);
      gateway.setMaximumQueueMemory(maxMemory);
      gateway.setBatchSize(batchSize);
      ((InternalGatewaySenderFactory) gateway).setLocatorDiscoveryCallback(new MyLocatorCallback());
      var transportFilters = new ArrayList<GatewayTransportFilter>();
      transportFilters.add(new CheckSumTransportFilter("CheckSumTransportFilter"));
      if (!transportFilters.isEmpty()) {
        for (var filter : transportFilters) {
          gateway.addGatewayTransportFilter(filter);
        }
      }
      if (isPersistent) {
        gateway.setPersistenceEnabled(true);
        gateway.setDiskStoreName(dsf.setDiskDirs(dirs1).create(dsName).getName());
      } else {
        var store = dsf.setDiskDirs(dirs1).create(dsName);
        gateway.setDiskStoreName(store.getName());
      }
      gateway.setBatchConflationEnabled(isConflation);
      gateway.create(dsName, remoteDsId);

    } else {
      var gateway = cache.createGatewaySenderFactory();
      gateway.setMaximumQueueMemory(maxMemory);
      gateway.setBatchSize(batchSize);
      gateway.setManualStart(isManualStart);
      ((InternalGatewaySenderFactory) gateway).setLocatorDiscoveryCallback(new MyLocatorCallback());
      var transportFilters = new ArrayList<GatewayTransportFilter>();
      transportFilters.add(new CheckSumTransportFilter("CheckSumTransportFilter"));
      if (!transportFilters.isEmpty()) {
        for (var filter : transportFilters) {
          gateway.addGatewayTransportFilter(filter);
        }
      }
      gateway.setBatchConflationEnabled(isConflation);
      if (isPersistent) {
        gateway.setPersistenceEnabled(true);
        gateway.setDiskStoreName(dsf.setDiskDirs(dirs1).create(dsName).getName());
      } else {
        var store = dsf.setDiskDirs(dirs1).create(dsName);
        gateway.setDiskStoreName(store.getName());
      }
      gateway.create(dsName, remoteDsId);
    }
  }

  static class CheckSumTransportFilter implements GatewayTransportFilter {

    Adler32 checker = new Adler32();

    private final String name;

    public CheckSumTransportFilter(String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return name;
    }

    @Override
    public InputStream getInputStream(InputStream stream) {
      return new CheckedInputStream(stream, checker);
    }

    @Override
    public OutputStream getOutputStream(OutputStream stream) {
      return new CheckedOutputStream(stream, checker);
    }

    @Override
    public void close() {}
  }
}
