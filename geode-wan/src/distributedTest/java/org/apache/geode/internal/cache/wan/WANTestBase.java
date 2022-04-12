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
package org.apache.geode.internal.cache.wan;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.CONSERVE_SOCKETS;
import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_HTTP_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.OFF_HEAP_MEMORY_SIZE;
import static org.apache.geode.distributed.ConfigurationProperties.REMOTE_LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.START_LOCATOR;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesMutator;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.ConnectionStats;
import org.apache.geode.cache.client.internal.LocatorDiscoveryCallbackAdapter;
import org.apache.geode.cache.persistence.PartitionOfflineException;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache.wan.GatewayEventFilter;
import org.apache.geode.cache.wan.GatewayQueueEvent;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.GatewaySender.OrderPolicy;
import org.apache.geode.cache.wan.GatewaySenderFactory;
import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.cache.wan.internal.GatewaySenderEventRemoteDispatcher;
import org.apache.geode.cache30.CacheTestCase;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.admin.remote.DistributionLocatorId;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.CustomerIDPartitionResolver;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCacheBuilder;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.internal.cache.execute.data.CustId;
import org.apache.geode.internal.cache.execute.data.Customer;
import org.apache.geode.internal.cache.execute.data.Order;
import org.apache.geode.internal.cache.execute.data.OrderId;
import org.apache.geode.internal.cache.execute.data.Shipment;
import org.apache.geode.internal.cache.execute.data.ShipmentId;
import org.apache.geode.internal.cache.partitioned.BecomePrimaryBucketMessage;
import org.apache.geode.internal.cache.partitioned.PRLocallyDestroyedException;
import org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil;
import org.apache.geode.internal.cache.wan.parallel.ConcurrentParallelGatewaySenderEventProcessor;
import org.apache.geode.internal.cache.wan.parallel.ConcurrentParallelGatewaySenderQueue;
import org.apache.geode.internal.cache.wan.parallel.ParallelGatewaySenderQueue;
import org.apache.geode.internal.cache.wan.serial.ConcurrentSerialGatewaySenderEventProcessor;
import org.apache.geode.internal.cache.wan.serial.SerialGatewaySenderQueue;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.AsyncEventQueueMXBean;
import org.apache.geode.management.GatewayReceiverMXBean;
import org.apache.geode.management.GatewaySenderMXBean;
import org.apache.geode.management.MBeanUtil;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.RegionMXBean;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.pdx.SimpleClass;
import org.apache.geode.pdx.SimpleClass1;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.DistributedTestCase;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.WanTest;

@Category({WanTest.class})
public class WANTestBase extends DistributedTestCase {

  protected static Cache cache;
  protected static Region region;

  protected static PartitionedRegion customerRegion;
  protected static PartitionedRegion orderRegion;
  protected static PartitionedRegion shipmentRegion;

  protected static final String customerRegionName = "CUSTOMER";
  protected static final String orderRegionName = "ORDER";
  protected static final String shipmentRegionName = "SHIPMENT";

  protected static VM vm0;
  protected static VM vm1;
  protected static VM vm2;
  protected static VM vm3;
  protected static VM vm4;
  protected static VM vm5;
  protected static VM vm6;
  protected static VM vm7;

  protected static QueueListener listener1;
  protected static QueueListener listener2;

  protected static AsyncEventListener eventListener1;
  protected static AsyncEventListener eventListener2;

  private static final long MAX_WAIT = 10000;

  protected static GatewayEventFilter eventFilter;

  protected static List<Integer> dispatcherThreads = new ArrayList<>(Arrays.asList(1, 3, 5));
  // this will be set for each test method run with one of the values from above list
  protected static int numDispatcherThreadsForTheRun = 1;

  private static final Logger logger = LogService.getLogger();

  @BeforeClass
  public static void beforeClassWANTestBase() throws Exception {
    vm0 = getHost(0).getVM(0);
    vm1 = getHost(0).getVM(1);
    vm2 = getHost(0).getVM(2);
    vm3 = getHost(0).getVM(3);
    vm4 = getHost(0).getVM(4);
    vm5 = getHost(0).getVM(5);
    vm6 = getHost(0).getVM(6);
    vm7 = getHost(0).getVM(7);
  }

  @Before
  public void setUpWANTestBase() throws Exception {
    shuffleNumDispatcherThreads();
    Invoke.invokeInEveryVM(() -> setNumDispatcherThreadsForTheRun(dispatcherThreads.get(0)));
    addIgnoredException("Connection refused");
    addIgnoredException("Software caused connection abort");
    addIgnoredException("Connection reset");
    postSetUpWANTestBase();
  }

  protected void postSetUpWANTestBase() throws Exception {
    // nothing
  }

  public static void shuffleNumDispatcherThreads() {
    Collections.shuffle(dispatcherThreads);
  }

  public static void setNumDispatcherThreadsForTheRun(int numThreads) {
    numDispatcherThreadsForTheRun = numThreads;
  }

  public static void stopOldLocator() {
    if (Locator.hasLocator()) {
      Locator.getLocator().stop();
    }
  }

  public static void createLocator(int dsId, int port, Set<String> localLocatorsList,
      Set<String> remoteLocatorsList) {
    var test = new WANTestBase();
    var props = test.getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(DISTRIBUTED_SYSTEM_ID, "" + dsId);
    var localLocatorBuffer = new StringBuilder(localLocatorsList.toString());
    localLocatorBuffer.deleteCharAt(0);
    localLocatorBuffer.deleteCharAt(localLocatorBuffer.lastIndexOf("]"));
    var localLocator = localLocatorBuffer.toString();
    localLocator = localLocator.replace(" ", "");

    props.setProperty(LOCATORS, localLocator);
    props.setProperty(START_LOCATOR,
        "localhost[" + port + "],server=true,peer=true,hostname-for-clients=localhost");
    var remoteLocatorBuffer = new StringBuilder(remoteLocatorsList.toString());
    remoteLocatorBuffer.deleteCharAt(0);
    remoteLocatorBuffer.deleteCharAt(remoteLocatorBuffer.lastIndexOf("]"));
    var remoteLocator = remoteLocatorBuffer.toString();
    remoteLocator = remoteLocator.replace(" ", "");
    props.setProperty(REMOTE_LOCATORS, remoteLocator);
    test.startLocatorDistributedSystem(props);
  }

  private void startLocator(int dsId, int locatorPort, int startLocatorPort, int remoteLocPort,
      boolean startServerLocator) {
    var props = getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(DISTRIBUTED_SYSTEM_ID, "" + dsId);
    props.setProperty(LOCATORS, "localhost[" + locatorPort + "]");
    props.setProperty(START_LOCATOR, "localhost[" + startLocatorPort + "],server="
        + startServerLocator + ",peer=true,hostname-for-clients=localhost");
    if (remoteLocPort != -1) {
      props.setProperty(REMOTE_LOCATORS, "localhost[" + remoteLocPort + "]");
    }
    startLocatorDistributedSystem(props);
  }

  private void startLocatorDistributedSystem(Properties props) {
    // Start start the locator with a LOCATOR_DM_TYPE and not a NORMAL_DM_TYPE
    System.setProperty(InternalLocator.FORCE_LOCATOR_DM_TYPE, "true");
    try {
      getSystem(props);
    } finally {
      System.clearProperty(InternalLocator.FORCE_LOCATOR_DM_TYPE);
    }
  }

  public static Integer createFirstLocatorWithDSId(int dsId) {
    stopOldLocator();
    var test = new WANTestBase();
    var port = getRandomAvailableTCPPort();
    test.startLocator(dsId, port, port, -1, true);
    return port;
  }

  public static Integer createFirstPeerLocator(int dsId) {
    stopOldLocator();
    var test = new WANTestBase();
    var port = getRandomAvailableTCPPort();
    test.startLocator(dsId, port, port, -1, false);
    return port;
  }

  public static Integer createSecondLocator(int dsId, int locatorPort) {
    stopOldLocator();
    var test = new WANTestBase();
    var port = getRandomAvailableTCPPort();
    test.startLocator(dsId, locatorPort, port, -1, true);
    return port;
  }

  public static Integer createSecondPeerLocator(int dsId, int locatorPort) {
    stopOldLocator();
    var test = new WANTestBase();
    var port = getRandomAvailableTCPPort();
    test.startLocator(dsId, locatorPort, port, -1, false);
    return port;
  }

  public static Integer createFirstRemoteLocator(int dsId, int remoteLocPort) {
    stopOldLocator();
    var test = new WANTestBase();
    var port = getRandomAvailableTCPPort();
    test.startLocator(dsId, port, port, remoteLocPort, true);
    return port;
  }

  public static void bringBackLocatorOnOldPort(int dsId, int remoteLocPort, int oldPort) {
    var test = new WANTestBase();
    test.startLocator(dsId, oldPort, oldPort, remoteLocPort, true);
  }


  public static Integer createFirstRemotePeerLocator(int dsId, int remoteLocPort) {
    stopOldLocator();
    var test = new WANTestBase();
    var port = getRandomAvailableTCPPort();
    test.startLocator(dsId, port, port, remoteLocPort, false);
    return port;
  }

  public static Integer createSecondRemoteLocator(int dsId, int localPort, int remoteLocPort) {
    stopOldLocator();
    var test = new WANTestBase();
    var port = getRandomAvailableTCPPort();
    test.startLocator(dsId, localPort, port, remoteLocPort, true);
    return port;
  }

  public static Integer createSecondRemoteLocatorWithAPI(int dsId, int localPort, int remoteLocPort,
      String hostnameForClients) throws IOException {
    stopOldLocator();
    var test = new WANTestBase();
    var props = test.getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(DISTRIBUTED_SYSTEM_ID, "" + dsId);
    props.setProperty(LOCATORS, "localhost[" + localPort + "]");
    props.setProperty(REMOTE_LOCATORS, "localhost[" + remoteLocPort + "]");
    var locator = Locator.startLocatorAndDS(0, null, null, props,
        true, true, hostnameForClients);
    return locator.getPort();
  }

  public static Integer createSecondRemotePeerLocator(int dsId, int localPort, int remoteLocPort) {
    stopOldLocator();
    var test = new WANTestBase();
    var port = getRandomAvailableTCPPort();
    test.startLocator(dsId, localPort, port, remoteLocPort, false);
    return port;
  }

  public static int createReceiverInSecuredCache() {
    var fact = WANTestBase.cache.createGatewayReceiverFactory();
    var port = getRandomAvailableTCPPort();
    fact.setStartPort(port);
    fact.setEndPort(port);
    fact.setManualStart(true);
    var receiver = fact.create();
    try {
      receiver.start();
    } catch (IOException e) {
      e.printStackTrace();
      org.apache.geode.test.dunit.Assert.fail("Failed to start GatewayReceiver on port " + port, e);
    }
    return port;
  }

  public static void createReplicatedRegion(String regionName, String senderIds, Boolean offHeap) {
    var exp =
        addIgnoredException(ForceReattemptException.class.getName());
    var exp1 =
        addIgnoredException(InterruptedException.class.getName());
    var exp2 =
        addIgnoredException(GatewaySenderException.class.getName());
    try {
      RegionFactory fact = cache.createRegionFactory(RegionShortcut.REPLICATE);
      if (senderIds != null) {
        var tokenizer = new StringTokenizer(senderIds, ",");
        while (tokenizer.hasMoreTokens()) {
          var senderId = tokenizer.nextToken();
          fact.addGatewaySenderId(senderId);
        }
      }

      fact.setOffHeap(offHeap);
      fact.create(regionName);
    } finally {
      exp.remove();
      exp1.remove();
      exp2.remove();
    }
  }

  public static void createReplicatedProxyRegion(String regionName, String senderIds,
      Boolean offHeap) {
    var exp =
        addIgnoredException(ForceReattemptException.class.getName());
    var exp1 =
        addIgnoredException(InterruptedException.class.getName());
    var exp2 =
        addIgnoredException(GatewaySenderException.class.getName());
    try {
      RegionFactory fact = cache.createRegionFactory(RegionShortcut.REPLICATE_PROXY);
      if (senderIds != null) {
        var tokenizer = new StringTokenizer(senderIds, ",");
        while (tokenizer.hasMoreTokens()) {
          var senderId = tokenizer.nextToken();
          fact.addGatewaySenderId(senderId);
        }
      }

      fact.setOffHeap(offHeap);
      fact.create(regionName);
    } finally {
      exp.remove();
      exp1.remove();
      exp2.remove();
    }
  }

  public static void createNormalRegion(String regionName, String senderIds) {
    RegionFactory fact = cache.createRegionFactory(RegionShortcut.LOCAL);
    if (senderIds != null) {
      var tokenizer = new StringTokenizer(senderIds, ",");
      while (tokenizer.hasMoreTokens()) {
        var senderId = tokenizer.nextToken();
        fact.addGatewaySenderId(senderId);
      }
    }

    fact.create(regionName);
  }

  public static void createPersistentReplicatedRegion(String regionName, String senderIds,
      Boolean offHeap) {
    RegionFactory fact = cache.createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT);
    if (senderIds != null) {
      var tokenizer = new StringTokenizer(senderIds, ",");
      while (tokenizer.hasMoreTokens()) {
        var senderId = tokenizer.nextToken();
        fact.addGatewaySenderId(senderId);
      }
    }

    fact.setOffHeap(offHeap);
    fact.create(regionName);
  }

  public static void createReplicatedRegionWithAsyncEventQueue(String regionName,
      String asyncQueueIds, Boolean offHeap) {
    var exp1 =
        addIgnoredException(ForceReattemptException.class.getName());
    try {
      RegionFactory fact = cache.createRegionFactory(RegionShortcut.REPLICATE);
      if (asyncQueueIds != null) {
        var tokenizer = new StringTokenizer(asyncQueueIds, ",");
        while (tokenizer.hasMoreTokens()) {
          var asyncQueueId = tokenizer.nextToken();
          fact.addAsyncEventQueueId(asyncQueueId);
        }
      }

      fact.setOffHeap(offHeap);
      fact.create(regionName);
    } finally {
      exp1.remove();
    }
  }

  public static void createReplicatedRegionWithSenderAndAsyncEventQueue(String regionName,
      String senderIds, String asyncChannelId, Boolean offHeap) {
    var exp =
        addIgnoredException(ForceReattemptException.class.getName());
    try {
      RegionFactory fact = cache.createRegionFactory(RegionShortcut.REPLICATE);
      if (senderIds != null) {
        var tokenizer = new StringTokenizer(senderIds, ",");
        while (tokenizer.hasMoreTokens()) {
          var senderId = tokenizer.nextToken();
          fact.addGatewaySenderId(senderId);
        }
      }

      fact.setOffHeap(offHeap);
      fact.addAsyncEventQueueId(asyncChannelId);
      fact.create(regionName);
    } finally {
      exp.remove();
    }
  }

  public static void createReplicatedRegion(String regionName, String senderIds, Scope scope,
      DataPolicy policy, Boolean offHeap) {
    createReplicatedRegion(regionName, senderIds, scope, policy, offHeap, false, true);
  }

  public static void createReplicatedRegion(String regionName, String senderIds, Scope scope,
      DataPolicy policy, Boolean offHeap, boolean statisticsEnabled,
      boolean concurrencyChecksEnabled) {
    RegionFactory fact = cache.createRegionFactory();
    if (senderIds != null) {
      var tokenizer = new StringTokenizer(senderIds, ",");
      while (tokenizer.hasMoreTokens()) {
        var senderId = tokenizer.nextToken();
        fact.addGatewaySenderId(senderId);
      }
    }

    fact.setDataPolicy(policy);
    fact.setScope(scope);
    fact.setOffHeap(offHeap);
    fact.setStatisticsEnabled(statisticsEnabled);
    fact.setConcurrencyChecksEnabled(concurrencyChecksEnabled);
    fact.create(regionName);
  }

  public static void createAsyncEventQueue(String asyncChannelId, boolean isParallel,
      Integer maxMemory, Integer batchSize, boolean isConflation, boolean isPersistent,
      String diskStoreName, boolean isDiskSynchronous) {

    if (diskStoreName != null) {
      var directory = new File(
          asyncChannelId + "_disk_" + System.currentTimeMillis() + "_" + VM.getCurrentVMNum());
      directory.mkdir();
      var dirs1 = new File[] {directory};
      var dsf = cache.createDiskStoreFactory();
      dsf.setDiskDirs(dirs1);
      dsf.create(diskStoreName);
    }

    AsyncEventListener asyncEventListener = new MyAsyncEventListener();

    var factory = cache.createAsyncEventQueueFactory();
    factory.setBatchSize(batchSize);
    factory.setPersistent(isPersistent);
    factory.setDiskStoreName(diskStoreName);
    factory.setDiskSynchronous(isDiskSynchronous);
    factory.setBatchConflationEnabled(isConflation);
    factory.setMaximumQueueMemory(maxMemory);
    factory.setParallel(isParallel);
    // set dispatcher threads
    factory.setDispatcherThreads(numDispatcherThreadsForTheRun);
    factory.create(asyncChannelId, asyncEventListener);
  }

  public static void createPartitionedRegion(String regionName, String senderIds,
      Integer redundantCopies, Integer totalNumBuckets, Boolean offHeap) {
    createPartitionedRegion(regionName, senderIds, redundantCopies, totalNumBuckets, offHeap,
        RegionShortcut.PARTITION);
  }

  public static void createPartitionedRegion(String regionName, String senderIds,
      Integer redundantCopies, Integer totalNumBuckets, Boolean offHeap, RegionShortcut shortcut) {
    createPartitionedRegion(regionName, senderIds, redundantCopies, totalNumBuckets, offHeap,
        shortcut, false, true);
  }

  public static void createPartitionedRegion(String regionName, String senderIds,
      Integer redundantCopies, Integer totalNumBuckets, Boolean offHeap, RegionShortcut shortcut,
      boolean statisticsEnabled, boolean concurrencyChecksEnabled) {
    var exp =
        addIgnoredException(ForceReattemptException.class.getName());
    var exp1 =
        addIgnoredException(PartitionOfflineException.class.getName());
    try {
      RegionFactory fact = cache.createRegionFactory(shortcut);
      if (senderIds != null) {
        var tokenizer = new StringTokenizer(senderIds, ",");
        while (tokenizer.hasMoreTokens()) {
          var senderId = tokenizer.nextToken();
          fact.addGatewaySenderId(senderId);
        }
      }
      var pfact = new PartitionAttributesFactory();
      pfact.setTotalNumBuckets(totalNumBuckets);
      pfact.setRedundantCopies(redundantCopies);
      pfact.setRecoveryDelay(0);
      fact.setPartitionAttributes(pfact.create());
      fact.setOffHeap(offHeap);
      fact.setStatisticsEnabled(statisticsEnabled);
      fact.setConcurrencyChecksEnabled(concurrencyChecksEnabled);
      fact.create(regionName);
    } finally {
      exp.remove();
      exp1.remove();
    }
  }

  // TODO:OFFHEAP: add offheap flavor
  public static void createPartitionedRegionWithPersistence(String regionName, String senderIds,
      Integer redundantCopies, Integer totalNumBuckets) {
    var exp =
        addIgnoredException(ForceReattemptException.class.getName());
    var exp1 =
        addIgnoredException(PartitionOfflineException.class.getName());
    try {
      RegionFactory fact = cache.createRegionFactory(RegionShortcut.PARTITION_PERSISTENT);
      if (senderIds != null) {
        var tokenizer = new StringTokenizer(senderIds, ",");
        while (tokenizer.hasMoreTokens()) {
          var senderId = tokenizer.nextToken();
          fact.addGatewaySenderId(senderId);
        }
      }
      var pfact = new PartitionAttributesFactory();
      pfact.setTotalNumBuckets(totalNumBuckets);
      pfact.setRedundantCopies(redundantCopies);
      pfact.setRecoveryDelay(0);
      fact.setPartitionAttributes(pfact.create());
      fact.create(regionName);
    } finally {
      exp.remove();
      exp1.remove();
    }
  }

  public static void createColocatedPartitionedRegion(String regionName, String senderIds,
      Integer redundantCopies, Integer totalNumBuckets, String colocatedWith) {
    var exp =
        addIgnoredException(ForceReattemptException.class.getName());
    var exp1 =
        addIgnoredException(PartitionOfflineException.class.getName());
    try {
      RegionFactory fact = cache.createRegionFactory(RegionShortcut.PARTITION);
      if (senderIds != null) {
        var tokenizer = new StringTokenizer(senderIds, ",");
        while (tokenizer.hasMoreTokens()) {
          var senderId = tokenizer.nextToken();
          fact.addGatewaySenderId(senderId);
        }
      }

      var pfact = new PartitionAttributesFactory();
      pfact.setTotalNumBuckets(totalNumBuckets);
      pfact.setRedundantCopies(redundantCopies);
      pfact.setRecoveryDelay(0);
      pfact.setColocatedWith(colocatedWith);
      fact.setPartitionAttributes(pfact.create());
      fact.create(regionName);
    } finally {
      exp.remove();
      exp1.remove();
    }
  }

  public static void addSenderThroughAttributesMutator(String regionName, String senderIds) {
    final Region<?, ?> r = cache.getRegion(SEPARATOR + regionName);
    AttributesMutator mutator = r.getAttributesMutator();
    mutator.addGatewaySenderId(senderIds);
  }

  public static void addAsyncEventQueueThroughAttributesMutator(String regionName, String queueId) {
    final Region<?, ?> r = cache.getRegion(SEPARATOR + regionName);
    AttributesMutator mutator = r.getAttributesMutator();
    mutator.addAsyncEventQueueId(queueId);
  }

  public static void createPartitionedRegionAsAccessor(String regionName, String senderIds,
      Integer redundantCopies, Integer totalNumBuckets) {
    RegionFactory fact = cache.createRegionFactory(RegionShortcut.PARTITION_PROXY);
    if (senderIds != null) {
      var tokenizer = new StringTokenizer(senderIds, ",");
      while (tokenizer.hasMoreTokens()) {
        var senderId = tokenizer.nextToken();
        fact.addGatewaySenderId(senderId);
      }
    }
    var pfact = new PartitionAttributesFactory();
    pfact.setTotalNumBuckets(totalNumBuckets);
    pfact.setRedundantCopies(redundantCopies);
    fact.setPartitionAttributes(pfact.create());
    fact.create(regionName);
  }

  public static void createPartitionedRegionWithSerialParallelSenderIds(String regionName,
      String serialSenderIds, String parallelSenderIds, String colocatedWith, Boolean offHeap) {
    RegionFactory fact = cache.createRegionFactory(RegionShortcut.PARTITION);
    if (serialSenderIds != null) {
      var tokenizer = new StringTokenizer(serialSenderIds, ",");
      while (tokenizer.hasMoreTokens()) {
        var senderId = tokenizer.nextToken();
        fact.addGatewaySenderId(senderId);
      }
    }
    if (parallelSenderIds != null) {
      var tokenizer = new StringTokenizer(parallelSenderIds, ",");
      while (tokenizer.hasMoreTokens()) {
        var senderId = tokenizer.nextToken();
        fact.addGatewaySenderId(senderId);
      }
    }
    var pfact = new PartitionAttributesFactory();
    pfact.setColocatedWith(colocatedWith);
    fact.setPartitionAttributes(pfact.create());
    fact.setOffHeap(offHeap);
    fact.create(regionName);
  }

  public static void createPersistentPartitionedRegion(String regionName, String senderIds,
      Integer redundantCopies, Integer totalNumBuckets, Boolean offHeap) {

    var exp =
        addIgnoredException(ForceReattemptException.class.getName());
    var exp1 =
        addIgnoredException(PartitionOfflineException.class.getName());
    try {

      RegionFactory fact = cache.createRegionFactory(RegionShortcut.PARTITION_PERSISTENT);
      if (senderIds != null) {
        var tokenizer = new StringTokenizer(senderIds, ",");
        while (tokenizer.hasMoreTokens()) {
          var senderId = tokenizer.nextToken();
          fact.addGatewaySenderId(senderId);
        }
      }
      var pfact = new PartitionAttributesFactory();
      pfact.setTotalNumBuckets(totalNumBuckets);
      pfact.setRedundantCopies(redundantCopies);
      fact.setPartitionAttributes(pfact.create());
      fact.setOffHeap(offHeap);
      fact.create(regionName);
    } finally {
      exp.remove();
      exp1.remove();
    }
  }

  public static void createCustomerOrderShipmentPartitionedRegion(String senderIds,
      Integer redundantCopies, Integer totalNumBuckets, Boolean offHeap) {
    var exp =
        addIgnoredException(ForceReattemptException.class.getName());
    try {
      RegionFactory fact = cache.createRegionFactory(RegionShortcut.PARTITION);
      if (senderIds != null) {
        var tokenizer = new StringTokenizer(senderIds, ",");
        while (tokenizer.hasMoreTokens()) {
          var senderId = tokenizer.nextToken();
          fact.addGatewaySenderId(senderId);
        }
      }

      var paf = new PartitionAttributesFactory();
      paf.setRedundantCopies(redundantCopies).setTotalNumBuckets(totalNumBuckets)
          .setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
      fact.setPartitionAttributes(paf.create());
      fact.setOffHeap(offHeap);
      customerRegion =
          (PartitionedRegion) fact.create(customerRegionName);
      logger.info("Partitioned Region CUSTOMER created Successfully :" + customerRegion.toString());

      paf = new PartitionAttributesFactory();
      paf.setRedundantCopies(redundantCopies).setTotalNumBuckets(totalNumBuckets)
          .setColocatedWith(customerRegionName)
          .setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
      fact = cache.createRegionFactory(RegionShortcut.PARTITION);
      if (senderIds != null) {
        var tokenizer = new StringTokenizer(senderIds, ",");
        while (tokenizer.hasMoreTokens()) {
          var senderId = tokenizer.nextToken();
          fact.addGatewaySenderId(senderId);
        }
      }
      fact.setPartitionAttributes(paf.create());
      fact.setOffHeap(offHeap);
      orderRegion =
          (PartitionedRegion) fact.create(orderRegionName);
      logger.info("Partitioned Region ORDER created Successfully :" + orderRegion.toString());

      paf = new PartitionAttributesFactory();
      paf.setRedundantCopies(redundantCopies).setTotalNumBuckets(totalNumBuckets)
          .setColocatedWith(orderRegionName)
          .setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
      fact = cache.createRegionFactory(RegionShortcut.PARTITION);
      if (senderIds != null) {
        var tokenizer = new StringTokenizer(senderIds, ",");
        while (tokenizer.hasMoreTokens()) {
          var senderId = tokenizer.nextToken();
          fact.addGatewaySenderId(senderId);
        }
      }
      fact.setPartitionAttributes(paf.create());
      fact.setOffHeap(offHeap);
      shipmentRegion =
          (PartitionedRegion) fact.create(shipmentRegionName);
      logger.info("Partitioned Region SHIPMENT created Successfully :" + shipmentRegion.toString());
    } finally {
      exp.remove();
    }
  }

  public static void createColocatedPartitionedRegions(String regionName, String senderIds,
      Integer redundantCopies, Integer totalNumBuckets, Boolean offHeap) {
    RegionFactory fact = cache.createRegionFactory(RegionShortcut.PARTITION);
    if (senderIds != null) {
      var tokenizer = new StringTokenizer(senderIds, ",");
      while (tokenizer.hasMoreTokens()) {
        var senderId = tokenizer.nextToken();
        fact.addGatewaySenderId(senderId);
      }
    }
    var pfact = new PartitionAttributesFactory();
    pfact.setTotalNumBuckets(totalNumBuckets);
    pfact.setRedundantCopies(redundantCopies);
    fact.setPartitionAttributes(pfact.create());
    fact.setOffHeap(offHeap);
    Region<?, ?> r = fact.create(regionName);

    pfact.setColocatedWith(r.getName());
    fact.setPartitionAttributes(pfact.create());
    fact.setOffHeap(offHeap);
    fact.create(regionName + "_child1");

    fact.create(regionName + "_child2");
  }

  public static void createColocatedPartitionedRegions2(String regionName, String senderIds,
      Integer redundantCopies, Integer totalNumBuckets, Boolean offHeap) {
    RegionFactory fact = cache.createRegionFactory(RegionShortcut.PARTITION);
    if (senderIds != null) {
      var tokenizer = new StringTokenizer(senderIds, ",");
      while (tokenizer.hasMoreTokens()) {
        var senderId = tokenizer.nextToken();
        fact.addGatewaySenderId(senderId);
      }
    }
    var pfact = new PartitionAttributesFactory();
    pfact.setTotalNumBuckets(totalNumBuckets);
    pfact.setRedundantCopies(redundantCopies);
    fact.setPartitionAttributes(pfact.create());
    fact.setOffHeap(offHeap);
    Region<?, ?> r = fact.create(regionName);

    fact = cache.createRegionFactory(RegionShortcut.PARTITION);
    pfact.setColocatedWith(r.getName());
    fact.setPartitionAttributes(pfact.create());
    fact.setOffHeap(offHeap);
    fact.create(regionName + "_child1");

    fact.create(regionName + "_child2");
  }

  public static void createCacheInVMs(Integer locatorPort, VM... vms) {
    for (var vm : vms) {
      vm.invoke(() -> createCache(locatorPort));
    }
  }

  public static void addListenerToSleepAfterCreateEvent(int milliSeconds, final String regionName) {
    cache.getRegion(regionName).getAttributesMutator()
        .addCacheListener(new CacheListenerAdapter<Object, Object>() {
          @Override
          public void afterCreate(final EntryEvent<Object, Object> event) {
            try {
              Thread.sleep(milliSeconds);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }
        });
  }

  private static CacheListener myListener;

  public static void removeCacheListener() {
    cache.getRegion(getTestMethodName() + "_RR_1").getAttributesMutator()
        .removeCacheListener(myListener);

  }


  public static void createCache(Integer locPort) {
    createCache(false, locPort);
  }

  public static void createManagementCache(Integer locPort) {
    createCache(true, locPort);
  }

  public static void createCacheConserveSockets(Boolean conserveSockets, Integer locPort) {
    var test = new WANTestBase();
    var props = test.getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + locPort + "]");
    props.setProperty(CONSERVE_SOCKETS, conserveSockets.toString());
    var ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
  }

  protected static void createCache(boolean management, Integer locPort) {
    var test = new WANTestBase();
    var props = test.getDistributedSystemProperties();
    if (management) {
      props.setProperty(JMX_MANAGER, "true");
      props.setProperty(JMX_MANAGER_START, "false");
      props.setProperty(JMX_MANAGER_PORT, "0");
      props.setProperty(JMX_MANAGER_HTTP_PORT, "0");
    }
    props.setProperty(MCAST_PORT, "0");
    var logLevel = System.getProperty(LOG_LEVEL, "info");
    props.setProperty(LOG_LEVEL, logLevel);
    props.setProperty(LOCATORS, "localhost[" + locPort + "]");
    var ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
  }

  protected static void createCacheWithSSL(Integer locPort) {
    var test = new WANTestBase();

    var gatewaySslenabled = true;
    var gatewaySslprotocols = "any";
    var gatewaySslciphers = "any";
    var gatewaySslRequireAuth = true;

    var gemFireProps = test.getDistributedSystemProperties();
    gemFireProps.put(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
    gemFireProps.put(GATEWAY_SSL_ENABLED, String.valueOf(gatewaySslenabled));
    gemFireProps.put(GATEWAY_SSL_PROTOCOLS, gatewaySslprotocols);
    gemFireProps.put(GATEWAY_SSL_CIPHERS, gatewaySslciphers);
    gemFireProps.put(GATEWAY_SSL_REQUIRE_AUTHENTICATION, String.valueOf(gatewaySslRequireAuth));

    gemFireProps.put(GATEWAY_SSL_KEYSTORE_TYPE, "jks");
    // this uses the default.keystore which is all jdk compliant in geode-dunit module
    gemFireProps.put(GATEWAY_SSL_KEYSTORE, createTempFileFromResource(WANTestBase.class,
        "/org/apache/geode/cache/client/internal/default.keystore").getAbsolutePath());
    gemFireProps.put(GATEWAY_SSL_KEYSTORE_PASSWORD, "password");
    gemFireProps.put(GATEWAY_SSL_TRUSTSTORE, createTempFileFromResource(WANTestBase.class,
        "/org/apache/geode/cache/client/internal/default.keystore").getAbsolutePath());
    gemFireProps.put(GATEWAY_SSL_TRUSTSTORE_PASSWORD, "password");

    gemFireProps.setProperty(MCAST_PORT, "0");
    gemFireProps.setProperty(LOCATORS, "localhost[" + locPort + "]");

    logger.info("Starting cache ds with following properties \n" + gemFireProps);

    var ds = test.getSystem(gemFireProps);
    cache = CacheFactory.create(ds);
  }

  public static void createCache_PDX(Integer locPort) {
    var test = new WANTestBase();
    var props = test.getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + locPort + "]");
    var ds = test.getSystem(props);

    cache = new InternalCacheBuilder(props)
        .setPdxPersistent(true)
        .setPdxDiskStore("PDX_TEST")
        .setIsExistingOk(false)
        .create(ds);

    var pdxDir = new File(CacheTestCase.getDiskDir(), "pdx");
    var dsf = cache.createDiskStoreFactory();
    var dirs1 = new File[] {pdxDir};
    dsf.setDiskDirs(dirs1).setMaxOplogSize(1).create("PDX_TEST");
  }

  public static void createCache(Integer locPort1, Integer locPort2) {
    var test = new WANTestBase();
    var props = test.getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + locPort1 + "],localhost[" + locPort2 + "]");
    var ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
  }

  public static void createCacheWithoutLocator(Integer mCastPort) {
    var test = new WANTestBase();
    var props = test.getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "" + mCastPort);
    var ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
  }

  /**
   * Method that creates a cache server
   *
   * @return Integer Port on which the server is started.
   */
  public static Integer createCacheServer() {
    var server1 = cache.addCacheServer();
    server1.setPort(0);
    try {
      server1.start();
    } catch (IOException e) {
      org.apache.geode.test.dunit.Assert.fail("Failed to start the Server", e);
    }
    assertTrue(server1.isRunning());

    return server1.getPort();
  }

  /**
   * Returns a Map that contains the count for number of cache server and number of Receivers.
   *
   */
  public static Map getCacheServers() {
    List cacheServers = cache.getCacheServers();

    Map cacheServersMap = new HashMap();
    var itr = cacheServers.iterator();
    var bridgeServerCounter = 0;
    var receiverServerCounter = 0;
    while (itr.hasNext()) {
      var cacheServer = (CacheServerImpl) itr.next();
      if (cacheServer.getAcceptor().isGatewayReceiver()) {
        receiverServerCounter++;
      } else {
        bridgeServerCounter++;
      }
    }
    cacheServersMap.put("BridgeServer", bridgeServerCounter);
    cacheServersMap.put("ReceiverServer", receiverServerCounter);
    return cacheServersMap;
  }

  public static void startSenderInVMs(String senderId, VM... vms) {
    for (var vm : vms) {
      vm.invoke(() -> startSender(senderId));
    }
  }

  public static void startSenderInVMsAsync(String senderId, VM... vms) {
    List<AsyncInvocation> tasks = new LinkedList<>();
    for (var vm : vms) {
      tasks.add(vm.invokeAsync(() -> startSender(senderId)));
    }
    for (var invocation : tasks) {
      try {
        invocation.await();
      } catch (InterruptedException e) {
        fail("Starting senders was interrupted");
      }
    }
  }


  public static void startSenderwithCleanQueuesInVMsAsync(String senderId, VM... vms) {
    List<AsyncInvocation> tasks = new LinkedList<>();
    for (var vm : vms) {
      tasks.add(vm.invokeAsync(() -> startSenderwithCleanQueues(senderId)));
    }
    for (var invocation : tasks) {
      try {
        invocation.await();
      } catch (InterruptedException e) {
        fail("Starting senders was interrupted");
      }
    }
  }

  public static void startSender(String senderId) {
    final var exln = addIgnoredException("Could not connect");

    var exp =
        addIgnoredException(ForceReattemptException.class.getName());
    var exp1 =
        addIgnoredException(InterruptedException.class.getName());
    try {
      var sender = getGatewaySender(senderId);
      sender.start();
    } finally {
      exp.remove();
      exp1.remove();
      exln.remove();
    }

  }

  public static void startSenderwithCleanQueues(String senderId) {
    final var exln = addIgnoredException("Could not connect");

    var exp =
        addIgnoredException(ForceReattemptException.class.getName());
    var exp1 =
        addIgnoredException(InterruptedException.class.getName());
    try {
      var sender = getGatewaySender(senderId);
      sender.startWithCleanQueue();
    } finally {
      exp.remove();
      exp1.remove();
      exln.remove();
    }

  }

  public static void enableConflation(String senderId) {
    var sender = (AbstractGatewaySender) getGatewaySender(senderId);
    sender.test_setBatchConflationEnabled(true);
  }

  public static Map getSenderToReceiverConnectionInfo(String senderId) {
    var sender = getGatewaySender(senderId);
    Map connectionInfo = null;
    if (!sender.isParallel() && ((AbstractGatewaySender) sender).isPrimary()) {
      connectionInfo = new HashMap();
      var dispatcher =
          ((AbstractGatewaySender) sender).getEventProcessor().getDispatcher();
      if (dispatcher instanceof GatewaySenderEventRemoteDispatcher) {
        var serverLocation =
            ((GatewaySenderEventRemoteDispatcher) dispatcher).getConnection(false).getServer();
        connectionInfo.put("serverHost", serverLocation.getHostName());
        connectionInfo.put("serverPort", serverLocation.getPort());

      }
    }
    return connectionInfo;
  }

  public static void moveAllPrimaryBuckets(String senderId, final DistributedMember destination,
      final String regionName) {

    var sender = (AbstractGatewaySender) cache.getGatewaySender(senderId);
    final RegionQueue regionQueue;
    regionQueue = sender.getQueues().toArray(new RegionQueue[1])[0];
    if (sender.isParallel()) {
      var parallelGatewaySenderQueue =
          (ConcurrentParallelGatewaySenderQueue) regionQueue;
      var prQ =
          parallelGatewaySenderQueue.getRegions().toArray(new PartitionedRegion[1])[0];

      var primaryBucketIds = prQ.getDataStore().getAllLocalPrimaryBucketIds();
      for (int bid : primaryBucketIds) {
        movePrimary(destination, regionName, bid);
      }

      // double check after moved all primary buckets
      primaryBucketIds = prQ.getDataStore().getAllLocalPrimaryBucketIds();
      assertTrue(primaryBucketIds.isEmpty());
    }
  }

  public static void movePrimary(final DistributedMember destination, final String regionName,
      final int bucketId) {
    var region = (PartitionedRegion) cache.getRegion(regionName);

    var response = BecomePrimaryBucketMessage
        .send((InternalDistributedMember) destination, region, bucketId, true);
    assertTrue(response.waitForResponse());
  }

  public static int getSecondaryQueueSizeInStats(String senderId) {
    var sender = (AbstractGatewaySender) cache.getGatewaySender(senderId);
    var statistics = sender.getStatistics();
    return statistics.getSecondaryEventQueueSize();
  }

  public static void checkQueueSizeInStats(String senderId, final int expectedQueueSize) {
    var sender = (AbstractGatewaySender) cache.getGatewaySender(senderId);
    var statistics = sender.getStatistics();
    await()
        .untilAsserted(() -> assertEquals("Expected queue size: " + expectedQueueSize
            + " but actual size: " + statistics.getEventQueueSize(), expectedQueueSize,
            statistics.getEventQueueSize()));
  }

  public static void checkConnectionStats(String senderId) {
    var sender =
        (AbstractGatewaySender) CacheFactory.getAnyInstance().getGatewaySender(senderId);

    Collection statsCollection = sender.getProxy().getEndpointManager().getAllStats().values();
    assertTrue(statsCollection.iterator().next() instanceof ConnectionStats);
  }

  public static List<Integer> getSenderStats(String senderId, final int expectedQueueSize) {
    var sender = (AbstractGatewaySender) cache.getGatewaySender(senderId);
    var statistics = sender.getStatistics();
    if (expectedQueueSize != -1) {
      final RegionQueue regionQueue;
      regionQueue = sender.getQueues().toArray(new RegionQueue[1])[0];
      if (sender.isParallel()) {
        var parallelGatewaySenderQueue =
            (ConcurrentParallelGatewaySenderQueue) regionQueue;
        var pr =
            parallelGatewaySenderQueue.getRegions().toArray(new PartitionedRegion[1])[0];
      }
      await()
          .untilAsserted(() -> assertEquals("Expected queue entries: " + expectedQueueSize
              + " but actual entries: " + regionQueue.size(), expectedQueueSize,
              regionQueue.size()));
    }
    var stats = new ArrayList<Integer>();
    stats.add(statistics.getEventQueueSize());
    stats.add(statistics.getEventsReceived());
    stats.add(statistics.getEventsQueued());
    stats.add(statistics.getEventsDistributed());
    stats.add(statistics.getBatchesDistributed());
    stats.add(statistics.getBatchesRedistributed());
    stats.add(statistics.getEventsFiltered());
    stats.add(statistics.getEventsNotQueuedConflated());
    stats.add(statistics.getEventsConflatedFromBatches());
    stats.add(statistics.getConflationIndexesMapSize());
    stats.add(statistics.getSecondaryEventQueueSize());
    stats.add(statistics.getEventsProcessedByPQRM());
    stats.add(statistics.getEventsExceedingAlertThreshold());
    stats.add((int) statistics.getBatchesWithIncompleteTransactions());
    return stats;
  }

  public static int getGatewaySenderPoolDisconnects(String senderId) {
    var sender =
        (AbstractGatewaySender) CacheFactory.getAnyInstance().getGatewaySender(senderId);

    var poolStats = sender.getProxy().getStats();

    return poolStats.getDisConnects();
  }

  public static List<Integer> getSenderStatsForDroppedEvents(String senderId) {
    var sender = (AbstractGatewaySender) cache.getGatewaySender(senderId);
    var statistics = sender.getStatistics();
    var stats = new ArrayList<Integer>();
    var eventNotQueued = statistics.getEventsDroppedDueToPrimarySenderNotRunning();
    if (eventNotQueued > 0) {
      logger
          .info("Found " + eventNotQueued + " events dropped due to primary sender is not running");
    }
    stats.add(eventNotQueued);
    stats.add(statistics.getEventsNotQueued());
    stats.add(statistics.getEventsNotQueuedConflated());
    return stats;
  }

  public static void checkQueueStats(String senderId, final int queueSize, final int eventsReceived,
      final int eventsQueued, final int eventsDistributed) {
    var statistics = getGatewaySenderStats(senderId);
    assertEquals(queueSize, statistics.getEventQueueSize());
    assertEquals(eventsReceived, statistics.getEventsReceived());
    assertEquals(eventsQueued, statistics.getEventsQueued());
    assert (statistics.getEventsDistributed() >= eventsDistributed);
  }

  public static void validateGatewaySenderQueueHasContent(String senderId, VM... targetVms) {
    await()
        .until(() -> Arrays.stream(targetVms).map(vm -> vm.invoke(() -> {
          var sender = (AbstractGatewaySender) cache.getGatewaySender(senderId);
          logger.info(displaySerialQueueContent(sender));
          return sender.getEventQueueSize();
        })).mapToInt(i -> i).sum(), greaterThan(0));
  }

  public static void checkGatewayReceiverStats(int processBatches, int eventsReceived,
      int creates) {
    checkGatewayReceiverStats(processBatches, eventsReceived, creates, false);
  }

  public static void checkGatewayReceiverStats(int processBatches, int eventsReceived,
      int creates, boolean isExact) {
    var gatewayReceivers = cache.getGatewayReceivers();
    var receiver = gatewayReceivers.iterator().next();
    var stats = ((CacheServerImpl) receiver.getServer()).getAcceptor().getStats();

    assertTrue(stats instanceof GatewayReceiverStats);
    var gatewayReceiverStats = (GatewayReceiverStats) stats;
    if (isExact) {
      assertTrue(gatewayReceiverStats.getProcessBatchRequests() == processBatches);
    } else {
      assertTrue(gatewayReceiverStats.getProcessBatchRequests() >= processBatches);
    }
    assertEquals(eventsReceived, gatewayReceiverStats.getEventsReceived());
    assertEquals(creates, gatewayReceiverStats.getCreateRequest());
  }

  public static List<Long> getReceiverStats() {
    var gatewayReceivers = cache.getGatewayReceivers();
    var receiver = gatewayReceivers.iterator().next();
    var stats = ((CacheServerImpl) receiver.getServer()).getAcceptor().getStats();
    assertTrue(stats instanceof GatewayReceiverStats);
    var gatewayReceiverStats = (GatewayReceiverStats) stats;
    List<Long> statsList = new ArrayList<>();
    statsList.add(gatewayReceiverStats.getEventsReceived());
    statsList.add(gatewayReceiverStats.getEventsRetried());
    statsList.add(gatewayReceiverStats.getProcessBatchRequests());
    statsList.add(gatewayReceiverStats.getDuplicateBatchesReceived());
    statsList.add(gatewayReceiverStats.getOutoforderBatchesReceived());
    statsList.add(gatewayReceiverStats.getEarlyAcks());
    statsList.add(gatewayReceiverStats.getExceptionsOccurred());
    return statsList;
  }

  public static void checkMinimumGatewayReceiverStats(int processBatches, int eventsReceived) {
    var gatewayReceivers = cache.getGatewayReceivers();
    var receiver = gatewayReceivers.iterator().next();
    var stats = ((CacheServerImpl) receiver.getServer()).getAcceptor().getStats();

    assertTrue(stats instanceof GatewayReceiverStats);
    var gatewayReceiverStats = (GatewayReceiverStats) stats;
    assertTrue(gatewayReceiverStats.getProcessBatchRequests() >= processBatches);
    assertTrue(gatewayReceiverStats.getEventsReceived() >= eventsReceived);
  }

  public static void checkExceptionStats(int exceptionsOccurred) {
    var gatewayReceivers = cache.getGatewayReceivers();
    var receiver = gatewayReceivers.iterator().next();
    var stats = ((CacheServerImpl) receiver.getServer()).getAcceptor().getStats();

    assertTrue(stats instanceof GatewayReceiverStats);
    var gatewayReceiverStats = (GatewayReceiverStats) stats;
    if (exceptionsOccurred == 0) {
      assertEquals(exceptionsOccurred, gatewayReceiverStats.getExceptionsOccurred());
    } else {
      assertTrue(gatewayReceiverStats.getExceptionsOccurred() >= exceptionsOccurred);
    }
  }

  public static void checkGatewayReceiverStatsHA(int processBatches, int eventsReceived,
      int creates) {
    var gatewayReceivers = cache.getGatewayReceivers();
    var receiver = gatewayReceivers.iterator().next();
    var stats = ((CacheServerImpl) receiver.getServer()).getAcceptor().getStats();

    assertTrue(stats instanceof GatewayReceiverStats);
    var gatewayReceiverStats = (GatewayReceiverStats) stats;
    assertTrue(gatewayReceiverStats.getProcessBatchRequests() >= processBatches);
    assertTrue(gatewayReceiverStats.getEventsReceived() >= eventsReceived);
    assertTrue(gatewayReceiverStats.getCreateRequest() >= creates);
  }

  public static void checkEventFilteredStats(String senderId, final int eventsFiltered) {
    var statistics = getGatewaySenderStats(senderId);
    assertEquals(eventsFiltered, statistics.getEventsFiltered());
  }

  public static void checkConflatedStats(String senderId, final int eventsConflated) {
    var statistics = getGatewaySenderStats(senderId);
    assertEquals(eventsConflated, statistics.getEventsNotQueuedConflated());
  }

  public static void checkStats_Failover(String senderId, final int eventsReceived) {
    var statistics = getGatewaySenderStats(senderId);
    assertEquals(eventsReceived, statistics.getEventsReceived());
    assertEquals(eventsReceived,
        (statistics.getEventsQueued() + statistics.getUnprocessedTokensAddedByPrimary()
            + statistics.getUnprocessedEventsRemovedByPrimary()));
  }

  public static void checkBatchStats(String senderId, final int batches) {
    checkBatchStats(senderId, batches, false);
  }

  public static void checkBatchStats(String senderId, final int batches, boolean isExact) {
    var statistics = getGatewaySenderStats(senderId);
    if (isExact) {
      assert (statistics.getBatchesDistributed() == batches);
    } else {
      assert (statistics.getBatchesDistributed() >= batches);
    }
    assertEquals(0, statistics.getBatchesRedistributed());
  }

  public static void checkBatchStats(String senderId, final int batches,
      boolean isExact, final boolean batchesRedistributed) {
    var statistics = getGatewaySenderStats(senderId);
    if (isExact) {
      assert (statistics.getBatchesDistributed() == batches);
    } else {
      assert (statistics.getBatchesDistributed() >= batches);
    }
    assertEquals(batchesRedistributed, (statistics.getBatchesRedistributed() > 0));
  }

  public static void checkBatchStats(String senderId, final boolean batchesDistributed,
      final boolean batchesRedistributed) {
    var statistics = getGatewaySenderStats(senderId);
    assertEquals(batchesDistributed, (statistics.getBatchesDistributed() > 0));
    assertEquals(batchesRedistributed, (statistics.getBatchesRedistributed() > 0));
  }

  public static void checkUnProcessedStats(String senderId, int events) {
    var statistics = getGatewaySenderStats(senderId);
    assertEquals(events, (statistics.getUnprocessedEventsAddedBySecondary()
        + statistics.getUnprocessedTokensRemovedBySecondary()));
    assertEquals(events, (statistics.getUnprocessedEventsRemovedByPrimary()
        + statistics.getUnprocessedTokensAddedByPrimary()));
  }

  public static GatewaySenderStats getGatewaySenderStats(String senderId) {
    var sender = cache.getGatewaySender(senderId);
    return ((AbstractGatewaySender) sender).getStatistics();
  }

  public static void waitForSenderRunningState(String senderId) {
    final var exln = addIgnoredException("Could not connect");
    try {
      var senders = cache.getGatewaySenders();
      final var sender = getGatewaySenderById(senders, senderId);
      await()
          .untilAsserted(
              () -> assertEquals("Expected sender isRunning state to " + "be true but is false",
                  true, (sender != null && sender.isRunning())));
    } finally {
      exln.remove();
    }
  }

  public static void waitForSenderNonRunningState(String senderId) {
    try (var ie = addIgnoredException("Could not connect")) {
      var senders = cache.getGatewaySenders();
      final var sender = getGatewaySenderById(senders, senderId);
      await()
          .untilAsserted(
              () -> assertEquals("Expected sender isRunning state to " + "be false but is true",
                  true, (sender != null && !sender.isRunning())));
    }
  }

  public static void waitForSenderToBecomePrimary(String senderId) {
    var senders = ((GemFireCacheImpl) cache).getAllGatewaySenders();
    final var sender = getGatewaySenderById(senders, senderId);
    await()
        .untilAsserted(
            () -> assertEquals("Expected sender primary state to " + "be true but is false",
                true, (sender != null && ((AbstractGatewaySender) sender).isPrimary())));
  }

  private static GatewaySender getGatewaySenderById(Set<GatewaySender> senders, String senderId) {
    for (var s : senders) {
      if (s.getId().equals(senderId)) {
        return s;
      }
    }
    // if none of the senders matches with the supplied senderid, return null
    return null;
  }

  public static HashMap checkQueue() {
    var listenerAttrs = new HashMap();
    listenerAttrs.put("Create", listener1.createList);
    listenerAttrs.put("Update", listener1.updateList);
    listenerAttrs.put("Destroy", listener1.destroyList);
    return listenerAttrs;
  }

  public static void checkQueueOnSecondary(final Map primaryUpdatesMap) {
    final var secondaryUpdatesMap = new HashMap();
    secondaryUpdatesMap.put("Create", listener1.createList);
    secondaryUpdatesMap.put("Update", listener1.updateList);
    secondaryUpdatesMap.put("Destroy", listener1.destroyList);

    await().untilAsserted(() -> {
      secondaryUpdatesMap.put("Create", listener1.createList);
      secondaryUpdatesMap.put("Update", listener1.updateList);
      secondaryUpdatesMap.put("Destroy", listener1.destroyList);
      assertEquals(
          "Expected secondary map to be " + primaryUpdatesMap + " but it is " + secondaryUpdatesMap,
          true, secondaryUpdatesMap.equals(primaryUpdatesMap));
    });
  }

  public static HashMap checkQueue2() {
    var listenerAttrs = new HashMap();
    listenerAttrs.put("Create", listener2.createList);
    listenerAttrs.put("Update", listener2.updateList);
    listenerAttrs.put("Destroy", listener2.destroyList);
    return listenerAttrs;
  }

  public static HashMap checkPR(String regionName) {
    var region = (PartitionedRegion) cache.getRegion(regionName);
    var listener = (QueueListener) region.getCacheListener();

    var listenerAttrs = new HashMap();
    listenerAttrs.put("Create", listener.createList);
    listenerAttrs.put("Update", listener.updateList);
    listenerAttrs.put("Destroy", listener.destroyList);
    return listenerAttrs;
  }

  public static HashMap checkBR(String regionName, int numBuckets) {
    var region = (PartitionedRegion) cache.getRegion(regionName);
    var listenerAttrs = new HashMap();
    for (var i = 0; i < numBuckets; i++) {
      var br = region.getBucketRegion(i);
      var listener = (QueueListener) br.getCacheListener();
      listenerAttrs.put("Create" + i, listener.createList);
      listenerAttrs.put("Update" + i, listener.updateList);
      listenerAttrs.put("Destroy" + i, listener.destroyList);
    }
    return listenerAttrs;
  }

  public static HashMap checkQueue_BR(String senderId, int numBuckets) {
    var sender = getGatewaySender(senderId);
    var parallelQueue =
        ((AbstractGatewaySender) sender).getQueues().toArray(new RegionQueue[1])[0];

    var region = (PartitionedRegion) parallelQueue.getRegion();
    var listenerAttrs = new HashMap();
    for (var i = 0; i < numBuckets; i++) {
      var br = region.getBucketRegion(i);
      if (br != null) {
        var listener = (QueueListener) br.getCacheListener();
        if (listener != null) {
          listenerAttrs.put("Create" + i, listener.createList);
          listenerAttrs.put("Update" + i, listener.updateList);
          listenerAttrs.put("Destroy" + i, listener.destroyList);
        }
      }
    }
    return listenerAttrs;
  }

  public static void addListenerOnBucketRegion(String regionName, int numBuckets) {
    var test = new WANTestBase();
    test.addCacheListenerOnBucketRegion(regionName, numBuckets);
  }

  private void addCacheListenerOnBucketRegion(String regionName, int numBuckets) {
    var region = (PartitionedRegion) cache.getRegion(regionName);
    for (var i = 0; i < numBuckets; i++) {
      var br = region.getBucketRegion(i);
      var mutator = br.getAttributesMutator();
      listener1 = new QueueListener();
      mutator.addCacheListener(listener1);
    }
  }

  public static void addListenerOnQueueBucketRegion(String senderId, int numBuckets) {
    var test = new WANTestBase();
    test.addCacheListenerOnQueueBucketRegion(senderId, numBuckets);
  }

  private void addCacheListenerOnQueueBucketRegion(String senderId, int numBuckets) {
    var sender = getGatewaySender(senderId);
    var parallelQueue =
        ((AbstractGatewaySender) sender).getQueues().toArray(new RegionQueue[1])[0];

    var region = (PartitionedRegion) parallelQueue.getRegion();
    for (var i = 0; i < numBuckets; i++) {
      var br = region.getBucketRegion(i);
      if (br != null) {
        var mutator = br.getAttributesMutator();
        CacheListener listener = new QueueListener();
        mutator.addCacheListener(listener);
      }
    }

  }

  public static void addQueueListener(String senderId, boolean isParallel) {
    var test = new WANTestBase();
    test.addCacheQueueListener(senderId, isParallel);
  }

  public static void addSecondQueueListener(String senderId, boolean isParallel) {
    var test = new WANTestBase();
    test.addSecondCacheQueueListener(senderId, isParallel);
  }

  public static void addListenerOnRegion(String regionName) {
    var test = new WANTestBase();
    test.addCacheListenerOnRegion(regionName);
  }

  private void addCacheListenerOnRegion(String regionName) {
    Region<?, ?> region = cache.getRegion(regionName);
    AttributesMutator mutator = region.getAttributesMutator();
    listener1 = new QueueListener();
    mutator.addCacheListener(listener1);
  }

  private void addCacheQueueListener(String senderId, boolean isParallel) {
    var sender = getGatewaySender(senderId);
    listener1 = new QueueListener();
    if (!isParallel) {
      var queues = ((AbstractGatewaySender) sender).getQueues();
      for (var q : queues) {
        q.addCacheListener(listener1);
      }
    } else {
      var parallelQueue =
          ((AbstractGatewaySender) sender).getQueues().toArray(new RegionQueue[1])[0];
      parallelQueue.addCacheListener(listener1);
    }
  }

  private void addSecondCacheQueueListener(String senderId, boolean isParallel) {
    var sender = getGatewaySender(senderId);
    listener2 = new QueueListener();
    if (!isParallel) {
      var queues = ((AbstractGatewaySender) sender).getQueues();
      for (var q : queues) {
        q.addCacheListener(listener2);
      }
    } else {
      var parallelQueue =
          ((AbstractGatewaySender) sender).getQueues().toArray(new RegionQueue[1])[0];
      parallelQueue.addCacheListener(listener2);
    }
  }

  public static void pauseSender(String senderId) {
    final var exln = addIgnoredException("Could not connect");
    var exp =
        addIgnoredException(ForceReattemptException.class.getName());
    try {
      var sender = getGatewaySender(senderId);
      sender.pause();
      ((AbstractGatewaySender) sender).getEventProcessor().waitForDispatcherToPause();

    } finally {
      exp.remove();
      exln.remove();
    }
  }

  public static void resumeSender(String senderId) {
    final var exln = addIgnoredException("Could not connect");
    var exp =
        addIgnoredException(ForceReattemptException.class.getName());
    try {
      var sender = getGatewaySender(senderId);
      sender.resume();
    } finally {
      exp.remove();
      exln.remove();
    }
  }

  public static void stopSenderInVMsAsync(String senderId, VM... vms) {
    List<AsyncInvocation<Void>> tasks = new LinkedList<>();
    for (var vm : vms) {
      tasks.add(vm.invokeAsync(() -> stopSender(senderId)));
    }
    for (AsyncInvocation invocation : tasks) {
      try {
        invocation.await();
      } catch (InterruptedException e) {
        fail("Stopping senders was interrupted");
      }
    }
  }

  public static void stopSender(String senderId) {
    final var exln = addIgnoredException("Could not connect");
    var exp =
        addIgnoredException(ForceReattemptException.class.getName());
    try {
      var sender = getGatewaySender(senderId);
      AbstractGatewaySenderEventProcessor eventProcessor = null;
      if (sender instanceof AbstractGatewaySender) {
        eventProcessor = ((AbstractGatewaySender) sender).getEventProcessor();
      }
      sender.stop();

      Set<RegionQueue> queues = null;
      if (eventProcessor instanceof ConcurrentSerialGatewaySenderEventProcessor) {
        queues = ((ConcurrentSerialGatewaySenderEventProcessor) eventProcessor).getQueues();
        for (var queue : queues) {
          if (queue instanceof SerialGatewaySenderQueue) {
            assertFalse(((SerialGatewaySenderQueue) queue).isRemovalThreadAlive());
          }
        }
      }
    } finally {
      exp.remove();
      exln.remove();
    }
  }

  public static void stopReceivers() {
    var receivers = cache.getGatewayReceivers();
    for (var receiver : receivers) {
      receiver.stop();
    }
  }

  public static void startReceivers() {
    var receivers = cache.getGatewayReceivers();
    for (var receiver : receivers) {
      try {
        receiver.start();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static GatewaySenderFactory configureGateway(DiskStoreFactory dsf, File[] dirs1,
      String dsName, boolean isParallel, Integer maxMemory, Integer batchSize, boolean isConflation,
      boolean isPersistent, GatewayEventFilter filter, boolean isManualStart, int numDispatchers,
      OrderPolicy policy, int socketBufferSize) {

    var gateway =
        (InternalGatewaySenderFactory) cache.createGatewaySenderFactory();
    gateway.setParallel(isParallel);
    gateway.setMaximumQueueMemory(maxMemory);
    gateway.setBatchSize(batchSize);
    gateway.setBatchConflationEnabled(isConflation);
    gateway.setManualStart(isManualStart);
    gateway.setDispatcherThreads(numDispatchers);
    gateway.setOrderPolicy(policy);
    gateway.setLocatorDiscoveryCallback(new MyLocatorCallback());
    gateway.setSocketBufferSize(socketBufferSize);
    if (filter != null) {
      eventFilter = filter;
      gateway.addGatewayEventFilter(filter);
    }
    if (isPersistent) {
      gateway.setPersistenceEnabled(true);
      gateway.setDiskStoreName(dsf.setDiskDirs(dirs1).create(dsName).getName());
    } else {
      var store = dsf.setDiskDirs(dirs1).create(dsName);
      gateway.setDiskStoreName(store.getName());
    }
    return gateway;
  }

  public static void createSender(String dsName, int remoteDsId, boolean isParallel,
      Integer maxMemory, Integer batchSize, boolean isConflation, boolean isPersistent,
      GatewayEventFilter filter, boolean isManualStart) {
    createSender(dsName, remoteDsId, isParallel, maxMemory, batchSize, isConflation, isPersistent,
        filter, isManualStart, false);
  }

  public static void createSender(String dsName, int remoteDsId, boolean isParallel,
      Integer maxMemory, Integer batchSize, boolean isConflation, boolean isPersistent,
      GatewayEventFilter filter, boolean isManualStart, boolean groupTransactionEvents) {
    final var exln = addIgnoredException("Could not connect");
    try {
      var persistentDirectory =
          new File(dsName + "_disk_" + System.currentTimeMillis() + "_" + VM.getCurrentVMNum());
      persistentDirectory.mkdir();
      var dsf = cache.createDiskStoreFactory();
      var dirs1 = new File[] {persistentDirectory};
      var gateway = configureGateway(dsf, dirs1, dsName, isParallel, maxMemory,
          batchSize, isConflation, isPersistent, filter, isManualStart,
          numDispatcherThreadsForTheRun, GatewaySender.DEFAULT_ORDER_POLICY,
          GatewaySender.DEFAULT_SOCKET_BUFFER_SIZE);
      gateway.setGroupTransactionEvents(groupTransactionEvents);
      if (groupTransactionEvents && gateway instanceof InternalGatewaySenderFactory) {
        // Set a very high value to avoid flakiness in test cases
        ((InternalGatewaySenderFactory) gateway).setRetriesToGetTransactionEventsFromQueue(1000);
      }
      gateway.create(dsName, remoteDsId);
    } finally {
      exln.remove();
    }
  }

  public static void createSenderWithMultipleDispatchers(String dsName, int remoteDsId,
      boolean isParallel, Integer maxMemory, Integer batchSize, boolean isConflation,
      boolean isPersistent, GatewayEventFilter filter, boolean isManualStart, int numDispatchers,
      OrderPolicy orderPolicy) {
    createSenderWithMultipleDispatchers(dsName, remoteDsId,
        isParallel, maxMemory, batchSize, isConflation,
        isPersistent, filter, isManualStart, numDispatchers,
        orderPolicy, GatewaySender.DEFAULT_SOCKET_BUFFER_SIZE);
  }

  public static void createSenderWithMultipleDispatchers(String dsName, int remoteDsId,
      boolean isParallel, Integer maxMemory, Integer batchSize, boolean isConflation,
      boolean isPersistent, GatewayEventFilter filter, boolean isManualStart, int numDispatchers,
      OrderPolicy orderPolicy, int socketBufferSize) {
    final var exln = addIgnoredException("Could not connect");
    try {
      var persistentDirectory =
          new File(dsName + "_disk_" + System.currentTimeMillis() + "_" + VM.getCurrentVMNum());
      persistentDirectory.mkdir();
      var dsf = cache.createDiskStoreFactory();
      var dirs1 = new File[] {persistentDirectory};
      var gateway =
          configureGateway(dsf, dirs1, dsName, isParallel, maxMemory, batchSize, isConflation,
              isPersistent, filter, isManualStart, numDispatchers, orderPolicy, socketBufferSize);
      gateway.create(dsName, remoteDsId);

    } finally {
      exln.remove();
    }
  }

  public static void createSenderWithoutDiskStore(String dsName, int remoteDsId, Integer maxMemory,
      Integer batchSize, boolean isConflation, boolean isManualStart) {

    var gateway = cache.createGatewaySenderFactory();
    gateway.setParallel(true);
    gateway.setMaximumQueueMemory(maxMemory);
    gateway.setBatchSize(batchSize);
    gateway.setManualStart(isManualStart);
    // set dispatcher threads
    gateway.setDispatcherThreads(numDispatcherThreadsForTheRun);
    gateway.setBatchConflationEnabled(isConflation);
    gateway.create(dsName, remoteDsId);
  }

  public static void createSenderAlertThresholdWithoutDiskStore(String dsName, int remoteDsId,
      Integer maxMemory,
      Integer batchSize, boolean isConflation, boolean isManualStart, int alertthreshold) {

    var gateway = cache.createGatewaySenderFactory();
    gateway.setAlertThreshold(alertthreshold);
    gateway.setParallel(true);
    gateway.setMaximumQueueMemory(maxMemory);
    gateway.setBatchSize(batchSize);
    gateway.setManualStart(isManualStart);
    // set dispatcher threads
    gateway.setDispatcherThreads(numDispatcherThreadsForTheRun);
    gateway.setBatchConflationEnabled(isConflation);
    gateway.create(dsName, remoteDsId);
  }

  public static void createConcurrentSender(String dsName, int remoteDsId, boolean isParallel,
      Integer maxMemory, Integer batchSize, boolean isConflation, boolean isPersistent,
      GatewayEventFilter filter, boolean isManualStart, int concurrencyLevel, OrderPolicy policy) {

    var persistentDirectory =
        new File(dsName + "_disk_" + System.currentTimeMillis() + "_" + VM.getCurrentVMNum());
    persistentDirectory.mkdir();
    var dsf = cache.createDiskStoreFactory();
    var dirs1 = new File[] {persistentDirectory};
    var gateway = configureGateway(dsf, dirs1, dsName, isParallel, maxMemory,
        batchSize, isConflation, isPersistent, filter, isManualStart, concurrencyLevel, policy,
        GatewaySender.DEFAULT_SOCKET_BUFFER_SIZE);
    gateway.create(dsName, remoteDsId);
  }

  public static void createSenderForValidations(String dsName, int remoteDsId, boolean isParallel,
      Integer alertThreshold, boolean isConflation, boolean isPersistent,
      List<GatewayEventFilter> eventFilters, List<GatewayTransportFilter> transportFilters,
      boolean isManualStart, boolean isDiskSync) {
    var exp1 =
        addIgnoredException(RegionDestroyedException.class.getName());
    try {
      var persistentDirectory =
          new File(dsName + "_disk_" + System.currentTimeMillis() + "_" + VM.getCurrentVMNum());
      persistentDirectory.mkdir();
      var dsf = cache.createDiskStoreFactory();
      var dirs1 = new File[] {persistentDirectory};

      if (isParallel) {
        var gateway = cache.createGatewaySenderFactory();
        gateway.setParallel(true);
        gateway.setAlertThreshold(alertThreshold);
        ((InternalGatewaySenderFactory) gateway)
            .setLocatorDiscoveryCallback(new MyLocatorCallback());
        if (eventFilters != null) {
          for (var filter : eventFilters) {
            gateway.addGatewayEventFilter(filter);
          }
        }
        if (transportFilters != null) {
          for (var filter : transportFilters) {
            gateway.addGatewayTransportFilter(filter);
          }
        }
        if (isPersistent) {
          gateway.setPersistenceEnabled(true);
          gateway.setDiskStoreName(dsf.setDiskDirs(dirs1).create(dsName + "_Parallel").getName());
        } else {
          var store = dsf.setDiskDirs(dirs1).create(dsName + "_Parallel");
          gateway.setDiskStoreName(store.getName());
        }
        gateway.setDiskSynchronous(isDiskSync);
        gateway.setBatchConflationEnabled(isConflation);
        gateway.setManualStart(isManualStart);
        // set dispatcher threads
        gateway.setDispatcherThreads(numDispatcherThreadsForTheRun);
        gateway.create(dsName, remoteDsId);

      } else {
        var gateway = cache.createGatewaySenderFactory();
        gateway.setAlertThreshold(alertThreshold);
        gateway.setManualStart(isManualStart);
        // set dispatcher threads
        gateway.setDispatcherThreads(numDispatcherThreadsForTheRun);
        ((InternalGatewaySenderFactory) gateway)
            .setLocatorDiscoveryCallback(new MyLocatorCallback());
        if (eventFilters != null) {
          for (var filter : eventFilters) {
            gateway.addGatewayEventFilter(filter);
          }
        }
        if (transportFilters != null) {
          for (var filter : transportFilters) {
            gateway.addGatewayTransportFilter(filter);
          }
        }
        gateway.setBatchConflationEnabled(isConflation);
        if (isPersistent) {
          gateway.setPersistenceEnabled(true);
          gateway.setDiskStoreName(dsf.setDiskDirs(dirs1).create(dsName + "_Serial").getName());
        } else {
          var store = dsf.setDiskDirs(dirs1).create(dsName + "_Serial");
          gateway.setDiskStoreName(store.getName());
        }
        gateway.setDiskSynchronous(isDiskSync);
        gateway.create(dsName, remoteDsId);
      }
    } finally {
      exp1.remove();
    }
  }

  public static String createSenderWithDiskStore(String dsName, int remoteDsId, boolean isParallel,
      Integer maxMemory, Integer batchSize, boolean isConflation, boolean isPersistent,
      GatewayEventFilter filter, String dsStore, boolean isManualStart) {
    File persistentDirectory = null;
    if (dsStore == null) {
      persistentDirectory =
          new File(dsName + "_disk_" + System.currentTimeMillis() + "_" + VM.getCurrentVMNum());
    } else {
      persistentDirectory = new File(dsStore);
    }
    logger.info("The ds is : " + persistentDirectory.getName());

    persistentDirectory.mkdir();
    var dsf = cache.createDiskStoreFactory();
    var dirs1 = new File[] {persistentDirectory};

    if (isParallel) {
      var gateway = cache.createGatewaySenderFactory();
      gateway.setParallel(true);
      gateway.setMaximumQueueMemory(maxMemory);
      gateway.setBatchSize(batchSize);
      gateway.setManualStart(isManualStart);
      // set dispatcher threads
      gateway.setDispatcherThreads(numDispatcherThreadsForTheRun);
      ((InternalGatewaySenderFactory) gateway).setLocatorDiscoveryCallback(new MyLocatorCallback());
      if (filter != null) {
        gateway.addGatewayEventFilter(filter);
      }
      if (isPersistent) {
        gateway.setPersistenceEnabled(true);
        var dsname = dsf.setDiskDirs(dirs1).create(dsName).getName();
        gateway.setDiskStoreName(dsname);
        logger.info("The DiskStoreName is : " + dsname);
      } else {
        var store = dsf.setDiskDirs(dirs1).create(dsName);
        gateway.setDiskStoreName(store.getName());
        logger.info("The ds is : " + store.getName());
      }
      gateway.setBatchConflationEnabled(isConflation);
      gateway.create(dsName, remoteDsId);

    } else {
      var gateway = cache.createGatewaySenderFactory();
      gateway.setMaximumQueueMemory(maxMemory);
      gateway.setBatchSize(batchSize);
      gateway.setManualStart(isManualStart);
      // set dispatcher threads
      gateway.setDispatcherThreads(numDispatcherThreadsForTheRun);
      ((InternalGatewaySenderFactory) gateway).setLocatorDiscoveryCallback(new MyLocatorCallback());
      if (filter != null) {
        gateway.addGatewayEventFilter(filter);
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
    return persistentDirectory.getName();
  }


  public static void createSenderWithListener(String dsName, int remoteDsName, boolean isParallel,
      Integer maxMemory, Integer batchSize, boolean isConflation, boolean isPersistent,
      GatewayEventFilter filter, boolean attachTwoListeners, boolean isManualStart) {
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
      gateway.setManualStart(isManualStart);
      // set dispatcher threads
      gateway.setDispatcherThreads(numDispatcherThreadsForTheRun);
      ((InternalGatewaySenderFactory) gateway).setLocatorDiscoveryCallback(new MyLocatorCallback());
      if (filter != null) {
        gateway.addGatewayEventFilter(filter);
      }
      if (isPersistent) {
        gateway.setPersistenceEnabled(true);
        gateway.setDiskStoreName(dsf.setDiskDirs(dirs1).create(dsName).getName());
      } else {
        var store = dsf.setDiskDirs(dirs1).create(dsName);
        gateway.setDiskStoreName(store.getName());
      }
      gateway.setBatchConflationEnabled(isConflation);
      gateway.create(dsName, remoteDsName);

    } else {
      var gateway = cache.createGatewaySenderFactory();
      gateway.setMaximumQueueMemory(maxMemory);
      gateway.setBatchSize(batchSize);
      gateway.setManualStart(isManualStart);
      // set dispatcher threads
      gateway.setDispatcherThreads(numDispatcherThreadsForTheRun);
      ((InternalGatewaySenderFactory) gateway).setLocatorDiscoveryCallback(new MyLocatorCallback());
      if (filter != null) {
        gateway.addGatewayEventFilter(filter);
      }
      gateway.setBatchConflationEnabled(isConflation);
      if (isPersistent) {
        gateway.setPersistenceEnabled(true);
        gateway.setDiskStoreName(dsf.setDiskDirs(dirs1).create(dsName).getName());
      } else {
        var store = dsf.setDiskDirs(dirs1).create(dsName);
        gateway.setDiskStoreName(store.getName());
      }

      eventListener1 = new MyGatewaySenderEventListener();
      ((InternalGatewaySenderFactory) gateway).addAsyncEventListener(eventListener1);
      if (attachTwoListeners) {
        eventListener2 = new MyGatewaySenderEventListener2();
        ((InternalGatewaySenderFactory) gateway).addAsyncEventListener(eventListener2);
      }
      ((InternalGatewaySenderFactory) gateway).create(dsName);
    }
  }

  public static void createReceiverInVMs(int maximumTimeBetweenPings, VM... vms) {
    for (var vm : vms) {
      vm.invoke(() -> createReceiverWithMaximumTimeBetweenPings(maximumTimeBetweenPings));
    }
  }


  public static void createReceiverInVMs(VM... vms) {
    createReceiverInVMs(-1, vms);
  }

  public static int createReceiver() {
    return createReceiverWithMaximumTimeBetweenPings(-1);
  }

  public static int createReceiverWithMaximumTimeBetweenPings(int maximumTimeBetweenPings) {
    var fact = cache.createGatewayReceiverFactory();
    var port = getRandomAvailableTCPPort();
    fact.setStartPort(port);
    fact.setEndPort(port);
    fact.setManualStart(true);
    if (maximumTimeBetweenPings > 0) {
      fact.setMaximumTimeBetweenPings(maximumTimeBetweenPings);
    }
    var receiver = fact.create();
    try {
      receiver.start();
    } catch (IOException e) {
      e.printStackTrace();
      Assert.fail(
          "Test " + getTestMethodName() + " failed to start GatewayReceiver on port " + port, e);
    }
    return port;
  }

  public static int createReceiverWithSSL(int locPort) {
    var test = new WANTestBase();

    var gemFireProps = test.getDistributedSystemProperties();

    gemFireProps.put(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
    gemFireProps.put(GATEWAY_SSL_ENABLED, "true");
    gemFireProps.put(GATEWAY_SSL_PROTOCOLS, "any");
    gemFireProps.put(GATEWAY_SSL_CIPHERS, "any");
    gemFireProps.put(GATEWAY_SSL_REQUIRE_AUTHENTICATION, "true");

    gemFireProps.put(GATEWAY_SSL_KEYSTORE_TYPE, "jks");
    gemFireProps.put(GATEWAY_SSL_KEYSTORE, createTempFileFromResource(WANTestBase.class,
        "/org/apache/geode/cache/client/internal/default.keystore").getAbsolutePath());
    gemFireProps.put(GATEWAY_SSL_KEYSTORE_PASSWORD, "password");
    gemFireProps.put(GATEWAY_SSL_TRUSTSTORE, createTempFileFromResource(WANTestBase.class,
        "/org/apache/geode/cache/client/internal/default.keystore").getAbsolutePath());
    gemFireProps.put(GATEWAY_SSL_TRUSTSTORE_PASSWORD, "password");

    gemFireProps.setProperty(MCAST_PORT, "0");
    gemFireProps.setProperty(LOCATORS, "localhost[" + locPort + "]");

    logger.info("Starting cache ds with following properties \n" + gemFireProps);

    var ds = test.getSystem(gemFireProps);
    cache = CacheFactory.create(ds);
    var fact = cache.createGatewayReceiverFactory();
    var port = getRandomAvailableTCPPort();
    fact.setStartPort(port);
    fact.setEndPort(port);
    fact.setManualStart(true);
    var receiver = fact.create();
    try {
      receiver.start();
    } catch (IOException e) {
      e.printStackTrace();
      fail("Test " + test.getName() + " failed to start GatewayReceiver on port " + port);
    }
    return port;
  }

  public static void createReceiverAndServer(int locPort) {
    var test = new WANTestBase();
    var props = test.getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + locPort + "]");

    var ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
    var fact = cache.createGatewayReceiverFactory();
    var receiverPort = getRandomAvailableTCPPort();
    fact.setStartPort(receiverPort);
    fact.setEndPort(receiverPort);
    fact.setManualStart(true);
    var receiver = fact.create();
    try {
      receiver.start();
    } catch (IOException e) {
      e.printStackTrace();
      fail("Test " + test.getName() + " failed to start GatewayReceiver on port " + receiverPort);
    }
    var server = cache.addCacheServer();
    var serverPort = getRandomAvailableTCPPort();
    server.setPort(serverPort);
    server.setHostnameForClients("localhost");
    try {
      server.start();
    } catch (IOException e) {
      org.apache.geode.test.dunit.Assert.fail("Failed to start server ", e);
    }
  }

  public static int createServer(int locPort) {
    return createServer(locPort, -1);
  }

  public static int createServer(int locPort, int maximumTimeBetweenPings) {
    var test = new WANTestBase();
    var properties = test.getDistributedSystemProperties();
    return createServer(locPort, maximumTimeBetweenPings, properties);
  }

  public static int createServer(int locPort, int maximumTimeBetweenPings, Properties props) {
    var test = new WANTestBase();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + locPort + "]");
    var ds = test.getSystem(props);
    cache = CacheFactory.create(ds);

    var server = cache.addCacheServer();
    var port = getRandomAvailableTCPPort();
    server.setPort(port);
    server.setHostnameForClients("localhost");
    if (maximumTimeBetweenPings > 0) {
      server.setMaximumTimeBetweenPings(maximumTimeBetweenPings);
    }
    try {
      server.start();
    } catch (IOException e) {
      org.apache.geode.test.dunit.Assert.fail("Failed to start server ", e);
    }
    return port;
  }

  public static void createClientWithLocatorAndRegion(int port0, String host, String regionName,
      ClientRegionShortcut regionType) {
    cache = (Cache) new ClientCacheFactory().addPoolLocator(host, port0).create();

    ((ClientCache) cache).createClientRegionFactory(regionType)
        .create(regionName);
  }

  public static void createClientWithLocatorAndRegion(int port0, String host, String regionName) {
    createClientWithLocatorAndRegion(port0, host);

    RegionFactory factory = cache.createRegionFactory(RegionShortcut.LOCAL);
    factory.setPoolName("pool");

    region = factory.create(regionName);
    region.registerInterest("ALL_KEYS");
    logger.info("Distributed Region " + regionName + " created Successfully :" + region.toString());
  }

  public static void createClientWithLocatorAndRegion(final int port0, final String host) {
    var test = new WANTestBase();
    var props = test.getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");

    var ds = test.getSystem(props);
    cache = CacheFactory.create(ds);

    assertNotNull(cache);
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool p;
    try {
      p = PoolManager.createFactory().addLocator(host, port0).setPingInterval(250)
          .setSubscriptionEnabled(true).setSubscriptionRedundancy(-1).setReadTimeout(20000)
          .setSocketBufferSize(1000).setMinConnections(6).setMaxConnections(10).setRetryAttempts(3)
          .create("pool");
    } finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }
  }

  public static int createReceiver_PDX(int locPort) {
    var test = new WANTestBase();
    var props = test.getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + locPort + "]");
    var ds = test.getSystem(props);
    var pdxDir = new File(CacheTestCase.getDiskDir(), "pdx");

    cache = new InternalCacheBuilder(props)
        .setPdxPersistent(true)
        .setPdxDiskStore("pdxStore")
        .setIsExistingOk(false)
        .create(ds);

    cache.createDiskStoreFactory().setDiskDirs(new File[] {pdxDir}).setMaxOplogSize(1)
        .create("pdxStore");
    var fact = cache.createGatewayReceiverFactory();
    var port = getRandomAvailableTCPPort();
    fact.setStartPort(port);
    fact.setEndPort(port);
    fact.setManualStart(true);
    var receiver = fact.create();
    try {
      receiver.start();
    } catch (IOException e) {
      e.printStackTrace();
      fail("Test " + test.getName() + " failed to start GatewayReceiver on port " + port);
    }
    return port;
  }

  public static void doDistTXPuts(String regionName, int numPuts) {
    var txMgr = cache.getCacheTransactionManager();
    txMgr.setDistributed(true);

    var exp1 =
        addIgnoredException(InterruptedException.class.getName());
    var exp2 =
        addIgnoredException(GatewaySenderException.class.getName());
    try {
      var r = cache.getRegion(SEPARATOR + regionName);
      for (long i = 1; i <= numPuts; i++) {
        txMgr.begin();
        r.put(i, i);
        txMgr.commit();
      }
    } finally {
      exp1.remove();
      exp2.remove();
    }
  }


  public static void doPuts(String regionName, int numPuts, Object value) {
    var exp1 =
        addIgnoredException(InterruptedException.class.getName());
    var exp2 =
        addIgnoredException(GatewaySenderException.class.getName());
    try {
      var r = cache.getRegion(SEPARATOR + regionName);
      for (long i = 0; i < numPuts; i++) {
        r.put(i, value);
      }
    } finally {
      exp1.remove();
      exp2.remove();
    }
  }

  public static void doPuts(String regionName, int numPuts) {
    var exp1 =
        addIgnoredException(InterruptedException.class.getName());
    var exp2 =
        addIgnoredException(GatewaySenderException.class.getName());
    try {
      var r = cache.getRegion(SEPARATOR + regionName);
      for (long i = 0; i < numPuts; i++) {
        r.put(i, "Value_" + i);
      }
    } finally {
      exp1.remove();
      exp2.remove();
    }
  }

  public static void doTxPuts(String regionName, int numPuts) {
    try (
        var ignored = addIgnoredException(InterruptedException.class);
        var ignored1 =
            addIgnoredException(GatewaySenderException.class)) {
      var r = cache.getRegion(SEPARATOR + regionName);
      for (long i = 0; i < numPuts; i++) {
        cache.getCacheTransactionManager().begin();
        r.put(i, "Value_" + i);
        cache.getCacheTransactionManager().commit();
      }
    }
  }

  public static void doPutsSameKey(String regionName, int numPuts, String key) {
    var exp1 =
        addIgnoredException(InterruptedException.class.getName());
    var exp2 =
        addIgnoredException(GatewaySenderException.class.getName());
    try {
      var r = cache.getRegion(SEPARATOR + regionName);
      for (long i = 0; i < numPuts; i++) {
        r.put(key, "Value_" + i);
      }
    } finally {
      exp1.remove();
      exp2.remove();
    }
  }


  public static void doPutsAfter300(String regionName, int numPuts) {
    var r = cache.getRegion(SEPARATOR + regionName);
    for (long i = 300; i < numPuts; i++) {
      r.put(i, "Value_" + i);
    }
  }

  public static void doPutsFrom(String regionName, int from, int numPuts) {
    var r = cache.getRegion(SEPARATOR + regionName);
    for (long i = from; i < numPuts; i++) {
      r.put(i, "Value_" + i);
    }
  }

  public static void doDestroys(String regionName, int keyNum) {
    var r = cache.getRegion(SEPARATOR + regionName);
    for (long i = 0; i < keyNum; i++) {
      r.destroy(i);
    }
  }

  public static void doPutAll(String regionName, int numPuts, int size) {
    Region<?, ?> r = cache.getRegion(SEPARATOR + regionName);
    for (long i = 0; i < numPuts; i++) {
      Map putAllMap = new HashMap();
      for (long j = 0; j < size; j++) {
        putAllMap.put((size * i) + j, "Value_" + i);
      }
      r.putAll(putAllMap, "putAllCallback");
      putAllMap.clear();
    }
  }


  public static void doPutsWithKeyAsString(String regionName, int numPuts) {
    Region<String, String> r = cache.getRegion(SEPARATOR + regionName);
    for (long i = 0; i < numPuts; i++) {
      r.put("Object_" + i, "Value_" + i);
    }
  }

  public static void putGivenKeyValue(String regionName, Map keyValues) {
    var r = cache.getRegion(SEPARATOR + regionName);
    for (var key : keyValues.keySet()) {
      r.put(key, keyValues.get(key));
    }
  }

  public static void doOrderAndShipmentPutsInsideTransactions(Map keyValues,
      int eventsPerTransaction) {
    Region orderRegion = cache.getRegion(orderRegionName);
    Region shipmentRegion = cache.getRegion(shipmentRegionName);
    var eventInTransaction = 0;
    var cacheTransactionManager = cache.getCacheTransactionManager();
    for (var key : keyValues.keySet()) {
      if (eventInTransaction == 0) {
        cacheTransactionManager.begin();
      }
      Region r;
      if (key instanceof OrderId) {
        r = orderRegion;
      } else {
        r = shipmentRegion;
      }
      r.put(key, keyValues.get(key));
      if (++eventInTransaction == eventsPerTransaction) {
        cacheTransactionManager.commit();
        eventInTransaction = 0;
      }
    }
    if (eventInTransaction != 0) {
      cacheTransactionManager.commit();
    }
  }

  public static void doPutsInsideTransactions(String regionName, Map keyValues,
      int eventsPerTransaction) {
    var r = cache.getRegion(Region.SEPARATOR + regionName);
    var eventInTransaction = 0;
    var cacheTransactionManager = cache.getCacheTransactionManager();
    for (var key : keyValues.keySet()) {
      if (eventInTransaction == 0) {
        cacheTransactionManager.begin();
      }
      r.put(key, keyValues.get(key));
      if (++eventInTransaction == eventsPerTransaction) {
        cacheTransactionManager.commit();
        eventInTransaction = 0;
      }
    }
    if (eventInTransaction != 0) {
      cacheTransactionManager.commit();
    }
  }

  public static void destroyRegion(String regionName) {
    destroyRegion(regionName, -1);
  }

  public static void destroyRegion(String regionName, final int min) {
    final Region<?, ?> r = cache.getRegion(SEPARATOR + regionName);
    await().until(() -> r.size() > min);
    r.destroyRegion();
  }

  public static void localDestroyRegion(String regionName) {
    var exp =
        addIgnoredException(PRLocallyDestroyedException.class.getName());
    try {
      Region<?, ?> r = cache.getRegion(SEPARATOR + regionName);
      r.localDestroyRegion();
    } finally {
      exp.remove();
    }
  }


  public static Map putCustomerPartitionedRegion(int numPuts) {
    var valueSuffix = "";
    return putCustomerPartitionedRegion(numPuts, valueSuffix);
  }

  public static Map updateCustomerPartitionedRegion(int numPuts) {
    var valueSuffix = "_update";
    return putCustomerPartitionedRegion(numPuts, valueSuffix);
  }

  protected static Map putCustomerPartitionedRegion(int numPuts, String valueSuffix) {
    Map custKeyValues = new HashMap();
    for (var i = 1; i <= numPuts; i++) {
      var custid = new CustId(i);
      var customer = new Customer("name" + i, "Address" + i + valueSuffix);
      try {
        customerRegion.put(custid, customer);
        assertTrue(customerRegion.containsKey(custid));
        assertEquals(customer, customerRegion.get(custid));
        custKeyValues.put(custid, customer);
      } catch (Exception e) {
        org.apache.geode.test.dunit.Assert.fail(
            "putCustomerPartitionedRegion : failed while doing put operation in CustomerPartitionedRegion ",
            e);
      }
      logger.info("Customer :- { " + custid + " : " + customer + " }");
    }
    return custKeyValues;
  }

  public static Map putOrderPartitionedRegion(int numPuts) {
    Map orderKeyValues = new HashMap();
    for (var i = 1; i <= numPuts; i++) {
      var custid = new CustId(i);
      var oid = i + 1;
      var orderId = new OrderId(oid, custid);
      var order = new Order("ORDER" + oid);
      try {
        orderRegion.put(orderId, order);
        orderKeyValues.put(orderId, order);
        assertTrue(orderRegion.containsKey(orderId));
        assertEquals(order, orderRegion.get(orderId));

      } catch (Exception e) {
        org.apache.geode.test.dunit.Assert.fail(
            "putOrderPartitionedRegion : failed while doing put operation in OrderPartitionedRegion ",
            e);
      }
      logger.info("Order :- { " + orderId + " : " + order + " }");
    }
    return orderKeyValues;
  }

  public static Map putOrderPartitionedRegionUsingCustId(int numPuts) {
    Map orderKeyValues = new HashMap();
    for (var i = 1; i <= numPuts; i++) {
      var custid = new CustId(i);
      var order = new Order("ORDER" + i);
      try {
        orderRegion.put(custid, order);
        orderKeyValues.put(custid, order);
        assertTrue(orderRegion.containsKey(custid));
        assertEquals(order, orderRegion.get(custid));

      } catch (Exception e) {
        org.apache.geode.test.dunit.Assert.fail(
            "putOrderPartitionedRegionUsingCustId : failed while doing put operation in OrderPartitionedRegion ",
            e);
      }
      logger.info("Order :- { " + custid + " : " + order + " }");
    }
    return orderKeyValues;
  }

  public static Map updateOrderPartitionedRegion(int numPuts) {
    Map orderKeyValues = new HashMap();
    for (var i = 1; i <= numPuts; i++) {
      var custid = new CustId(i);
      var oid = i + 1;
      var orderId = new OrderId(oid, custid);
      var order = new Order("ORDER" + oid + "_update");
      try {
        orderRegion.put(orderId, order);
        orderKeyValues.put(orderId, order);
        assertTrue(orderRegion.containsKey(orderId));
        assertEquals(order, orderRegion.get(orderId));

      } catch (Exception e) {
        org.apache.geode.test.dunit.Assert.fail(
            "updateOrderPartitionedRegion : failed while doing put operation in OrderPartitionedRegion ",
            e);
      }
      logger.info("Order :- { " + orderId + " : " + order + " }");
    }
    return orderKeyValues;
  }

  public static Map updateOrderPartitionedRegionUsingCustId(int numPuts) {
    Map orderKeyValues = new HashMap();
    for (var i = 1; i <= numPuts; i++) {
      var custid = new CustId(i);
      var order = new Order("ORDER" + i + "_update");
      try {
        orderRegion.put(custid, order);
        assertTrue(orderRegion.containsKey(custid));
        assertEquals(order, orderRegion.get(custid));
        orderKeyValues.put(custid, order);
      } catch (Exception e) {
        org.apache.geode.test.dunit.Assert.fail(
            "updateOrderPartitionedRegionUsingCustId : failed while doing put operation in OrderPartitionedRegion ",
            e);
      }
      logger.info("Order :- { " + custid + " : " + order + " }");
    }
    return orderKeyValues;
  }

  public static Map putShipmentPartitionedRegion(int numPuts) {
    Map shipmentKeyValue = new HashMap();
    for (var i = 1; i <= numPuts; i++) {
      var custid = new CustId(i);
      var oid = i + 1;
      var orderId = new OrderId(oid, custid);
      var sid = oid + 1;
      var shipmentId = new ShipmentId(sid, orderId);
      var shipment = new Shipment("Shipment" + sid);
      try {
        shipmentRegion.put(shipmentId, shipment);
        assertTrue(shipmentRegion.containsKey(shipmentId));
        assertEquals(shipment, shipmentRegion.get(shipmentId));
        shipmentKeyValue.put(shipmentId, shipment);
      } catch (Exception e) {
        org.apache.geode.test.dunit.Assert.fail(
            "putShipmentPartitionedRegion : failed while doing put operation in ShipmentPartitionedRegion ",
            e);
      }
      logger.info("Shipment :- { " + shipmentId + " : " + shipment + " }");
    }
    return shipmentKeyValue;
  }

  public static void putcolocatedPartitionedRegion(int numPuts) {
    for (var i = 1; i <= numPuts; i++) {
      var custid = new CustId(i);
      var customer = new Customer("Customer" + custid, "Address" + custid);
      customerRegion.put(custid, customer);
      var oid = i + 1;
      var orderId = new OrderId(oid, custid);
      var order = new Order("Order" + orderId);
      orderRegion.put(orderId, order);
      var sid = oid + 1;
      var shipmentId = new ShipmentId(sid, orderId);
      var shipment = new Shipment("Shipment" + sid);
      shipmentRegion.put(shipmentId, shipment);
    }
  }

  public static Map putShipmentPartitionedRegionUsingCustId(int numPuts) {
    Map shipmentKeyValue = new HashMap();
    for (var i = 1; i <= numPuts; i++) {
      var custid = new CustId(i);
      var shipment = new Shipment("Shipment" + i);
      try {
        shipmentRegion.put(custid, shipment);
        assertTrue(shipmentRegion.containsKey(custid));
        assertEquals(shipment, shipmentRegion.get(custid));
        shipmentKeyValue.put(custid, shipment);
      } catch (Exception e) {
        org.apache.geode.test.dunit.Assert.fail(
            "putShipmentPartitionedRegionUsingCustId : failed while doing put operation in ShipmentPartitionedRegion ",
            e);
      }
      logger.info("Shipment :- { " + custid + " : " + shipment + " }");
    }
    return shipmentKeyValue;
  }

  public static Map updateShipmentPartitionedRegion(int numPuts) {
    Map shipmentKeyValue = new HashMap();
    for (var i = 1; i <= numPuts; i++) {
      var custid = new CustId(i);
      var oid = i + 1;
      var orderId = new OrderId(oid, custid);
      var sid = oid + 1;
      var shipmentId = new ShipmentId(sid, orderId);
      var shipment = new Shipment("Shipment" + sid + "_update");
      try {
        shipmentRegion.put(shipmentId, shipment);
        assertTrue(shipmentRegion.containsKey(shipmentId));
        assertEquals(shipment, shipmentRegion.get(shipmentId));
        shipmentKeyValue.put(shipmentId, shipment);
      } catch (Exception e) {
        org.apache.geode.test.dunit.Assert.fail(
            "updateShipmentPartitionedRegion : failed while doing put operation in ShipmentPartitionedRegion ",
            e);
      }
      logger.info("Shipment :- { " + shipmentId + " : " + shipment + " }");
    }
    return shipmentKeyValue;
  }

  public static Map updateShipmentPartitionedRegionUsingCustId(int numPuts) {
    Map shipmentKeyValue = new HashMap();
    for (var i = 1; i <= numPuts; i++) {
      var custid = new CustId(i);
      var shipment = new Shipment("Shipment" + i + "_update");
      try {
        shipmentRegion.put(custid, shipment);
        assertTrue(shipmentRegion.containsKey(custid));
        assertEquals(shipment, shipmentRegion.get(custid));
        shipmentKeyValue.put(custid, shipment);
      } catch (Exception e) {
        org.apache.geode.test.dunit.Assert.fail(
            "updateShipmentPartitionedRegionUsingCustId : failed while doing put operation in ShipmentPartitionedRegion ",
            e);
      }
      logger.info("Shipment :- { " + custid + " : " + shipment + " }");
    }
    return shipmentKeyValue;
  }

  public static void doPutsPDXSerializable(String regionName, int numPuts) {
    var r = cache.getRegion(SEPARATOR + regionName);
    for (var i = 0; i < numPuts; i++) {
      r.put("Key_" + i, new SimpleClass(i, (byte) i));
    }
  }

  public static void doPutsPDXSerializable2(String regionName, int numPuts) {
    var r = cache.getRegion(SEPARATOR + regionName);
    for (var i = 0; i < numPuts; i++) {
      r.put("Key_" + i, new SimpleClass1(false, (short) i, "" + i, i, "" + i, "" + i, i, i));
    }
  }


  public static void doTxPuts(String regionName) {
    var r = cache.getRegion(SEPARATOR + regionName);
    var mgr = cache.getCacheTransactionManager();

    mgr.begin();
    r.put(0, 0);
    r.put(100, 100);
    r.put(200, 200);
    mgr.commit();
  }

  public static void doTxPutsWithRetryIfError(String regionName, final long putsPerTransaction,
      final long transactions, long offset) {
    var r = cache.getRegion(Region.SEPARATOR + regionName);

    var keyOffset = offset * ((putsPerTransaction + (10 * transactions)) * 100);
    long j = 0;
    var mgr = cache.getCacheTransactionManager();
    for (var i = 0; i < transactions; i++) {
      var done = false;
      do {
        try {
          mgr.begin();
          for (j = 0; j < putsPerTransaction; j++) {
            var key = keyOffset + ((j + (10 * i)) * 100);
            var value = "Value_" + key;
            r.put(key, value);
          }
          mgr.commit();
          done = true;
        } catch (TransactionException e) {
          logger.info("Something went wrong with transaction [{},{}]. Retrying. Error: {}", i, j,
              e.getMessage());
          e.printStackTrace();
        } catch (IllegalStateException e1) {
          logger.info("Something went wrong with transaction [{},{}]. Retrying. Error: {}", i, j,
              e1.getMessage());
          e1.printStackTrace();
          try {
            mgr.rollback();
            logger.info("Rolled back transaction [{},{}]. Retrying. Error: {}", i, j,
                e1.getMessage());
          } catch (Exception e2) {
            logger.info(
                "Something went wrong when rolling back transaction [{},{}]. Retrying transaction. Error: {}",
                i, j, e2.getMessage());
            e2.printStackTrace();
          }
        }
      } while (!done);
    }
  }

  public static void doNextPuts(String regionName, int start, int numPuts) {
    var exp =
        addIgnoredException(CacheClosedException.class.getName());
    try {
      var r = cache.getRegion(SEPARATOR + regionName);
      for (long i = start; i < numPuts; i++) {
        r.put(i, i);
      }
    } finally {
      exp.remove();
    }
  }

  public static void checkQueueSize(String senderId, int numQueueEntries) {
    await()
        .untilAsserted(() -> testQueueSize(senderId, numQueueEntries));
  }

  public static void testQueueSize(String senderId, int numQueueEntries) {
    var sender = cache.getGatewaySender(senderId);
    if (sender.isParallel()) {
      var totalSize = 0;
      var queues = ((AbstractGatewaySender) sender).getQueues();
      for (var q : queues) {
        var prQ = (ConcurrentParallelGatewaySenderQueue) q;
        totalSize += prQ.size();
      }
      assertEquals(numQueueEntries, totalSize);
    } else {
      var queues = ((AbstractGatewaySender) sender).getQueues();
      var size = 0;
      for (var q : queues) {
        size += q.size();
      }
      assertEquals(numQueueEntries, size);
    }
  }

  /**
   * To be used only for ParallelGatewaySender.
   *
   * @param senderId Id of the ParallelGatewaySender
   * @param numQueueEntries Expected number of ParallelGatewaySenderQueue entries
   */
  public static void checkPRQLocalSize(String senderId, final int numQueueEntries) {
    GatewaySender sender = null;
    for (var s : cache.getGatewaySenders()) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }

    if (sender.isParallel()) {
      final var queues = ((AbstractGatewaySender) sender).getQueues();
      await().untilAsserted(() -> {
        var size = 0;
        for (var q : queues) {
          var prQ = (ConcurrentParallelGatewaySenderQueue) q;
          size += prQ.localSize();
        }
        assertEquals(
            " Expected local queue entries: " + numQueueEntries + " but actual entries: " + size,
            numQueueEntries, size);
      });
    }
  }

  /**
   * To be used only for ParallelGatewaySender.
   *
   * @param senderId Id of the ParallelGatewaySender
   */
  public static int getPRQLocalSize(String senderId) {
    GatewaySender sender = null;
    for (var s : cache.getGatewaySenders()) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }
    if (sender.isParallel()) {
      var totalSize = 0;
      var queues = ((AbstractGatewaySender) sender).getQueues();
      for (var q : queues) {
        var prQ = (ConcurrentParallelGatewaySenderQueue) q;
        totalSize += prQ.localSize();
      }
      return totalSize;
    }
    return -1;
  }

  public static void doMultiThreadedPuts(String regionName, int numPuts) {
    final var ai = new AtomicInteger(-1);
    final var execService = Executors.newFixedThreadPool(5, new ThreadFactory() {
      final AtomicInteger threadNum = new AtomicInteger();

      @Override
      public Thread newThread(final Runnable r) {
        var result = new Thread(r, "Client Put Thread-" + threadNum.incrementAndGet());
        result.setDaemon(true);
        return result;
      }
    });

    final Region<?, ?> r = cache.getRegion(SEPARATOR + regionName);

    List<Callable<Object>> tasks = new ArrayList<>();
    for (long i = 0; i < 5; i++) {
      tasks.add(new PutTask(r, ai, numPuts));
    }

    try {
      var l = execService.invokeAll(tasks);
      for (var f : l) {
        f.get();
      }
    } catch (InterruptedException e1) { // TODO: eats exception
      e1.printStackTrace();
    } catch (ExecutionException e) { // TODO: eats exceptions
      e.printStackTrace();
    }
    execService.shutdown();
  }

  public static void validateRegionSize(String regionName, final int regionSize) {
    var exp =
        addIgnoredException(ForceReattemptException.class.getName());
    var exp1 =
        addIgnoredException(CacheClosedException.class.getName());
    try {
      final Region<?, ?> r = cache.getRegion(SEPARATOR + regionName);
      if (regionSize != r.keySet().size()) {
        await()
            .untilAsserted(() -> assertEquals(
                "Expected region entries: " + regionSize + " but actual entries: "
                    + r.keySet().size() + " present region keyset " + r.keySet(),
                regionSize, r.keySet().size()));
      }
    } finally {
      exp.remove();
      exp1.remove();
    }
  }

  public List<Object> getKeys(String regionName) {
    final Region<?, ?> r = cache.getRegion(SEPARATOR + regionName);
    return new ArrayList<>(r.keySet());
  }

  public void checkEqualRegionData(String regionName, VM vm1, VM vm2,
      boolean concurrencyChecksEnabled) {
    assertThat(vm1.invoke(() -> getRegionSize(regionName)))
        .isEqualTo(vm2.invoke(() -> getRegionSize(regionName)));
    var regionData1 = vm1.invoke(() -> getRegionData(regionName));
    var regionData2 = vm2.invoke(() -> getRegionData(regionName));
    assertThat(regionData1).isEqualTo(regionData2);

    if (concurrencyChecksEnabled) {
      var regionKeysTimestamps1 = vm1.invoke(() -> getKeysTimestamps(regionName));
      var regionKeysTimestamps2 = vm2.invoke(() -> getKeysTimestamps(regionName));
      assertThat(regionKeysTimestamps1).isEqualTo(regionKeysTimestamps2);
    }
  }

  private Map getRegionData(String regionName) {
    final Region<?, ?> region = cache.getRegion(SEPARATOR + regionName);
    Map map = new HashMap();
    for (Object key : region.keySet()) {
      map.put(key, region.get(key));
    }
    return map;
  }

  private Map getKeysTimestamps(String regionName) {
    final Region<?, ?> region = cache.getRegion(SEPARATOR + regionName);
    Map map = new HashMap();
    for (Object key : region.keySet()) {
      map.put(key, getTimestampForEntry(key, regionName));
    }
    return map;
  }

  public static Object getValueForEntry(long key, String regionName) {
    final Region<?, ?> r = cache.getRegion(SEPARATOR + regionName);
    return r.get(key);
  }

  public static long getTimestampForEntry(Object key, String regionName) {
    final Region<?, ?> r = cache.getRegion(SEPARATOR + regionName);
    if (r.getEntry(key) == null) {
      return 0;
    }
    return r.getEntry(key).getStatistics().getLastModifiedTime();
  }

  public static void validateAsyncEventListener(String asyncQueueId, final int expectedSize) {
    AsyncEventListener theListener = null;

    var asyncEventQueues = cache.getAsyncEventQueues();
    for (var asyncQueue : asyncEventQueues) {
      if (asyncQueueId.equals(asyncQueue.getId())) {
        theListener = asyncQueue.getAsyncEventListener();
      }
    }

    final var eventsMap = ((MyAsyncEventListener) theListener).getEventsMap();
    await()
        .untilAsserted(() -> assertEquals(
            "Expected map entries: " + expectedSize + " but actual entries: " + eventsMap.size(),
            expectedSize, eventsMap.size()));
  }

  public static void waitForAsyncQueueToGetEmpty(String asyncQueueId) {
    AsyncEventQueue theAsyncEventQueue = null;

    var asyncEventChannels = cache.getAsyncEventQueues();
    for (var asyncChannel : asyncEventChannels) {
      if (asyncQueueId.equals(asyncChannel.getId())) {
        theAsyncEventQueue = asyncChannel;
      }
    }

    final GatewaySender sender = ((AsyncEventQueueImpl) theAsyncEventQueue).getSender();

    if (sender.isParallel()) {
      final var queues = ((AbstractGatewaySender) sender).getQueues();
      await().untilAsserted(() -> {
        var size = 0;
        for (var q : queues) {
          size += q.size();
        }
        assertEquals("Expected queue size to be : " + 0 + " but actual entries: " + size, 0, size);
      });
    } else {
      await().untilAsserted(() -> {
        var queues = ((AbstractGatewaySender) sender).getQueues();
        var size = 0;
        for (var q : queues) {
          size += q.size();
        }
        assertEquals("Expected queue size to be : " + 0 + " but actual entries: " + size, 0, size);
      });
    }
  }

  public static int getAsyncEventListenerMapSize(String asyncEventQueueId) {
    AsyncEventListener theListener = null;

    var asyncEventQueues = cache.getAsyncEventQueues();
    for (var asyncQueue : asyncEventQueues) {
      if (asyncEventQueueId.equals(asyncQueue.getId())) {
        theListener = asyncQueue.getAsyncEventListener();
      }
    }

    final var eventsMap = ((MyAsyncEventListener) theListener).getEventsMap();
    logger.info("The events map size is " + eventsMap.size());
    return eventsMap.size();
  }

  public static void validateRegionSize_PDX(String regionName, final int regionSize) {
    final Region<?, ?> r = cache.getRegion(SEPARATOR + regionName);
    await().untilAsserted(() -> {
      assertEquals("Expected region entries: " + regionSize + " but actual entries: "
          + r.keySet().size() + " present region keyset " + r.keySet(), true,
          (regionSize <= r.keySet().size()));
      assertEquals("Expected region size: " + regionSize + " but actual size: " + r.size(), true,
          (regionSize == r.size()));
    });
    for (var i = 0; i < regionSize; i++) {
      final var temp = i;
      await()
          .untilAsserted(() -> assertEquals(
              "keySet = " + r.keySet() + " values() = " + r.values() + "Region Size = " + r.size(),
              new SimpleClass(temp, (byte) temp), r.get("Key_" + temp)));
    }
  }

  public static void validateRegionSizeOnly_PDX(String regionName, final int regionSize) {
    final Region<?, ?> r = cache.getRegion(SEPARATOR + regionName);
    await()
        .untilAsserted(
            () -> assertEquals(
                "Expected region entries: " + regionSize + " but actual entries: "
                    + r.keySet().size() + " present region keyset " + r.keySet(),
                true, (regionSize <= r.keySet().size())));
  }

  public static void validateQueueSizeStat(String id, final int queueSize) {
    final var sender = (AbstractGatewaySender) cache.getGatewaySender(id);
    await()
        .untilAsserted(() -> assertEquals(queueSize, sender.getEventQueueSize()));
    assertEquals(queueSize, sender.getEventQueueSize());
  }

  public static void validateSecondaryQueueSizeStat(String id, final int queueSize) {
    final var sender = (AbstractGatewaySender) cache.getGatewaySender(id);
    await()
        .untilAsserted(() -> assertEquals(
            "Expected unprocessedEventMap is drained but actual is "
                + sender.getStatistics().getUnprocessedEventMapSize(),
            queueSize, sender.getStatistics().getUnprocessedEventMapSize()));
    assertEquals(queueSize, sender.getStatistics().getUnprocessedEventMapSize());
  }

  /**
   * This method is specifically written for pause and stop operations. This method validates that
   * the region size remains same for at least minimum number of verification attempts and also it
   * remains below a specified limit value. This validation will suffice for testing of pause/stop
   * operations.
   *
   */
  public static void validateRegionSizeRemainsSame(String regionName, final int regionSizeLimit) {
    final Region<?, ?> r = cache.getRegion(SEPARATOR + regionName);
    var wc = new WaitCriterion() {
      final int MIN_VERIFICATION_RUNS = 20;
      int sameRegionSizeCounter = 0;
      long previousSize = -1;

      @Override
      public boolean done() {
        if (r.keySet().size() == previousSize) {
          sameRegionSizeCounter++;
          var s = r.keySet().size();
          // if the sameRegionSizeCounter exceeds the minimum verification runs and regionSize is
          // below specified limit, then return true
          return sameRegionSizeCounter >= MIN_VERIFICATION_RUNS && s <= regionSizeLimit;

        } else { // current regionSize is not same as recorded previous regionSize
          previousSize = r.keySet().size(); // update the previousSize variable with current region
                                            // size
          sameRegionSizeCounter = 0;// reset the sameRegionSizeCounter
          return false;
        }
      }

      @Override
      public String description() {
        return "Expected region size to remain same below a specified limit but actual region size does not remain same or exceeded the specified limit "
            + sameRegionSizeCounter + " :regionSize " + previousSize;
      }
    };
    await().untilAsserted(wc);
  }

  public static String getRegionFullPath(String regionName) {
    final Region<?, ?> r = cache.getRegion(SEPARATOR + regionName);
    return r.getFullPath();
  }

  public static Integer getRegionSize(String regionName) {
    final Region<?, ?> r = cache.getRegion(SEPARATOR + regionName);
    return r.keySet().size();
  }

  public static void validateRegionContents(String regionName, final Map keyValues) {
    final Region<?, ?> r = cache.getRegion(SEPARATOR + regionName);
    await().untilAsserted(() -> {
      var matchFlag = true;
      for (var key : keyValues.keySet()) {
        if (!r.get(key).equals(keyValues.get(key))) {
          logger.info("The values are for key " + "  " + key + " " + r.get(key) + " in the map "
              + keyValues.get(key));
          matchFlag = false;
        }
      }
      assertEquals("Expected region entries doesn't match", true, matchFlag);
    });
  }



  public static void doHeavyPuts(String regionName, int numPuts) {
    var r = cache.getRegion(SEPARATOR + regionName);
    // GatewaySender.DEFAULT_BATCH_SIZE * OBJECT_SIZE should be more than MAXIMUM_QUEUE_MEMORY
    // to guarantee overflow
    for (long i = 0; i < numPuts; i++) {
      r.put(i, new byte[1024 * 1024]);
    }
  }

  public static void addCacheListenerAndDestroyRegion(String regionName) {
    final Region<?, ?> region = cache.getRegion(SEPARATOR + regionName);
    var cl = new CacheListenerAdapter() {
      @Override
      public void afterCreate(EntryEvent event) {
        if ((Long) event.getKey() == 99) {
          region.destroyRegion();
        }
      }
    };
    region.getAttributesMutator().addCacheListener(cl);
  }

  public static Boolean killSender(String senderId) {
    final var exln = addIgnoredException("Could not connect");
    var exp =
        addIgnoredException(CacheClosedException.class.getName());
    var exp1 =
        addIgnoredException(ForceReattemptException.class.getName());
    try {
      var sender = (AbstractGatewaySender) getGatewaySender(senderId);
      if (sender.isPrimary()) {
        logger.info("Gateway sender is killed by a test");
        cache.getDistributedSystem().disconnect();
        return Boolean.TRUE;
      }
      return Boolean.FALSE;
    } finally {
      exp.remove();
      exp1.remove();
      exln.remove();
    }
  }

  public static void killSender() {
    logger.info("Gateway sender is going to be killed by a test");
    cache.close();
    cache.getDistributedSystem().disconnect();
    logger.info("Gateway sender is killed by a test");
  }

  public static void checkAllSiteMetaData(
      Map<Integer, Set<InetSocketAddress>> dsIdToLocatorAddresses, final int siteSizeToCheck) {
    var locatorsConfigured = Locator.getLocators();
    var locator = locatorsConfigured.get(0);
    await().untilAsserted(() -> {
      Map<Integer, Set<DistributionLocatorId>> allSiteMetaData =
          ((InternalLocator) locator).getLocatorMembershipListener().getAllLocatorsInfo();
      assertThat(allSiteMetaData.size()).isEqualTo(siteSizeToCheck);
      for (var entry : dsIdToLocatorAddresses.entrySet()) {
        var foundLocatorIds = allSiteMetaData.get(entry.getKey());
        var expectedLocators = entry.getValue();
        final var foundLocators = foundLocatorIds.stream()
            .map(distributionLocatorId -> new InetSocketAddress(
                distributionLocatorId.getHostnameForClients(), distributionLocatorId.getPort()))
            .collect(Collectors.toSet());
        assertEquals(expectedLocators, foundLocators);
      }
    });
  }

  public static Long checkAllSiteMetaDataFor3Sites(final Map<Integer, Set<String>> dsVsPort) {
    await()
        .untilAsserted(
            () -> assertEquals("System is not initialized", true, (getSystemStatic() != null)));
    var locatorsConfigured = Locator.getLocators();
    var locator = locatorsConfigured.get(0);
    var listener = ((InternalLocator) locator).getLocatorMembershipListener();
    if (listener == null) {
      fail(
          "No locator membership listener available. WAN is likely not enabled. Is this test in the WAN project?");
    }
    final Map<Integer, Set<DistributionLocatorId>> allSiteMetaData = listener.getAllLocatorsInfo();
    System.out.println("allSiteMetaData : " + allSiteMetaData);

    await().untilAsserted(() -> {
      assertEquals(true, (dsVsPort.size() == allSiteMetaData.size()));
      var completeFlag = true;
      for (var entry : dsVsPort.entrySet()) {
        var locators = allSiteMetaData.get(entry.getKey());
        for (var locatorInMetaData : entry.getValue()) {
          var locatorId = new DistributionLocatorId(locatorInMetaData);
          if (!locators.contains(locatorId)) {
            completeFlag = false;
            break;
          }
        }
        if (false == completeFlag) {
          break;
        }
      }
      assertEquals(
          "Expected site Metadata: " + dsVsPort + " but actual meta data: " + allSiteMetaData, true,
          completeFlag);
    });
    return System.currentTimeMillis();
  }

  public static void putRemoteSiteLocators(int remoteDsId, Set<String> remoteLocators) {
    var locatorsConfigured = Locator.getLocators();
    var locator = locatorsConfigured.get(0);
    if (remoteLocators != null) {
      // Add fake remote locators to the locators map
      ((InternalLocator) locator).getLocatorMembershipListener().getAllServerLocatorsInfo()
          .put(remoteDsId, remoteLocators);
    }
  }

  public static void checkLocatorsinSender(String senderId, InetSocketAddress locatorToWaitFor)
      throws InterruptedException {

    var sender = getGatewaySender(senderId);

    var callback =
        (MyLocatorCallback) ((AbstractGatewaySender) sender).getLocatorDiscoveryCallback();

    var discovered = callback.waitForDiscovery(locatorToWaitFor, MAX_WAIT);
    assertTrue("Waited " + MAX_WAIT + " for " + locatorToWaitFor
        + " to be discovered on client. List is now: " + callback.getDiscovered(), discovered);
  }

  public static void validateQueueContents(final String senderId, final int regionSize) {
    var exp1 =
        addIgnoredException(InterruptedException.class.getName());
    var exp2 =
        addIgnoredException(GatewaySenderException.class.getName());
    try {
      var sender = getGatewaySender(senderId);

      if (!sender.isParallel()) {
        final var queues = ((AbstractGatewaySender) sender).getQueues();
        await().untilAsserted(() -> {
          var size = 0;
          for (var q : queues) {
            size += q.size();
          }
          assertEquals("Expected queue entries: " + regionSize + " but actual entries: " + size,
              regionSize, size);
        });
      } else if (sender.isParallel()) {
        final RegionQueue regionQueue;
        regionQueue = ((AbstractGatewaySender) sender).getQueues().toArray(new RegionQueue[1])[0];
        await().untilAsserted(() -> assertEquals(
            "Expected queue entries: " + regionSize + " but actual entries: " + regionQueue.size(),
            regionSize, regionQueue.size()));
      }
    } finally {
      exp1.remove();
      exp2.remove();
    }

  }

  // Ensure that the sender's queue(s) have been closed.
  public static void validateQueueClosedForConcurrentSerialGatewaySender(final String senderId) {
    var sender = getGatewaySender(senderId);
    final Set<RegionQueue> regionQueue;
    if (sender instanceof AbstractGatewaySender) {
      regionQueue = ((AbstractGatewaySender) sender).getQueues();
    } else {
      regionQueue = null;
    }
    assertEquals(null, regionQueue);
  }

  public static void validateQueueContentsForConcurrentSerialGatewaySender(final String senderId,
      final int regionSize) {
    var sender = getGatewaySender(senderId);
    final Set<RegionQueue> regionQueue;
    if (!sender.isParallel()) {
      regionQueue = ((AbstractGatewaySender) sender).getQueues();
    } else {
      regionQueue = null;
    }
    await().untilAsserted(() -> {
      var size = 0;
      for (var q : regionQueue) {
        size += q.size();
      }
      assertEquals(true, regionSize == size);
    });
  }

  public static Integer getSecondaryQueueContentSize(final String senderId) {
    var sender = getGatewaySender(senderId);
    var abstractSender = (AbstractGatewaySender) sender;
    var size = abstractSender.getSecondaryEventQueueSize();
    return size;
  }

  public static String displayQueueContent(final RegionQueue queue) {
    if (queue instanceof ParallelGatewaySenderQueue) {
      var pgsq = (ParallelGatewaySenderQueue) queue;
      return pgsq.displayContent();
    } else if (queue instanceof ConcurrentParallelGatewaySenderQueue) {
      var pgsq = (ConcurrentParallelGatewaySenderQueue) queue;
      return pgsq.displayContent();
    }
    return null;
  }

  public static String displaySerialQueueContent(final AbstractGatewaySender sender) {
    var message = new StringBuilder();
    message.append("Is Primary: ").append(sender.isPrimary()).append(", ").append("Queue Size: ")
        .append(sender.getEventQueueSize());

    if (sender.getQueues() != null) {
      message.append(", ").append("Queue Count: ").append(sender.getQueues().size());
      Stream<Object> stream = sender.getQueues().stream()
          .map(regionQueue -> ((SerialGatewaySenderQueue) regionQueue).displayContent());

      var list = stream.collect(Collectors.toList());
      message.append(", ").append("Keys: ").append(list);
    }

    var abstractProcessor = sender.getEventProcessor();
    if (abstractProcessor == null) {
      message.append(", ").append("Null Event Processor: ");
    }
    if (!sender.isPrimary()) {
      message.append("\n").append("Unprocessed Events: ")
          .append(abstractProcessor.printUnprocessedEvents()).append("\n");
      message.append("\n").append("Unprocessed Tokens: ")
          .append(abstractProcessor.printUnprocessedTokens()).append("\n");
    }

    return message.toString();
  }

  public static Integer getQueueContentSize(final String senderId) {
    return getQueueContentSize(senderId, false);
  }

  public static Integer getQueueContentSize(final String senderId, boolean includeSecondary) {
    var sender = getGatewaySender(senderId);

    if (!sender.isParallel()) {
      // if sender is serial, the queues will be all primary or all secondary at one member
      final var queues = ((AbstractGatewaySender) sender).getQueues();
      var size = 0;
      for (var q : queues) {
        size += q.size();
      }
      return size;
    } else if (sender.isParallel()) {
      RegionQueue regionQueue = null;
      regionQueue = ((AbstractGatewaySender) sender).getQueues().toArray(new RegionQueue[1])[0];
      if (regionQueue instanceof ConcurrentParallelGatewaySenderQueue) {
        return ((ConcurrentParallelGatewaySenderQueue) regionQueue).localSize(includeSecondary);
      } else if (regionQueue instanceof ParallelGatewaySenderQueue) {
        return ((ParallelGatewaySenderQueue) regionQueue).localSize(includeSecondary);
      } else {
        fail("Not implemented yet");
      }
    }
    fail("Not yet implemented?");
    return 0;
  }

  public static void validateParallelSenderQueueBucketSize(final String senderId,
      final int bucketSize) {
    var sender = getGatewaySender(senderId);
    var regionQueue =
        ((AbstractGatewaySender) sender).getQueues().toArray(new RegionQueue[1])[0];
    var buckets = ((PartitionedRegion) regionQueue.getRegion()).getDataStore()
        .getAllLocalPrimaryBucketRegions();
    for (var bucket : buckets) {
      assertEquals(
          "Expected bucket entries for bucket " + bucket.getId() + " is different than actual.",
          bucketSize, bucket.keySet().size());
    }
  }

  public static void validateParallelSenderQueueAllBucketsDrained(final String senderId) {
    var exp =
        addIgnoredException(RegionDestroyedException.class.getName());
    var exp1 =
        addIgnoredException(ForceReattemptException.class.getName());
    try {
      var sender = getGatewaySender(senderId);
      final var abstractSender = (AbstractGatewaySender) sender;
      var queue = abstractSender.getEventProcessor().queue;
      await().untilAsserted(() -> {
        assertEquals("Expected events in all primary queues are drained but actual is "
            + abstractSender.getEventQueueSize() + ". Queue content is: "
            + displayQueueContent(queue), 0, abstractSender.getEventQueueSize());
      });
      assertEquals("Expected events in all primary queues after drain is 0", 0,
          abstractSender.getEventQueueSize());
      await().untilAsserted(() -> {
        assertEquals("Expected events in all secondary queues are drained but actual is "
            + abstractSender.getSecondaryEventQueueSize() + ". Queue content is: "
            + displayQueueContent(queue), 0, abstractSender.getSecondaryEventQueueSize());
      });
      assertEquals("Expected events in all secondary queues after drain is 0", 0,
          abstractSender.getSecondaryEventQueueSize());
    } finally {
      exp.remove();
      exp1.remove();
    }

  }

  public static Integer validateAfterAck(final String senderId) {
    var sender = getGatewaySender(senderId);

    final var filter =
        (MyGatewayEventFilter_AfterAck) sender.getGatewayEventFilters().get(0);
    return filter.getAckList().size();
  }

  public static int verifyAndGetEventsDispatchedByConcurrentDispatchers(final String senderId) {
    var sender = getGatewaySender(senderId);
    var cProc =
        (ConcurrentParallelGatewaySenderEventProcessor) ((AbstractGatewaySender) sender)
            .getEventProcessor();
    if (cProc == null) {
      return 0;
    }

    var totalDispatched = 0;
    for (var lProc : cProc.getProcessors()) {
      totalDispatched += lProc.getNumEventsDispatched();
    }
    assertTrue(totalDispatched > 0);
    return totalDispatched;
  }

  public static Long getNumberOfEntriesOverflownToDisk(final String senderId) {
    var sender = getGatewaySender(senderId);

    long numEntries = 0;
    if (sender.isParallel()) {
      RegionQueue regionQueue;
      regionQueue = ((AbstractGatewaySender) sender).getQueues().toArray(new RegionQueue[1])[0];
      numEntries = ((ConcurrentParallelGatewaySenderQueue) regionQueue)
          .getNumEntriesOverflowOnDiskTestOnly();
    }
    return numEntries;
  }

  public static Long getNumberOfEntriesInVM(final String senderId) {
    var sender = getGatewaySender(senderId);
    RegionQueue regionQueue;
    long numEntries = 0;
    if (sender.isParallel()) {
      regionQueue = ((AbstractGatewaySender) sender).getQueues().toArray(new RegionQueue[1])[0];
      numEntries = ((ConcurrentParallelGatewaySenderQueue) regionQueue).getNumEntriesInVMTestOnly();
    }
    return numEntries;
  }

  public static int getNumOfPossibleDuplicateEvents(final String senderId) {
    var sender = getGatewaySender(senderId);
    RegionQueue regionQueue;
    var numEntries = 0;
    if (sender.isParallel()) {
      regionQueue = ((AbstractGatewaySender) sender).getQueues().toArray(new RegionQueue[1])[0];
      numEntries =
          ((ConcurrentParallelGatewaySenderQueue) regionQueue).getNumOfPossibleDuplicateEvents();
    }
    return numEntries;
  }

  public static void verifyTmpDroppedEventSize(String senderId, int size) {
    var sender = getGatewaySender(senderId);

    var ags = (AbstractGatewaySender) sender;
    await().untilAsserted(() -> assertEquals("Expected tmpDroppedEvents size: " + size
        + " but actual size: " + ags.getTmpDroppedEventSize(), size, ags.getTmpDroppedEventSize()));
  }

  /**
   * Checks that the bucketToTempQueueMap for a partitioned region
   * that holds events for buckets that are not available locally, is empty.
   */
  public static void validateEmptyBucketToTempQueueMap(String senderId) {
    var sender = getGatewaySender(senderId);

    var size = 0;
    var queues = ((AbstractGatewaySender) sender).getQueues();
    for (Object queue : queues) {
      var region =
          (PartitionedRegion) ((ConcurrentParallelGatewaySenderQueue) queue).getRegion();
      var buckets = region.getTotalNumberOfBuckets();
      for (var bucket = 0; bucket < buckets; bucket++) {
        var newQueue =
            ((ConcurrentParallelGatewaySenderQueue) queue).getBucketTmpQueue(bucket);
        if (newQueue != null) {
          size += newQueue.size();
        }
      }
    }

    final var finalSize = size;
    assertEquals("Expected elements in TempQueueMap: " + 0
        + " but actual size: " + finalSize, 0, finalSize);

  }

  private static GatewaySender getGatewaySender(String senderId) {
    var senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (var s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }
    return sender;
  }

  public static void verifyQueueSize(String senderId, int size) {
    var sender = getGatewaySender(senderId);

    if (!sender.isParallel()) {
      final var queues = ((AbstractGatewaySender) sender).getQueues();
      var queueSize = 0;
      for (var q : queues) {
        queueSize += q.size();
      }

      assertEquals("verifyQueueSize failed for sender " + senderId, size, queueSize);
    } else if (sender.isParallel()) {
      var regionQueue =
          ((AbstractGatewaySender) sender).getQueues().toArray(new RegionQueue[1])[0];
      assertEquals("verifyQueueSize failed for sender " + senderId, size, regionQueue.size());
    }
  }

  public static void verifyRegionQueueNotEmpty(String senderId) {
    var sender = getGatewaySender(senderId);

    if (!sender.isParallel()) {
      final var queues = ((AbstractGatewaySender) sender).getQueues();
      var queueSize = 0;
      for (var q : queues) {
        queueSize += q.size();
      }
      assertTrue(queues.size() > 0);
      assertTrue(queueSize > 0);
    } else if (sender.isParallel()) {
      var regionQueue =
          ((AbstractGatewaySender) sender).getQueues().toArray(new RegionQueue[1])[0];
      assertTrue(regionQueue.size() > 0);
    }
  }

  public static void waitForConcurrentSerialSenderQueueToDrain(String senderId) {
    var senders = cache.getGatewaySenders();
    var sender =
        senders.stream().filter(s -> s.getId().equals(senderId)).findFirst().get();

    await().untilAsserted(() -> {
      var queues = ((AbstractGatewaySender) sender).getQueues();
      for (var q : queues) {
        assertEquals(0, q.size());
      }
    });
  }

  /**
   * Test methods for sender operations
   *
   */
  public static void verifySenderPausedState(String senderId) {
    var sender = cache.getGatewaySender(senderId);
    assertTrue(sender.isPaused());
  }

  public static void verifySenderResumedState(String senderId) {
    var sender = cache.getGatewaySender(senderId);
    assertFalse(sender.isPaused());
    assertTrue(sender.isRunning());
  }

  public static void verifySenderStoppedState(String senderId) {
    var sender = cache.getGatewaySender(senderId);
    assertFalse(sender.isRunning());
  }

  public static void verifySenderRunningState(String senderId) {
    var sender = cache.getGatewaySender(senderId);
    assertTrue(sender.isRunning());
  }

  public static void verifySenderConnectedState(String senderId, boolean shouldBeConnected) {
    var sender = (AbstractGatewaySender) cache.getGatewaySender(senderId);
    if (shouldBeConnected) {
      assertThat(sender.getEventProcessor().getDispatcher().isConnectedToRemote()).isTrue();
    } else {
      assertThat(sender.getEventProcessor().getDispatcher().isConnectedToRemote()).isFalse();
    }
  }

  public static void verifyPool(String senderId, boolean poolShouldExist,
      int expectedPoolLocatorsSize) {
    var sender = (AbstractGatewaySender) cache.getGatewaySender(senderId);
    var pool = sender.getProxy();
    if (poolShouldExist) {
      assertEquals(expectedPoolLocatorsSize, pool.getLocators().size());
    } else {
      assertNull(pool);
    }
  }

  public static void removeSenderFromTheRegion(String senderId, String regionName) {
    Region<?, ?> region = cache.getRegion(regionName);
    region.getAttributesMutator().removeGatewaySenderId(senderId);
  }

  public static void destroySender(String senderId) {
    var sender = getGatewaySender(senderId);
    sender.destroy();
  }

  public static void verifySenderDestroyed(String senderId, boolean isParallel) {
    var sender = (AbstractGatewaySender) getGatewaySender(senderId);
    assertNull(sender);

    String queueRegionNameSuffix = null;
    if (isParallel) {
      queueRegionNameSuffix = ParallelGatewaySenderQueue.QSTRING;
    } else {
      queueRegionNameSuffix = "_SERIAL_GATEWAY_SENDER_QUEUE";
    }

    var allRegions = ((GemFireCacheImpl) cache).getAllRegions();
    for (var region : allRegions) {
      if (region.getName().indexOf(senderId + queueRegionNameSuffix) != -1) {
        fail("Region underlying the sender is not destroyed.");
      }
    }
  }

  public static void destroyAsyncEventQueue(String id) {
    var aeq = (AsyncEventQueueImpl) cache.getAsyncEventQueue(id);
    aeq.destroy();
  }

  protected static void verifyListenerEvents(final long expectedNumEvents) {
    await()
        .until(() -> listener1.getNumEvents() == expectedNumEvents);
  }

  protected Integer[] createLNAndNYLocators() {
    var lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    var nyPort = vm1.invoke(() -> createFirstRemoteLocator(2, lnPort));
    return new Integer[] {lnPort, nyPort};
  }

  protected void validateRegionSizes(String regionName, int expectedRegionSize, VM... vms) {
    for (var vm : vms) {
      vm.invoke(() -> validateRegionSize(regionName, expectedRegionSize));
    }
  }

  public static class MyLocatorCallback extends LocatorDiscoveryCallbackAdapter {

    private final Set discoveredLocators = new HashSet();

    private final Set removedLocators = new HashSet();

    @Override
    public synchronized void locatorsDiscovered(List locators) {
      discoveredLocators.addAll(locators);
      notifyAll();
    }

    @Override
    public synchronized void locatorsRemoved(List locators) {
      removedLocators.addAll(locators);
      notifyAll();
    }

    public boolean waitForDiscovery(InetSocketAddress locator, long time)
        throws InterruptedException {
      return waitFor(discoveredLocators, locator, time);
    }


    private synchronized boolean waitFor(Set set, InetSocketAddress locator, long time)
        throws InterruptedException {
      var remaining = time;
      var endTime = System.currentTimeMillis() + time;
      while (!set.contains(locator) && remaining >= 0) {
        wait(remaining);
        remaining = endTime - System.currentTimeMillis();
      }
      return set.contains(locator);
    }

    public synchronized Set getDiscovered() {
      return new HashSet(discoveredLocators);
    }

  }

  protected static class PutTask implements Callable {
    private final Region region;

    private final AtomicInteger key_value;

    private final int numPuts;

    public PutTask(Region region, AtomicInteger key_value, int numPuts) {
      this.region = region;
      this.key_value = key_value;
      this.numPuts = numPuts;
    }

    @Override
    public Object call() throws Exception {
      while (true) {
        var key = key_value.incrementAndGet();
        if (key < numPuts) {
          region.put(key, key);
        } else {
          break;
        }
      }
      return null;
    }
  }

  public static class MyGatewayEventFilter implements GatewayEventFilter, Serializable {

    String Id = "MyGatewayEventFilter";

    boolean beforeEnqueueInvoked;
    boolean beforeTransmitInvoked;
    boolean afterAckInvoked;

    public MyGatewayEventFilter() {}

    @Override
    public boolean beforeEnqueue(GatewayQueueEvent event) {
      beforeEnqueueInvoked = true;
      return !((Long) event.getKey() >= 500 && (Long) event.getKey() < 600);
    }

    @Override
    public boolean beforeTransmit(GatewayQueueEvent event) {
      beforeTransmitInvoked = true;
      return !((Long) event.getKey() >= 600 && (Long) event.getKey() < 700);
    }

    @Override
    public void close() {
      // TODO Auto-generated method stub

    }

    public String toString() {
      return Id;
    }

    @Override
    public void afterAcknowledgement(GatewayQueueEvent event) {
      afterAckInvoked = true;
      // TODO Auto-generated method stub
    }

    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof MyGatewayEventFilter)) {
        return false;
      }
      var filter = (MyGatewayEventFilter) obj;
      return Id.equals(filter.Id);
    }
  }

  public static class MyGatewayEventFilter_AfterAck implements GatewayEventFilter, Serializable {

    String Id = "MyGatewayEventFilter_AfterAck";

    ConcurrentSkipListSet<Long> ackList = new ConcurrentSkipListSet<>();

    public MyGatewayEventFilter_AfterAck() {}

    @Override
    public boolean beforeEnqueue(GatewayQueueEvent event) {
      return true;
    }

    @Override
    public boolean beforeTransmit(GatewayQueueEvent event) {
      return true;
    }

    @Override
    public void close() {
      // TODO Auto-generated method stub

    }

    public String toString() {
      return Id;
    }

    @Override
    public void afterAcknowledgement(GatewayQueueEvent event) {
      ackList.add((Long) event.getKey());
    }

    public Set getAckList() {
      return ackList;
    }

    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof MyGatewayEventFilter)) {
        return false;
      }
      var filter = (MyGatewayEventFilter) obj;
      return Id.equals(filter.Id);
    }
  }

  public static class PDXGatewayEventFilter implements GatewayEventFilter, Serializable {

    String Id = "PDXGatewayEventFilter";

    public int beforeEnqueueInvoked;
    public int beforeTransmitInvoked;
    public int afterAckInvoked;

    public PDXGatewayEventFilter() {}

    @Override
    public boolean beforeEnqueue(GatewayQueueEvent event) {
      System.out.println("Invoked enqueue for " + event);
      beforeEnqueueInvoked++;
      return true;
    }

    @Override
    public boolean beforeTransmit(GatewayQueueEvent event) {
      System.out.println("Invoked transmit for " + event);
      beforeTransmitInvoked++;
      return true;
    }

    @Override
    public void close() {
      // TODO Auto-generated method stub

    }

    public String toString() {
      return Id;
    }

    @Override
    public void afterAcknowledgement(GatewayQueueEvent event) {
      System.out.println("Invoked afterAck for " + event);
      afterAckInvoked++;
      // TODO Auto-generated method stub
    }

    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof MyGatewayEventFilter)) {
        return false;
      }
      var filter = (MyGatewayEventFilter) obj;
      return Id.equals(filter.Id);
    }
  }

  @Override
  public final void preTearDown() throws Exception {
    cleanupVM();
    List<AsyncInvocation> invocations = new ArrayList<>();
    final var host = getHost(0);
    for (var i = 0; i < host.getVMCount(); i++) {
      invocations.add(host.getVM(i).invokeAsync(WANTestBase::cleanupVM));
    }
    for (var invocation : invocations) {
      invocation.await();
    }
  }

  public static void cleanupVM() throws IOException {
    if (Locator.hasLocator()) {
      Locator.getLocator().stop();
    }
    closeCache();
    JUnit4DistributedTestCase.cleanDiskDirs();
  }

  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
      cache = null;
    } else {
      var test = new WANTestBase();
      if (test.isConnectedToDS()) {
        test.getSystem().disconnect();
      }
    }
  }

  public static void deletePDXDir() throws IOException {
    var pdxDir = new File(CacheTestCase.getDiskDir(), "pdx");
    FileUtils.deleteDirectory(pdxDir);
  }

  public static void shutdownLocator() {
    var test = new WANTestBase();
    test.getSystem().disconnect();
  }

  public static void printEventListenerMap() {
    ((MyGatewaySenderEventListener) eventListener1).printMap();
  }

  @Override
  public Properties getDistributedSystemProperties() {
    // For now all WANTestBase tests allocate off-heap memory even though
    // many of them never use it.
    // The problem is that WANTestBase has static methods that create instances
    // of WANTestBase (instead of instances of the subclass). So we can't override
    // this method so that only the off-heap subclasses allocate off heap memory.
    var props = new Properties();
    props.setProperty(OFF_HEAP_MEMORY_SIZE, "200m");

    return props;
  }

  /**
   * Returns true if the test should create off-heap regions. OffHeap tests should over-ride this
   * method and return false.
   */
  public boolean isOffHeap() {
    return false;
  }

  /**
   * Checks whether a Async Queue MBean is created or not
   *
   * @param vm reference to VM
   */
  @SuppressWarnings("serial")
  public static void checkAsyncQueueMBean(final VM vm, final boolean shouldExist) {
    var checkAsyncQueueMBean =
        new SerializableRunnable("Check Async Queue MBean") {
          @Override
          public void run() {
            var service = ManagementService.getManagementService(cache);
            var bean = service.getLocalAsyncEventQueueMXBean("pn");
            if (shouldExist) {
              assertNotNull(bean);
            } else {
              assertNull(bean);
            }
          }
        };
    vm.invoke(checkAsyncQueueMBean);
  }

  /**
   * Checks Proxy GatewayReceiver
   *
   * @param vm reference to VM
   */
  @SuppressWarnings("serial")
  public static void checkProxyReceiver(final VM vm, final DistributedMember senderMember) {
    var checkProxySender = new SerializableRunnable("Check Proxy Receiver") {
      @Override
      public void run() {
        var service = ManagementService.getManagementService(cache);
        GatewayReceiverMXBean bean = null;
        try {
          bean = MBeanUtil.getGatewayReceiverMbeanProxy(senderMember);
        } catch (Exception e) {
          fail("Could not obtain Sender Proxy in desired time " + e);
        }
        assertNotNull(bean);
        final var receiverMBeanName = service.getGatewayReceiverMBeanName(senderMember);
        try {
          MBeanUtil.printBeanDetails(receiverMBeanName);
        } catch (Exception e) {
          fail("Error while Printing Bean Details " + e);
        }

      }
    };
    vm.invoke(checkProxySender);
  }

  /**
   * Checks Proxy GatewaySender
   *
   * @param vm reference to VM
   */
  @SuppressWarnings("serial")
  public static void checkProxySender(final VM vm, final DistributedMember senderMember) {
    var checkProxySender = new SerializableRunnable("Check Proxy Sender") {
      @Override
      public void run() {
        var service = ManagementService.getManagementService(cache);
        GatewaySenderMXBean bean = null;
        try {
          bean = MBeanUtil.getGatewaySenderMbeanProxy(senderMember, "pn");
        } catch (Exception e) {
          fail("Could not obtain Sender Proxy in desired time " + e);
        }
        assertNotNull(bean);
        final var senderMBeanName = service.getGatewaySenderMBeanName(senderMember, "pn");
        try {
          MBeanUtil.printBeanDetails(senderMBeanName);
        } catch (Exception e) {
          fail("Error while Printing Bean Details " + e);
        }

        if (service.isManager()) {
          var dsBean = service.getDistributedSystemMXBean();
          await().untilAsserted(() -> {
            var dsMap = dsBean.viewRemoteClusterStatus();
            dsMap.entrySet().stream()
                .forEach(entry -> assertTrue("Should be true " + entry.getKey(), entry.getValue()));
          });
        }

      }
    };
    vm.invoke(checkProxySender);
  }

  /**
   * Checks whether a GatewayReceiverMBean is created or not
   *
   * @param vm reference to VM
   */
  @SuppressWarnings("serial")
  public static void checkReceiverMBean(final VM vm) {
    var checkMBean = new SerializableRunnable("Check Receiver MBean") {
      @Override
      public void run() {
        var service = ManagementService.getManagementService(cache);
        var bean = service.getLocalGatewayReceiverMXBean();
        assertNotNull(bean);
      }
    };
    vm.invoke(checkMBean);
  }

  @SuppressWarnings("serial")
  public static void checkReceiverNavigationAPIS(final VM vm,
      final DistributedMember receiverMember) {
    var checkNavigationAPIS =
        new SerializableRunnable("Check Receiver Navigation APIs") {
          @Override
          public void run() {
            var service = ManagementService.getManagementService(cache);
            var bean = service.getDistributedSystemMXBean();
            var expectedName = service.getGatewayReceiverMBeanName(receiverMember);
            try {
              await("wait for member to be added to DistributedSystemBridge membership list")
                  .untilAsserted(
                      () -> assertThat(bean.fetchGatewayReceiverObjectName(receiverMember.getId()))
                          .isNotNull());
              var actualName = bean.fetchGatewayReceiverObjectName(receiverMember.getId());
              assertEquals(expectedName, actualName);
            } catch (Exception e) {
              fail("Receiver Navigation Failed " + e);
            }

            assertEquals(1, bean.listGatewayReceiverObjectNames().length);

          }
        };
    vm.invoke(checkNavigationAPIS);
  }

  /**
   * Checks whether a GatewayReceiverMBean is created or not
   *
   * @param vm reference to VM
   */
  @SuppressWarnings("serial")
  public static void checkSenderMBean(final VM vm, final String regionPath, boolean connected) {
    var checkMBean = new SerializableRunnable("Check Sender MBean") {
      @Override
      public void run() {
        var service = ManagementService.getManagementService(cache);

        var bean = service.getLocalGatewaySenderMXBean("pn");
        assertNotNull(bean);
        await()
            .untilAsserted(() -> assertEquals(connected, bean.isConnected()));

        var regionBeanName = service.getRegionMBeanName(
            cache.getDistributedSystem().getDistributedMember(), SEPARATOR + regionPath);
        var rBean = service.getMBeanInstance(regionBeanName, RegionMXBean.class);
        assertTrue(rBean.isGatewayEnabled());


      }
    };
    vm.invoke(checkMBean);
  }

  @SuppressWarnings("serial")
  public static void checkSenderNavigationAPIS(final VM vm, final DistributedMember senderMember) {
    var checkNavigationAPIS =
        new SerializableRunnable("Check Sender Navigation APIs") {
          @Override
          public void run() {
            var service = ManagementService.getManagementService(cache);
            var bean = service.getDistributedSystemMXBean();
            var expectedName = service.getGatewaySenderMBeanName(senderMember, "pn");
            try {
              var actualName = bean.fetchGatewaySenderObjectName(senderMember.getId(), "pn");
              assertEquals(expectedName, actualName);
            } catch (Exception e) {
              fail("Sender Navigation Failed " + e);
            }

            assertEquals(2, bean.listGatewaySenderObjectNames().length);
            try {
              assertEquals(1, bean.listGatewaySenderObjectNames(senderMember.getId()).length);
            } catch (Exception e) {
              fail("Sender Navigation Failed " + e);
            }

          }
        };
    vm.invoke(checkNavigationAPIS);
  }

  /**
   * start a gateway sender
   *
   * @param vm reference to VM
   */
  @SuppressWarnings("serial")
  public static void startGatewaySender(final VM vm) {
    var stopGatewaySender = new SerializableRunnable("Start Gateway Sender") {
      @Override
      public void run() {
        var service = ManagementService.getManagementService(cache);
        var bean = service.getLocalGatewaySenderMXBean("pn");
        bean.start();
        assertTrue(bean.isRunning());
      }
    };
    vm.invoke(stopGatewaySender);
  }

  /**
   * stops a gateway sender
   *
   * @param vm reference to VM
   */
  @SuppressWarnings("serial")
  public static void stopGatewaySender(final VM vm) {
    var stopGatewaySender = new SerializableRunnable("Stop Gateway Sender") {
      @Override
      public void run() {
        var service = ManagementService.getManagementService(cache);
        var bean = service.getLocalGatewaySenderMXBean("pn");
        bean.stop();
        assertFalse(bean.isRunning());
      }
    };
    vm.invoke(stopGatewaySender);
  }

  /**
   * Checks Proxy Async Queue
   *
   * @param vm reference to VM
   */
  @SuppressWarnings("serial")
  public static void checkProxyAsyncQueue(final VM vm, final DistributedMember senderMember,
      final boolean shouldExist) {
    var checkProxyAsyncQueue =
        new SerializableRunnable("Check Proxy Async Queue") {
          @Override
          public void run() {
            var service =
                (SystemManagementService) ManagementService.getManagementService(cache);
            final var queueMBeanName =
                service.getAsyncEventQueueMBeanName(senderMember, "pn");
            AsyncEventQueueMXBean bean = null;
            if (shouldExist) {
              // Verify the MBean proxy exists
              try {
                bean = MBeanUtil.getAsyncEventQueueMBeanProxy(senderMember, "pn");
              } catch (Exception e) {
                fail("Could not obtain Sender Proxy in desired time " + e);
              }
              assertNotNull(bean);

              try {
                MBeanUtil.printBeanDetails(queueMBeanName);
              } catch (Exception e) {
                fail("Error while Printing Bean Details " + e);
              }
            } else {
              // Verify the MBean proxy doesn't exist
              bean = service.getMBeanProxy(queueMBeanName, AsyncEventQueueMXBean.class);
              assertNull(bean);
            }
          }
        };
    vm.invoke(checkProxyAsyncQueue);
  }

  public static DistributedMember getMember() {
    return ((GemFireCacheImpl) cache).getMyId();
  }

  public static ManagementService getManagementService() {
    return ManagementService.getManagementService(cache);
  }

  /**
   * Checks Proxy GatewaySender
   *
   * @param vm reference to VM
   */
  @SuppressWarnings("serial")
  public static void checkRemoteClusterStatus(final VM vm, final DistributedMember senderMember) {
    var checkProxySender = new SerializableRunnable("DS Map Size") {
      @Override
      public void run() {
        await().untilAsserted(() -> {
          final var service = ManagementService.getManagementService(cache);
          final var dsBean = service.getDistributedSystemMXBean();
          assertEquals(
              "Failed while waiting for getDistributedSystemMXBean to complete and get results",
              true, dsBean != null);
        });
        var service = ManagementService.getManagementService(cache);
        final var dsBean = service.getDistributedSystemMXBean();
        var dsMap = dsBean.viewRemoteClusterStatus();
        logger.info("Ds Map is: " + dsMap.size());
        assertEquals(true, dsMap.size() > 0);
      }
    };
    vm.invoke(checkProxySender);
  }


}
