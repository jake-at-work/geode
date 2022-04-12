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
package org.apache.geode.cache30;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.ROLES;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;
import org.xml.sax.SAXException;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CacheXmlException;
import org.apache.geode.cache.CustomExpiry;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.DiskWriteAttributesFactory;
import org.apache.geode.cache.DynamicRegionFactory;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.FixedPartitionAttributes;
import org.apache.geode.cache.InterestPolicy;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.LossAction;
import org.apache.geode.cache.MembershipAttributes;
import org.apache.geode.cache.MirrorType;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Region.Entry;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.ResumptionAction;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.SubscriptionAttributes;
import org.apache.geode.cache.TransactionListener;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.server.ServerLoad;
import org.apache.geode.cache.server.ServerLoadProbeAdapter;
import org.apache.geode.cache.server.ServerMetrics;
import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.cache.util.TransactionListenerAdapter;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.InternalInstantiator;
import org.apache.geode.internal.cache.DiskWriteAttributesImpl;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.FixedPartitionAttributesImpl;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PoolFactoryImpl;
import org.apache.geode.internal.cache.functions.TestFunction;
import org.apache.geode.internal.cache.partitioned.fixed.QuarterPartitionResolver;
import org.apache.geode.internal.cache.persistence.DefaultDiskDirs;
import org.apache.geode.internal.cache.xmlcache.CacheCreation;
import org.apache.geode.internal.cache.xmlcache.CacheTransactionManagerCreation;
import org.apache.geode.internal.cache.xmlcache.CacheXml;
import org.apache.geode.internal.cache.xmlcache.CacheXmlGenerator;
import org.apache.geode.internal.cache.xmlcache.CacheXmlParser;
import org.apache.geode.internal.cache.xmlcache.ClientCacheCreation;
import org.apache.geode.internal.cache.xmlcache.Declarable2;
import org.apache.geode.internal.cache.xmlcache.FunctionServiceCreation;
import org.apache.geode.internal.cache.xmlcache.RegionAttributesCreation;
import org.apache.geode.internal.cache.xmlcache.RegionCreation;
import org.apache.geode.internal.cache.xmlcache.ResourceManagerCreation;
import org.apache.geode.internal.cache.xmlcache.SerializerCreation;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.SerializableCallable;

/**
 * Tests 7.0 cache.xml feature : Fixed Partitioning.
 *
 * @since GemFire 6.6
 */
public abstract class CacheXml66DUnitTest extends CacheXmlTestCase {

  @Override
  protected abstract String getGemFireVersion();

  private static final String ALIAS1;

  private static final String ALIAS2;

  static {
    var tmp_alias1 = "localhost";
    var tmp_alias2 = "localhost";
    ALIAS1 = tmp_alias1;
    ALIAS2 = tmp_alias2;
  }

  private static InetSocketAddress createINSA(String host, int port) {
    try {
      var hostAddr = InetAddress.getByName(host);
      return new InetSocketAddress(hostAddr, port);
    } catch (UnknownHostException cause) {
      throw new IllegalArgumentException("Unknown host " + host, cause);
    }
  }

  /**
   * test for checking default value of PR_Single_Hop feature. Test for checking default value of
   * multiuser-authentication attribute.
   */
  @Test
  public void testDefaultConnectionPool() throws Exception {
    getSystem();
    var cache = new CacheCreation();
    var f = cache.createPoolFactory();
    f.addLocator(ALIAS2, 3777);
    f.create("mypool");
    var attrs = new RegionAttributesCreation(cache);
    attrs.setPoolName("mypool");
    cache.createVMRegion("rootNORMAL", attrs);
    testXml(cache);
    Cache c = getCache();
    assertNotNull(c);
    Region r = c.getRegion("rootNORMAL");
    assertNotNull(r);
    assertEquals("mypool", r.getAttributes().getPoolName());
    var cp = PoolManager.find("mypool");
    assertNotNull(cp);
    assertEquals(1, cp.getLocators().size());
    assertEquals(0, cp.getServers().size());
    assertEquals(createINSA(ALIAS2, 3777), cp.getLocators().get(0));
    assertEquals(PoolFactory.DEFAULT_FREE_CONNECTION_TIMEOUT, cp.getFreeConnectionTimeout());
    assertEquals(PoolFactory.DEFAULT_SERVER_CONNECTION_TIMEOUT, cp.getServerConnectionTimeout());
    assertEquals(PoolFactory.DEFAULT_LOAD_CONDITIONING_INTERVAL, cp.getLoadConditioningInterval());
    assertEquals(PoolFactory.DEFAULT_SOCKET_BUFFER_SIZE, cp.getSocketBufferSize());
    assertEquals(PoolFactory.DEFAULT_THREAD_LOCAL_CONNECTIONS, cp.getThreadLocalConnections());
    assertEquals(PoolFactory.DEFAULT_READ_TIMEOUT, cp.getReadTimeout());
    assertEquals(PoolFactory.DEFAULT_MIN_CONNECTIONS, cp.getMinConnections());
    assertEquals(PoolFactory.DEFAULT_MAX_CONNECTIONS, cp.getMaxConnections());
    assertEquals(PoolFactory.DEFAULT_RETRY_ATTEMPTS, cp.getRetryAttempts());
    assertEquals(PoolFactory.DEFAULT_IDLE_TIMEOUT, cp.getIdleTimeout());
    assertEquals(PoolFactory.DEFAULT_PING_INTERVAL, cp.getPingInterval());
    assertEquals(PoolFactory.DEFAULT_STATISTIC_INTERVAL, cp.getStatisticInterval());
    assertEquals(PoolFactory.DEFAULT_SERVER_GROUP, cp.getServerGroup());
    assertEquals(PoolFactory.DEFAULT_SUBSCRIPTION_ENABLED, cp.getSubscriptionEnabled());
    assertEquals(PoolFactory.DEFAULT_PR_SINGLE_HOP_ENABLED, cp.getPRSingleHopEnabled());
    assertEquals(PoolFactory.DEFAULT_SUBSCRIPTION_REDUNDANCY, cp.getSubscriptionRedundancy());
    assertEquals(PoolFactory.DEFAULT_SUBSCRIPTION_MESSAGE_TRACKING_TIMEOUT,
        cp.getSubscriptionMessageTrackingTimeout());
    assertEquals(PoolFactory.DEFAULT_SUBSCRIPTION_ACK_INTERVAL, cp.getSubscriptionAckInterval());
    assertEquals(PoolFactory.DEFAULT_MULTIUSER_AUTHENTICATION, cp.getMultiuserAuthentication());
  }

  @Test
  public void testDiskStore() throws Exception {
    var cache = new CacheCreation();
    var dsf = cache.createDiskStoreFactory();
    var dirs1 = new File[] {new File("").getAbsoluteFile()};
    var ds1 =
        dsf.setAllowForceCompaction(true).setAutoCompact(true).setCompactionThreshold(100)
            .setMaxOplogSize(2).setTimeInterval(10).setWriteBufferSize(15).setQueueSize(12)
            .setDiskDirsAndSizes(dirs1, new int[] {1024 * 20}).create(getUniqueName() + 1);
    var dirs2 = new File[] {new File("").getAbsoluteFile()};
    var ds2 =
        dsf.setAllowForceCompaction(false).setAutoCompact(false).setCompactionThreshold(99)
            .setMaxOplogSize(1).setTimeInterval(9).setWriteBufferSize(14).setQueueSize(11)
            .setDiskDirsAndSizes(dirs2, new int[] {1024 * 40}).create(getUniqueName() + 2);

    var attrs = new RegionAttributesCreation(cache);
    attrs.setScope(Scope.DISTRIBUTED_ACK);
    attrs.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    attrs.setDiskStoreName(getUniqueName() + 1);
    attrs.setDiskSynchronous(true);
    var root = (RegionCreation) cache.createRegion("root", attrs);
    {
      attrs = new RegionAttributesCreation(cache);
      attrs.setScope(Scope.DISTRIBUTED_ACK);
      attrs.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
      attrs.setDiskStoreName(getUniqueName() + 2);
      var subwithdiskstore = root.createSubregion("subwithdiskstore", attrs);
    }

    {
      attrs = new RegionAttributesCreation(cache);
      attrs.setScope(Scope.DISTRIBUTED_ACK);
      attrs.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
      var subwithdefaultdiskstore = root.createSubregion("subwithdefaultdiskstore", attrs);
    }

    testXml(cache);
  }

  /**
   * test for enabling PRsingleHop feature. Test for enabling multiuser-authentication attribute.
   */
  @Test
  public void testExplicitConnectionPool() throws Exception {
    getSystem();
    var cache = new CacheCreation();
    var f = cache.createPoolFactory();
    f.addServer(ALIAS2, 3777).addServer(ALIAS1, 3888);
    f.setFreeConnectionTimeout(12345).setServerConnectionTimeout(111)
        .setLoadConditioningInterval(12345).setSocketBufferSize(12345)
        .setThreadLocalConnections(true).setPRSingleHopEnabled(true).setReadTimeout(12345)
        .setMinConnections(12346).setMaxConnections(12347).setRetryAttempts(12348)
        .setIdleTimeout(12349).setPingInterval(12350).setStatisticInterval(12351)
        .setServerGroup("mygroup")
        // commented this out until queues are implemented
        // .setQueueEnabled(true)
        .setSubscriptionRedundancy(12345).setSubscriptionMessageTrackingTimeout(12345)
        .setSubscriptionAckInterval(333).setMultiuserAuthentication(true);
    f.create("mypool");
    var attrs = new RegionAttributesCreation(cache);
    attrs.setPoolName("mypool");
    attrs.setDataPolicy(DataPolicy.EMPTY); // required for multiuser mode
    cache.createVMRegion("rootNORMAL", attrs);
    addIgnoredException("Connection refused: connect");
    testXml(cache);
    Cache c = getCache();
    assertNotNull(c);
    Region r = c.getRegion("rootNORMAL");
    assertNotNull(r);
    assertEquals("mypool", r.getAttributes().getPoolName());
    var cp = PoolManager.find("mypool");
    assertNotNull(cp);
    assertEquals(0, cp.getLocators().size());
    assertEquals(2, cp.getServers().size());
    assertEquals(createINSA(ALIAS2, 3777), cp.getServers().get(0));
    assertEquals(createINSA(ALIAS1, 3888), cp.getServers().get(1));
    assertEquals(12345, cp.getFreeConnectionTimeout());
    assertEquals(111, cp.getServerConnectionTimeout());
    assertEquals(12345, cp.getLoadConditioningInterval());
    assertEquals(12345, cp.getSocketBufferSize());
    assertEquals(true, cp.getThreadLocalConnections());
    assertEquals(true, cp.getPRSingleHopEnabled());
    assertEquals(12345, cp.getReadTimeout());
    assertEquals(12346, cp.getMinConnections());
    assertEquals(12347, cp.getMaxConnections());
    assertEquals(12348, cp.getRetryAttempts());
    assertEquals(12349, cp.getIdleTimeout());
    assertEquals(12350, cp.getPingInterval());
    assertEquals(12351, cp.getStatisticInterval());
    assertEquals("mygroup", cp.getServerGroup());
    // TODO: commented this out until queues are implemented
    // assertIndexDetailsEquals(true, cp.getQueueEnabled());
    assertEquals(12345, cp.getSubscriptionRedundancy());
    assertEquals(12345, cp.getSubscriptionMessageTrackingTimeout());
    assertEquals(333, cp.getSubscriptionAckInterval());
    assertEquals(true, cp.getMultiuserAuthentication());
  }

  @Test
  public void testDiskStoreValidation() throws Exception {
    var cache = new CacheCreation();
    var dsf = cache.createDiskStoreFactory();
    var ds1 = dsf.create(getUniqueName());

    var attrs = new RegionAttributesCreation(cache);
    attrs.setScope(Scope.DISTRIBUTED_ACK);
    attrs.setDataPolicy(DataPolicy.REPLICATE);
    attrs.setDiskStoreName(getUniqueName());
    RegionCreation root;
    try {
      root = (RegionCreation) cache.createRegion("root", attrs);
      fail("Expected IllegalStateException to be thrown");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains(
          "Only regions with persistence or overflow to disk can specify DiskStore"));
    }

    var ea =
        EvictionAttributes.createLRUEntryAttributes(1000, EvictionAction.OVERFLOW_TO_DISK);
    attrs.setEvictionAttributes(ea);
    root = (RegionCreation) cache.createRegion("root", attrs);

    var dir = new File("testDiskStoreValidation");
    dir.mkdir();
    dir.deleteOnExit();

    var dirs2 = new File[] {dir, new File("").getAbsoluteFile()};
    try {
      var factory = new AttributesFactory();
      factory.setDiskDirs(dirs2);
      factory.setDiskStoreName(getUniqueName());
      var ra = factory.create();
      fail("Expected IllegalStateException to be thrown");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage()
          .contains(String.format("Deprecated API %s cannot be used with DiskStore %s",
              "setDiskDirs or setDiskWriteAttributes", getUniqueName())));
    }

    try {
      var factory = new AttributesFactory();
      factory.setDiskStoreName(getUniqueName());
      factory.setDiskDirs(dirs2);
      var ra = factory.create();
      fail("Expected IllegalStateException to be thrown");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage()
          .contains(String.format("Deprecated API %s cannot be used with DiskStore %s",
              "setDiskDirs", getUniqueName())));
    }

    testXml(cache);
  }

  @Test
  public void testDiskStoreFactory() throws Exception {
    var cache = new CacheCreation();
    var dsf = cache.createDiskStoreFactory();

    dsf.setDiskDirs(new File[] {new File("non_exist_dir")});
    var ds1 = dsf.create(getUniqueName());

    dsf.setDiskDirs(new File[] {new File(".")});
    ds1 = dsf.create(getUniqueName());

    var attrs = new RegionAttributesCreation(cache);
    attrs.setScope(Scope.DISTRIBUTED_ACK);
    attrs.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    attrs.setDiskStoreName(getUniqueName());
    var factory = new AttributesFactory(attrs);
    var ra = factory.create();

    var root = (RegionCreation) cache.createRegion("root", ra);

    factory = new AttributesFactory();
    factory.setDiskStoreName(getUniqueName());
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);

    ra = factory.create();

    var root2 = (RegionCreation) cache.createRegion("root2", ra);

    factory = new AttributesFactory();
    factory.setDiskStoreName(null);
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);

    ra = factory.create();

    var root3 = (RegionCreation) cache.createRegion("root3", ra);

    testXml(cache);
  }

  @Test
  public void testRedefineOfDefaultDiskStore() throws Exception {
    var cache = new CacheCreation();
    var dsf = cache.createDiskStoreFactory();
    dsf.setAutoCompact(!DiskStoreFactory.DEFAULT_AUTO_COMPACT);
    var ds1 = dsf.create(DiskStoreFactory.DEFAULT_DISK_STORE_NAME);

    var attrs = new RegionAttributesCreation(cache);
    attrs.setScope(Scope.DISTRIBUTED_ACK);
    attrs.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    var factory = new AttributesFactory(attrs);
    var ra = factory.create();

    var root = (RegionCreation) cache.createRegion("root", ra);

    testXml(cache);

    Cache c = getCache();
    assertNotNull(c);

    var ds2 = c.findDiskStore(DiskStoreFactory.DEFAULT_DISK_STORE_NAME);
    assertNotNull(ds2);
    assertEquals(ds1.getAutoCompact(), ds2.getAutoCompact());
  }

  /**
   * Make sure you can create a persistent partitioned region from xml.
   */
  @Test
  public void testPersistentPartition() throws Exception {
    var cache = new CacheCreation();
    var attrs = new RegionAttributesCreation(cache);
    attrs.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);

    cache.createRegion("parRoot", attrs);

    var r = cache.getRegion("parRoot");
    assertEquals(DataPolicy.PERSISTENT_PARTITION, r.getAttributes().getDataPolicy());

    testXml(cache);

    Cache c = getCache();
    assertNotNull(c);

    Region region = c.getRegion("parRoot");
    assertNotNull(region);

    assertEquals(DataPolicy.PERSISTENT_PARTITION, region.getAttributes().getDataPolicy());
    // since CacheTestCase remoteTearDown does not destroy PartitionedRegion
    region.localDestroyRegion();
  }

  @Test
  public void testBridgeAttributesRelatedToHAOverFlow65() throws Exception {
    var cache = new CacheCreation();
    cache.setMessageSyncInterval(3445);
    var bs = cache.addCacheServer();
    var csc = bs.getClientSubscriptionConfig();
    csc.setEvictionPolicy("entry");
    cache.getLogger().config("EvictionPolicy : " + csc.getEvictionPolicy());
    csc.setCapacity(501);
    cache.getLogger().config("EvictionCapacity : " + csc.getCapacity());
    var overflowDirectory = new File("overFlow");
    overflowDirectory.mkdirs();
    var dsf = cache.createDiskStoreFactory();
    var dirs1 = new File[] {overflowDirectory};
    var ds1 = dsf.setDiskDirs(dirs1).create(getUniqueName());
    csc.setDiskStoreName(getUniqueName());
    try {
      csc.setOverflowDirectory("overFlow");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage()
          .contains(String.format("Deprecated API %s cannot be used with DiskStore %s",
              "setOverflowDirectory", getUniqueName())));
    }

    cache.getLogger().config("Eviction disk store : " + csc.getDiskStoreName());
    bs.setPort(AvailablePortHelper.getRandomAvailableTCPPort());
    var attrs = new RegionAttributesCreation(cache);
    attrs.setDataPolicy(DataPolicy.NORMAL);
    cache.createVMRegion("rootNORMAL", attrs);
    testXml(cache);
    Cache c = getCache();
    assertNotNull(c);
    var server = cache.getCacheServers().iterator().next();
    assertNotNull(server);
    var chaqf = server.getClientSubscriptionConfig();
    assertEquals("entry", chaqf.getEvictionPolicy());
    assertEquals(501, chaqf.getCapacity());
    var dsi = cache.findDiskStore(chaqf.getDiskStoreName());
    assertEquals("overFlow", dsi.getDiskDirs()[0].toString());
  }

  @Test
  public void testClientSubscriptionQueueUsingDefaultDS() throws Exception {
    var cache = new CacheCreation();
    cache.setMessageSyncInterval(3445);
    var bs = cache.addCacheServer();
    var csc = bs.getClientSubscriptionConfig();
    csc.setEvictionPolicy("entry");
    cache.getLogger().config("EvictionPolicy : " + csc.getEvictionPolicy());
    csc.setCapacity(501);
    // don't set diskstore or overflowdir
    cache.getLogger().config("EvictionCapacity : " + csc.getCapacity());
    cache.getLogger().config("Eviction disk store : " + csc.getDiskStoreName());
    bs.setPort(AvailablePortHelper.getRandomAvailableTCPPort());
    var attrs = new RegionAttributesCreation(cache);
    attrs.setDataPolicy(DataPolicy.NORMAL);
    cache.createVMRegion("rootNORMAL", attrs);
    testXml(cache);
    Cache c = getCache();
    assertNotNull(c);
    var server = cache.getCacheServers().iterator().next();
    assertNotNull(server);
    var chaqf = server.getClientSubscriptionConfig();
    assertEquals("entry", chaqf.getEvictionPolicy());
    assertEquals(501, chaqf.getCapacity());
    var defaultDiskDir = DefaultDiskDirs.getDefaultDiskDirs()[0].getAbsoluteFile();
    var lockFile =
        new File(defaultDiskDir, "DRLK_IF" + GemFireCacheImpl.getDefaultDiskStoreName() + ".lk");
    assertTrue(lockFile.exists());
  }

  /**
   * Tests that a region created with a named attributes set programmatically for delta propogation
   * has the correct attributes.
   */
  @Test
  public void testTransactionWriter() throws Exception {
    var creation = new CacheCreation();
    var ctmc = new CacheTransactionManagerCreation();
    ctmc.setWriter(new TestTransactionWriter());
    creation.addCacheTransactionManagerCreation(ctmc);
    testXml(creation);

    Cache c = getCache();
    assertTrue(c instanceof GemFireCacheImpl);
    c.loadCacheXml(generate(creation));

    var tw = c.getCacheTransactionManager().getWriter();
    assertTrue("tw should be TransactionWriter, but it is:" + tw,
        tw instanceof TestTransactionWriter);
  }

  /**
   * Tests that a region created with a named attributes with diskstore
   */
  @Test
  public void testDiskStoreInTemplates() throws Exception {
    var dir = new File("west");
    dir.mkdir();
    dir.deleteOnExit();

    dir = new File("east");
    dir.mkdir();
    dir.deleteOnExit();

    setXmlFile(findFile("ewtest.xml"));

    var regionName_west = "orders" + SEPARATOR + "west";
    var regionName_east = "orders" + SEPARATOR + "east";

    Cache cache = getCache();

    // verify diskstores
    var ds = cache.findDiskStore("persistentDiskStore1");
    assertNotNull(ds);
    assertEquals(500, ds.getQueueSize());
    var dirs = ds.getDiskDirs();
    assertEquals("west", dirs[0].getPath());

    ds = cache.findDiskStore("persistentDiskStore2");
    assertNotNull(ds);
    assertEquals(500, ds.getQueueSize());
    dirs = ds.getDiskDirs();
    assertEquals("east", dirs[0].getPath());

    // verify templates
    assertNotNull(cache.getRegionAttributes("nack"));
    RegionAttributes attrs = cache.getRegionAttributes("persistent");
    assertEquals(DataPolicy.PERSISTENT_REPLICATE, attrs.getDataPolicy());
    assertEquals(false, attrs.isDiskSynchronous());
    assertEquals("persistentDiskStore1", attrs.getDiskStoreName());

    Region region = cache.getRegion(regionName_west);
    assertNotNull(region);

    attrs = region.getAttributes();
    assertEquals(DataPolicy.PERSISTENT_REPLICATE, attrs.getDataPolicy());
    assertEquals(false, attrs.isDiskSynchronous());
    assertEquals("persistentDiskStore1", attrs.getDiskStoreName());

    region = cache.getRegion(regionName_east);
    assertNotNull(region);

    // Make sure that attributes can be "overridden"
    attrs = region.getAttributes();
    assertEquals(DataPolicy.PERSISTENT_REPLICATE, attrs.getDataPolicy());
    assertEquals(false, attrs.isDiskSynchronous());
    assertEquals("persistentDiskStore2", attrs.getDiskStoreName());

    // bug 41934
    var regionName_datap = "data-p";
    region = cache.getRegion(regionName_datap);
    assertNotNull(region);
    attrs = region.getAttributes();
    var pa = attrs.getPartitionAttributes();
    assertEquals(1, pa.getRedundantCopies());
    assertEquals(3, pa.getTotalNumBuckets());
    assertEquals(DataPolicy.PERSISTENT_PARTITION, attrs.getDataPolicy());
  }

  @Test
  public void testBackupFiles() throws Exception {
    var cache = new CacheCreation();
    var backup1 = new File("/back/me/up");
    var backup2 = new File("/me/too/please");
    cache.addBackup(backup1);
    cache.addBackup(backup2);
    testXml(cache);
    var c = (GemFireCacheImpl) getCache();
    assertEquals(Arrays.asList(backup1, backup2), c.getBackupFiles());
  }

  @Test
  public void testClientCache() throws Exception {
    var cache = new ClientCacheCreation();
    cache.setCopyOnRead(true);
    testXml(cache);
    var c = (GemFireCacheImpl) getCache();
    assertEquals(true, c.getCopyOnRead());
    assertEquals(true, c.isClient());
    for (var pra : ClientRegionShortcut.values()) {
      assertNotNull(c.getRegionAttributes(pra.name()));
    }
    assertEquals(ClientRegionShortcut.values().length, c.listRegionAttributes().size());
  }

  @Test
  public void testNormalCache() throws Exception {
    var cache = new CacheCreation();
    cache.setCopyOnRead(true);
    testXml(cache);
    var c = (GemFireCacheImpl) getCache();
    assertEquals(true, c.getCopyOnRead());
    assertEquals(false, c.isClient());
    for (var pra : RegionShortcut.values()) {
      assertNotNull(c.getRegionAttributes(pra.name()));
    }
    assertEquals(RegionShortcut.values().length, c.listRegionAttributes().size());
  }

  @Test
  public void testPARTITION() throws Exception {
    var cache = new CacheCreation();
    var root = (RegionCreation) cache.createRegion("partition", "PARTITION");
    testXml(cache);
    var c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("partition");
    assertNotNull(r);
    var ra = r.getAttributes();
    assertEquals(DataPolicy.PARTITION, ra.getDataPolicy());
    assertNotNull(ra.getPartitionAttributes());
    assertEquals(0, ra.getPartitionAttributes().getRedundantCopies());
  }

  @Test
  public void testPARTITION_REDUNDANT() throws Exception {
    var cache = new CacheCreation();
    var root = (RegionCreation) cache.createRegion("rpartition", "PARTITION_REDUNDANT");
    testXml(cache);
    var c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("rpartition");
    assertNotNull(r);
    var ra = r.getAttributes();
    assertEquals(DataPolicy.PARTITION, ra.getDataPolicy());
    assertNotNull(ra.getPartitionAttributes());
    assertEquals(1, ra.getPartitionAttributes().getRedundantCopies());
  }

  @Test
  public void testPARTITION_PERSISTENT() throws Exception {
    var cache = new CacheCreation();
    var root = (RegionCreation) cache.createRegion("ppartition", "PARTITION_PERSISTENT");
    testXml(cache);
    var c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("ppartition");
    assertNotNull(r);
    var ra = r.getAttributes();
    assertEquals(DataPolicy.PERSISTENT_PARTITION, ra.getDataPolicy());
    assertNotNull(ra.getPartitionAttributes());
    assertEquals(0, ra.getPartitionAttributes().getRedundantCopies());
  }

  @Test
  public void testPARTITION_REDUNDANT_PERSISTENT() throws Exception {
    var cache = new CacheCreation();
    var root =
        (RegionCreation) cache.createRegion("prpartition", "PARTITION_REDUNDANT_PERSISTENT");
    testXml(cache);
    var c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("prpartition");
    assertNotNull(r);
    var ra = r.getAttributes();
    assertEquals(DataPolicy.PERSISTENT_PARTITION, ra.getDataPolicy());
    assertNotNull(ra.getPartitionAttributes());
    assertEquals(1, ra.getPartitionAttributes().getRedundantCopies());
  }

  @Test
  public void testPARTITION_OVERFLOW() throws Exception {
    var cache = new CacheCreation();
    var rmc = new ResourceManagerCreation();
    rmc.setEvictionHeapPercentage(55.0f);
    rmc.setCriticalHeapPercentage(80.0f);
    cache.setResourceManagerCreation(rmc);
    var root =
        (RegionCreation) cache.createRegion("partitionoverflow", "PARTITION_OVERFLOW");
    testXml(cache);
    var c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("partitionoverflow");
    assertNotNull(r);
    var ra = r.getAttributes();
    assertEquals(DataPolicy.PARTITION, ra.getDataPolicy());
    assertNotNull(ra.getPartitionAttributes());
    assertEquals(0, ra.getPartitionAttributes().getRedundantCopies());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK),
        ra.getEvictionAttributes());
    assertEquals(55.0f, c.getResourceManager().getEvictionHeapPercentage(), 0);
    assertEquals(80.0f, c.getResourceManager().getCriticalHeapPercentage(), 0);
  }

  @Test
  public void testPARTITION_REDUNDANT_OVERFLOW() throws Exception {
    var cache = new CacheCreation();
    var root =
        (RegionCreation) cache.createRegion("rpartitionoverflow", "PARTITION_REDUNDANT_OVERFLOW");
    testXml(cache);
    var c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("rpartitionoverflow");
    assertNotNull(r);
    var ra = r.getAttributes();
    assertEquals(DataPolicy.PARTITION, ra.getDataPolicy());
    assertNotNull(ra.getPartitionAttributes());
    assertEquals(1, ra.getPartitionAttributes().getRedundantCopies());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK),
        ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
        c.getResourceManager().getEvictionHeapPercentage(), 0);
  }

  @Test
  public void testPARTITION_PERSISTENT_OVERFLOW() throws Exception {
    var cache = new CacheCreation();
    var rmc = new ResourceManagerCreation();
    rmc.setCriticalHeapPercentage(80.0f);
    cache.setResourceManagerCreation(rmc);
    var root =
        (RegionCreation) cache.createRegion("ppartitionoverflow", "PARTITION_PERSISTENT_OVERFLOW");
    testXml(cache);
    var c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("ppartitionoverflow");
    assertNotNull(r);
    var ra = r.getAttributes();
    assertEquals(DataPolicy.PERSISTENT_PARTITION, ra.getDataPolicy());
    assertNotNull(ra.getPartitionAttributes());
    assertEquals(0, ra.getPartitionAttributes().getRedundantCopies());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK),
        ra.getEvictionAttributes());
    assertEquals(80.0f, c.getResourceManager().getCriticalHeapPercentage(), 0);
    assertEquals(75.0f, c.getResourceManager().getEvictionHeapPercentage(), 0);
  }

  @Test
  public void testPARTITION_REDUNDANT_PERSISTENT_OVERFLOW() throws Exception {
    var cache = new CacheCreation();
    var rmc = new ResourceManagerCreation();
    rmc.setEvictionHeapPercentage(0.0f); // test bug 42130
    cache.setResourceManagerCreation(rmc);
    var root = (RegionCreation) cache.createRegion("prpartitionoverflow",
        "PARTITION_REDUNDANT_PERSISTENT_OVERFLOW");
    testXml(cache);
    var c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("prpartitionoverflow");
    assertNotNull(r);
    var ra = r.getAttributes();
    assertEquals(DataPolicy.PERSISTENT_PARTITION, ra.getDataPolicy());
    assertNotNull(ra.getPartitionAttributes());
    assertEquals(1, ra.getPartitionAttributes().getRedundantCopies());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK),
        ra.getEvictionAttributes());
    assertEquals(0.0f, c.getResourceManager().getEvictionHeapPercentage(), 0);
  }

  @Test
  public void testPARTITION_HEAP_LRU() throws Exception {
    var cache = new CacheCreation();
    var root = (RegionCreation) cache.createRegion("partitionlru", "PARTITION_HEAP_LRU");
    testXml(cache);
    var c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("partitionlru");
    assertNotNull(r);
    var ra = r.getAttributes();
    assertEquals(DataPolicy.PARTITION, ra.getDataPolicy());
    assertNotNull(ra.getPartitionAttributes());
    assertEquals(0, ra.getPartitionAttributes().getRedundantCopies());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(), ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
        c.getResourceManager().getEvictionHeapPercentage(), 0);
  }

  @Test
  public void testPARTITION_REDUNDANT_HEAP_LRU() throws Exception {
    var cache = new CacheCreation();
    var root =
        (RegionCreation) cache.createRegion("rpartitionlru", "PARTITION_REDUNDANT_HEAP_LRU");
    testXml(cache);
    var c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("rpartitionlru");
    assertNotNull(r);
    var ra = r.getAttributes();
    assertEquals(DataPolicy.PARTITION, ra.getDataPolicy());
    assertNotNull(ra.getPartitionAttributes());
    assertEquals(1, ra.getPartitionAttributes().getRedundantCopies());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(), ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
        c.getResourceManager().getEvictionHeapPercentage(), 0);
  }

  @Test
  public void testREPLICATE() throws Exception {
    var cache = new CacheCreation();
    var root = (RegionCreation) cache.createRegion("replicate", "REPLICATE");
    testXml(cache);
    var c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("replicate");
    assertNotNull(r);
    var ra = r.getAttributes();
    assertEquals(DataPolicy.REPLICATE, ra.getDataPolicy());
    assertEquals(Scope.DISTRIBUTED_ACK, ra.getScope());
  }

  @Test
  public void testREPLICATE_PERSISTENT() throws Exception {
    var cache = new CacheCreation();
    var root = (RegionCreation) cache.createRegion("preplicate", "REPLICATE_PERSISTENT");
    testXml(cache);
    var c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("preplicate");
    assertNotNull(r);
    var ra = r.getAttributes();
    assertEquals(DataPolicy.PERSISTENT_REPLICATE, ra.getDataPolicy());
    assertEquals(Scope.DISTRIBUTED_ACK, ra.getScope());
  }

  @Test
  public void testREPLICATE_OVERFLOW() throws Exception {
    var cache = new CacheCreation();
    var root =
        (RegionCreation) cache.createRegion("replicateoverflow", "REPLICATE_OVERFLOW");
    testXml(cache);
    var c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("replicateoverflow");
    assertNotNull(r);
    var ra = r.getAttributes();
    assertEquals(DataPolicy.REPLICATE, ra.getDataPolicy());
    assertEquals(Scope.DISTRIBUTED_ACK, ra.getScope());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK),
        ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
        c.getResourceManager().getEvictionHeapPercentage(), 0);
  }

  @Test
  public void testREPLICATE_PERSISTENT_OVERFLOW() throws Exception {
    var cache = new CacheCreation();
    var root =
        (RegionCreation) cache.createRegion("preplicateoverflow", "REPLICATE_PERSISTENT_OVERFLOW");
    testXml(cache);
    var c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("preplicateoverflow");
    assertNotNull(r);
    var ra = r.getAttributes();
    assertEquals(DataPolicy.PERSISTENT_REPLICATE, ra.getDataPolicy());
    assertEquals(Scope.DISTRIBUTED_ACK, ra.getScope());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK),
        ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
        c.getResourceManager().getEvictionHeapPercentage(), 0);
  }

  @Test
  public void testREPLICATE_HEAP_LRU() throws Exception {
    var cache = new CacheCreation();
    var root = (RegionCreation) cache.createRegion("replicatelru", "REPLICATE_HEAP_LRU");
    testXml(cache);
    var c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("replicatelru");
    assertNotNull(r);
    var ra = r.getAttributes();
    assertEquals(DataPolicy.PRELOADED, ra.getDataPolicy());
    assertEquals(new SubscriptionAttributes(InterestPolicy.ALL), ra.getSubscriptionAttributes());
    assertEquals(Scope.DISTRIBUTED_ACK, ra.getScope());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(), ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
        c.getResourceManager().getEvictionHeapPercentage(), 0);
  }

  @Test
  public void testLOCAL() throws Exception {
    var cache = new CacheCreation();
    var root = (RegionCreation) cache.createRegion("local", "LOCAL");
    testXml(cache);
    var c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("local");
    assertNotNull(r);
    var ra = r.getAttributes();
    assertEquals(DataPolicy.NORMAL, ra.getDataPolicy());
    assertEquals(Scope.LOCAL, ra.getScope());
  }

  @Test
  public void testLOCAL_PERSISTENT() throws Exception {
    var cache = new CacheCreation();
    var root = (RegionCreation) cache.createRegion("plocal", "LOCAL_PERSISTENT");
    testXml(cache);
    var c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("plocal");
    assertNotNull(r);
    var ra = r.getAttributes();
    assertEquals(DataPolicy.PERSISTENT_REPLICATE, ra.getDataPolicy());
    assertEquals(Scope.LOCAL, ra.getScope());
  }

  @Test
  public void testLOCAL_HEAP_LRU() throws Exception {
    var cache = new CacheCreation();
    var root = (RegionCreation) cache.createRegion("locallru", "LOCAL_HEAP_LRU");
    testXml(cache);
    var c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("locallru");
    assertNotNull(r);
    var ra = r.getAttributes();
    assertEquals(DataPolicy.NORMAL, ra.getDataPolicy());
    assertEquals(Scope.LOCAL, ra.getScope());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(), ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
        c.getResourceManager().getEvictionHeapPercentage(), 0);
  }

  @Test
  public void testLOCAL_OVERFLOW() throws Exception {
    var cache = new CacheCreation();
    var root = (RegionCreation) cache.createRegion("localoverflow", "LOCAL_OVERFLOW");
    testXml(cache);
    var c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("localoverflow");
    assertNotNull(r);
    var ra = r.getAttributes();
    assertEquals(DataPolicy.NORMAL, ra.getDataPolicy());
    assertEquals(Scope.LOCAL, ra.getScope());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK),
        ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
        c.getResourceManager().getEvictionHeapPercentage(), 0);
  }

  @Test
  public void testLOCAL_PERSISTENT_OVERFLOW() throws Exception {
    var cache = new CacheCreation();
    var root =
        (RegionCreation) cache.createRegion("cpolocal", "LOCAL_PERSISTENT_OVERFLOW");
    testXml(cache);
    var c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("cpolocal");
    assertNotNull(r);
    var ra = r.getAttributes();
    assertEquals(DataPolicy.PERSISTENT_REPLICATE, ra.getDataPolicy());
    assertEquals(Scope.LOCAL, ra.getScope());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK),
        ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
        c.getResourceManager().getEvictionHeapPercentage(), 0);
  }

  @Test
  public void testPARTITION_PROXY() throws Exception {
    var cache = new CacheCreation();
    var root = (RegionCreation) cache.createRegion("partitionProxy", "PARTITION_PROXY");
    testXml(cache);
    var c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("partitionProxy");
    assertNotNull(r);
    var ra = r.getAttributes();
    assertEquals(DataPolicy.PARTITION, ra.getDataPolicy());
    assertNotNull(ra.getPartitionAttributes());
    assertEquals(0, ra.getPartitionAttributes().getRedundantCopies());
    assertEquals(0, ra.getPartitionAttributes().getLocalMaxMemory());
  }

  @Test
  public void testPARTITION_PROXY_REDUNDANT() throws Exception {
    var cache = new CacheCreation();
    var root =
        (RegionCreation) cache.createRegion("rpartitionProxy", "PARTITION_PROXY_REDUNDANT");
    testXml(cache);
    var c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("rpartitionProxy");
    assertNotNull(r);
    var ra = r.getAttributes();
    assertEquals(DataPolicy.PARTITION, ra.getDataPolicy());
    assertNotNull(ra.getPartitionAttributes());
    assertEquals(1, ra.getPartitionAttributes().getRedundantCopies());
    assertEquals(0, ra.getPartitionAttributes().getLocalMaxMemory());
  }

  @Test
  public void testREPLICATE_PROXY() throws Exception {
    var cache = new CacheCreation();
    var root = (RegionCreation) cache.createRegion("replicateProxy", "REPLICATE_PROXY");
    testXml(cache);
    var c = (GemFireCacheImpl) getCache();
    Region r = c.getRegion("replicateProxy");
    assertNotNull(r);
    var ra = r.getAttributes();
    assertEquals(DataPolicy.EMPTY, ra.getDataPolicy());
    assertEquals(Scope.DISTRIBUTED_ACK, ra.getScope());
  }

  @Test
  public void testPROXY() throws Exception {
    var cache = new ClientCacheCreation();
    var root = (RegionCreation) cache.createRegion("proxy", "PROXY");
    testXml(cache);
    var c = (GemFireCacheImpl) getCache();
    assertEquals(true, c.isClient());
    Region r = c.getRegion("proxy");
    assertNotNull(r);
    var ra = r.getAttributes();
    assertEquals(DataPolicy.EMPTY, ra.getDataPolicy());
    assertEquals(Scope.LOCAL, ra.getScope());
    assertEquals("DEFAULT", ra.getPoolName());
  }

  @Test
  public void testCACHING_PROXY() throws Exception {
    var cache = new ClientCacheCreation();
    var root = (RegionCreation) cache.createRegion("cproxy", "CACHING_PROXY");
    testXml(cache);
    var c = (GemFireCacheImpl) getCache();
    assertEquals(true, c.isClient());
    Region r = c.getRegion("cproxy");
    assertNotNull(r);
    var ra = r.getAttributes();
    assertEquals(DataPolicy.NORMAL, ra.getDataPolicy());
    assertEquals(Scope.LOCAL, ra.getScope());
    assertEquals("DEFAULT", ra.getPoolName());
  }

  @Test
  public void testCACHING_PROXY_HEAP_LRU() throws Exception {
    var cache = new ClientCacheCreation();
    var root =
        (RegionCreation) cache.createRegion("cproxylru", "CACHING_PROXY_HEAP_LRU");
    testXml(cache);
    var c = (GemFireCacheImpl) getCache();
    assertEquals(true, c.isClient());
    Region r = c.getRegion("cproxylru");
    assertNotNull(r);
    var ra = r.getAttributes();
    assertEquals(DataPolicy.NORMAL, ra.getDataPolicy());
    assertEquals(Scope.LOCAL, ra.getScope());
    assertEquals("DEFAULT", ra.getPoolName());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(), ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
        c.getResourceManager().getEvictionHeapPercentage(), 0);
  }

  @Test
  public void testCACHING_PROXY_OVERFLOW() throws Exception {
    var cache = new ClientCacheCreation();
    var root =
        (RegionCreation) cache.createRegion("cproxyoverflow", "CACHING_PROXY_OVERFLOW");
    testXml(cache);
    var c = (GemFireCacheImpl) getCache();
    assertEquals(true, c.isClient());
    Region r = c.getRegion("cproxyoverflow");
    assertNotNull(r);
    var ra = r.getAttributes();
    assertEquals(DataPolicy.NORMAL, ra.getDataPolicy());
    assertEquals(Scope.LOCAL, ra.getScope());
    assertEquals("DEFAULT", ra.getPoolName());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK),
        ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
        c.getResourceManager().getEvictionHeapPercentage(), 0);
  }

  @Test
  public void testClientLOCAL() throws Exception {
    var cache = new ClientCacheCreation();
    var root = (RegionCreation) cache.createRegion("local", "LOCAL");
    testXml(cache);
    var c = (GemFireCacheImpl) getCache();
    assertEquals(true, c.isClient());
    Region r = c.getRegion("local");
    assertNotNull(r);
    var ra = r.getAttributes();
    assertEquals(DataPolicy.NORMAL, ra.getDataPolicy());
    assertEquals(Scope.LOCAL, ra.getScope());
    assertEquals(null, ra.getPoolName());
  }

  @Test
  public void testClientLOCAL_HEAP_LRU() throws Exception {
    var cache = new ClientCacheCreation();
    var root = (RegionCreation) cache.createRegion("locallru", "LOCAL_HEAP_LRU");
    testXml(cache);
    var c = (GemFireCacheImpl) getCache();
    assertEquals(true, c.isClient());
    Region r = c.getRegion("locallru");
    assertNotNull(r);
    var ra = r.getAttributes();
    assertEquals(DataPolicy.NORMAL, ra.getDataPolicy());
    assertEquals(Scope.LOCAL, ra.getScope());
    assertEquals(null, ra.getPoolName());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(), ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
        c.getResourceManager().getEvictionHeapPercentage(), 0);
  }

  @Test
  public void testClientLOCAL_OVERFLOW() throws Exception {
    var cache = new ClientCacheCreation();
    var root = (RegionCreation) cache.createRegion("localoverflow", "LOCAL_OVERFLOW");
    testXml(cache);
    var c = (GemFireCacheImpl) getCache();
    assertEquals(true, c.isClient());
    Region r = c.getRegion("localoverflow");
    assertNotNull(r);
    var ra = r.getAttributes();
    assertEquals(DataPolicy.NORMAL, ra.getDataPolicy());
    assertEquals(Scope.LOCAL, ra.getScope());
    assertEquals(null, ra.getPoolName());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK),
        ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
        c.getResourceManager().getEvictionHeapPercentage(), 0);
  }

  @Test
  public void testClientLOCAL_PERSISTENT() throws Exception {
    var cache = new ClientCacheCreation();
    var root = (RegionCreation) cache.createRegion("cplocal", "LOCAL_PERSISTENT");
    testXml(cache);
    var c = (GemFireCacheImpl) getCache();
    assertEquals(true, c.isClient());
    Region r = c.getRegion("cplocal");
    assertNotNull(r);
    var ra = r.getAttributes();
    assertEquals(DataPolicy.PERSISTENT_REPLICATE, ra.getDataPolicy());
    assertEquals(Scope.LOCAL, ra.getScope());
    assertEquals(null, ra.getPoolName());
  }

  @Test
  public void testClientLOCAL_PERSISTENT_OVERFLOW() throws Exception {
    var cache = new ClientCacheCreation();
    var root =
        (RegionCreation) cache.createRegion("cpolocal", "LOCAL_PERSISTENT_OVERFLOW");
    testXml(cache);
    var c = (GemFireCacheImpl) getCache();
    assertEquals(true, c.isClient());
    Region r = c.getRegion("cpolocal");
    assertNotNull(r);
    var ra = r.getAttributes();
    assertEquals(DataPolicy.PERSISTENT_REPLICATE, ra.getDataPolicy());
    assertEquals(Scope.LOCAL, ra.getScope());
    assertEquals(null, ra.getPoolName());
    assertEquals(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK),
        ra.getEvictionAttributes());
    assertEquals(LocalRegion.DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE,
        c.getResourceManager().getEvictionHeapPercentage(), 0);
  }

  /**
   * Tests that a partitioned region is created with FixedPartitionAttributes set programatically
   * and correct cache.xml is generated with the same FixedPartitionAttributes
   */
  @Test
  public void testFixedPartitioning() throws Exception {

    var cache = new CacheCreation();
    var attrs = new RegionAttributesCreation();
    var fpa1 = FixedPartitionAttributes.createFixedPartition("Q1");
    var fpa2 = FixedPartitionAttributes.createFixedPartition("Q2", true);
    var fpa3 = FixedPartitionAttributes.createFixedPartition("Q3", 3);
    var fpa4 = FixedPartitionAttributes.createFixedPartition("Q4", false, 3);
    List<FixedPartitionAttributes> fpattrsList = new ArrayList<>();
    fpattrsList.add(fpa1);
    fpattrsList.add(fpa2);
    fpattrsList.add(fpa3);
    fpattrsList.add(fpa4);

    var resolver = new QuarterPartitionResolver();

    var paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(1).setPartitionResolver(resolver).addFixedPartitionAttributes(fpa1)
        .addFixedPartitionAttributes(fpa2).addFixedPartitionAttributes(fpa3)
        .addFixedPartitionAttributes(fpa4);

    attrs.setPartitionAttributes(paf.create());
    cache.createRegion("Quarter", attrs);
    var r = cache.getRegion("Quarter");
    validateAttributes(r, fpattrsList, resolver, false);

    testXml(cache);

    Cache c = getCache();
    assertNotNull(c);
    Region region = c.getRegion("Quarter");
    assertNotNull(region);
    validateAttributes(region, fpattrsList, resolver, false);
  }

  @Test
  public void testFixedPartitioning_colocation_WithAttributes() throws Exception {
    var cache = new CacheCreation();
    var fpa1 = FixedPartitionAttributes.createFixedPartition("Q1");
    var fpa2 = FixedPartitionAttributes.createFixedPartition("Q2", true);
    var fpa3 = FixedPartitionAttributes.createFixedPartition("Q3", 3);
    var fpa4 = FixedPartitionAttributes.createFixedPartition("Q4", false, 3);
    List<FixedPartitionAttributes> fpattrsList = new ArrayList<>();
    fpattrsList.add(fpa1);
    fpattrsList.add(fpa2);
    fpattrsList.add(fpa3);
    fpattrsList.add(fpa4);
    var resolver = new QuarterPartitionResolver();
    Region customerRegion = null;
    Region orderRegion = null;

    {
      var attrs = new RegionAttributesCreation();
      var paf = new PartitionAttributesFactory();
      paf.setRedundantCopies(1).setPartitionResolver(resolver).addFixedPartitionAttributes(fpa1)
          .addFixedPartitionAttributes(fpa2).addFixedPartitionAttributes(fpa3)
          .addFixedPartitionAttributes(fpa4);
      attrs.setPartitionAttributes(paf.create());
      cache.createRegion("Customer", attrs);
    }
    customerRegion = cache.getRegion("Customer");
    validateAttributes(customerRegion, fpattrsList, resolver, false);

    try {
      var attrs = new RegionAttributesCreation();
      var paf = new PartitionAttributesFactory();
      paf.setRedundantCopies(1).setPartitionResolver(resolver).addFixedPartitionAttributes(fpa1)
          .addFixedPartitionAttributes(fpa2).addFixedPartitionAttributes(fpa3)
          .addFixedPartitionAttributes(fpa4).setColocatedWith("Customer");

      attrs.setPartitionAttributes(paf.create());
      cache.createRegion("Order", attrs);
      orderRegion = cache.getRegion("Order");
      validateAttributes(orderRegion, fpattrsList, resolver, true);
    } catch (Exception illegal) {
      // TODO: clean this expected exception up
      if (!((illegal instanceof IllegalStateException) && (illegal.getMessage()
          .contains("can not be specified in PartitionAttributesFactory")))) {
        Assert.fail("Expected IllegalStateException ", illegal);
      }

      var attrs = new RegionAttributesCreation();
      var paf = new PartitionAttributesFactory();
      paf.setRedundantCopies(1).setPartitionResolver(resolver).setColocatedWith("Customer");

      attrs.setPartitionAttributes(paf.create());
      cache.createRegion("Order", attrs);
      orderRegion = cache.getRegion("Order");
      validateAttributes(orderRegion, fpattrsList, resolver, true);
    }

    testXml(cache);

    Cache c = getCache();
    assertNotNull(c);
    customerRegion = c.getRegion("Customer");
    assertNotNull(customerRegion);
    validateAttributes(customerRegion, fpattrsList, resolver, false);

    orderRegion = c.getRegion("Order");
    assertNotNull(orderRegion);
    validateAttributes(orderRegion, fpattrsList, resolver, true);
  }

  private void validateAttributes(Region region, List<FixedPartitionAttributes> fpattrsList,
      QuarterPartitionResolver resolver, boolean isColocated) {
    var regionAttrs = region.getAttributes();
    var pa = regionAttrs.getPartitionAttributes();

    assertEquals(pa.getRedundantCopies(), 1);
    assertNotNull(pa.getPartitionResolver().getClass());
    assertEquals(pa.getPartitionResolver(), resolver);
    List<FixedPartitionAttributesImpl> fixedPartitionsList = pa.getFixedPartitionAttributes();
    if (isColocated) {
      assertNull(fixedPartitionsList);
      assertNotNull(pa.getColocatedWith());
    } else {
      assertNull(pa.getColocatedWith());
      assertEquals(fixedPartitionsList.size(), 4);
      assertEquals(fixedPartitionsList.containsAll(fpattrsList), true);
      for (FixedPartitionAttributes fpa : fixedPartitionsList) {
        if (fpa.getPartitionName().equals("Q1")) {
          assertEquals(fpa.getNumBuckets(), 1);
          assertEquals(fpa.isPrimary(), false);
        }
        if (fpa.getPartitionName().equals("Q2")) {
          assertEquals(fpa.getNumBuckets(), 1);
          assertEquals(fpa.isPrimary(), true);
        }
        if (fpa.getPartitionName().equals("Q3")) {
          assertEquals(fpa.getNumBuckets(), 3);
          assertEquals(fpa.isPrimary(), false);
        }
        if (fpa.getPartitionName().equals("Q4")) {
          assertEquals(fpa.getNumBuckets(), 3);
          assertEquals(fpa.isPrimary(), false);
        }
      }
    }
  }

  @Test
  public void testPdxDefaults() throws Exception {
    var creation = new CacheCreation();
    testXml(creation);

    Cache c = getCache();
    assertTrue(c instanceof GemFireCacheImpl);
    c.loadCacheXml(generate(creation));

    assertEquals(null, c.getPdxDiskStore());
    assertEquals(null, c.getPdxSerializer());
    assertEquals(false, c.getPdxPersistent());
    assertEquals(false, c.getPdxReadSerialized());
    assertEquals(false, c.getPdxIgnoreUnreadFields());
  }

  @Test
  public void testPdxAttributes() throws Exception {
    var creation = new CacheCreation();
    creation.setPdxPersistent(true);
    creation.setPdxReadSerialized(true);
    creation.setPdxIgnoreUnreadFields(true);
    creation.setPdxDiskStore("my_disk_store");
    var serializer = new TestPdxSerializer();
    var props = new Properties();
    props.setProperty("hello", "there");
    serializer.init(props);
    creation.setPdxSerializer(serializer);
    testXml(creation);

    Cache c = getCache();
    assertTrue(c instanceof GemFireCacheImpl);
    c.loadCacheXml(generate(creation));

    assertEquals("my_disk_store", c.getPdxDiskStore());
    assertEquals(serializer, c.getPdxSerializer());
    assertEquals(true, c.getPdxPersistent());
    assertEquals(true, c.getPdxReadSerialized());
    assertEquals(true, c.getPdxIgnoreUnreadFields());

    // test that we can override the cache.xml attributes
    {
      closeCache();
      var cf = new CacheFactory();
      cf.setPdxDiskStore("new disk store");
      c = getCache(cf);
      assertTrue(c instanceof GemFireCacheImpl);
      c.loadCacheXml(generate(creation));

      assertEquals("new disk store", c.getPdxDiskStore());
      assertEquals(serializer, c.getPdxSerializer());
      assertEquals(true, c.getPdxPersistent());
      assertEquals(true, c.getPdxReadSerialized());
      assertEquals(true, c.getPdxIgnoreUnreadFields());
    }

    {
      closeCache();
      var cf = new CacheFactory();
      cf.setPdxPersistent(false);
      cf.setPdxIgnoreUnreadFields(false);
      c = getCache(cf);
      assertTrue(c instanceof GemFireCacheImpl);
      c.loadCacheXml(generate(creation));

      assertEquals("my_disk_store", c.getPdxDiskStore());
      assertEquals(serializer, c.getPdxSerializer());
      assertEquals(false, c.getPdxPersistent());
      assertEquals(true, c.getPdxReadSerialized());
      assertEquals(false, c.getPdxIgnoreUnreadFields());
    }

    {
      closeCache();
      var cf = new CacheFactory();
      cf.setPdxSerializer(null);
      c = getCache(cf);
      assertTrue(c instanceof GemFireCacheImpl);
      c.loadCacheXml(generate(creation));

      assertEquals("my_disk_store", c.getPdxDiskStore());
      assertEquals(null, c.getPdxSerializer());
      assertEquals(true, c.getPdxPersistent());
      assertEquals(true, c.getPdxReadSerialized());
      assertEquals(true, c.getPdxIgnoreUnreadFields());
    }

    {
      closeCache();
      var cf = new CacheFactory();
      cf.setPdxReadSerialized(false);
      c = getCache(cf);
      assertTrue(c instanceof GemFireCacheImpl);
      c.loadCacheXml(generate(creation));

      assertEquals("my_disk_store", c.getPdxDiskStore());
      assertEquals(serializer, c.getPdxSerializer());
      assertEquals(true, c.getPdxPersistent());
      assertEquals(false, c.getPdxReadSerialized());
      assertEquals(true, c.getPdxIgnoreUnreadFields());
    }

  }

  @Test
  public void testTXManagerOnClientCache() throws Exception {
    var cc = new ClientCacheCreation();
    // CacheCreation cc = new CacheCreation();
    var txMgrCreation = new CacheTransactionManagerCreation();
    txMgrCreation.addListener(new TestTXListener());
    cc.addCacheTransactionManagerCreation(txMgrCreation);
    testXml(cc);

    Cache c = getCache();
    assertTrue(c instanceof ClientCache);
    c.loadCacheXml(generate(cc));

    var clientC = (ClientCache) c;
    var mgr = clientC.getCacheTransactionManager();
    assertNotNull(mgr);
    assertTrue(mgr.getListeners()[0] instanceof TestTXListener);

  }

  @Test
  public void testNoTXWriterOnClient() throws Exception {
    // test writer is not created
    var cc = new ClientCacheCreation();
    var txMgrCreation = new CacheTransactionManagerCreation();
    txMgrCreation.setWriter(new TestTransactionWriter());
    cc.addCacheTransactionManagerCreation(txMgrCreation);
    var expectedException =
        addIgnoredException("A TransactionWriter cannot be registered on a client");
    try {
      testXml(cc);
      fail("expected exception not thrown");
    } catch (IllegalStateException ignored) {
    } finally {
      expectedException.remove();
    }
  }

  public static class TestTXListener extends TransactionListenerAdapter implements Declarable {
    @Override
    public void init(Properties props) {}

    @Override
    public boolean equals(Object other) {
      return other instanceof TestTXListener;
    }
  }

  /**
   * Tests that a region created with a named attributes set programmatically for delta propogation
   * has the correct attributes.
   */
  @Test
  public void testRegionAttributesForRegionEntryCloning() throws Exception {
    final var rNameBase = getUniqueName();
    final var r1 = rNameBase + "1";

    // Setting multi-cast via nested region attributes
    var creation = new CacheCreation();
    var attrs = new RegionAttributesCreation(creation);
    attrs.setScope(Scope.LOCAL);
    attrs.setEarlyAck(false);
    attrs.setCloningEnable(false);
    attrs.setMulticastEnabled(true);
    creation.createRegion(r1, attrs);

    testXml(creation);

    Cache c = getCache();
    assertTrue(c instanceof GemFireCacheImpl);
    c.loadCacheXml(generate(creation));

    Region reg1 = c.getRegion(r1);
    assertNotNull(reg1);
    assertEquals(Scope.LOCAL, reg1.getAttributes().getScope());
    assertFalse(reg1.getAttributes().getEarlyAck());
    assertTrue(reg1.getAttributes().getMulticastEnabled());
    assertFalse(reg1.getAttributes().getCloningEnabled());

    // changing Clonned setting
    reg1.getAttributesMutator().setCloningEnabled(true);
    assertTrue(reg1.getAttributes().getCloningEnabled());

    reg1.getAttributesMutator().setCloningEnabled(false);
    assertFalse(reg1.getAttributes().getCloningEnabled());

    // for sub region - a child attribute should be inherited
    var sub = "subRegion";
    var attrsSub = new RegionAttributesCreation(creation);
    attrsSub.setScope(Scope.LOCAL);
    reg1.createSubregion(sub, attrsSub);
    var subRegion = reg1.getSubregion(sub);
    assertFalse(subRegion.getAttributes().getCloningEnabled());
    subRegion.getAttributesMutator().setCloningEnabled(true);
    assertTrue(subRegion.getAttributes().getCloningEnabled());
  }

  /**
   * Tests that a region created with a named attributes set programatically for recovery-delay has
   * the correct attributes.
   */
  @Test
  public void testRecoveryDelayAttributes() throws Exception {
    var cache = new CacheCreation();
    var attrs = new RegionAttributesCreation(cache);

    var paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(1);
    paf.setTotalMaxMemory(500);
    paf.setLocalMaxMemory(100);
    paf.setRecoveryDelay(33);
    paf.setStartupRecoveryDelay(270);

    attrs.setPartitionAttributes(paf.create());

    cache.createRegion("parRoot", attrs);

    var r = cache.getRegion("parRoot");
    assertEquals(r.getAttributes().getPartitionAttributes().getRedundantCopies(), 1);
    assertEquals(r.getAttributes().getPartitionAttributes().getLocalMaxMemory(), 100);
    assertEquals(r.getAttributes().getPartitionAttributes().getTotalMaxMemory(), 500);
    assertEquals(33, r.getAttributes().getPartitionAttributes().getRecoveryDelay());
    assertEquals(270, r.getAttributes().getPartitionAttributes().getStartupRecoveryDelay());

    testXml(cache);

    Cache c = getCache();
    assertNotNull(c);

    Region region = c.getRegion("parRoot");
    assertNotNull(region);

    var regionAttrs = region.getAttributes();
    var pa = regionAttrs.getPartitionAttributes();

    assertEquals(pa.getRedundantCopies(), 1);
    assertEquals(pa.getLocalMaxMemory(), 100);
    assertEquals(pa.getTotalMaxMemory(), 500);
    assertEquals(33, r.getAttributes().getPartitionAttributes().getRecoveryDelay());
    assertEquals(270, r.getAttributes().getPartitionAttributes().getStartupRecoveryDelay());
  }

  /**
   * Tests that a region created with a named attributes set programmatically for recovery-delay has
   * the correct attributes.
   */
  @Test
  public void testDefaultRecoveryDelayAttributes() throws Exception {
    var cache = new CacheCreation();
    var attrs = new RegionAttributesCreation(cache);

    var paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(1);
    paf.setTotalMaxMemory(500);
    paf.setLocalMaxMemory(100);
    attrs.setPartitionAttributes(paf.create());

    cache.createRegion("parRoot", attrs);

    var r = cache.getRegion("parRoot");
    assertEquals(r.getAttributes().getPartitionAttributes().getRedundantCopies(), 1);
    assertEquals(r.getAttributes().getPartitionAttributes().getLocalMaxMemory(), 100);
    assertEquals(r.getAttributes().getPartitionAttributes().getTotalMaxMemory(), 500);
    assertEquals(-1, r.getAttributes().getPartitionAttributes().getRecoveryDelay());
    assertEquals(0, r.getAttributes().getPartitionAttributes().getStartupRecoveryDelay());

    testXml(cache);

    Cache c = getCache();
    assertNotNull(c);

    Region region = c.getRegion("parRoot");
    assertNotNull(region);

    var regionAttrs = region.getAttributes();
    var pa = regionAttrs.getPartitionAttributes();

    assertEquals(pa.getRedundantCopies(), 1);
    assertEquals(pa.getLocalMaxMemory(), 100);
    assertEquals(pa.getTotalMaxMemory(), 500);
    assertEquals(-1, r.getAttributes().getPartitionAttributes().getRecoveryDelay());
    assertEquals(0, r.getAttributes().getPartitionAttributes().getStartupRecoveryDelay());
  }

  /**
   * Test the ResourceManager element's critical-heap-percentage and eviction-heap-percentage
   * attributes
   */
  @Test
  public void testResourceManagerThresholds() throws Exception {
    var cache = new CacheCreation();
    final var low = 90.0f;
    final var high = 95.0f;

    Cache c;
    var rmc = new ResourceManagerCreation();
    rmc.setEvictionHeapPercentage(low);
    rmc.setCriticalHeapPercentage(high);
    cache.setResourceManagerCreation(rmc);
    testXml(cache);
    {
      c = getCache();
      assertEquals(low, c.getResourceManager().getEvictionHeapPercentage(), 0);
      assertEquals(high, c.getResourceManager().getCriticalHeapPercentage(), 0);
    }
    closeCache();

    rmc = new ResourceManagerCreation();
    // Set them to similar values
    rmc.setEvictionHeapPercentage(low);
    rmc.setCriticalHeapPercentage(low + 1);
    cache.setResourceManagerCreation(rmc);
    testXml(cache);
    {
      c = getCache();
      assertEquals(low, c.getResourceManager().getEvictionHeapPercentage(), 0);
      assertEquals(low + 1, c.getResourceManager().getCriticalHeapPercentage(), 0);
    }
    closeCache();

    rmc = new ResourceManagerCreation();
    rmc.setEvictionHeapPercentage(high);
    rmc.setCriticalHeapPercentage(low);
    cache.setResourceManagerCreation(rmc);
    var expectedException = addIgnoredException(
        "Eviction percentage must be less than the critical percentage.");
    try {
      testXml(cache);
      assertTrue(false);
    } catch (IllegalArgumentException ignored) {
    } finally {
      expectedException.remove();
      closeCache();
    }

    // Disable eviction
    rmc = new ResourceManagerCreation();
    rmc.setEvictionHeapPercentage(0);
    rmc.setCriticalHeapPercentage(low);
    cache.setResourceManagerCreation(rmc);
    testXml(cache);
    {
      c = getCache();
      assertEquals(0f, c.getResourceManager().getEvictionHeapPercentage(), 0);
      assertEquals(low, c.getResourceManager().getCriticalHeapPercentage(), 0);
    }
    closeCache();

    // Disable refusing ops in "red zone"
    rmc = new ResourceManagerCreation();
    rmc.setEvictionHeapPercentage(low);
    rmc.setCriticalHeapPercentage(0);
    cache.setResourceManagerCreation(rmc);
    testXml(cache);
    {
      c = getCache();
      assertEquals(low, c.getResourceManager().getEvictionHeapPercentage(), 0);
      assertEquals(0f, c.getResourceManager().getCriticalHeapPercentage(), 0);
    }
    closeCache();

    // Disable both
    rmc = new ResourceManagerCreation();
    rmc.setEvictionHeapPercentage(0);
    rmc.setCriticalHeapPercentage(0);
    cache.setResourceManagerCreation(rmc);
    testXml(cache);
    c = getCache();
    assertEquals(0f, c.getResourceManager().getEvictionHeapPercentage(), 0);
    assertEquals(0f, c.getResourceManager().getCriticalHeapPercentage(), 0);
  }

  // A bunch of classes for use in testing the serialization schtuff
  public static class DS1 implements DataSerializable {
    @Override
    public void fromData(DataInput in) {}

    @Override
    public void toData(DataOutput out) {}
  }

  public static class DS2 implements DataSerializable {
    @Override
    public void fromData(DataInput in) {}

    @Override
    public void toData(DataOutput out) {}
  }

  public static class NotDataSerializable implements Serializable {
  }

  public static class GoodSerializer extends DataSerializer {
    public GoodSerializer() {}

    @Override
    public Object fromData(DataInput in) {
      return null;
    }

    @Override
    public int getId() {
      return 101;
    }

    @Override
    public Class[] getSupportedClasses() {
      return new Class[] {DS1.class};
    }

    @Override
    public boolean toData(Object o, DataOutput out) {
      return false;
    }
  }

  public static class BadSerializer extends DataSerializer {
    @Override
    public Object fromData(DataInput in) {
      return null;
    }

    @Override
    public int getId() {
      return 101;
    }

    @Override
    public Class[] getSupportedClasses() {
      return null;
    }

    @Override
    public boolean toData(Object o, DataOutput out) {
      return false;
    }
  }

  @Test
  public void testSerializationRegistration() throws Exception {
    var cc = new CacheCreation();
    var sc = new SerializerCreation();

    cc.setSerializerCreation(sc);

    sc.registerInstantiator(DS1.class, 15);
    sc.registerInstantiator(DS2.class, 16);
    sc.registerSerializer(GoodSerializer.class);

    testXml(cc);

    // Now make sure all of the classes were registered....
    assertEquals(15, InternalInstantiator.getClassId(DS1.class));
    assertEquals(16, InternalInstantiator.getClassId(DS2.class));
    assertEquals(GoodSerializer.class, InternalDataSerializer.getSerializer(101).getClass());

    sc = new SerializerCreation();
    sc.registerInstantiator(NotDataSerializable.class, 15);
    closeCache();
    cc.setSerializerCreation(sc);

    var expectedException =
        addIgnoredException("While reading Cache XML file");
    try {
      testXml(cc);
      fail("Instantiator should not have registered due to bad class.");
    } catch (Exception ignored) {
    } finally {
      expectedException.remove();
    }

    sc = new SerializerCreation();
    sc.registerSerializer(BadSerializer.class);
    closeCache();
    cc.setSerializerCreation(sc);

    var expectedException1 =
        addIgnoredException("While reading Cache XML file");
    try {
      testXml(cc);
      fail("Serializer should not have registered due to bad class.");
    } catch (Exception ignored) {
    } finally {
      expectedException1.remove();
    }
  }

  /**
   * Tests that a region created with a named attributes set programmatically for partition-resolver
   * has the correct attributes.
   */
  @Test
  public void testPartitionedRegionAttributesForCustomPartitioning() throws Exception {
    var cache = new CacheCreation();
    var attrs = new RegionAttributesCreation(cache);

    var partitionResolver = new CacheXMLPartitionResolver();
    var params = new Properties();
    params.setProperty("initial-index-value", "1000");
    params.setProperty("secondary-index-value", "5000");
    partitionResolver.init(params);

    var paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(1);
    paf.setTotalMaxMemory(500);
    paf.setLocalMaxMemory(100);
    paf.setPartitionResolver(partitionResolver);

    attrs.setPartitionAttributes(paf.create());

    cache.createRegion("parRoot", attrs);

    var r = cache.getRegion("parRoot");
    assertEquals(r.getAttributes().getPartitionAttributes().getRedundantCopies(), 1);
    assertEquals(r.getAttributes().getPartitionAttributes().getLocalMaxMemory(), 100);
    assertEquals(r.getAttributes().getPartitionAttributes().getTotalMaxMemory(), 500);
    assertEquals(r.getAttributes().getPartitionAttributes().getPartitionResolver(),
        partitionResolver);

    testXml(cache);

    Cache c = getCache();
    assertNotNull(c);

    Region region = c.getRegion("parRoot");
    assertNotNull(region);

    var regionAttrs = region.getAttributes();
    var pa = regionAttrs.getPartitionAttributes();

    assertEquals(pa.getRedundantCopies(), 1);
    assertEquals(pa.getLocalMaxMemory(), 100);
    assertEquals(pa.getTotalMaxMemory(), 500);
    assertNotNull(pa.getPartitionResolver().getClass());
    assertEquals(pa.getPartitionResolver(), partitionResolver);
  }

  /**
   * Tests that a cache created with FunctionService and registered functions has correct registered
   * functions.
   */
  @Test
  public void testCacheCreationWithFunctionService() throws Exception {
    var cache = new CacheCreation();
    var fsc = new FunctionServiceCreation();
    var function1 = new TestFunction(true, TestFunction.TEST_FUNCTION2);
    var function2 = new TestFunction(true, TestFunction.TEST_FUNCTION3);
    var function3 = new TestFunction(true, TestFunction.TEST_FUNCTION4);
    fsc.registerFunction(function1);
    fsc.registerFunction(function2);
    fsc.registerFunction(function3);
    fsc.create();
    cache.setFunctionServiceCreation(fsc);

    testXml(cache);
    getCache();
    var functionIdMap = FunctionService.getRegisteredFunctions();
    assertEquals(3, functionIdMap.size());

    assertTrue(function1.equals(functionIdMap.get(function1.getId())));
    assertTrue(function2.equals(functionIdMap.get(function2.getId())));
    assertTrue(function3.equals(functionIdMap.get(function3.getId())));
  }

  /**
   * Tests that a Partitioned Region can be created with a named attributes set programmatically for
   * ExpirationAttributes
   */
  @Test
  public void testPartitionedRegionAttributesForExpiration() throws Exception {
    var cache = new CacheCreation();
    var attrs = new RegionAttributesCreation(cache);
    attrs.setStatisticsEnabled(true);
    RegionAttributes rootAttrs = null;
    var expiration = new ExpirationAttributes(60, ExpirationAction.DESTROY);

    var partitionResolver = new CacheXMLPartitionResolver();
    var params = new Properties();
    params.setProperty("initial-index-value", "1000");
    params.setProperty("secondary-index-value", "5000");
    partitionResolver.init(params);

    var paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(1);
    paf.setTotalMaxMemory(500);
    paf.setLocalMaxMemory(100);
    paf.setPartitionResolver(partitionResolver);

    var fac = new AttributesFactory(attrs);
    fac.setEntryTimeToLive(expiration);
    fac.setEntryIdleTimeout(expiration);

    fac.setPartitionAttributes(paf.create());
    rootAttrs = fac.create();
    cache.createRegion("parRoot", rootAttrs);

    var r = cache.getRegion("parRoot");
    assertNotNull(r);
    assertEquals(r.getAttributes().getPartitionAttributes().getRedundantCopies(), 1);
    assertEquals(r.getAttributes().getPartitionAttributes().getLocalMaxMemory(), 100);
    assertEquals(r.getAttributes().getPartitionAttributes().getTotalMaxMemory(), 500);
    assertEquals(r.getAttributes().getPartitionAttributes().getPartitionResolver(),
        partitionResolver);

    assertEquals(r.getAttributes().getEntryIdleTimeout().getTimeout(), expiration.getTimeout());
    assertEquals(r.getAttributes().getEntryTimeToLive().getTimeout(), expiration.getTimeout());

    testXml(cache);

    Cache c = getCache();
    assertNotNull(c);

    Region region = c.getRegion("parRoot");
    assertNotNull(region);

    var regionAttrs = region.getAttributes();
    var pa = regionAttrs.getPartitionAttributes();

    assertEquals(pa.getRedundantCopies(), 1);
    assertEquals(pa.getLocalMaxMemory(), 100);
    assertEquals(pa.getTotalMaxMemory(), 500);
    assertNotNull(pa.getPartitionResolver().getClass());
    assertEquals(pa.getPartitionResolver(), partitionResolver);

    assertEquals(regionAttrs.getEntryIdleTimeout().getTimeout(), expiration.getTimeout());
    assertEquals(regionAttrs.getEntryTimeToLive().getTimeout(), expiration.getTimeout());
  }

  /**
   * Tests that a Partitioned Region can be created with a named attributes set programmatically for
   * ExpirationAttributes
   */
  @Test
  public void testPartitionedRegionAttributesForEviction() throws Exception {
    final var redundantCopies = 1;
    var cache = new CacheCreation();
    if (getGemFireVersion().equals(CacheXml.VERSION_6_0)) {
      var rm = new ResourceManagerCreation();
      rm.setCriticalHeapPercentage(95);
      cache.setResourceManagerCreation(rm);
    }
    var attrs = new RegionAttributesCreation(cache);
    attrs.setStatisticsEnabled(true);
    RegionAttributes rootAttrs = null;

    var expiration = new ExpirationAttributes(60, ExpirationAction.DESTROY);

    var partitionResolver = new CacheXMLPartitionResolver();
    var params = new Properties();
    params.setProperty("initial-index-value", "1000");
    params.setProperty("secondary-index-value", "5000");
    partitionResolver.init(params);

    var paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies);
    paf.setTotalMaxMemory(500);
    paf.setLocalMaxMemory(100);
    paf.setPartitionResolver(partitionResolver);

    var fac = new AttributesFactory(attrs);

    // TODO: Move test back to using LRUHeap when config issues have settled
    // if (getGemFireVersion().equals(CacheXml.GEMFIRE_6_0)) {
    // fac.setEvictionAttributes(EvictionAttributes.createLRUHeapAttributes(null,
    // EvictionAction.OVERFLOW_TO_DISK));
    // } else {
    fac.setEvictionAttributes(
        EvictionAttributes.createLRUMemoryAttributes(100, null, EvictionAction.OVERFLOW_TO_DISK));
    // }

    fac.setEntryTimeToLive(expiration);
    fac.setEntryIdleTimeout(expiration);

    var dwaf = new DiskWriteAttributesFactory();
    dwaf.setSynchronous(true);

    fac.setPartitionAttributes(paf.create());
    rootAttrs = fac.create();
    cache.createRegion("parRoot", rootAttrs);

    var r = cache.getRegion("parRoot");
    assertNotNull(r);
    assertEquals(r.getAttributes().getPartitionAttributes().getRedundantCopies(), redundantCopies);
    assertEquals(r.getAttributes().getPartitionAttributes().getLocalMaxMemory(), 100);
    assertEquals(r.getAttributes().getPartitionAttributes().getTotalMaxMemory(), 500);
    assertEquals(r.getAttributes().getPartitionAttributes().getPartitionResolver(),
        partitionResolver);

    assertEquals(r.getAttributes().getEntryIdleTimeout().getTimeout(), expiration.getTimeout());
    assertEquals(r.getAttributes().getEntryTimeToLive().getTimeout(), expiration.getTimeout());

    testXml(cache);

    Cache c = getCache();
    assertNotNull(c);

    Region region = c.getRegion("parRoot");
    assertNotNull(region);

    var regionAttrs = region.getAttributes();
    var pa = regionAttrs.getPartitionAttributes();
    var ea = regionAttrs.getEvictionAttributes();

    assertEquals(pa.getRedundantCopies(), 1);
    assertEquals(pa.getLocalMaxMemory(), 100);
    assertEquals(pa.getTotalMaxMemory(), 500);
    assertNotNull(pa.getPartitionResolver().getClass());
    assertEquals(pa.getPartitionResolver(), partitionResolver);

    assertEquals(regionAttrs.getEntryIdleTimeout().getTimeout(), expiration.getTimeout());
    assertEquals(regionAttrs.getEntryTimeToLive().getTimeout(), expiration.getTimeout());
    // TODO: Move test back to using LRUHeap when config issues have settled
    // if (getGemFireVersion().equals(CacheXml.GEMFIRE_6_0)) {
    // assertIndexDetailsEquals(ea.getAlgorithm(),EvictionAlgorithm.LRU_HEAP);
    // } else {
    assertEquals(ea.getAlgorithm(), EvictionAlgorithm.LRU_MEMORY);
    // }
    assertEquals(ea.getAction(), EvictionAction.OVERFLOW_TO_DISK);
  }

  @Test
  public void testPartitionedRegionAttributesForCoLocation() throws Exception {
    closeCache();
    var cache = new CacheCreation();
    var custAttrs = new RegionAttributesCreation(cache);
    var orderAttrs = new RegionAttributesCreation(cache);
    var custPaf = new PartitionAttributesFactory();
    var orderPaf = new PartitionAttributesFactory();
    custPaf.setRedundantCopies(1);
    custPaf.setTotalMaxMemory(500);
    custPaf.setLocalMaxMemory(100);
    custAttrs.setPartitionAttributes(custPaf.create());
    cache.createRegion("Customer", custAttrs);

    orderPaf.setRedundantCopies(1);
    orderPaf.setTotalMaxMemory(500);
    orderPaf.setLocalMaxMemory(100);
    orderPaf.setColocatedWith("Customer");
    orderAttrs.setPartitionAttributes(orderPaf.create());
    cache.createRegion("Order", orderAttrs);

    testXml(cache);

    Cache c = getCache();
    assertNotNull(c);

    Region cust = c.getRegion(SEPARATOR + "Customer");
    assertNotNull(cust);
    Region order = c.getRegion(SEPARATOR + "Order");
    assertNotNull(order);
    var coLocatedRegion = order.getAttributes().getPartitionAttributes().getColocatedWith();
    assertEquals("Customer", coLocatedRegion);
  }

  @Test
  public void testPartitionedRegionAttributesForCoLocation2() throws Exception {
    closeCache();
    setXmlFile(findFile("coLocation.xml"));
    Cache c = getCache();
    assertNotNull(c);
    Region cust = c.getRegion(SEPARATOR + "Customer");
    assertNotNull(cust);
    Region order = c.getRegion(SEPARATOR + "Order");
    assertNotNull(order);

    assertTrue(cust.getAttributes().getPartitionAttributes().getColocatedWith() == null);
    assertTrue(
        order.getAttributes().getPartitionAttributes().getColocatedWith().equals("Customer"));
  }

  @Test
  public void testPartitionedRegionAttributesForMemLruWithoutMaxMem() throws Exception {
    final var redundantCopies = 1;
    var cache = new CacheCreation();

    var attrs = new RegionAttributesCreation(cache);
    attrs.setStatisticsEnabled(true);
    var paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies);
    paf.setTotalMaxMemory(500);
    paf.setLocalMaxMemory(100);

    var fac = new AttributesFactory(attrs);
    fac.setEvictionAttributes(
        EvictionAttributes.createLRUMemoryAttributes(null, EvictionAction.LOCAL_DESTROY));
    fac.setPartitionAttributes(paf.create());
    cache.createRegion("parRoot", fac.create());

    testXml(cache);

    Cache c = getCache();
    assertNotNull(c);

    Region region = c.getRegion("parRoot");
    assertNotNull(region);

    var regionAttrs = region.getAttributes();
    var pa = regionAttrs.getPartitionAttributes();
    var ea = regionAttrs.getEvictionAttributes();

    assertEquals(pa.getRedundantCopies(), 1);
    assertEquals(pa.getLocalMaxMemory(), 100);
    assertEquals(pa.getTotalMaxMemory(), 500);

    assertEquals(ea.getAlgorithm(), EvictionAlgorithm.LRU_MEMORY);
    assertEquals(ea.getAction(), EvictionAction.LOCAL_DESTROY);
    assertEquals(ea.getMaximum(), pa.getLocalMaxMemory());
  }

  @Test
  public void testPartitionedRegionAttributesForMemLruWithMaxMem() throws Exception {
    final var redundantCopies = 1;
    final var maxMem = 25;
    var cache = new CacheCreation();

    var attrs = new RegionAttributesCreation(cache);
    attrs.setStatisticsEnabled(true);
    var paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies);
    paf.setTotalMaxMemory(500);
    paf.setLocalMaxMemory(100);

    var fac = new AttributesFactory(attrs);
    fac.setEvictionAttributes(
        EvictionAttributes.createLRUMemoryAttributes(maxMem, null, EvictionAction.LOCAL_DESTROY));
    fac.setPartitionAttributes(paf.create());
    cache.createRegion("parRoot", fac.create());

    testXml(cache);

    Cache c = getCache();
    assertNotNull(c);

    Region region = c.getRegion("parRoot");
    assertNotNull(region);

    var regionAttrs = region.getAttributes();
    var pa = regionAttrs.getPartitionAttributes();
    var ea = regionAttrs.getEvictionAttributes();

    assertEquals(pa.getRedundantCopies(), 1);
    assertEquals(pa.getLocalMaxMemory(), 100);
    assertEquals(pa.getTotalMaxMemory(), 500);

    assertEquals(ea.getAlgorithm(), EvictionAlgorithm.LRU_MEMORY);
    assertEquals(ea.getAction(), EvictionAction.LOCAL_DESTROY);
    assertNotSame(ea.getMaximum(), maxMem);
    assertEquals(ea.getMaximum(), pa.getLocalMaxMemory());
  }

  @Test
  public void testReplicatedRegionAttributesForMemLruWithoutMaxMem() throws Exception {
    final var redundantCopies = 1;
    var cache = new CacheCreation();

    var fac = new AttributesFactory();
    fac.setDataPolicy(DataPolicy.REPLICATE);
    fac.setEvictionAttributes(
        EvictionAttributes.createLRUMemoryAttributes(null, EvictionAction.OVERFLOW_TO_DISK));
    cache.createRegion("parRoot", fac.create());

    testXml(cache);

    Cache c = getCache();
    assertNotNull(c);

    Region region = c.getRegion("parRoot");
    assertNotNull(region);

    var regionAttrs = region.getAttributes();
    var ea = regionAttrs.getEvictionAttributes();

    assertEquals(ea.getAlgorithm(), EvictionAlgorithm.LRU_MEMORY);
    assertEquals(ea.getAction(), EvictionAction.OVERFLOW_TO_DISK);
    assertEquals(ea.getMaximum(), EvictionAttributes.DEFAULT_MEMORY_MAXIMUM);
  }

  @Test
  public void testReplicatedRegionAttributesForMemLruWithMaxMem() throws Exception {
    final var redundantCopies = 1;
    final var maxMem = 25;
    var cache = new CacheCreation();

    var fac = new AttributesFactory();
    fac.setDataPolicy(DataPolicy.REPLICATE);
    fac.setEvictionAttributes(EvictionAttributes.createLRUMemoryAttributes(maxMem, null,
        EvictionAction.OVERFLOW_TO_DISK));
    cache.createRegion("parRoot", fac.create());

    testXml(cache);

    Cache c = getCache();
    assertNotNull(c);

    Region region = c.getRegion("parRoot");
    assertNotNull(region);

    var regionAttrs = region.getAttributes();
    var ea = regionAttrs.getEvictionAttributes();
    assertEquals(ea.getAlgorithm(), EvictionAlgorithm.LRU_MEMORY);
    assertEquals(ea.getAction(), EvictionAction.OVERFLOW_TO_DISK);
    assertEquals(ea.getMaximum(), maxMem);
  }

  /**
   * Tests the groups subelement on bridge-server.
   */
  @Test
  public void testDefaultCacheServerGroups() throws Exception {
    var cache = new CacheCreation();
    var bs = cache.addCacheServer();
    bs.setPort(AvailablePortHelper.getRandomAvailableTCPPort());
    bs.setGroups(CacheServer.DEFAULT_GROUPS);
    testXml(cache);
    Cache c = getCache();
    assertNotNull(c);
    var server = cache.getCacheServers().iterator().next();
    assertNotNull(server);
    assertEquals(CacheServer.DEFAULT_GROUPS, server.getGroups());
  }

  @Test
  public void testOneCacheServerGroups() throws Exception {
    var cache = new CacheCreation();
    var bs = cache.addCacheServer();
    bs.setPort(AvailablePortHelper.getRandomAvailableTCPPort());
    var groups = new String[] {"group1"};
    bs.setGroups(groups);
    testXml(cache);
    Cache c = getCache();
    assertNotNull(c);
    var server = cache.getCacheServers().iterator().next();
    assertNotNull(server);
    assertEquals(Arrays.asList(groups), Arrays.asList(server.getGroups()));
  }

  @Test
  public void testTwoCacheServerGroups() throws Exception {
    var cache = new CacheCreation();
    var bs = cache.addCacheServer();
    bs.setPort(AvailablePortHelper.getRandomAvailableTCPPort());
    var groups = new String[] {"group1", "group2"};
    bs.setGroups(groups);
    testXml(cache);
    Cache c = getCache();
    assertNotNull(c);
    var server = cache.getCacheServers().iterator().next();
    assertNotNull(server);
    assertEquals(Arrays.asList(groups), Arrays.asList(server.getGroups()));
  }

  @Test
  public void testDefaultCacheServerBindAddress() throws Exception {
    var cache = new CacheCreation();
    var bs = cache.addCacheServer();
    bs.setPort(AvailablePortHelper.getRandomAvailableTCPPort());
    testXml(cache);
    Cache c = getCache();
    assertNotNull(c);
    var server = cache.getCacheServers().iterator().next();
    assertNotNull(server);
    assertEquals(CacheServer.DEFAULT_BIND_ADDRESS, server.getBindAddress());
  }

  @Test
  public void testCacheServerBindAddress() throws Exception {
    var cache = new CacheCreation();
    var bs = cache.addCacheServer();
    bs.setPort(AvailablePortHelper.getRandomAvailableTCPPort());
    final var BA = ALIAS1;
    bs.setBindAddress(BA);
    testXml(cache);
    Cache c = getCache();
    assertNotNull(c);
    var server = cache.getCacheServers().iterator().next();
    assertNotNull(server);
    assertEquals(BA, server.getBindAddress());
  }

  @Test
  public void testCacheServerHostnameForClients() throws Exception {
    var cache = new CacheCreation();
    var bs = cache.addCacheServer();
    bs.setPort(AvailablePortHelper.getRandomAvailableTCPPort());
    final var BA = ALIAS1;
    bs.setBindAddress(BA);
    bs.setHostnameForClients("clientHostName");
    testXml(cache);
    Cache c = getCache();
    assertNotNull(c);
    var server = cache.getCacheServers().iterator().next();
    assertNotNull(server);
    assertEquals(BA, server.getBindAddress());
    assertEquals("clientHostName", server.getHostnameForClients());
  }

  @Test
  public void testTwoConnectionPools() throws Exception {
    getSystem();
    var cache = new CacheCreation();
    var f = cache.createPoolFactory();
    f.addLocator(ALIAS2, 3777).create("mypool");
    f.reset().addLocator(ALIAS1, 3888).create("mypool2");
    try {
      f.create("mypool");
      fail("expected IllegalStateException");
    } catch (IllegalStateException ignored) {
    }
    try {
      f.create("mypool2");
      fail("expected IllegalStateException");
    } catch (IllegalStateException ignored) {
    }
    var attrs = new RegionAttributesCreation(cache);
    attrs.setPoolName("mypool");
    cache.createVMRegion("rootNORMAL", attrs);
    testXml(cache);
    Cache c = getCache();
    assertNotNull(c);
    Region r = c.getRegion("rootNORMAL");
    assertNotNull(r);
    assertEquals("mypool", r.getAttributes().getPoolName());
    var cp = PoolManager.find("mypool");
    assertNotNull(cp);
    assertEquals(0, cp.getServers().size());
    assertEquals(1, cp.getLocators().size());
    assertEquals(createINSA(ALIAS2, 3777), cp.getLocators().get(0));
    cp = PoolManager.find("mypool2");
    assertNotNull(cp);
    assertEquals(0, cp.getServers().size());
    assertEquals(1, cp.getLocators().size());
    assertEquals(createINSA(ALIAS1, 3888), cp.getLocators().get(0));
  }

  @Test
  public void testNoConnectionPools() throws Exception {
    var cache = new CacheCreation();
    var attrs = new RegionAttributesCreation(cache);
    attrs.setPoolName("mypool");
    cache.createVMRegion("rootNORMAL", attrs);
    var expectedException = addIgnoredException(
        String.format("The connection pool %s has not been created",
            "mypool"));
    try {
      testXml(cache);
      fail("expected IllegalStateException");
    } catch (IllegalStateException ignored) {
    } finally {
      expectedException.remove();
    }
  }

  @Test
  public void testAlreadyExistingPool() throws Exception {
    getSystem();
    var f = (PoolFactoryImpl) PoolManager.createFactory();
    f.setStartDisabled(true).addLocator(ALIAS2, 12345).create("mypool");
    try {
      // now make sure declarative cache can't create the same pool
      var cache = new CacheCreation();
      cache.createPoolFactory().addLocator(ALIAS2, 12345).create("mypool");
      var expectedException = addIgnoredException(
          String.format("A pool named %s already exists", "mypool"));
      try {
        testXml(cache);
        fail("expected IllegalStateException");
      } catch (IllegalStateException ignored) {
      } finally {
        expectedException.remove();
      }
    } finally {
      PoolManager.close();
    }
  }

  @Test
  public void testDynamicRegionFactoryConnectionPool() throws Exception {
    addIgnoredException("Connection reset");
    addIgnoredException("SocketTimeoutException");
    addIgnoredException("ServerConnectivityException");
    addIgnoredException("Socket Closed");
    getSystem();
    var vm0 = Host.getHost(0).getVM(0);
    final var port = getRandomAvailableTCPPort();
    vm0.invoke(new SerializableCallable("Create cache server") {
      @Override
      public Object call() throws IOException {
        DynamicRegionFactory.get().open();
        Cache cache = getCache();
        var bridge = cache.addCacheServer();
        bridge.setPort(port);
        bridge.setNotifyBySubscription(true);
        bridge.start();
        return null;
      }
    });
    var cache = new CacheCreation();
    cache.createPoolFactory().addServer(NetworkUtils.getServerHostName(vm0.getHost()), port)
        .setSubscriptionEnabled(true).create("connectionPool");
    cache.setDynamicRegionFactoryConfig(
        new DynamicRegionFactory.Config(null, "connectionPool", false, false));
    var attrs = new RegionAttributesCreation(cache);
    cache.createRegion("root", attrs);
    // note that testXml can't check if they are same because enabling
    // dynamic regions causes a meta region to be produced.
    testXml(cache, false);
    assertEquals(false, DynamicRegionFactory.get().getConfig().getRegisterInterest());
    assertEquals(false, DynamicRegionFactory.get().getConfig().getPersistBackup());
    assertEquals(true, DynamicRegionFactory.get().isOpen());
    assertEquals(null, DynamicRegionFactory.get().getConfig().getDiskDir());
    assertEquals("connectionPool", DynamicRegionFactory.get().getConfig().getPoolName());
    Region dr = getCache().getRegion("__DynamicRegions");
    if (dr != null) {
      dr.localDestroyRegion();
    }
  }

  /**
   * Tests the client subscription attributes ({@code eviction-policy}, {@code capacity} and
   * {@code overflow-directory}) related to client subscription config in gemfire cache-server
   * framework
   *
   * @since GemFire 5.7
   */
  @Test
  public void testBridgeAttributesRelatedToHAOverFlow() throws Exception {
    var cache = new CacheCreation();
    cache.setMessageSyncInterval(3445);
    var bs = cache.addCacheServer();
    var csc = bs.getClientSubscriptionConfig();
    csc.setEvictionPolicy("entry");
    cache.getLogger().config("EvictionPolicy : " + csc.getEvictionPolicy());
    csc.setCapacity(501);
    cache.getLogger().config("EvictionCapacity : " + csc.getCapacity());
    csc.setOverflowDirectory("overFlow");
    cache.getLogger().config("EvictionOverflowDirectory : " + csc.getOverflowDirectory());
    bs.setPort(AvailablePortHelper.getRandomAvailableTCPPort());
    var attrs = new RegionAttributesCreation(cache);
    attrs.setDataPolicy(DataPolicy.NORMAL);
    cache.createVMRegion("rootNORMAL", attrs);
    testXml(cache);
    Cache c = getCache();
    assertNotNull(c);
    var server = cache.getCacheServers().iterator().next();
    assertNotNull(server);
    var chaqf = server.getClientSubscriptionConfig();
    assertEquals("entry", chaqf.getEvictionPolicy());
    assertEquals(501, chaqf.getCapacity());
    assertEquals("overFlow", chaqf.getOverflowDirectory());
  }

  @Test
  public void testBridgeLoadProbe() throws Exception {
    var cache = new CacheCreation();
    var server = cache.addCacheServer();
    server.setPort(AvailablePortHelper.getRandomAvailableTCPPort());
    server.setLoadProbe(new MyLoadProbe());

    testXml(cache);

    Cache c = getCache();
    server = c.getCacheServers().get(0);
    assertEquals(MyLoadProbe.class, server.getLoadProbe().getClass());
  }

  @Test
  public void testLoadPollInterval() throws Exception {
    var cache = new CacheCreation();
    var server = cache.addCacheServer();
    server.setPort(AvailablePortHelper.getRandomAvailableTCPPort());
    server.setLoadPollInterval(12345);

    testXml(cache);

    Cache c = getCache();
    server = c.getCacheServers().get(0);
    assertEquals(12345, server.getLoadPollInterval());
  }

  public static class MyLoadProbe extends ServerLoadProbeAdapter implements Declarable {
    @Override
    public ServerLoad getLoad(ServerMetrics metrics) {
      return null;
    }

    @Override
    public void init(Properties props) {}

    @Override
    public boolean equals(Object o) {
      return o instanceof MyLoadProbe;
    }
  }

  public static class Expiry1 implements CustomExpiry, Declarable {
    @Override
    public ExpirationAttributes getExpiry(Entry entry) {
      return null;
    }

    @Override
    public void init(Properties props) {}

    @Override
    public void close() {}
  }

  public static class Expiry2 implements CustomExpiry, Declarable {
    @Override
    public ExpirationAttributes getExpiry(Entry entry) {
      return null;
    }

    @Override
    public void init(Properties props) {}

    @Override
    public void close() {}
  }

  public static class Expiry3 implements CustomExpiry, Declarable {
    @Override
    public ExpirationAttributes getExpiry(Entry entry) {
      return null;
    }

    @Override
    public void init(Properties props) {}

    @Override
    public void close() {}
  }

  public static class Expiry4 implements CustomExpiry, Declarable {
    @Override
    public ExpirationAttributes getExpiry(Entry entry) {
      return null;
    }

    @Override
    public void init(Properties props) {}

    @Override
    public void close() {}
  }

  public static class Expiry5 implements CustomExpiry, Declarable2 {
    @Override
    public ExpirationAttributes getExpiry(Entry entry) {
      return null;
    }

    @Override
    public void init(Properties props) {}

    @Override
    public void close() {}

    @Override
    public Properties getConfig() {
      var p = new Properties();
      p.put("prop1", "val1");
      p.put("prop2", "val2");
      return p;
    }
  }

  /**
   * Test both customEntryIdleTime and customEntryTimeToLife
   */
  @Test
  public void testCustomEntryXml() throws Exception {
    var cache = new CacheCreation();

    var attrs = new RegionAttributesCreation(cache);
    attrs.setScope(Scope.DISTRIBUTED_NO_ACK);

    var root = (RegionCreation) cache.createRegion("root", attrs);

    {
      attrs = new RegionAttributesCreation(cache);
      attrs.setScope(Scope.DISTRIBUTED_NO_ACK);
      attrs.setInitialCapacity(142);
      attrs.setLoadFactor(42.42f);
      attrs.setStatisticsEnabled(true);
      attrs.setCustomEntryIdleTimeout(new Expiry1());
      attrs.setCustomEntryTimeToLive(new Expiry5());

      root.createSubregion("one", attrs);
    }

    {
      attrs = new RegionAttributesCreation(cache);
      attrs.setScope(Scope.DISTRIBUTED_ACK);
      attrs.setInitialCapacity(242);
      attrs.setStatisticsEnabled(true);
      attrs.setCustomEntryIdleTimeout(new Expiry2());

      var region = root.createSubregion("two", attrs);

      {
        attrs = new RegionAttributesCreation(cache);
        attrs.setScope(Scope.DISTRIBUTED_ACK);
        attrs.setLoadFactor(43.43f);
        attrs.setStatisticsEnabled(true);
        attrs.setCustomEntryIdleTimeout(new Expiry3());
        attrs.setCustomEntryTimeToLive(new Expiry4());

        region.createSubregion("three", attrs);
      }
    }

    testXml(cache);
  }

  @Test
  public void testPreloadDataPolicy() throws Exception {
    var cache = new CacheCreation();

    {
      var attrs = new RegionAttributesCreation(cache);
      attrs.setDataPolicy(DataPolicy.NORMAL);
      cache.createRegion("rootNORMAL", attrs);
    }
    {
      var attrs = new RegionAttributesCreation(cache);
      attrs.setDataPolicy(DataPolicy.NORMAL);
      attrs.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
      cache.createRegion("rootNORMAL_ALL", attrs);
    }
    {
      var attrs = new RegionAttributesCreation(cache);
      attrs.setMirrorType(MirrorType.KEYS_VALUES);
      cache.createRegion("rootREPLICATE", attrs);
    }
    {
      var attrs = new RegionAttributesCreation(cache);
      attrs.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
      cache.createRegion("rootPERSISTENT_REPLICATE", attrs);
    }
    {
      var attrs = new RegionAttributesCreation(cache);
      attrs.setDataPolicy(DataPolicy.EMPTY);
      cache.createRegion("rootEMPTY", attrs);
    }
    {
      var attrs = new RegionAttributesCreation(cache);
      attrs.setDataPolicy(DataPolicy.EMPTY);
      attrs.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
      cache.createRegion("rootEMPTY_ALL", attrs);
    }
    {
      var attrs = new RegionAttributesCreation(cache);
      attrs.setDataPolicy(DataPolicy.PRELOADED);
      attrs.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
      cache.createRegion("rootPRELOADED_ALL", attrs);
    }

    testXml(cache);
  }

  /**
   * Test EnableSubscriptionConflation region attribute
   *
   * @since GemFire 5.7
   */
  @Test
  public void testEnableSubscriptionConflationAttribute() throws Exception {
    var cache = new CacheCreation();
    var attrs = new RegionAttributesCreation(cache);
    attrs.setEnableSubscriptionConflation(true);
    cache.createRegion("root", attrs);
    testXml(cache);
    assertEquals(true, cache.getRegion("root").getAttributes().getEnableSubscriptionConflation());
  }

  /**
   * Tests that a region created with a named attributes has the correct attributes.
   */
  @Test
  public void testPartitionedRegionXML() throws Exception {
    setXmlFile(findFile("partitionedRegion51.xml"));
    final var regionName = "pRoot";

    Cache cache = getCache();
    Region region = cache.getRegion(regionName);
    assertNotNull(region);

    var host = Host.getHost(0);
    final var vm0 = host.getVM(0);
    final var vm1 = host.getVM(1);

    var init =
        new CacheSerializableRunnable("initUsingPartitionedRegionXML") {
          @Override
          public void run2() throws CacheException {
            final Cache c;
            try {
              lonerDistributedSystem = false;
              c = getCache();
            } finally {
              lonerDistributedSystem = true;
            }
            Region r = c.getRegion(regionName);
            assertNotNull(r);
            var attrs = r.getAttributes();
            assertNotNull(attrs.getPartitionAttributes());

            var pa = attrs.getPartitionAttributes();
            assertEquals(pa.getRedundantCopies(), 1);
            assertEquals(pa.getLocalMaxMemory(), 32);
            assertEquals(pa.getTotalMaxMemory(), 96);
            assertEquals(pa.getTotalNumBuckets(), 119);

            r = c.getRegion("bug37905");
            assertTrue("region should have been an instance of PartitionedRegion but was not",
                r instanceof PartitionedRegion);
          }
        };

    init.run2();
    vm0.invoke(init);
    vm1.invoke(init);
    vm0.invoke(new CacheSerializableRunnable("putUsingPartitionedRegionXML1") {
      @Override
      public void run2() throws CacheException {
        final var val = "prValue0";
        final Integer key = 10;
        Cache c = getCache();
        Region r = c.getRegion(regionName);
        assertNotNull(r);
        r.put(key, val);
        assertEquals(val, r.get(key));
      }
    });
    vm1.invoke(new CacheSerializableRunnable("putUsingPartitionedRegionXML2") {
      @Override
      public void run2() throws CacheException {
        final var val = "prValue1";
        final Integer key = 14;
        Cache c = getCache();
        Region r = c.getRegion(regionName);
        assertNotNull(r);
        r.put(key, val);
        assertEquals(val, r.get(key));
      }
    });
  }

  /**
   * Tests the {@code message-sync-interval} attribute of attribute is related to HA of
   * client-queues in gemfire ca framework. This attribute is the frequency at which a messent by
   * the primary cache-server node to all the secondary cache-server nodes to remove the events
   * which have already been dispatched from the queue
   */
  @Test
  public void testMessageSyncInterval() throws Exception {
    var cache = new CacheCreation();
    cache.setMessageSyncInterval(123);
    var attrs = new RegionAttributesCreation(cache);
    attrs.setDataPolicy(DataPolicy.NORMAL);
    cache.createVMRegion("rootNORMAL", attrs);
    testXml(cache);
    Cache c = getCache();
    assertNotNull(c);
    assertEquals(123, c.getMessageSyncInterval());
  }

  /**
   * Tests the bridge-server attributes ({@code maximum-message-count} and
   * {@code message-time-to-live}) related to HA of client-queues in gemfire cache-server framework
   */
  @Test
  public void testBridgeAttributesRelatedToClientQueuesHA() throws Exception {
    var cache = new CacheCreation();
    cache.setMessageSyncInterval(3445);
    var bs = cache.addCacheServer();
    bs.setMaximumMessageCount(12345);
    bs.setMessageTimeToLive(56789);
    bs.setPort(AvailablePortHelper.getRandomAvailableTCPPort());
    var attrs = new RegionAttributesCreation(cache);
    attrs.setDataPolicy(DataPolicy.NORMAL);
    cache.createVMRegion("rootNORMAL", attrs);
    testXml(cache);
    Cache c = getCache();
    assertNotNull(c);
    var server = cache.getCacheServers().iterator().next();
    assertNotNull(server);
    assertEquals(12345, server.getMaximumMessageCount());
    assertEquals(56789, server.getMessageTimeToLive());
  }

  /**
   * Tests that a region created with a named attributes has the correct attributes.
   *
   * This tests currently fails due to (what seem to me as) limitations in the XML generator and the
   * comparison of the XML. I have run this test by hand and looked at the generated XML and there
   * were no significant problems, however because of the limitations, I am disabling this test, but
   * leaving the functionality for future comparisons (by hand of course).
   */
  @Test
  public void testPartitionedRegionInstantiation() {
    var cache = new CacheCreation();
    var attrs = new RegionAttributesCreation(cache);

    var paf = new PartitionAttributesFactory();

    paf.setLocalMaxMemory(4).setTotalNumBuckets(17).setTotalMaxMemory(8);
    attrs.setPartitionAttributes(paf.create());
    cache.createRegion("pRoot", attrs);
  }

  /**
   * Tests the bridge-server attributes ({@code max-threads}
   */
  @Test
  public void testBridgeMaxThreads() throws Exception {
    var cache = new CacheCreation();

    var bs = cache.addCacheServer();
    bs.setMaxThreads(37);
    bs.setMaxConnections(999);
    bs.setPort(AvailablePortHelper.getRandomAvailableTCPPort());
    var attrs = new RegionAttributesCreation(cache);
    attrs.setDataPolicy(DataPolicy.NORMAL);
    cache.createVMRegion("rootNORMAL", attrs);
    testXml(cache);
  }

  /**
   * Tests that loading cache XML with multi-cast set will set the multi-cast
   */
  @Test
  public void testRegionMulticastSetViaCacheXml() throws Exception {
    final var rNameBase = getUniqueName();
    final var r1 = rNameBase + "1";
    final var r2 = rNameBase + "2";
    final var r3 = rNameBase + "3";

    // Setting multi-cast via nested region attributes
    var creation = new CacheCreation();
    var attrs = new RegionAttributesCreation(creation);
    attrs.setScope(Scope.LOCAL);
    attrs.setEarlyAck(false);
    attrs.setMulticastEnabled(true);
    creation.createRegion(r1, attrs);

    // Setting multi-cast via named region attributes
    final var attrId = "region_attrs_with_multicast";
    attrs = new RegionAttributesCreation(creation);
    attrs.setId(attrId);
    attrs.setScope(Scope.DISTRIBUTED_NO_ACK);
    attrs.setEarlyAck(false);
    attrs.setMulticastEnabled(true);
    creation.setRegionAttributes(attrs.getId(), attrs);
    attrs = new RegionAttributesCreation(creation);
    attrs.setRefid(attrId);
    creation.createRegion(r3, attrs);

    testXml(creation);

    creation = new CacheCreation();
    attrs = new RegionAttributesCreation(creation);
    attrs.setScope(Scope.DISTRIBUTED_ACK);
    attrs.setEarlyAck(false);
    attrs.setMulticastEnabled(true);
    creation.createRegion(r2, attrs);

    Cache c = getCache();
    assertTrue(c instanceof GemFireCacheImpl);
    c.loadCacheXml(generate(creation));

    {
      Region reg1 = c.getRegion(r1);
      assertNotNull(reg1);
      assertEquals(Scope.LOCAL, reg1.getAttributes().getScope());
      assertFalse(reg1.getAttributes().getEarlyAck());
      assertTrue(reg1.getAttributes().getMulticastEnabled());
    }

    {
      Region reg2 = c.getRegion(r2);
      assertNotNull(reg2);
      assertEquals(Scope.DISTRIBUTED_ACK, reg2.getAttributes().getScope());
      assertFalse(reg2.getAttributes().getEarlyAck());
      assertTrue(reg2.getAttributes().getMulticastEnabled());
    }

    {
      Region reg3 = c.getRegion(r3);
      assertNotNull(reg3);
      assertEquals(Scope.DISTRIBUTED_NO_ACK, reg3.getAttributes().getScope());
      assertFalse(reg3.getAttributes().getEarlyAck());
      assertTrue(reg3.getAttributes().getMulticastEnabled());
    }
  }

  @Test
  public void testRollOplogs() throws Exception {
    var cache = new CacheCreation();
    // Set properties for Asynch writes

    var attrs = new RegionAttributesCreation(cache);

    var root = (RegionCreation) cache.createRegion("root", attrs);

    {
      attrs = new RegionAttributesCreation(cache);
      var dwaf = new DiskWriteAttributesFactory();
      dwaf.setSynchronous(true);
      dwaf.setRollOplogs(true);
      attrs.setDiskWriteAttributes(dwaf.create());
      root.createSubregion("sync", attrs);
    }

    {
      attrs = new RegionAttributesCreation(cache);
      var dwaf = new DiskWriteAttributesFactory();
      dwaf.setTimeInterval(123L);
      dwaf.setBytesThreshold(456L);
      dwaf.setRollOplogs(false);
      attrs.setDiskWriteAttributes(dwaf.create());
      root.createSubregion("async", attrs);
    }

    testXml(cache);
  }

  @Test
  public void testMaxOplogSize() throws Exception {
    var cache = new CacheCreation();
    // Set properties for Asynch writes

    var attrs = new RegionAttributesCreation(cache);

    var root = (RegionCreation) cache.createRegion("root", attrs);

    {
      attrs = new RegionAttributesCreation(cache);
      var dwaf = new DiskWriteAttributesFactory();
      dwaf.setSynchronous(true);
      dwaf.setMaxOplogSize(1);
      attrs.setDiskWriteAttributes(dwaf.create());
      root.createSubregion("sync", attrs);
    }

    {
      attrs = new RegionAttributesCreation(cache);
      var dwaf = new DiskWriteAttributesFactory();
      dwaf.setTimeInterval(123L);
      dwaf.setBytesThreshold(456L);
      dwaf.setMaxOplogSize(1);
      attrs.setDiskWriteAttributes(dwaf.create());
      root.createSubregion("async", attrs);
    }

    testXml(cache);
  }

  @Test
  public void testDataPolicy() throws Exception {
    var cache = new CacheCreation();

    {
      var attrs = new RegionAttributesCreation(cache);
      attrs.setDataPolicy(DataPolicy.NORMAL);
      cache.createRegion("rootNORMAL", attrs);
    }
    {
      var attrs = new RegionAttributesCreation(cache);
      attrs.setDataPolicy(DataPolicy.NORMAL);
      attrs.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
      cache.createRegion("rootNORMAL_ALL", attrs);
    }
    {
      var attrs = new RegionAttributesCreation(cache);
      attrs.setMirrorType(MirrorType.KEYS_VALUES);
      cache.createRegion("rootREPLICATE", attrs);
    }
    {
      var attrs = new RegionAttributesCreation(cache);
      attrs.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
      cache.createRegion("rootPERSISTENT_REPLICATE", attrs);
    }
    {
      var attrs = new RegionAttributesCreation(cache);
      attrs.setDataPolicy(DataPolicy.EMPTY);
      cache.createRegion("rootEMPTY", attrs);
    }
    {
      var attrs = new RegionAttributesCreation(cache);
      attrs.setDataPolicy(DataPolicy.EMPTY);
      attrs.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
      cache.createRegion("rootEMPTY_ALL", attrs);
    }

    testXml(cache);
  }

  /**
   * These properties, if any, will be added to the properties used for getSystem calls
   */
  protected Properties xmlProps = null;

  @Override
  public Properties getDistributedSystemProperties() {
    var props = super.getDistributedSystemProperties();
    if (xmlProps != null) {
      for (final var objectObjectEntry : xmlProps.entrySet()) {
        var entry = (Map.Entry) objectObjectEntry;
        var key = (String) entry.getKey();
        var value = (String) entry.getValue();
        props.setProperty(key, value);
      }
    }
    return props;
  }

  /**
   * Test xml support of MembershipAttributes.
   */
  @Test
  public void testMembershipAttributes() throws Exception {
    final var MY_ROLES = "Foo, Bip, BAM";
    final var roles = new String[][] {{"Foo"}, {"Bip", "BAM"}};

    final var policies =
        (LossAction[]) LossAction.VALUES.toArray(new LossAction[LossAction.VALUES.size()]);

    final var actions = (ResumptionAction[]) ResumptionAction.VALUES
        .toArray(new ResumptionAction[ResumptionAction.VALUES.size()]);

    var cache = new CacheCreation();

    // for each policy, try each action and each role...
    for (var policy = 0; policy < policies.length; policy++) {
      for (var action = 0; action < actions.length; action++) {
        for (var role = 0; role < roles.length; role++) {
          var theRoles = roles[role];
          var thePolicy = policies[policy];
          var theAction = actions[action];

          // if (theRoles.length == 0 && (thePolicy != LossAction.NONE || theAction !=
          // ResumptionAction.NONE

          var attrs = new RegionAttributesCreation(cache);
          var ra = new MembershipAttributes(theRoles, thePolicy, theAction);
          attrs.setMembershipAttributes(ra);
          var region = "rootMEMBERSHIP_ATTRIBUTES_" + policy + "_" + action + "_" + role;
          cache.createRegion(region, attrs);
        }
      }
    }

    {
      // make our system play the roles used by this test so the create regions
      // will not think the a required role is missing
      var config = new Properties();
      config.setProperty(ROLES, MY_ROLES);
      xmlProps = config;
    }
    DistributedRegion.ignoreReconnect = true;
    try {
      testXml(cache);
    } finally {
      xmlProps = null;
      try {
        preTearDown();
      } finally {
        DistributedRegion.ignoreReconnect = false;
      }
    }
  }

  /**
   * Tests multiple cache listeners on one region
   *
   * @since GemFire 5.0
   */
  @Test
  public void testMultipleCacheListener() throws Exception {
    var cache = new CacheCreation();
    var attrs = new RegionAttributesCreation(cache);

    CacheListener l1 = new MyTestCacheListener();
    CacheListener l2 = new MySecondTestCacheListener();
    attrs.addCacheListener(l1);
    attrs.addCacheListener(l2);

    cache.createRegion("root", attrs);

    testXml(cache);
    {
      Cache c = getCache();
      Region r = c.getRegion("root");
      assertEquals(Arrays.asList(l1, l2),
          Arrays.asList(r.getAttributes().getCacheListeners()));
      var am = r.getAttributesMutator();
      am.removeCacheListener(l2);
      assertEquals(Arrays.asList(l1),
          Arrays.asList(r.getAttributes().getCacheListeners()));
      am.removeCacheListener(l1);
      assertEquals(Arrays.asList(),
          Arrays.asList(r.getAttributes().getCacheListeners()));
      am.addCacheListener(l1);
      assertEquals(Arrays.asList(l1),
          Arrays.asList(r.getAttributes().getCacheListeners()));
      am.addCacheListener(l1);
      assertEquals(Arrays.asList(l1),
          Arrays.asList(r.getAttributes().getCacheListeners()));
      am.addCacheListener(l2);
      assertEquals(Arrays.asList(l1, l2),
          Arrays.asList(r.getAttributes().getCacheListeners()));
      am.removeCacheListener(l1);
      assertEquals(Arrays.asList(l2),
          Arrays.asList(r.getAttributes().getCacheListeners()));
      am.removeCacheListener(l1);
      assertEquals(Arrays.asList(l2),
          Arrays.asList(r.getAttributes().getCacheListeners()));
      am.initCacheListeners(new CacheListener[] {l1, l2});
      assertEquals(Arrays.asList(l1, l2),
          Arrays.asList(r.getAttributes().getCacheListeners()));
    }
  }

  /**
   * A {@code CacheListener} that is {@code Declarable}, but not {@code Declarable2}.
   */
  public static class MySecondTestCacheListener extends TestCacheListener implements Declarable {

    @Override
    public void init(Properties props) {}

    @Override
    public boolean equals(Object o) {
      return o instanceof MySecondTestCacheListener;
    }
  }

  @Test
  public void testHeapLRUEviction() throws Exception {
    final var name = getUniqueName();
    beginCacheXml();
    var factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    var ev =
        EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK);
    factory.setEvictionAttributes(ev);
    // RegionAttributes atts = factory.create();
    createRegion(name, factory.create());
    finishCacheXml(temporaryFolder.getRoot(), getUniqueName(), getUseSchema(),
        getGemFireVersion());
    Region r = getRootRegion().getSubregion(name);

    var hlea = r.getAttributes().getEvictionAttributes();
    assertEquals(EvictionAction.OVERFLOW_TO_DISK, hlea.getAction());
  }

  /**
   * Tests multiple transaction listeners
   *
   * @since GemFire 5.0
   */
  @Test
  public void testMultipleTXListener() throws Exception {
    var cache = new CacheCreation();
    var txMgrCreation = new CacheTransactionManagerCreation();
    TransactionListener l1 = new MyTestTransactionListener();
    TransactionListener l2 = new MySecondTestTransactionListener();
    txMgrCreation.addListener(l1);
    txMgrCreation.addListener(l2);
    cache.addCacheTransactionManagerCreation(txMgrCreation);
    testXml(cache);
    {
      var tm = getCache().getCacheTransactionManager();
      assertEquals(Arrays.asList(l1, l2),
          Arrays.asList(tm.getListeners()));
      tm.removeListener(l2);
      assertEquals(Arrays.asList(l1), Arrays.asList(tm.getListeners()));
      tm.removeListener(l1);
      assertEquals(Arrays.asList(), Arrays.asList(tm.getListeners()));
      tm.addListener(l1);
      assertEquals(Arrays.asList(l1), Arrays.asList(tm.getListeners()));
      tm.addListener(l1);
      assertEquals(Arrays.asList(l1), Arrays.asList(tm.getListeners()));
      tm.addListener(l2);
      assertEquals(Arrays.asList(l1, l2),
          Arrays.asList(tm.getListeners()));
      tm.removeListener(l1);
      assertEquals(Arrays.asList(l2), Arrays.asList(tm.getListeners()));
      tm.removeListener(l1);
      assertEquals(Arrays.asList(l2), Arrays.asList(tm.getListeners()));
      tm.initListeners(new TransactionListener[] {l1, l2});
      assertEquals(Arrays.asList(l1, l2),
          Arrays.asList(tm.getListeners()));
    }
  }

  /**
   * A {@code TransactionListener} that is {@code Declarable}, but not {@code Declarable2}.
   */
  public static class MySecondTestTransactionListener extends TestTransactionListener
      implements Declarable {

    @Override
    public void init(Properties props) {}

    @Override
    public boolean equals(Object o) {
      return o instanceof MySecondTestTransactionListener;
    }
  }

  private void setBridgeAttributes(CacheServer bridge1) {
    bridge1.setPort(0);
    bridge1.setMaxConnections(100);
    bridge1.setMaximumTimeBetweenPings(12345);
    bridge1.setNotifyBySubscription(true);
    bridge1.setSocketBufferSize(98765);
  }

  /**
   * Tests that named region attributes are registered when the cache is created.
   */
  @Test
  public void testRegisteringNamedRegionAttributes() throws Exception {
    var cache = new CacheCreation();
    RegionAttributesCreation attrs;

    var id1 = "id1";
    attrs = new RegionAttributesCreation(cache);
    attrs.setScope(Scope.DISTRIBUTED_ACK);
    attrs.setMirrorType(MirrorType.KEYS);
    cache.setRegionAttributes(id1, attrs);

    var id2 = "id2";
    attrs = new RegionAttributesCreation(cache);
    attrs.setScope(Scope.DISTRIBUTED_NO_ACK);
    attrs.setMirrorType(MirrorType.KEYS_VALUES);
    attrs.setConcurrencyLevel(15);
    cache.setRegionAttributes(id2, attrs);

    var id3 = "id3";
    attrs = new RegionAttributesCreation(cache);
    attrs.setScope(Scope.LOCAL);
    attrs.setValueConstraint(Integer.class);
    cache.setRegionAttributes(id3, attrs);

    testXml(cache);
  }

  /**
   * Tests that a region created with a named attributes has the correct attributes.
   */
  @Test
  public void testNamedAttributes() throws Exception {
    setXmlFile(findFile("namedAttributes.xml"));

    Class keyConstraint = String.class;
    Class valueConstraint = Integer.class;
    var id = "id1";
    var regionName = "root";

    Cache cache = getCache();
    RegionAttributes attrs = cache.getRegionAttributes(id);
    assertEquals(keyConstraint, attrs.getKeyConstraint());
    assertEquals(valueConstraint, attrs.getValueConstraint());
    assertEquals(45, attrs.getEntryIdleTimeout().getTimeout());
    assertEquals(ExpirationAction.INVALIDATE, attrs.getEntryIdleTimeout().getAction());

    Region region = cache.getRegion(regionName);
    assertNotNull(region);

    attrs = region.getAttributes();
    assertEquals(keyConstraint, attrs.getKeyConstraint());
    assertEquals(valueConstraint, attrs.getValueConstraint());
    assertEquals(45, attrs.getEntryIdleTimeout().getTimeout());
    assertEquals(ExpirationAction.INVALIDATE, attrs.getEntryIdleTimeout().getAction());

    // Make sure that attributes can be "overridden"
    var subregion = region.getSubregion("subregion");
    assertNotNull(subregion);

    attrs = subregion.getAttributes();
    assertEquals(keyConstraint, attrs.getKeyConstraint());
    assertEquals(Long.class, attrs.getValueConstraint());
    assertEquals(90, attrs.getEntryIdleTimeout().getTimeout());
    assertEquals(ExpirationAction.DESTROY, attrs.getEntryIdleTimeout().getAction());

    // Make sure that a named region attributes used in a region
    // declaration is registered
    assertNotNull(cache.getRegionAttributes("id2"));
  }

  /**
   * Tests that trying to parse an XML file that declares a region whose attributes refer to an
   * unknown named region attributes throws an {@link IllegalStateException}.
   */
  @Test
  public void testUnknownNamedAttributes() throws Exception {
    setXmlFile(findFile("unknownNamedAttributes.xml"));

    var expectedException = addIgnoredException(
        "Cannot reference non-existing region attributes named");
    try {
      getCache();
      fail("Should have thrown an IllegalStateException");

    } catch (IllegalStateException ex) {
      // pass...
    } finally {
      expectedException.remove();
    }
  }

  /**
   * Tests to make sure that we cannot create the same region multiple times in a {@code cache.xml}
   * file.
   */
  @Test
  public void testCreateSameRegionTwice() throws Exception {
    var cache = new CacheCreation();
    var attrs = new RegionAttributesCreation(cache);
    var name = "root";

    cache.createRegion(name, attrs);

    try {
      cache.createRegion(name, attrs);
      fail("Should have thrown a RegionExistsException");

    } catch (RegionExistsException ex) {
      // pass...
    }

    setXmlFile(findFile("sameRootRegion.xml"));

    addIgnoredException("While reading Cache XML file");
    addIgnoredException("org.apache.geode.cache.RegionExistsException");
    try {
      getCache();
      fail("Should have thrown a CacheXmlException");

    } catch (CacheXmlException ex) {
      var cause = ex.getCause();
      assertTrue(cause instanceof SAXException);
      cause = ((SAXException) cause).getException();
      if (!(cause instanceof RegionExistsException)) {
        Assert.fail("Expected a RegionExistsException, not a " + cause.getClass().getName(), cause);
      }
    } finally {
      IgnoredException.removeAllExpectedExceptions();
    }
  }

  /**
   * Tests to make sure that we cannot create the same subregion multiple times in a
   * {@code cache.xml} file.
   */
  @Test
  public void testCreateSameSubregionTwice() throws Exception {
    var cache = new CacheCreation();
    var attrs = new RegionAttributesCreation(cache);
    var name = getUniqueName();

    var root = cache.createRegion("root", attrs);

    root.createSubregion(name, attrs);

    try {
      root.createSubregion(name, attrs);
      fail("Should have thrown a RegionExistsException");

    } catch (RegionExistsException ex) {
      // pass...
    }

    setXmlFile(findFile("sameSubregion.xml"));

    addIgnoredException("While reading Cache XML file");
    addIgnoredException("org.apache.geode.cache.RegionExistsException");
    try {
      getCache();
      fail("Should have thrown a CacheXmlException");

    } catch (CacheXmlException ex) {
      var cause = ex.getCause();
      assertTrue(cause instanceof SAXException);
      cause = ((SAXException) cause).getException();
      if (!(cause instanceof RegionExistsException)) {
        Assert.fail("Expected a RegionExistsException, not a " + cause.getClass().getName(), cause);
      }
    } finally {
      IgnoredException.removeAllExpectedExceptions();
    }
  }

  /**
   * Generates XML from the given {@code CacheCreation} and returns an {@code InputStream} for
   * reading that XML.
   */
  public InputStream generate(CacheCreation creation) {
    var baos = new ByteArrayOutputStream();

    final var version = getGemFireVersion();

    var pw = new PrintWriter(new OutputStreamWriter(baos), true);
    CacheXmlGenerator.generate(creation, pw, getUseSchema(), version);
    pw.close();

    var bytes = baos.toByteArray();
    return new ByteArrayInputStream(bytes);
  }

  /**
   * Tests that loading cache XML effects mutable cache attributes.
   */
  @Test
  public void testModifyCacheAttributes() throws Exception {
    var copyOnRead1 = false;
    var isServer1 = true;
    var lockLease1 = 123;
    var lockTimeout1 = 345;
    var searchTimeout1 = 567;

    var creation = new CacheCreation();
    creation.setCopyOnRead(copyOnRead1);
    creation.setIsServer(isServer1);
    creation.setLockLease(lockLease1);
    creation.setLockTimeout(lockTimeout1);
    creation.setSearchTimeout(searchTimeout1);

    testXml(creation);

    Cache cache = getCache();
    assertEquals(copyOnRead1, cache.getCopyOnRead());
    assertEquals(isServer1, cache.isServer());
    assertEquals(lockLease1, cache.getLockLease());
    assertEquals(lockTimeout1, cache.getLockTimeout());
    assertEquals(searchTimeout1, cache.getSearchTimeout());

    var copyOnRead2 = true;
    var isServer2 = false;
    var lockLease2 = 234;
    var lockTimeout2 = 456;
    var searchTimeout2 = 678;

    creation = new CacheCreation();
    creation.setCopyOnRead(copyOnRead2);
    creation.setIsServer(isServer2);
    creation.setLockLease(lockLease2);
    creation.setLockTimeout(lockTimeout2);
    creation.setSearchTimeout(searchTimeout2);

    cache.loadCacheXml(generate(creation));

    assertEquals(copyOnRead2, cache.getCopyOnRead());
    assertEquals(isServer2, cache.isServer());
    assertEquals(lockLease2, cache.getLockLease());
    assertEquals(lockTimeout2, cache.getLockTimeout());
    assertEquals(searchTimeout2, cache.getSearchTimeout());
  }

  /**
   * Tests that loading cache XML can create a region.
   */
  @Test
  public void testAddRegionViaCacheXml() throws Exception {
    var creation = new CacheCreation();

    testXml(creation);

    Cache cache = getCache();
    assertTrue(cache.rootRegions().isEmpty());

    creation = new CacheCreation();

    var attrs = new RegionAttributesCreation(creation);
    attrs.setScope(Scope.GLOBAL);
    attrs.setKeyConstraint(Integer.class);
    attrs.setCacheListener(new MyTestCacheListener());
    var root = creation.createRegion("root", attrs);

    attrs = new RegionAttributesCreation(creation);
    attrs.setScope(Scope.LOCAL);
    attrs.setEarlyAck(true);
    attrs.setValueConstraint(String.class);
    var subregion = root.createSubregion("subregion", attrs);

    cache.loadCacheXml(generate(creation));

    root = cache.getRegion("root");
    assertNotNull(root);
    assertEquals(Scope.GLOBAL, root.getAttributes().getScope());
    assertEquals(Integer.class, root.getAttributes().getKeyConstraint());
    assertTrue(root.getAttributes().getCacheListener() instanceof MyTestCacheListener);

    subregion = root.getSubregion("subregion");
    assertNotNull(subregion);
    assertEquals(Scope.LOCAL, subregion.getAttributes().getScope());
    assertTrue(subregion.getAttributes().getEarlyAck());
    assertFalse(subregion.getAttributes().getMulticastEnabled());
    assertEquals(String.class, subregion.getAttributes().getValueConstraint());

    // Create a subregion of a region that already exists

    creation = new CacheCreation();
    attrs = new RegionAttributesCreation(creation);
    root = creation.createRegion("root", attrs);

    attrs = new RegionAttributesCreation(creation);
    attrs.setScope(Scope.DISTRIBUTED_ACK);
    attrs.setEarlyAck(false);
    attrs.setValueConstraint(Long.class);
    var subregion2 = root.createSubregion("subregion2", attrs);

    cache.loadCacheXml(generate(creation));

    subregion2 = root.getSubregion("subregion2");
    assertNotNull(subregion2);
    assertEquals(Scope.DISTRIBUTED_ACK, subregion2.getAttributes().getScope());
    assertTrue(!subregion2.getAttributes().getEarlyAck());
    assertEquals(Long.class, subregion2.getAttributes().getValueConstraint());
  }

  /**
   * Tests that loading cache XML can modify a region.
   */
  @Test
  public void testModifyRegionViaCacheXml() throws Exception {
    var creation = new CacheCreation();

    var timeout1a = 123;
    var action1a = ExpirationAction.LOCAL_DESTROY;
    var timeout1b = 456;
    var action1b = ExpirationAction.DESTROY;

    var attrs = new RegionAttributesCreation(creation);
    attrs.setStatisticsEnabled(true);
    attrs.setEntryIdleTimeout(new ExpirationAttributes(timeout1a, action1a));
    var root = creation.createRegion("root", attrs);

    attrs = new RegionAttributesCreation(creation);
    attrs.setStatisticsEnabled(true);
    attrs.setEntryIdleTimeout(new ExpirationAttributes(timeout1b, action1b));
    var subregion = root.createSubregion("subregion", attrs);

    testXml(creation);

    Cache cache = getCache();

    root = cache.getRegion("root");
    assertEquals(timeout1a, root.getAttributes().getEntryIdleTimeout().getTimeout());
    assertEquals(action1a, root.getAttributes().getEntryIdleTimeout().getAction());

    subregion = root.getSubregion("subregion");
    assertEquals(timeout1b, subregion.getAttributes().getEntryIdleTimeout().getTimeout());
    assertEquals(action1b, subregion.getAttributes().getEntryIdleTimeout().getAction());

    creation = new CacheCreation();

    var timeout2a = 234;
    var action2a = ExpirationAction.LOCAL_INVALIDATE;
    var timeout2b = 567;
    var action2b = ExpirationAction.INVALIDATE;

    attrs = new RegionAttributesCreation(creation);
    attrs.setStatisticsEnabled(true);
    attrs.setEntryIdleTimeout(new ExpirationAttributes(timeout2a, action2a));
    attrs.setCacheListener(new MyTestCacheListener());
    root = creation.createRegion("root", attrs);

    attrs = new RegionAttributesCreation(creation);
    attrs.setStatisticsEnabled(true);
    attrs.setEntryIdleTimeout(new ExpirationAttributes(timeout2b, action2b));
    subregion = root.createSubregion("subregion", attrs);

    cache.loadCacheXml(generate(creation));

    root = cache.getRegion("root");
    subregion = root.getSubregion("subregion");

    assertEquals(timeout2a, root.getAttributes().getEntryIdleTimeout().getTimeout());
    assertEquals(action2a, root.getAttributes().getEntryIdleTimeout().getAction());
    assertTrue(root.getAttributes().getCacheListener() instanceof MyTestCacheListener);

    assertEquals(timeout2b, subregion.getAttributes().getEntryIdleTimeout().getTimeout());
    assertEquals(action2b, subregion.getAttributes().getEntryIdleTimeout().getAction());
  }

  /**
   * Tests that loading cache XML can add/update entries to a region.
   */
  @Test
  public void testAddEntriesViaCacheXml() throws Exception {
    var key1 = "KEY1";
    var value1 = "VALUE1";

    var creation = new CacheCreation();

    var attrs = new RegionAttributesCreation(creation);
    attrs.setScope(Scope.LOCAL);

    var root = creation.createRegion("root", attrs);
    root.put(key1, value1);

    testXml(creation);

    Cache cache = getCache();
    root = cache.getRegion("root");
    assertEquals(1, root.entrySet(false).size());
    assertEquals(value1, root.get(key1));

    creation = new CacheCreation();

    attrs = new RegionAttributesCreation(creation);
    attrs.setScope(Scope.LOCAL);

    var value2 = "VALUE2";
    var key2 = "KEY2";
    var value3 = "VALUE3";

    root = creation.createRegion("root", attrs);
    root.put(key1, value2);
    root.put(key2, value3);

    cache.loadCacheXml(generate(creation));

    root = cache.getRegion("root");
    assertEquals(2, root.entrySet(false).size());
    assertEquals(value2, root.get(key1));
    assertEquals(value3, root.get(key2));
  }

  /**
   * Test EnableBridgeConflation region attribute
   *
   * @since GemFire 4.2
   */
  @Test
  public void testEnableBridgeConflationAttribute() throws Exception {
    var cache = new CacheCreation();
    var attrs = new RegionAttributesCreation(cache);
    attrs.setEnableBridgeConflation(true);
    cache.createRegion("root", attrs);
    testXml(cache);
    assertEquals(true, cache.getRegion("root").getAttributes().getEnableBridgeConflation());
  }

  /**
   * Test EnableAsyncConflation region attribute
   *
   * @since GemFire 4.2
   */
  @Test
  public void testEnableAsyncConflationAttribute() throws Exception {
    var cache = new CacheCreation();
    var attrs = new RegionAttributesCreation(cache);
    attrs.setEnableAsyncConflation(true);
    cache.createRegion("root", attrs);
    testXml(cache);
    assertEquals(true, cache.getRegion("root").getAttributes().getEnableAsyncConflation());
  }

  /**
   * @since GemFire 4.3
   */
  @Test
  public void testDynamicRegionFactoryDefault() throws Exception {
    var cache = new CacheCreation();
    cache.setDynamicRegionFactoryConfig(new DynamicRegionFactory.Config());
    var attrs = new RegionAttributesCreation(cache);
    cache.createRegion("root", attrs);
    // note that testXml can't check if they are same because enabling
    // dynamic regions causes a meta region to be produced.
    testXml(cache, false);
    assertEquals(true, DynamicRegionFactory.get().getConfig().getRegisterInterest());
    assertEquals(true, DynamicRegionFactory.get().getConfig().getPersistBackup());
    assertEquals(true, DynamicRegionFactory.get().isOpen());
    assertEquals(null, DynamicRegionFactory.get().getConfig().getDiskDir());
    Region dr = getCache().getRegion("__DynamicRegions");
    if (dr != null) {
      dr.localDestroyRegion();
    }
  }

  @Test
  public void testDynamicRegionFactoryNonDefault() throws Exception {
    var cache = new CacheCreation();
    cache.setDynamicRegionFactoryConfig(
        new DynamicRegionFactory.Config(null, null, false, false));
    var attrs = new RegionAttributesCreation(cache);
    cache.createRegion("root", attrs);
    // note that testXml can't check if they are same because enabling
    // dynamic regions causes a meta region to be produced.
    testXml(cache, false);
    assertEquals(false, DynamicRegionFactory.get().getConfig().getRegisterInterest());
    assertEquals(false, DynamicRegionFactory.get().getConfig().getPersistBackup());
    assertEquals(true, DynamicRegionFactory.get().isOpen());
    assertEquals(null, DynamicRegionFactory.get().getConfig().getDiskDir());
    Region dr = getCache().getRegion("__DynamicRegions");
    if (dr != null) {
      dr.localDestroyRegion();
    }
  }

  /**
   * @since GemFire 4.3
   */
  @Test
  public void testDynamicRegionFactoryDiskDir() throws Exception {
    var cache = new CacheCreation();
    var f = new File("diskDir");
    f.mkdirs();
    cache.setDynamicRegionFactoryConfig(new DynamicRegionFactory.Config(f, null, true, true));
    var attrs = new RegionAttributesCreation(cache);
    cache.createRegion("root", attrs);
    // note that testXml can't check if they are same because enabling
    // dynamic regions causes a meta region to be produced.
    testXml(cache, false);
    assertEquals(true, DynamicRegionFactory.get().isOpen());
    assertEquals(f.getAbsoluteFile(), DynamicRegionFactory.get().getConfig().getDiskDir());
    Region dr = getCache().getRegion("__DynamicRegions");
    if (dr != null) {
      dr.localDestroyRegion();
    }
  }

  /**
   * Tests the cache server attribute
   *
   * @since GemFire 4.0
   */
  @Test
  public void testServer() throws Exception {
    var cache = new CacheCreation();
    cache.setIsServer(true);
    assertTrue(cache.isServer());

    testXml(cache);
  }

  /**
   * Tests declarative cache servers
   *
   * @since GemFire 4.0
   */
  @Test
  public void testBridgeServers() throws Exception {
    var cache = new CacheCreation();

    var bridge1 = cache.addCacheServer();
    setBridgeAttributes(bridge1);

    var bridge2 = cache.addCacheServer();
    setBridgeAttributes(bridge2);

    testXml(cache);
  }

  /**
   * Tests the is-lock-grantor attribute in xml.
   */
  @Test
  public void testIsLockGrantorAttribute() throws Exception {
    var cache = new CacheCreation();
    var attrs = new RegionAttributesCreation(cache);

    attrs.setLockGrantor(true);
    attrs.setScope(Scope.GLOBAL);
    attrs.setMirrorType(MirrorType.KEYS_VALUES);

    cache.createRegion("root", attrs);

    testXml(cache);
    assertEquals(true, cache.getRegion("root").getAttributes().isLockGrantor());
  }

  /**
   * Tests a cache listener with no parameters
   *
   * @since GemFire 4.0
   */
  @Test
  public void testTransactionListener() throws Exception {
    var cache = new CacheCreation();
    var txMgrCreation = new CacheTransactionManagerCreation();
    txMgrCreation.setListener(new MyTestTransactionListener());
    cache.addCacheTransactionManagerCreation(txMgrCreation);
    testXml(cache);
  }

  /**
   * Tests transaction manager with no listener
   *
   * @since GemFire 4.0
   */
  @Test
  public void testCacheTransactionManager() throws Exception {
    var cache = new CacheCreation();
    var txMgrCreation = new CacheTransactionManagerCreation();
    cache.addCacheTransactionManagerCreation(txMgrCreation);
    testXml(cache);
  }

  /**
   * Tests the value constraints region attribute that was added in GemFire 4.0.
   *
   * @since GemFire 4.1
   */
  @Test
  public void testConstrainedValues() throws Exception {
    var cache = new CacheCreation();
    var attrs = new RegionAttributesCreation(cache);
    attrs.setValueConstraint(String.class);

    cache.createRegion("root", attrs);

    testXml(cache);
  }

  /**
   * Tests creating a cache with a non-existent XML file
   */
  @Test
  public void testNonExistentFile() {
    // System.out.println("testNonExistentFile - start: " + System.currentTimeMillis());
    var nonExistent = new File(getName() + ".xml");
    nonExistent.delete();
    // System.out.println("testNonExistentFile - deleted: " + System.currentTimeMillis());
    setXmlFile(nonExistent);
    // System.out.println("testNonExistentFile - set: " + System.currentTimeMillis());

    var expectedException = addIgnoredException(
        String.format("Declarative Cache XML file/resource %s does not exist.",
            nonExistent.getPath()));
    try {
      getCache();
      fail("Should have thrown a CacheXmlException");

    } catch (CacheXmlException ex) {
      // System.out.println("testNonExistentFile - caught: " + System.currentTimeMillis());
      // pass...
    } finally {
      expectedException.remove();
    }
  }

  /**
   * Tests creating a cache with a XML file that is a directory
   */
  @Test
  public void testXmlFileIsDirectory() {
    var dir = new File(getName() + "dir");
    dir.mkdirs();
    dir.deleteOnExit();
    setXmlFile(dir);

    var expectedException = addIgnoredException(
        String.format("Declarative XML file %s is not a file.", dir));
    try {
      getCache();
      fail("Should have thrown a CacheXmlException");

    } catch (CacheXmlException ex) {
      // pass...
    } finally {
      expectedException.remove();
    }
  }

  /**
   * Tests creating a cache with the default lock-timeout, lock-lease, and search-timeout.
   */
  @Test
  public void testDefaultCache() throws Exception {
    var cache = new CacheCreation();

    testXml(cache);
  }

  /**
   * Tests creating a cache with non-default lock-timeout, lock-lease, and search-timeout.
   */
  @Test
  public void testNonDefaultCache() throws Exception {
    var cache = new CacheCreation();
    cache.setLockTimeout(42);
    cache.setLockLease(43);
    cache.setSearchTimeout(44);

    if (getGemFireVersion().compareTo(CacheXml.VERSION_4_0) >= 0) {
      cache.setCopyOnRead(true);
    }

    testXml(cache);
  }

  /**
   * Tests creating a cache with entries defined in the root region
   */
  @Test
  public void testEntriesInRootRegion() throws Exception {
    var cache = new CacheCreation();
    var root =
        (RegionCreation) cache.createRegion("root", new RegionAttributesCreation(cache));
    root.put("KEY1", "VALUE1");
    root.put("KEY2", "VALUE2");
    root.put("KEY3", "VALUE3");

    testXml(cache);
  }

  /**
   * Tests creating a cache whose keys are constrained
   */
  @Test
  public void testConstrainedKeys() throws Exception {
    var cache = new CacheCreation();
    var attrs = new RegionAttributesCreation(cache);
    attrs.setKeyConstraint(String.class);
    cache.createRegion("root", attrs);

    testXml(cache);
  }

  /**
   * Tests creating a cache with a various {@link ExpirationAttributes}.
   */
  @Test
  public void testExpirationAttriubutes() throws Exception {
    var cache = new CacheCreation();
    var attrs = new RegionAttributesCreation(cache);
    attrs.setStatisticsEnabled(true);

    {
      var expire = new ExpirationAttributes(42, ExpirationAction.INVALIDATE);
      attrs.setRegionTimeToLive(expire);
    }

    {
      var expire = new ExpirationAttributes(43, ExpirationAction.DESTROY);
      attrs.setRegionIdleTimeout(expire);
    }

    {
      var expire = new ExpirationAttributes(44, ExpirationAction.LOCAL_INVALIDATE);
      attrs.setEntryTimeToLive(expire);
    }

    {
      var expire = new ExpirationAttributes(45, ExpirationAction.LOCAL_DESTROY);
      attrs.setEntryIdleTimeout(expire);
    }

    cache.createRegion("root", attrs);

    testXml(cache);
  }

  /**
   * Tests a cache loader an interesting combination of declarables
   */
  @Test
  public void testCacheLoaderWithDeclarables() throws Exception {
    var cache = new CacheCreation();
    var attrs = new RegionAttributesCreation(cache);

    var loader = new CacheLoaderWithDeclarables();
    attrs.setCacheLoader(loader);

    cache.createRegion("root", attrs);

    testXml(cache);
  }

  /**
   * Tests a cache writer with no parameters
   */
  @Test
  public void testCacheWriter() throws Exception {
    var cache = new CacheCreation();
    var attrs = new RegionAttributesCreation(cache);

    CacheWriter writer = new MyTestCacheWriter();
    attrs.setCacheWriter(writer);

    cache.createRegion("root", attrs);

    testXml(cache);
  }

  /**
   * Tests a cache listener with no parameters
   */
  @Test
  public void testCacheListener() throws Exception {
    var cache = new CacheCreation();
    var attrs = new RegionAttributesCreation(cache);

    CacheListener listener = new MyTestCacheListener();
    attrs.setCacheListener(listener);

    cache.createRegion("root", attrs);

    testXml(cache);
  }

  /**
   * Tests a region with non-default region attributes
   */
  @Test
  public void testNonDefaultRegionAttributes() throws Exception {
    var cache = new CacheCreation();
    var attrs = new RegionAttributesCreation(cache);

    attrs.setScope(Scope.DISTRIBUTED_NO_ACK);
    attrs.setMirrorType(MirrorType.KEYS_VALUES);
    attrs.setInitialCapacity(142);
    attrs.setLoadFactor(42.42f);
    attrs.setStatisticsEnabled(false);

    cache.createRegion("root", attrs);

    testXml(cache);
  }

  /**
   * Tests parsing a malformed XML file
   */
  @Test
  public void testMalformed() throws Exception {
    setXmlFile(findFile("malformed.xml"));

    var expectedException =
        addIgnoredException("While reading Cache XML file");
    try {
      getCache();
      fail("Should have thrown a CacheXmlException");

    } catch (CacheXmlException ex) {
      assertTrue(ex.getCause() instanceof SAXException);
    } finally {
      expectedException.remove();
    }
  }

  /**
   * Tests parsing an XML file with a bad integer
   */
  @Test
  public void testBadInt() throws Exception {
    setXmlFile(findFile("badInt.xml"));

    var expectedException =
        addIgnoredException("While reading Cache XML file");
    try {
      getCache();
      fail("Should have thrown a CacheXmlException");

    } catch (CacheXmlException ex) {
      var cause = ex.getCause();
      assertNotNull("Expected a cause", cause);
      assertTrue("Didn't expect cause:" + cause + " (a " + cause.getClass().getName() + ")",
          cause instanceof NumberFormatException);
    } finally {
      expectedException.remove();
    }
  }

  /**
   * Tests parsing an XML file with a bad float
   */
  @Test
  public void testBadFloat() throws Exception {
    setXmlFile(findFile("badFloat.xml"));

    var expectedException =
        addIgnoredException("While reading Cache XML file");
    try {
      getCache();
      fail("Should have thrown a CacheXmlException");

    } catch (CacheXmlException ex) {
      assertTrue(ex.getCause() instanceof NumberFormatException);
    } finally {
      expectedException.remove();
    }
  }

  /**
   * Tests parsing an XML file with a bad scope. This error should be caught by the XML parser.
   */
  @Test
  public void testBadScope() throws Exception {
    setXmlFile(findFile("badScope.xml"));

    var expectedException =
        addIgnoredException("While reading Cache XML file");
    try {
      getCache();
      fail("Should have thrown a CacheXmlException");

    } catch (CacheXmlException ex) {
      assertTrue(ex.getCause() instanceof SAXException);
    } finally {
      expectedException.remove();
    }
  }

  /**
   * Tests parsing an XML file with a non-existent key constraint class.
   */
  @Test
  public void testBadKeyConstraintClass() throws Exception {
    setXmlFile(findFile("badKeyConstraintClass.xml"));

    var expectedException =
        addIgnoredException("While reading Cache XML file");
    try {
      getCache();
      fail("Should have thrown a CacheXmlException");

    } catch (CacheXmlException ex) {
      assertTrue(ex.getCause() instanceof ClassNotFoundException);
    } finally {
      expectedException.remove();
    }
  }

  /**
   * Tests parsing an XML file that specifies a cache listener that is not {@link Declarable}.
   */
  @Test
  public void testCallbackNotExplicitlyDeclarableIsStillOK() throws Exception {
    setXmlFile(findFile("callbackNotDeclarable.xml"));

    getCache();
  }

  /**
   * Tests parsing an XML file that specifies a cache listener whose constructor throws an
   * {@linkplain AssertionError exception}.
   */
  @Test
  public void testCallbackWithException() throws Exception {
    setXmlFile(findFile("callbackWithException.xml"));

    var expectedException =
        addIgnoredException("While reading Cache XML file");
    try {
      getCache();
      fail("Should have thrown a CacheXmlException");

    } catch (CacheXmlException ex) {
      if (!(ex.getCause() instanceof AssertionError)) {
        throw ex;
      }
    } finally {
      expectedException.remove();
    }

  }

  /**
   * Tests parsing an XML file that specifies a cache listener that is not a {@code CacheLoader}.
   */
  @Test
  public void testLoaderNotLoader() throws Exception {
    setXmlFile(findFile("loaderNotLoader.xml"));

    var expectedException =
        addIgnoredException("While reading Cache XML file");
    try {
      getCache();
      fail("Should have thrown a CacheXmlException");

    } catch (CacheXmlException ex) {
      var cause = ex.getCause();
      assertNull("Didn't expect a " + cause, cause);
    } finally {
      expectedException.remove();
    }
  }

  /**
   * Tests nested regions
   */
  @Test
  public void testNestedRegions() throws Exception {
    var cache = new CacheCreation();

    var attrs = new RegionAttributesCreation(cache);
    attrs.setScope(Scope.DISTRIBUTED_NO_ACK);

    var root = (RegionCreation) cache.createRegion("root", attrs);

    {
      attrs = new RegionAttributesCreation(cache);
      attrs.setScope(Scope.DISTRIBUTED_NO_ACK);
      attrs.setMirrorType(MirrorType.KEYS_VALUES);
      attrs.setInitialCapacity(142);
      attrs.setLoadFactor(42.42f);
      attrs.setStatisticsEnabled(false);

      root.createSubregion("one", attrs);
    }

    {
      attrs = new RegionAttributesCreation(cache);
      attrs.setScope(Scope.DISTRIBUTED_ACK);
      attrs.setMirrorType(MirrorType.KEYS);
      attrs.setInitialCapacity(242);

      var region = root.createSubregion("two", attrs);

      {
        attrs = new RegionAttributesCreation(cache);
        attrs.setScope(Scope.GLOBAL);
        attrs.setLoadFactor(43.43f);

        region.createSubregion("three", attrs);
      }
    }

    testXml(cache);
  }

  /**
   * Tests whether or not XML attributes can appear in any order. See bug 30050.
   */
  @Test
  public void testAttributesUnordered() throws Exception {
    setXmlFile(findFile("attributesUnordered.xml"));
    getCache();
  }

  /**
   * Tests disk directories
   */
  @Test
  public void testDiskDirs() throws Exception {
    var cache = new CacheCreation();

    var attrs = new RegionAttributesCreation(cache);
    var dirs = new File[] {new File(getUniqueName() + "-dir1"),
        new File(getUniqueName() + "-dir2")};
    for (final var dir : dirs) {
      dir.mkdirs();
      dir.deleteOnExit();
    }

    var diskSizes = new int[] {DiskWriteAttributesImpl.DEFAULT_DISK_DIR_SIZE,
        DiskWriteAttributesImpl.DEFAULT_DISK_DIR_SIZE};
    attrs.setDiskDirsAndSize(dirs, diskSizes);
    cache.createRegion("root", attrs);

    testXml(cache);
  }

  /**
   * Tests the {@code overflowThreshold} and {@code persistBackup} related attributes
   */
  @Test
  public void testOverflowAndBackup() throws Exception {
    var cache = new CacheCreation();

    var attrs = new RegionAttributesCreation(cache);
    attrs.setMirrorType(MirrorType.KEYS_VALUES);
    attrs.setPersistBackup(true);

    cache.createRegion("root", attrs);

    testXml(cache);
  }

  /**
   * Tests {@code DiskWriteAttributes}
   */
  @Test
  public void testDiskWriteAttributes() throws Exception {
    var cache = new CacheCreation();
    // Set properties for Asynch writes

    var attrs = new RegionAttributesCreation(cache);

    var root = (RegionCreation) cache.createRegion("root", attrs);

    {
      attrs = new RegionAttributesCreation(cache);
      var dwaf = new DiskWriteAttributesFactory();
      dwaf.setSynchronous(true);
      attrs.setDiskWriteAttributes(dwaf.create());
      root.createSubregion("sync", attrs);
    }

    {
      attrs = new RegionAttributesCreation(cache);
      var dwaf = new DiskWriteAttributesFactory();
      dwaf.setTimeInterval(123L);
      dwaf.setBytesThreshold(456L);
      attrs.setDiskWriteAttributes(dwaf.create());
      root.createSubregion("async", attrs);
    }

    testXml(cache);
  }

  /**
   * Tests to make sure that the example cache.xml file in the API documentation conforms to the
   * DTD.
   *
   * @since GemFire 3.2.1
   */
  @Test
  @Ignore // TODO: why is testExampleCacheXmlFile @Ignored?
  public void testExampleCacheXmlFile() throws Exception {
    // Check for old example files
    var dirName = "examples_" + getGemFireVersion();
    File dir = null;
    try {
      dir = findFile(dirName);
    } catch (AssertionError e) {
      // ignore, no directory.
    }
    if (dir != null && dir.exists()) {
      var xmlFiles = dir.listFiles((dir1, name) -> name.endsWith(".xml"));
      assertTrue("No XML files in " + dirName, xmlFiles.length > 0);
      for (var xmlFile : xmlFiles) {
        LogWriterUtils.getLogWriter().info("Parsing " + xmlFile);

        var fis = new FileInputStream(xmlFile);
        CacheXmlParser.parse(fis);
      }

    } else {

      var example = new File(createTempFileFromResource(getClass(),
          "/org/apache/geode/cache/doc-files/example-cache.xml").getAbsolutePath());
      var fis = new FileInputStream(example);
      CacheXmlParser.parse(fis);

      var example2 = new File(createTempFileFromResource(getClass(),
          "/org/apache/geode/cache/doc-files/example2-cache.xml").getAbsolutePath());
      fis = new FileInputStream(example2);
      CacheXmlParser.parse(fis);

      var example3 = new File(createTempFileFromResource(getClass(),
          "/org/apache/geode/cache/doc-files/example3-cache.xml").getAbsolutePath());
      fis = new FileInputStream(example3);
      CacheXmlParser.parse(fis);
    }
  }

  @Test
  public void testEvictionLRUEntryAttributes() throws Exception {
    final var rName = getUniqueName();
    var cache = new CacheCreation();
    var attrs = new RegionAttributesCreation(cache);
    attrs.setEvictionAttributes(
        EvictionAttributes.createLRUEntryAttributes(80, EvictionAction.LOCAL_DESTROY));
    cache.createRegion(rName, attrs);
    testXml(cache);
  }

  public static class EvictionObjectSizer implements ObjectSizer, Declarable2 {
    Properties props = new Properties();

    @Override
    public int sizeof(Object o) {
      return 1;
    }

    @Override
    public Properties getConfig() {
      if (null == props) {
        props = new Properties();
      }
      props.setProperty("EvictionObjectSizerColor", "blue");
      return props;
    }

    @Override
    public void init(Properties props) {
      this.props = props;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof EvictionObjectSizer)) {
        return false;
      }
      var other = (EvictionObjectSizer) obj;
      return props.equals(other.props);
    }
  }

  @Test
  public void testEvictionLRUMemoryAttributes() throws Exception {
    final var rName = getUniqueName();
    var cache = new CacheCreation();
    var attrs = new RegionAttributesCreation(cache);
    attrs.setEvictionAttributes(
        EvictionAttributes.createLRUMemoryAttributes(10, new EvictionObjectSizer()));
    cache.createRegion(rName, attrs);
    testXml(cache);
  }

  @Test
  public void testEvictionLRUHeapAttributes() throws Exception {
    final var rName = getUniqueName();
    var cache = new CacheCreation();
    var attrs = new RegionAttributesCreation(cache);
    attrs.setEvictionAttributes(EvictionAttributes
        .createLRUHeapAttributes(new EvictionObjectSizer(), EvictionAction.LOCAL_DESTROY));
    cache.createRegion(rName, attrs);
    testXml(cache);
  }


  /**
   * A cache listener that is not {@link Declarable}
   *
   * @see #testCallbackNotExplicitlyDeclarableIsStillOK()
   */
  public static class NotDeclarableCacheListener extends TestCacheListener {
    // empty
  }

  public static class AssertionError extends RuntimeException {
    public AssertionError() {
      super("Test Exception");
    }
  }

  /**
   * A cache listener whose constructor throws an exception
   *
   * @see #testCallbackWithException()
   */
  public static class ExceptionalCacheListener extends TestCacheListener {

    public ExceptionalCacheListener() {
      throw new AssertionError();
    }
  }

  /**
   * A {@code CacheListener} that is {@code Declarable}, but not {@code Declarable2}.
   */
  public static class MyTestCacheListener extends TestCacheListener implements Declarable {

    @Override
    public void init(Properties props) {}

    @Override
    public boolean equals(Object o) {
      return o instanceof MyTestCacheListener;
    }
  }

  /**
   * A {@code CacheWriter} that is {@code Declarable}, but not {@code Declarable2}.
   */
  public static class MyTestCacheWriter extends TestCacheWriter implements Declarable {

    @Override
    public void init(Properties props) {}

    @Override
    public boolean equals(Object o) {
      return o instanceof MyTestCacheWriter;
    }
  }

  /**
   * A {@code TransactionListener} that is {@code Declarable}, but not {@code Declarable2}.
   */
  public static class MyTestTransactionListener extends TestTransactionListener
      implements Declarable {

    @Override
    public void init(Properties props) {}

    @Override
    public boolean equals(Object o) {
      return o instanceof MyTestTransactionListener;
    }
  }

  /**
   * A {@code CacheLoader} that is {@code Declarable} and has some interesting parameters.
   */
  public static class CacheLoaderWithDeclarables implements CacheLoader, Declarable2 {

    /** This loader's properties */
    private final Properties props;

    /** Was this declarable initialized */
    private boolean initialized = false;

    /**
     * Creates a new loader and initializes its properties
     */
    public CacheLoaderWithDeclarables() {
      props = new Properties();
      props.put("KEY1", "VALUE1");
      props.put("KEY2", new TestDeclarable());
    }

    /**
     * Returns whether or not this {@code Declarable} was initialized.
     */
    public boolean isInitialized() {
      return initialized;
    }

    @Override
    public void init(Properties props) {
      initialized = true;
      assertEquals(this.props, props);
    }

    @Override
    public Properties getConfig() {
      return props;
    }

    @Override
    public Object load(LoaderHelper helper) throws CacheLoaderException {

      fail("Loader shouldn't be invoked");
      return null;
    }

    public boolean equals(Object o) {
      if (o instanceof CacheLoaderWithDeclarables) {
        var other = (CacheLoaderWithDeclarables) o;
        return props.equals(other.props);

      } else {
        return false;
      }
    }

    @Override
    public void close() {}

  }

  public static class TestDeclarable implements Declarable {
    @Override
    public void init(Properties props) {}

    public boolean equals(Object o) {
      return o instanceof TestDeclarable;
    }
  }

}
