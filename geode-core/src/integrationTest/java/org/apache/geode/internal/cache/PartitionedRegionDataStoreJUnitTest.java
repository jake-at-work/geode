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

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.internal.statistics.StatisticsClockFactory.disabledClock;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.distributed.DistributedSystem;

/**
 * This test checks functionality of the PartitionedRegionDatastore on a sinle node.
 *
 * Created on Dec 23, 2005
 *
 *
 */
public class PartitionedRegionDataStoreJUnitTest {
  static DistributedSystem sys;

  static Cache cache;

  byte[] obj = new byte[10240];

  String regionName = "DataStoreRegion";


  @Before
  public void setUp() {
    var dsProps = new Properties();
    dsProps.setProperty(MCAST_PORT, "0");

    // Connect to a DS and create a Cache.
    sys = DistributedSystem.connect(dsProps);
    cache = CacheFactory.create(sys);
  }

  @After
  public void tearDown() {
    sys.disconnect();
  }

  @Test
  public void testRemoveBrokenNode() throws Exception {

    var paf = new PartitionAttributesFactory();

    var pa = paf.setRedundantCopies(0).setLocalMaxMemory(0).create();
    var af = new AttributesFactory();
    af.setPartitionAttributes(pa);
    var ra = af.create();

    PartitionedRegion pr = null;
    pr = (PartitionedRegion) cache.createRegion("PR2", ra);
    paf.setLocalProperties(null).create();
    /* PartitionedRegionDataStore prDS = */ new PartitionedRegionDataStore(pr, disabledClock());
    /*
     * PartitionedRegionHelper.removeGlobalMetadataForFailedNode(PartitionedRegion.node,
     * prDS.partitionedRegion.getRegionIdentifier(), prDS.partitionedRegion.cache);
     */
  }

  @Test
  public void testLocalPut() throws Exception {
    var paf = new PartitionAttributesFactory();

    var globalProps = new Properties();
    globalProps.put(PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_PROPERTY, "100");

    var pa = paf.setRedundantCopies(0).setLocalMaxMemory(100).create();
    var af = new AttributesFactory();
    af.setPartitionAttributes(pa);
    var ra = af.create();

    PartitionedRegion pr = null;
    pr = (PartitionedRegion) cache.createRegion("PR3", ra);

    var key = "User";
    var value = "1";

    pr.put(key, value);
    assertEquals(pr.get(key), value);

  }

  /**
   * This method checks whether the canAccomodateMoreBytesSafely returns false after reaching the
   * localMax memory.
   *
   */
  @Test
  public void testCanAccommodateMoreBytesSafely() throws Exception {
    var key = 0;
    final var numMBytes = 5;

    final var regionAck = (PartitionedRegion) new RegionFactory()
        .setPartitionAttributes(new PartitionAttributesFactory().setRedundantCopies(0)
            .setLocalMaxMemory(numMBytes).create())
        .create(regionName);

    assertTrue(regionAck.getDataStore().canAccommodateMoreBytesSafely(0));

    var numk = numMBytes * 1024;
    var num = numk * 1024;
    assertTrue(regionAck.getDataStore().canAccommodateMoreBytesSafely(num - 1));
    assertFalse(regionAck.getDataStore().canAccommodateMoreBytesSafely(num));
    assertFalse(regionAck.getDataStore().canAccommodateMoreBytesSafely(num + 1));
    final var OVERHEAD = CachedDeserializableFactory.getByteSize(new byte[0]);
    for (key = 0; key < numk; key++) {
      regionAck.put(key, new byte[1024 - OVERHEAD]);
    }
    assertTrue(regionAck.getDataStore().canAccommodateMoreBytesSafely(-1));
    assertFalse(regionAck.getDataStore().canAccommodateMoreBytesSafely(0));
    assertFalse(regionAck.getDataStore().canAccommodateMoreBytesSafely(1));
    regionAck.invalidate(--key);
    assertTrue(regionAck.getDataStore().canAccommodateMoreBytesSafely(1023));
    assertFalse(regionAck.getDataStore().canAccommodateMoreBytesSafely(1024));
    assertFalse(regionAck.getDataStore().canAccommodateMoreBytesSafely(1025));
    regionAck.put(key, new byte[1024 - OVERHEAD]);
    assertTrue(regionAck.getDataStore().canAccommodateMoreBytesSafely(-1));
    assertFalse(regionAck.getDataStore().canAccommodateMoreBytesSafely(0));
    assertFalse(regionAck.getDataStore().canAccommodateMoreBytesSafely(1));
    regionAck.destroy(key);
    assertTrue(regionAck.getDataStore().canAccommodateMoreBytesSafely(1023));
    assertFalse(regionAck.getDataStore().canAccommodateMoreBytesSafely(1024));
    assertFalse(regionAck.getDataStore().canAccommodateMoreBytesSafely(1025));
    regionAck.put(key, new byte[1023 - OVERHEAD]);
    assertTrue(regionAck.getDataStore().canAccommodateMoreBytesSafely(0));
    assertFalse(regionAck.getDataStore().canAccommodateMoreBytesSafely(1));
    assertFalse(regionAck.getDataStore().canAccommodateMoreBytesSafely(2));

    for (key = 0; key < numk; key++) {
      regionAck.destroy(key);
    }
    assertEquals(0, regionAck.size());
    assertTrue(regionAck.getDataStore().canAccommodateMoreBytesSafely(num - 1));
    assertFalse(regionAck.getDataStore().canAccommodateMoreBytesSafely(num));
    assertFalse(regionAck.getDataStore().canAccommodateMoreBytesSafely(num + 1));

    for (key = 0; key < numk; key++) {
      regionAck.put(key, "foo");
    }
  }

  @Test
  public void doesNotCreateBucketIfOverMemoryLimit() {
    final var numMBytes = 5;
    final var regionAck = (PartitionedRegion) new RegionFactory()
        .setPartitionAttributes(new PartitionAttributesFactory().setRedundantCopies(0)
            .setLocalMaxMemory(numMBytes).create())
        .create(regionName);

    var createdBucket =
        regionAck.getDataStore().handleManageBucketRequest(1, Integer.MAX_VALUE, null, false);
    assertFalse(createdBucket);
  }

  @Test
  public void createsBucketWhenForcedIfOverMemoryLimit() {
    final var numMBytes = 5;
    final var regionAck = (PartitionedRegion) new RegionFactory()
        .setPartitionAttributes(new PartitionAttributesFactory().setRedundantCopies(0)
            .setLocalMaxMemory(numMBytes).create())
        .create(regionName);

    var createdBucket =
        regionAck.getDataStore().handleManageBucketRequest(1, Integer.MAX_VALUE, null, true);
    assertTrue(createdBucket);
  }
}
