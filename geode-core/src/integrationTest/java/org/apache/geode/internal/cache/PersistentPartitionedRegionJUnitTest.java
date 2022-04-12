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

import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.util.ObjectSizer;

public class PersistentPartitionedRegionJUnitTest {

  private File dir;
  private Cache cache;

  @Before
  public void setUp() throws Exception {
    dir = new File("diskDir");
    dir.mkdir();
  }

  @After
  public void tearDown() throws Exception {
    if (cache != null && !cache.isClosed()) {
      cache.close();
    }
    FileUtils.deleteDirectory(dir);
  }

  @Test
  public void testChangeTTL() throws InterruptedException {
    var region = createRegion(-1);
    region.put("A", "B");
    cache.close();
    region = createRegion(60000);
    assertEquals("B", region.get("A"));
  }

  @Test
  public void testStatsPersistAttributesChangeNoRecovery() throws InterruptedException {
    System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, "false");
    try {
      var region = (PartitionedRegion) createRegion(-1);
      region.put("A", "B");
      cache.close();
      region = (PartitionedRegion) createRegion(60000);

      var bucket = region.getBucketRegion("A");
      assertEquals(1, bucket.getDiskRegion().getStats().getNumOverflowOnDisk());
      assertEquals(0, bucket.getDiskRegion().getStats().getNumEntriesInVM());
      assertEquals("B", region.get("A"));
      assertEquals(0, bucket.getDiskRegion().getStats().getNumOverflowOnDisk());
      assertEquals(1, bucket.getDiskRegion().getStats().getNumEntriesInVM());
    } finally {
      System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, "true");
    }
  }

  @Test
  public void testStatsPersistAttributesChangeSyncRecovery() throws InterruptedException {
    System.setProperty(DiskStoreImpl.RECOVER_VALUES_SYNC_PROPERTY_NAME, "true");
    try {
      var region = (PartitionedRegion) createRegion(-1);
      region.put("A", "B");
      cache.close();
      region = (PartitionedRegion) createRegion(60000);

      var bucket = region.getBucketRegion("A");
      assertEquals(0, bucket.getDiskRegion().getStats().getNumOverflowOnDisk());
      assertEquals(1, bucket.getDiskRegion().getStats().getNumEntriesInVM());
      assertEquals("B", region.get("A"));
      assertEquals(0, bucket.getDiskRegion().getStats().getNumOverflowOnDisk());
      assertEquals(1, bucket.getDiskRegion().getStats().getNumEntriesInVM());
    } finally {
      System.setProperty(DiskStoreImpl.RECOVER_VALUES_SYNC_PROPERTY_NAME, "false");
    }
  }

  @Test
  public void testStatsPersistAttributesChangeAsyncRecovery() throws InterruptedException {
    var region = (PartitionedRegion) createRegion(-1);
    for (var i = 0; i < 1000; i++) {
      region.put(i, "B");
    }
    cache.close();
    region = (PartitionedRegion) createRegion(60000);

    var bucket = region.getBucketRegion("A");
    for (var i = 0; i < 1000; i++) {
      region.get(i);
    }
    // There is a race where the async value recovery thread may be handling
    // the stats for the last entry (even though it is already faulted in)
    var count = 0;
    while (0 != bucket.getDiskRegion().getStats().getNumOverflowOnDisk()) {
      Thread.sleep(50);
      if (++count == 20) {
        break;
      }
    }
    assertEquals(0, bucket.getDiskRegion().getStats().getNumOverflowOnDisk());
    assertEquals(1000, bucket.getDiskRegion().getStats().getNumEntriesInVM());
  }

  @Test
  public void testStatsPersistNoRecovery() throws InterruptedException {
    System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, "false");
    try {
      var region = (PartitionedRegion) createRegion(-1);
      region.put("A", "B");
      cache.close();
      region = (PartitionedRegion) createRegion(-1);

      var bucket = region.getBucketRegion("A");
      assertEquals(1, bucket.getDiskRegion().getStats().getNumOverflowOnDisk());
      assertEquals(0, bucket.getDiskRegion().getStats().getNumEntriesInVM());
      assertEquals("B", region.get("A"));
      assertEquals(0, bucket.getDiskRegion().getStats().getNumOverflowOnDisk());
      assertEquals(1, bucket.getDiskRegion().getStats().getNumEntriesInVM());
    } finally {
      System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, "true");
    }
  }

  @Test
  public void testStatsChangeSyncRecovery() throws InterruptedException {
    System.setProperty(DiskStoreImpl.RECOVER_VALUES_SYNC_PROPERTY_NAME, "true");
    try {
      var region = (PartitionedRegion) createRegion(-1);
      region.put("A", "B");
      cache.close();
      region = (PartitionedRegion) createRegion(-1);

      var bucket = region.getBucketRegion("A");
      assertEquals(0, bucket.getDiskRegion().getStats().getNumOverflowOnDisk());
      assertEquals(1, bucket.getDiskRegion().getStats().getNumEntriesInVM());
      assertEquals("B", region.get("A"));
      assertEquals(0, bucket.getDiskRegion().getStats().getNumOverflowOnDisk());
      assertEquals(1, bucket.getDiskRegion().getStats().getNumEntriesInVM());
    } finally {
      System.setProperty(DiskStoreImpl.RECOVER_VALUES_SYNC_PROPERTY_NAME, "false");
    }
  }

  @Test
  public void testStatsPersistAsyncRecovery() throws InterruptedException {
    var region = (PartitionedRegion) createRegion(-1);
    for (var i = 0; i < 1000; i++) {
      region.put(i, "B");
    }
    cache.close();
    region = (PartitionedRegion) createRegion(-1);

    var bucket = region.getBucketRegion("A");
    for (var i = 0; i < 1000; i++) {
      region.get(i);
    }

    // There is a race where the async value recovery thread may be handling
    // the stats for the last entry (even though it is already faulted in)
    var count = 0;
    while (0 != bucket.getDiskRegion().getStats().getNumOverflowOnDisk()) {
      Thread.sleep(50);
      if (++count == 20) {
        break;
      }
    }
    assertEquals(0, bucket.getDiskRegion().getStats().getNumOverflowOnDisk());
    assertEquals(1000, bucket.getDiskRegion().getStats().getNumEntriesInVM());
  }

  @Test
  public void testValuesAreNotRecoveredForHeapLruRegions() {
    createLRURegionAndValidateRecovery(false, true, 10, 0);
  }

  @Test
  public void testValuesAreNotRecoveredForHeapLruRegionsWithRegionClose() {
    createLRURegionAndValidateRecovery(true, true, 10, 0);
  }

  @Test
  public void testValuesAreNotRecoveredForHeapLruRegionsWithRecoverPropertySet() {
    var oldValue = System.getProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
    System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, "true");

    try {
      createLRURegionAndValidateRecovery(false, true, 10, 0);
    } finally {
      if (oldValue != null) {
        System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, oldValue);
      } else {
        System.clearProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
      }
    }
  }

  @Test
  public void testValuesAreRecoveredForHeapLruRegionsWithRecoverValueAndRecoverLruPropertySet() {
    var oldValue = System.getProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
    System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, "true");

    var lruOldValue = System.getProperty(DiskStoreImpl.RECOVER_LRU_VALUES_PROPERTY_NAME);
    System.setProperty(DiskStoreImpl.RECOVER_LRU_VALUES_PROPERTY_NAME, "true");

    try {
      createLRURegionAndValidateRecovery(false, true, 10, 10);
    } finally {
      if (oldValue != null) {
        System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, oldValue);
      } else {
        System.clearProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME);
      }

      if (lruOldValue != null) {
        System.setProperty(DiskStoreImpl.RECOVER_LRU_VALUES_PROPERTY_NAME, lruOldValue);
      } else {
        System.clearProperty(DiskStoreImpl.RECOVER_LRU_VALUES_PROPERTY_NAME);
      }
    }
  }

  @Test
  public void testValuesAreNotRecoveredForEntryLruRegion() {
    createLRURegionAndValidateRecovery(false, false, 10, 0);
  }

  @Test
  public void testValuesAreNotRecoveredForEntryLruRegionWithRegionClose() {
    createLRURegionAndValidateRecovery(true, false, 10, 0);
  }

  private void createLRURegionAndValidateRecovery(boolean isRegionClose, boolean heapLru, int size,
      int expectedInMemory) {
    PartitionedRegion region;
    var entryLru = !heapLru;
    region = (PartitionedRegion) createRegion(-1, heapLru, entryLru);

    for (var i = 0; i < size; i++) {
      region.put(i, i);
    }

    if (isRegionClose) {
      region.close();
    } else {
      cache.close();
    }

    final var recoveryDone = new CountDownLatch(1);
    DiskStoreObserver.setInstance(new DiskStoreObserver() {
      @Override
      public void afterAsyncValueRecovery(DiskStoreImpl store) {
        recoveryDone.countDown();
      }
    });
    region = (PartitionedRegion) createRegion(-1, heapLru, entryLru);

    // Wait for recovery to finish.
    try {
      recoveryDone.await();
    } catch (InterruptedException ie) {
      fail("Found interrupted exception while waiting for recovery.");
    }

    var valuesInVm = getValuesInVM(region, size);
    assertEquals("Values for lru regions should not be recovered from Disk.", expectedInMemory,
        valuesInVm);

    var bucket = region.getBucketRegion("A");
    assertEquals((size - expectedInMemory),
        bucket.getDiskRegion().getStats().getNumOverflowOnDisk());
    assertEquals(expectedInMemory, bucket.getDiskRegion().getStats().getNumEntriesInVM());

    // Load values into memory using get.
    for (var i = 0; i < size; i++) {
      region.get(i);
    }
    assertEquals(size, bucket.getDiskRegion().getStats().getNumEntriesInVM());
  }

  private int getValuesInVM(Region region, int size) {
    var valuesInVm = 0;
    for (var i = 0; i < size; i++) {
      try {
        var value = ((LocalRegion) region).getValueInVM(i);
        if (value != null) {
          valuesInVm++;
        }
      } catch (EntryNotFoundException e) {
        fail("Entry not found not expected but occurred ");
      }
    }
    return valuesInVm;
  }

  private Region createRegion(int ttl) {
    return createRegion(ttl, false, false);
  }

  private Region createRegion(int ttl, boolean isHeapEviction, boolean isEntryEviction) {
    var props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOG_LEVEL, "info");
    // props.setProperty("log-file", "junit.log");
    cache = new CacheFactory(props).create();
    cache.createDiskStoreFactory().setMaxOplogSize(1).setDiskDirs(new File[] {dir}).create("disk");

    var rf = cache.createRegionFactory()
        .setDataPolicy(DataPolicy.PERSISTENT_PARTITION).setDiskStoreName("disk");
    rf.setPartitionAttributes(new PartitionAttributesFactory().setTotalNumBuckets(1).create());
    if (ttl > 0) {
      rf.setEntryTimeToLive(new ExpirationAttributes(ttl, ExpirationAction.DESTROY));
    }

    if (isEntryEviction) {
      rf.setEvictionAttributes(
          EvictionAttributes.createLRUEntryAttributes(10, EvictionAction.OVERFLOW_TO_DISK));
    } else if (isHeapEviction) {
      rf.setEvictionAttributes(EvictionAttributes.createLRUHeapAttributes(ObjectSizer.DEFAULT,
          EvictionAction.OVERFLOW_TO_DISK));
    }

    Region region = rf.create("region");
    return region;
  }
}
