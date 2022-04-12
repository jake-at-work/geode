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

import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.apache.geode.internal.cache.PartitionedRegionGetSomeKeys.getSomeKeys;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.DataSerializable;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;

/**
 * Confirm that the utils used for testing work as advertised
 *
 * @since GemFire 5.0
 */

public class PartitionedRegionTestUtilsDUnitTest extends CacheTestCase {

  private static final int TOTAL_NUM_BUCKETS = 5;
  private static final int MAX_KEYS = 50;

  private String regionName;

  private VM vm0;
  private VM vm1;
  private VM vm2;
  private VM vm3;

  @Before
  public void setUp() {
    vm0 = getHost(0).getVM(0);
    vm1 = getHost(0).getVM(1);
    vm2 = getHost(0).getVM(2);
    vm3 = getHost(0).getVM(3);

    regionName = getUniqueName();
  }

  @Override
  public Properties getDistributedSystemProperties() {
    var config = new Properties();
    config.put(SERIALIZABLE_OBJECT_FILTER, TestPRKey.class.getName());
    return config;
  }

  /**
   * Test the {@link PartitionedRegionGetSomeKeys#getSomeKeys(PartitionedRegion, Random)} method,
   * making sure it returns keys when there are keys and {@link java.util.Collections#EMPTY_SET}
   * when there are none.
   */
  @Test
  public void testGetKeys() throws Exception {
    vm0.invoke(() -> createPartitionedRegion());
    vm1.invoke(() -> createPartitionedRegion());
    vm2.invoke(() -> createPartitionedRegion());

    vm0.invoke(() -> {
      var partitionedRegion = (PartitionedRegion) getCache().getRegion(regionName);
      var random = new Random(123);
      // Assert that its empty
      for (var i = 0; i < 5; i++) {
        Set someKeys = getSomeKeys(partitionedRegion, random);
        assertNotNull(someKeys);
        assertTrue(someKeys.isEmpty());
      }

      for (var i = 0; i < MAX_KEYS; i++) {
        partitionedRegion.put("testKey" + i, i);
      }

      // Assert not empty and has value in an acceptable range
      for (var i = 0; i < 5; i++) {
        Set someKeys = getSomeKeys(partitionedRegion, random);
        assertNotNull(someKeys);
        assertFalse(someKeys.isEmpty());
        for (var key : someKeys) {
          var val = (Integer) partitionedRegion.get(key);
          assertNotNull(val);
          assertTrue(val >= 0);
          assertTrue(val < MAX_KEYS);
        }
      }
    });
  }

  @Test
  public void testGetNodes() throws Exception {
    var validatorVM = vm2;

    validatorVM.invoke(this::createPRAndTestGetAllNodes);

    validatorVM.invoke(() -> {
      var partitionedRegion = (PartitionedRegion) getCache().getRegion(regionName);
      var allNodes = partitionedRegion.getAllNodes();
      assertThat(allNodes).isNotNull().hasSize(1);
    });

    vm0.invoke(this::createPRAndTestGetAllNodes);

    validatorVM.invoke(() -> {
      var partitionedRegion = (PartitionedRegion) getCache().getRegion(regionName);
      var allNodes = partitionedRegion.getAllNodes();
      assertThat(allNodes).isNotNull().hasSize(2);
    });

    vm1.invoke(this::createPRAndTestGetAllNodes);

    validatorVM.invoke(() -> {
      var partitionedRegion = (PartitionedRegion) getCache().getRegion(regionName);
      var allNodes = partitionedRegion.getAllNodes();
      assertThat(allNodes).isNotNull().hasSize(3);
    });
  }

  /**
   * Test the test utilities that allow investigation of a PartitionedRegion's local cache.
   */
  @Test
  public void testLocalCacheOps() throws Exception {
    vm0.invoke(() -> {
      var paf = new PartitionAttributesFactory();
      paf.setTotalNumBuckets(TOTAL_NUM_BUCKETS);
      paf.setLocalMaxMemory(8);
      RegionFactory regionFactory = getCache().createRegionFactory(RegionShortcut.PARTITION);
      regionFactory.setPartitionAttributes(paf.create());
      regionFactory.create(regionName);
    });

    vm2.invoke(() -> {
      var paf = new PartitionAttributesFactory();
      paf.setTotalNumBuckets(TOTAL_NUM_BUCKETS);
      paf.setLocalMaxMemory(0);
      RegionFactory regionFactory = getCache().createRegionFactory(RegionShortcut.PARTITION);
      regionFactory.setPartitionAttributes(paf.create());
      var partitionedRegion = (PartitionedRegion) regionFactory.create(regionName);

      var key3 = "lcKey3";
      var val3 = "lcVal3";
      var key4 = "lcKey4";
      var val4 = "lcVal4";

      // Test localCacheContainsKey
      assertThat(partitionedRegion.localCacheContainsKey(key3)).isFalse();
      assertThat(partitionedRegion.localCacheContainsKey(key4)).isFalse();
      partitionedRegion.put(key3, val3);
      assertThat(partitionedRegion.localCacheContainsKey(key3)).isFalse();
      assertThat(partitionedRegion.localCacheContainsKey(key4)).isFalse();
      assertThat(partitionedRegion.get(key3)).isEqualTo(val3);
      assertThat(partitionedRegion.localCacheContainsKey(key3)).isFalse();
      assertThat(partitionedRegion.localCacheContainsKey(key4)).isFalse();

      // test localCacheKeySet
      var localCacheKeySet = partitionedRegion.localCacheKeySet();
      assertThat(localCacheKeySet.contains(key3)).isFalse();
      assertThat(localCacheKeySet.contains(key4)).isFalse();

      // test localCacheGet
      assertThat(partitionedRegion.localCacheGet(key3)).isNull();
      assertThat(partitionedRegion.localCacheGet(key4)).isNull();
      partitionedRegion.put(key4, val4);
      assertThat(partitionedRegion.localCacheGet(key4)).isNull();
      assertThat(partitionedRegion.get(key4)).isEqualTo(val4);
      assertThat(partitionedRegion.localCacheGet(key4)).isNull();
    });
  }

  /**
   * Test the test method PartitionedRegion.getAllNodes Verify that it returns nodes after a value
   * has been placed into the PartitionedRegion.
   *
   * @see PartitionedRegion#getAllNodes()
   */
  @Test
  public void testGetBucketKeys() throws Exception {
    vm2.invoke(() -> createPartitionedRegion());
    vm3.invoke(() -> createPartitionedRegion());

    // Create an accessor
    var paf = new PartitionAttributesFactory();
    paf.setLocalMaxMemory(0);
    paf.setTotalNumBuckets(TOTAL_NUM_BUCKETS);

    RegionFactory regionFactory = getCache().createRegionFactory(RegionShortcut.PARTITION);
    regionFactory.setPartitionAttributes(paf.create());

    var partitionedRegion = (PartitionedRegion) regionFactory.create(regionName);

    var numberOfBuckets = partitionedRegion.getTotalNumberOfBuckets();

    for (var whichBucket = numberOfBuckets - 1; whichBucket >= 0; whichBucket--) {
      var bucketKeys = partitionedRegion.getBucketKeys(whichBucket);
      assertThat(bucketKeys).isEmpty();
    }

    // Create bucket number of keys, assuming a mod per key hashCode
    // There should be one key per bucket
    partitionedRegion.put(new TestPRKey(0, 1), 0);
    partitionedRegion.put(new TestPRKey(0, 2), 1);
    partitionedRegion.put(new TestPRKey(0, 3), 2);

    Set<TestPRKey> bucketKeys = partitionedRegion.getBucketKeys(0);

    assertThat(bucketKeys).hasSize(3);
    assertThat(bucketKeys.iterator().next().hashCode()).isEqualTo(0);
    assertThat(bucketKeys.iterator().next().hashCode()).isEqualTo(0);
    assertThat(bucketKeys.iterator().next().hashCode()).isEqualTo(0);

    // Skip bucket zero since we have three keys there, but fill out all the rest with keys
    for (var whichBucket = numberOfBuckets - 1; whichBucket > 0; whichBucket--) {
      var key = new TestPRKey(whichBucket, 0);
      partitionedRegion.put(key, whichBucket);
    }

    // Assert that the proper number of keys are placed in each bucket
    for (var whichBucket = 1; whichBucket < numberOfBuckets; whichBucket++) {
      bucketKeys = partitionedRegion.getBucketKeys(whichBucket);
      assertThat(bucketKeys).hasSize(1);
      var key = bucketKeys.iterator().next();
      assertThat(key.hashCode()).isEqualTo(whichBucket);
    }
  }

  /**
   * Test the test method {@link PartitionedRegion#getBucketOwnersForValidation(int)} Verify that
   * the information it discovers is the same as the local advisor.
   */
  @Test
  public void testGetBucketOwners() throws Exception {
    var regionName1 = getUniqueName() + "-r0";
    var regionName2 = getUniqueName() + "-r1";
    var regionName3 = getUniqueName() + "-r2";
    var regions = new String[] {regionName1, regionName2, regionName3};

    var numberOfBuckets = 3;

    var datastore1VM = vm2;
    var datastore2VM = vm3;
    var datastore3VM = vm0;
    var accessorVM = vm1;

    datastore1VM
        .invoke(() -> createPartitionedRegionsWithIncreasingRedundancy(numberOfBuckets, regions));
    datastore2VM
        .invoke(() -> createPartitionedRegionsWithIncreasingRedundancy(numberOfBuckets, regions));
    datastore3VM
        .invoke(() -> createPartitionedRegionsWithIncreasingRedundancy(numberOfBuckets, regions));

    accessorVM.invoke(() -> {
      var paf = new PartitionAttributesFactory();
      paf.setLocalMaxMemory(0);
      paf.setTotalNumBuckets(numberOfBuckets);

      RegionFactory regionFactory = getCache().createRegionFactory(RegionShortcut.PARTITION);

      for (var redundancy = 0; redundancy < regions.length; redundancy++) {
        paf.setRedundantCopies(redundancy);
        regionFactory.setPartitionAttributes(paf.create());
        var region = regionFactory.create(regions[redundancy]);
        assertThat(region.size()).isEqualTo(0);
      }
    });

    datastore1VM.invoke(() -> validateNoBucketOwners(regions, numberOfBuckets));
    datastore2VM.invoke(() -> validateNoBucketOwners(regions, numberOfBuckets));
    datastore3VM.invoke(() -> validateNoBucketOwners(regions, numberOfBuckets));
    accessorVM.invoke(() -> validateNoBucketOwners(regions, numberOfBuckets));

    accessorVM.invoke(() -> {
      for (var regionName : regions) {
        var partitionedRegion = (PartitionedRegion) getCache().getRegion(regionName);
        assertThat(partitionedRegion.getTotalNumberOfBuckets()).isEqualTo(3);
        // Create one bucket
        partitionedRegion.put(0, "zero");
        assertThat(partitionedRegion.getRegionAdvisor().getCreatedBucketsCount()).isEqualTo(1);
      }
    });

    accessorVM.invoke(() -> validateOneBucketPrimary(regions));
    datastore1VM.invoke(() -> validateOneBucketPrimary(regions));
    datastore2VM.invoke(() -> validateOneBucketPrimary(regions));
    datastore3VM.invoke(() -> validateOneBucketPrimary(regions));
  }

  private void validateOneBucketPrimary(String[] regions) {
    for (var regionName : regions) {
      var partitionedRegion = (PartitionedRegion) getCache().getRegion(regionName);
      try {
        for (int bucketId : partitionedRegion.getRegionAdvisor().getBucketSet()) {
          assertThat(partitionedRegion.getRegionAdvisor().getBucketOwners(bucketId))
              .hasSize(partitionedRegion.getRedundantCopies() + 1);

          List primaries = partitionedRegion.getBucketOwnersForValidation(bucketId);
          assertThat(primaries).hasSize(partitionedRegion.getRedundantCopies() + 1);

          var primaryCount = 0;
          for (var primaryInfo : primaries) {
            var memberAndBoolean = (Object[]) primaryInfo;
            assertThat(memberAndBoolean).hasSize(3); // memberId, isPrimary and hostToken(new)
            assertThat(memberAndBoolean[0]).isInstanceOf(DistributedMember.class);
            assertThat(memberAndBoolean[1].getClass()).isSameAs(Boolean.class);
            var isPrimary = (Boolean) memberAndBoolean[1];
            if (isPrimary) {
              primaryCount++;
            }
          }
          assertThat(primaryCount).isEqualTo(1);
        }
      } catch (ForceReattemptException noGood) {
        Assert.fail("Unexpected force retry", noGood);
      }
    }
  }

  private void validateNoBucketOwners(String[] regions, int numberOfBuckets)
      throws ForceReattemptException {
    for (var regionName : regions) {
      var partitionedRegion = (PartitionedRegion) getCache().getRegion(regionName);
      assertThat(partitionedRegion.getTotalNumberOfBuckets()).isEqualTo(numberOfBuckets);

      for (var whichBucket = 0; whichBucket < partitionedRegion
          .getTotalNumberOfBuckets(); whichBucket++) {
        assertThat(partitionedRegion.getRegionAdvisor().getBucketOwners(whichBucket)).isEmpty();
        assertThat(partitionedRegion.getBucketOwnersForValidation(whichBucket)).isEmpty();
      }
    }
  }

  private void createPartitionedRegionsWithIncreasingRedundancy(int numberOfBuckets,
      String[] regions) {
    var paf = new PartitionAttributesFactory();
    paf.setTotalNumBuckets(numberOfBuckets);

    RegionFactory regionFactory = getCache().createRegionFactory(RegionShortcut.PARTITION);

    for (var redundancy = 0; redundancy < regions.length; redundancy++) {
      paf.setRedundantCopies(redundancy);
      regionFactory.setPartitionAttributes(paf.create());
      var region = regionFactory.create(regions[redundancy]);
      assertThat(region.size()).isEqualTo(0);
    }
  }

  private void createPartitionedRegion() {
    var paf = new PartitionAttributesFactory();
    paf.setTotalNumBuckets(TOTAL_NUM_BUCKETS);

    RegionFactory regionFactory = getCache().createRegionFactory(RegionShortcut.PARTITION);
    regionFactory.setPartitionAttributes(paf.create());

    regionFactory.create(regionName);
  }

  private void createPRAndTestGetAllNodes() {
    var paf = new PartitionAttributesFactory();
    paf.setTotalNumBuckets(TOTAL_NUM_BUCKETS);

    RegionFactory<Object, Integer> regionFactory =
        getCache().createRegionFactory(RegionShortcut.PARTITION);
    regionFactory.setPartitionAttributes(paf.create());

    var region = regionFactory.create(regionName);

    // For each invocation, create a key that has a sequential hashCode.
    // Putting this key into the PR should force a new bucket allocation on
    // each new VM (assuming a mod on the hashCode), forcing the number of VMs to increase
    // when we call getAllNodes each time this method is called.
    var counter = region.get("Counter");
    Integer keyHash;
    if (counter == null) {
      counter = 0;
    } else {
      counter = counter + 1;
    }
    keyHash = counter;
    region.put("Counter", counter);
    region.put(new TestGetNodesKey(keyHash), counter);

    var allNodes = ((PartitionedRegion) region).getAllNodes();
    assertThat(allNodes).isNotNull().isNotEmpty();
  }

  /**
   * Test the test method PartitionedRegion.getAllNodes Verify that it returns nodes after a value
   * has been placed into the PartitionedRegion.
   *
   * @see PartitionedRegion#getAllNodes()
   */
  private static class TestGetNodesKey implements DataSerializable {

    private int hashCode;

    TestGetNodesKey(int hashCode) {
      this.hashCode = hashCode;
    }

    public TestGetNodesKey() {
      // nothing
    }

    public int hashCode() {
      return hashCode;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      out.writeInt(hashCode);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      hashCode = in.readInt();
    }
  }

  private static class TestPRKey implements Serializable {

    private final int hashCode;
    private final int differentiator;

    TestPRKey(int hashCode, int differentiator) {
      this.hashCode = hashCode;
      this.differentiator = differentiator;
    }

    public int hashCode() {
      return hashCode;
    }

    public boolean equals(Object obj) {
      return obj instanceof TestPRKey && ((TestPRKey) obj).differentiator == differentiator;
    }

    public String toString() {
      return getClass().getSimpleName() + " " + hashCode + " diff " + differentiator;
    }
  }
}
