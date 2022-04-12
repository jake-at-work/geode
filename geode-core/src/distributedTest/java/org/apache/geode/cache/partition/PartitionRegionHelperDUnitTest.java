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
package org.apache.geode.cache.partition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.FixedPartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.internal.cache.partitioned.fixed.QuarterPartitionResolver;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;


public class PartitionRegionHelperDUnitTest extends JUnit4CacheTestCase {

  public PartitionRegionHelperDUnitTest() {
    super();
  }

  @Test
  public void testAssignBucketsToPartitions() throws Throwable {
    var host = Host.getHost(0);
    var vm0 = host.getVM(0);
    var vm1 = host.getVM(1);
    var vm2 = host.getVM(2);

    var createPrRegion = new SerializableRunnable("createRegion") {
      @Override
      public void run() {
        Cache cache = getCache();
        var attr = new AttributesFactory();
        var paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        paf.setTotalNumBuckets(12);
        var prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        cache.createRegion("region1", attr.create());
      }
    };
    vm0.invoke(createPrRegion);
    vm1.invoke(createPrRegion);
    vm2.invoke(createPrRegion);

    var assignBuckets = new SerializableRunnable("assign partitions") {
      @Override
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        PartitionRegionHelper.assignBucketsToPartitions(region);
      }
    };

    AsyncInvocation future1 = vm0.invokeAsync(assignBuckets);
    AsyncInvocation future2 = vm1.invokeAsync(assignBuckets);
    AsyncInvocation future3 = vm2.invokeAsync(assignBuckets);
    future1.join(60 * 1000);
    future2.join(60 * 1000);
    future3.join(60 * 1000);
    if (future1.exceptionOccurred()) {
      throw future1.getException();
    }
    if (future2.exceptionOccurred()) {
      throw future2.getException();
    }
    if (future3.exceptionOccurred()) {
      throw future3.getException();
    }

    var checkAssignment = new SerializableRunnable("check assignment") {
      @Override
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        var info = PartitionRegionHelper.getPartitionRegionInfo(region);
        assertEquals(12, info.getCreatedBucketCount());
        assertEquals(0, info.getLowRedundancyBucketCount());
        for (var member : info.getPartitionMemberInfo()) {
          assertEquals(8, member.getBucketCount());
          // TODO unfortunately, with redundancy, we can end up
          // not balancing primaries in favor of balancing buckets. The problem
          // is that We could create too many redundant copies on a node, which means
          // when we get to the last bucket, which should be primary on that node, we
          // don't even put a copy of the bucket on that node
          // See bug #40470
          // assertIndexDetailsEquals(4, member.getPrimaryCount());
        }
      }
    };

    vm0.invoke(checkAssignment);
  }


  @Test
  public void testAssignBucketsToPartitions_FPR() throws Throwable {
    var host = Host.getHost(0);
    var vm0 = host.getVM(0);
    var vm1 = host.getVM(1);
    var vm2 = host.getVM(2);
    var vm3 = host.getVM(3);

    var createPrRegion1 = new SerializableRunnable("createRegion") {
      @Override
      public void run() {
        Cache cache = getCache();
        var fpa1 =
            FixedPartitionAttributes.createFixedPartition("Q1", true, 3);
        var fpa2 =
            FixedPartitionAttributes.createFixedPartition("Q2", false, 3);
        var attr = new AttributesFactory();
        var paf = new PartitionAttributesFactory();
        paf.addFixedPartitionAttributes(fpa1);
        paf.addFixedPartitionAttributes(fpa2);
        paf.setPartitionResolver(new QuarterPartitionResolver());
        paf.setRedundantCopies(1);
        paf.setTotalNumBuckets(12);
        var prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        cache.createRegion("region1", attr.create());
      }
    };
    vm0.invoke(createPrRegion1);
    var createPrRegion2 = new SerializableRunnable("createRegion") {
      @Override
      public void run() {
        Cache cache = getCache();
        var fpa1 =
            FixedPartitionAttributes.createFixedPartition("Q2", true, 3);
        var fpa2 =
            FixedPartitionAttributes.createFixedPartition("Q3", false, 3);
        var attr = new AttributesFactory();
        var paf = new PartitionAttributesFactory();
        paf.addFixedPartitionAttributes(fpa1);
        paf.addFixedPartitionAttributes(fpa2);
        paf.setPartitionResolver(new QuarterPartitionResolver());
        paf.setRedundantCopies(1);
        paf.setTotalNumBuckets(12);
        var prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        cache.createRegion("region1", attr.create());
      }
    };
    vm1.invoke(createPrRegion2);
    var createPrRegion3 = new SerializableRunnable("createRegion") {
      @Override
      public void run() {
        Cache cache = getCache();
        var fpa1 =
            FixedPartitionAttributes.createFixedPartition("Q3", true, 3);
        var fpa2 =
            FixedPartitionAttributes.createFixedPartition("Q1", false, 3);
        var attr = new AttributesFactory();
        var paf = new PartitionAttributesFactory();
        paf.addFixedPartitionAttributes(fpa1);
        paf.addFixedPartitionAttributes(fpa2);
        paf.setPartitionResolver(new QuarterPartitionResolver());
        paf.setRedundantCopies(1);
        paf.setTotalNumBuckets(12);
        var prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        cache.createRegion("region1", attr.create());
      }
    };
    vm2.invoke(createPrRegion3);

    var assignBuckets = new SerializableRunnable("assign partitions") {
      @Override
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        PartitionRegionHelper.assignBucketsToPartitions(region);
      }
    };

    AsyncInvocation future1 = vm0.invokeAsync(assignBuckets);
    AsyncInvocation future2 = vm1.invokeAsync(assignBuckets);
    AsyncInvocation future3 = vm2.invokeAsync(assignBuckets);
    future1.join();
    future2.join();
    future3.join();
    if (future1.exceptionOccurred()) {
      throw future1.getException();
    }
    if (future2.exceptionOccurred()) {
      throw future2.getException();
    }
    if (future3.exceptionOccurred()) {
      throw future3.getException();
    }

    var checkAssignment = new SerializableRunnable("check assignment") {
      @Override
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        var info = PartitionRegionHelper.getPartitionRegionInfo(region);
        assertEquals(9, info.getCreatedBucketCount());
        assertEquals(0, info.getLowRedundancyBucketCount());
        for (var member : info.getPartitionMemberInfo()) {
          assertEquals(6, member.getBucketCount());
        }
      }
    };
    vm0.invoke(checkAssignment);

    var createPrRegion4 = new SerializableRunnable("createRegion") {
      @Override
      public void run() {
        Cache cache = getCache();
        var attr = new AttributesFactory();
        var paf = new PartitionAttributesFactory();
        paf.setPartitionResolver(new QuarterPartitionResolver());
        paf.setLocalMaxMemory(0);
        paf.setRedundantCopies(1);
        paf.setTotalNumBuckets(12);
        var prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        var region = cache.createRegion("region1", attr.create());
        for (var month : Months_Accessor.values()) {
          var dateString = 10 + "-" + month + "-" + "2009";
          var DATE_FORMAT = "dd-MMM-yyyy";
          var sdf = new SimpleDateFormat(DATE_FORMAT, Locale.US);
          Date date = null;
          try {
            date = sdf.parse(dateString);
          } catch (ParseException e) {
            Assert.fail("Exception Occurred while parseing date", e);
          }
          var value = month.toString() + 10;
          region.put(date, value);
        }
      }
    };
    vm3.invoke(createPrRegion4);

    var checkMembers = new SerializableRunnable("createRegion") {
      @Override
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region1");
        for (var month : Months_Accessor.values()) {
          var dateString = 10 + "-" + month + "-" + "2009";
          var DATE_FORMAT = "dd-MMM-yyyy";
          var sdf = new SimpleDateFormat(DATE_FORMAT, Locale.US);
          Date date = null;
          try {
            date = sdf.parse(dateString);
          } catch (ParseException e) {
            Assert.fail("Exception Occurred while parseing date", e);
          }
          var key1Pri = PartitionRegionHelper.getPrimaryMemberForKey(region, date);
          assertNotNull(key1Pri);
          Set<DistributedMember> buk0AllMems =
              PartitionRegionHelper.getAllMembersForKey(region, date);
          assertEquals(2, buk0AllMems.size());
          Set<DistributedMember> buk0RedundantMems =
              PartitionRegionHelper.getRedundantMembersForKey(region, date);
          assertEquals(1, buk0RedundantMems.size());
        }
      }
    };
    vm3.invoke(checkMembers);

  }

  @Test
  public void testMembersForKey() throws Exception {
    var host = Host.getHost(0);
    var accessor = host.getVM(0);
    var ds1 = host.getVM(1);
    var ds2 = host.getVM(2);
    var ds3 = host.getVM(3);
    final var prName = getUniqueName();
    final var tb = 11;
    final var rc = 1;

    accessor.invoke(new SerializableRunnable("createAccessor") {
      @Override
      public void run() {
        Cache cache = getCache();
        var attr = new AttributesFactory();
        attr.setPartitionAttributes(new PartitionAttributesFactory().setLocalMaxMemory(0)
            .setRedundantCopies(rc).setTotalNumBuckets(tb).create());
        cache.createRegion(prName, attr.create());
      }
    });

    var d2v = new HashMap<DistributedMember, VM>();
    var createPrRegion = new SerializableCallable("createDataStore") {
      @Override
      public Object call() throws Exception {
        Cache cache = getCache();
        var attr = new AttributesFactory();
        attr.setPartitionAttributes(new PartitionAttributesFactory().setRedundantCopies(rc)
            .setTotalNumBuckets(tb).create());
        cache.createRegion(prName, attr.create());
        return cache.getDistributedSystem().getDistributedMember();
      }
    };
    var dm = (DistributedMember) ds1.invoke(createPrRegion);
    d2v.put(dm, ds1);
    dm = (DistributedMember) ds2.invoke(createPrRegion);
    d2v.put(dm, ds2);
    dm = (DistributedMember) ds3.invoke(createPrRegion);
    d2v.put(dm, ds3);

    final Integer buk0Key1 = 0;
    final Integer buk0Key2 = buk0Key1 + tb;
    final Integer buk1Key1 = 1;

    accessor.invoke(new CacheSerializableRunnable("nonPRcheck") {
      @SuppressWarnings("unchecked")
      @Override
      public void run2() throws CacheException {
        var attr = new AttributesFactory();
        {
          attr.setScope(Scope.LOCAL);
          var lr = getCache().createRegion(prName + "lr", attr.create());
          try {
            // no-pr check
            nonPRMemberForKey(lr, buk0Key1);
          } finally {
            lr.destroyRegion();
          }
        }

        {
          attr = new AttributesFactory();
          attr.setScope(Scope.DISTRIBUTED_ACK);
          var dr = getCache().createRegion(prName + "dr", attr.create());
          try {
            // no-pr check
            nonPRMemberForKey(dr, buk0Key1);
          } finally {
            dr.destroyRegion();
          }
        }
      }

      private void nonPRMemberForKey(Region lr, final Object key) {
        try {
          PartitionRegionHelper.getPrimaryMemberForKey(lr, key);
          fail();
        } catch (IllegalArgumentException ignored) {
        }
        try {
          PartitionRegionHelper.getAllMembersForKey(lr, key);
          fail();
        } catch (IllegalArgumentException ignored) {
        }
        try {
          PartitionRegionHelper.getRedundantMembersForKey(lr, key);
          fail();
        } catch (IllegalArgumentException ignored) {
        }
      }
    });

    var noKeyThenKeyStuff =
        (Object[]) accessor.invoke(new SerializableCallable("noKeyThenKey") {
          @Override
          public Object call() throws Exception {
            Region<Integer, String> r = getCache().getRegion(prName);

            // NPE check
            try {
              PartitionRegionHelper.getPrimaryMemberForKey(r, null);
              fail();
            } catch (IllegalStateException ignored) {
            }
            try {
              PartitionRegionHelper.getAllMembersForKey(r, null);
              fail();
            } catch (IllegalStateException ignored) {
            }
            try {
              PartitionRegionHelper.getRedundantMembersForKey(r, null);
              fail();
            } catch (IllegalStateException ignored) {
            }

            // buk0
            assertNull(PartitionRegionHelper.getPrimaryMemberForKey(r, buk0Key1));
            assertTrue(PartitionRegionHelper.getAllMembersForKey(r, buk0Key1).isEmpty());
            assertTrue(PartitionRegionHelper.getRedundantMembersForKey(r, buk0Key1).isEmpty());
            // buk1
            assertNull(PartitionRegionHelper.getPrimaryMemberForKey(r, buk1Key1));
            assertTrue(PartitionRegionHelper.getAllMembersForKey(r, buk1Key1).isEmpty());
            assertTrue(PartitionRegionHelper.getRedundantMembersForKey(r, buk1Key1).isEmpty());

            r.put(buk0Key1, "zero");
            // buk0, key1
            var key1Pri = PartitionRegionHelper.getPrimaryMemberForKey(r, buk0Key1);
            assertNotNull(key1Pri);
            var buk0AllMems =
                PartitionRegionHelper.getAllMembersForKey(r, buk0Key1);
            assertEquals(rc + 1, buk0AllMems.size());
            var buk0RedundantMems =
                PartitionRegionHelper.getRedundantMembersForKey(r, buk0Key1);
            assertEquals(rc, buk0RedundantMems.size());
            var me = r.getCache().getDistributedSystem().getDistributedMember();
            try {
              buk0AllMems.add(me);
              fail();
            } catch (UnsupportedOperationException ignored) {
            }
            try {
              buk0AllMems.remove(me);
              fail();
            } catch (UnsupportedOperationException ignored) {
            }
            try {
              buk0RedundantMems.add(me);
              fail();
            } catch (UnsupportedOperationException ignored) {
            }
            try {
              buk0RedundantMems.remove(me);
              fail();
            } catch (UnsupportedOperationException ignored) {
            }
            assertTrue(buk0AllMems.containsAll(buk0RedundantMems));
            assertTrue(buk0AllMems.contains(key1Pri));
            assertTrue(!buk0RedundantMems.contains(key1Pri));

            // buk0, key2
            var key2Pri = PartitionRegionHelper.getPrimaryMemberForKey(r, buk0Key2);
            assertNotNull(key2Pri);
            buk0AllMems = PartitionRegionHelper.getAllMembersForKey(r, buk0Key2);
            assertEquals(rc + 1, buk0AllMems.size());
            buk0RedundantMems = PartitionRegionHelper.getRedundantMembersForKey(r, buk0Key2);
            assertEquals(rc, buk0RedundantMems.size());
            assertTrue(buk0AllMems.containsAll(buk0RedundantMems));
            assertTrue(buk0AllMems.contains(key2Pri));
            assertTrue(!buk0RedundantMems.contains(key2Pri));

            // buk1
            assertNull(PartitionRegionHelper.getPrimaryMemberForKey(r, buk1Key1));
            assertTrue(PartitionRegionHelper.getAllMembersForKey(r, buk1Key1).isEmpty());
            assertTrue(PartitionRegionHelper.getRedundantMembersForKey(r, buk1Key1).isEmpty());
            return new Object[] {key1Pri, buk0AllMems, buk0RedundantMems};
          }
        });
    final var buk0Key1Pri = (DistributedMember) noKeyThenKeyStuff[0];
    final var buk0AllMems = (Set<DistributedMember>) noKeyThenKeyStuff[1];
    final var buk0Redundants = (Set<DistributedMember>) noKeyThenKeyStuff[2];

    var buk0Key1PriVM = d2v.get(buk0Key1Pri);
    buk0Key1PriVM.invoke(new CacheSerializableRunnable("assertPrimaryness") {
      @Override
      public void run2() throws CacheException {
        var pr = (PartitionedRegion) getCache().getRegion(prName);
        Integer bucketId =
            PartitionedRegionHelper.getHashKey(pr, null, buk0Key1, null, null);
        try {
          var buk0 = pr.getDataStore().getInitializedBucketForId(buk0Key1, bucketId);
          assertNotNull(buk0);
          assertTrue(buk0.getBucketAdvisor().isPrimary());
        } catch (ForceReattemptException e) {
          LogWriterUtils.getLogWriter().severe(e);
          fail();
        }
      }
    });
    var assertHasBucket =
        new CacheSerializableRunnable("assertHasBucketAndKey") {
          @Override
          public void run2() throws CacheException {
            var pr = (PartitionedRegion) getCache().getRegion(prName);
            Integer bucketId =
                PartitionedRegionHelper.getHashKey(pr, null, buk0Key1, null, null);
            try {
              var buk0 = pr.getDataStore().getInitializedBucketForId(buk0Key1, bucketId);
              assertNotNull(buk0);
              var k1e = buk0.getEntry(buk0Key1);
              assertNotNull(k1e);
            } catch (ForceReattemptException e) {
              LogWriterUtils.getLogWriter().severe(e);
              fail();
            }
          }
        };
    for (var bom : buk0AllMems) {
      var v = d2v.get(bom);
      LogWriterUtils.getLogWriter()
          .info("Visiting bucket owner member " + bom + " for key " + buk0Key1);
      v.invoke(assertHasBucket);
    }

    var assertRed = new CacheSerializableRunnable("assertRedundant") {
      @Override
      public void run2() throws CacheException {
        var pr = (PartitionedRegion) getCache().getRegion(prName);
        Integer bucketId =
            PartitionedRegionHelper.getHashKey(pr, null, buk0Key1, null, null);
        try {
          var buk0 = pr.getDataStore().getInitializedBucketForId(buk0Key1, bucketId);
          assertNotNull(buk0);
          assertFalse(buk0.getBucketAdvisor().isPrimary());
        } catch (ForceReattemptException e) {
          LogWriterUtils.getLogWriter().severe(e);
          fail();
        }
      }
    };
    for (var redm : buk0Redundants) {
      var v = d2v.get(redm);
      LogWriterUtils.getLogWriter()
          .info("Visiting redundant member " + redm + " for key " + buk0Key1);
      v.invoke(assertRed);
    }
  }

  @Test
  public void testMoveSingleBucket() {
    var host = Host.getHost(0);
    var vm0 = host.getVM(0);
    var vm1 = host.getVM(1);
    var vm2 = host.getVM(2);

    var createPrRegion = new SerializableCallable("createRegion") {
      @Override
      public Object call() {
        Cache cache = getCache();
        var attr = new AttributesFactory();
        var paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        paf.setTotalNumBuckets(12);
        var prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        cache.createRegion("region1", attr.create());
        return cache.getDistributedSystem().getDistributedMember();
      }
    };
    final var member0 = (DistributedMember) vm0.invoke(createPrRegion);
    final var member1 = (DistributedMember) vm1.invoke(createPrRegion);

    // populate the region with data so we have some buckets
    vm0.invoke(new SerializableRunnable("create data") {
      @Override
      public void run() {
        for (var i = 0; i < 8; i++) {
          var region = getCache().getRegion("region1");
          region.put(i, "one");
        }
      }
    });

    // Create VM 2 later so that it doesn't have any buckets
    final var member2 = (DistributedMember) vm2.invoke(createPrRegion);

    // Try some explicit moves
    vm0.invoke(new SerializableRunnable("create data") {
      @Override
      public void run() {
        var region = getCache().getRegion("region1");

        assertHasMembers(PartitionRegionHelper.getAllMembersForKey(region, 1), member0, member1);


        // Try to move a bucket to a member that already has the bucket
        try {
          PartitionRegionHelper.moveBucketByKey(region, member0, member1, 1);
          fail("Should have received an exception");
        } catch (IllegalStateException expected) {
          System.err.println(expected);
        }
        assertHasMembers(PartitionRegionHelper.getAllMembersForKey(region, 1), member0, member1);

        // Try to move the bucket from a member that doesn't have the bucket
        try {
          PartitionRegionHelper.moveBucketByKey(region, member2, member2, 1);
          fail("Should have received an exception");
        } catch (IllegalStateException expected) {
          System.err.println(expected);
        }
        assertHasMembers(PartitionRegionHelper.getAllMembersForKey(region, 1), member0, member1);

        // Try to move the bucket from an invalid member
        try {
          PartitionRegionHelper.moveBucketByKey(region, member2,
              new InternalDistributedMember("localhost", 5), 1);
          fail("Should have received an exception");
        } catch (IllegalStateException expected) {
          System.err.println(expected);
        }
        assertHasMembers(PartitionRegionHelper.getAllMembersForKey(region, 1), member0, member1);

        // Try to move the bucket that doesn't exist
        try {
          PartitionRegionHelper.moveBucketByKey(region, member0, member2, 10);
          fail("Should have received an exception");
        } catch (IllegalStateException expected) {
          System.err.println(expected);
        }
        assertHasMembers(PartitionRegionHelper.getAllMembersForKey(region, 10));

        // Do some successful moves.
        PartitionRegionHelper.moveBucketByKey(region, member0, member2, 1);
        assertHasMembers(PartitionRegionHelper.getAllMembersForKey(region, 1), member1, member2);
        PartitionRegionHelper.moveBucketByKey(region, member2, member0, 1);
        assertHasMembers(PartitionRegionHelper.getAllMembersForKey(region, 1), member0, member1);

        PartitionRegionHelper.moveBucketByKey(region, member0, member2, 2);
        PartitionRegionHelper.moveBucketByKey(region, member1, member2, 3);
        PartitionRegionHelper.moveBucketByKey(region, member1, member2, 4);
        assertHasMembers(PartitionRegionHelper.getAllMembersForKey(region, 2), member1, member2);
        assertHasMembers(PartitionRegionHelper.getAllMembersForKey(region, 3), member0, member2);
        assertHasMembers(PartitionRegionHelper.getAllMembersForKey(region, 4), member0, member2);
      }
    });
  }

  @Test
  public void testMovePercentage() {
    var host = Host.getHost(0);
    var vm0 = host.getVM(0);
    var vm1 = host.getVM(1);
    var vm2 = host.getVM(2);

    var createPrRegion = new SerializableCallable("createRegion") {
      @Override
      public Object call() {
        Cache cache = getCache();
        var attr = new AttributesFactory();
        var paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        paf.setTotalNumBuckets(12);
        var prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        cache.createRegion("region1", attr.create());
        return cache.getDistributedSystem().getDistributedMember();
      }
    };
    final var member0 = (DistributedMember) vm0.invoke(createPrRegion);
    final var member1 = (DistributedMember) vm1.invoke(createPrRegion);

    // populate the region with data so we have some buckets
    vm0.invoke(new SerializableRunnable("create data") {
      @Override
      public void run() {
        for (var i = 0; i < 8; i++) {
          var region = getCache().getRegion("region1");
          region.put(i, "one");
        }
      }
    });

    // Create VM 2 later so that it doesn't have any buckets
    final var member2 = (DistributedMember) vm2.invoke(createPrRegion);

    // Try some percentage moves
    vm0.invoke(new SerializableRunnable("create data") {
      @Override
      public void run() {
        var region = getCache().getRegion("region1");

        assertHasMembers(PartitionRegionHelper.getAllMembersForKey(region, 1), member0, member1);


        // Try to move the bucket to an invalid member
        try {
          PartitionRegionHelper.moveData(region, member1,
              new InternalDistributedMember("localhost", 5), 100f);
          fail("Should have received an exception");
        } catch (IllegalStateException expected) {
          System.err.println(expected);
        }
        assertHasMembers(PartitionRegionHelper.getAllMembersForKey(region, 1), member0, member1);

        // Try to move the bucket from an invalid member
        try {
          PartitionRegionHelper.moveData(region, new InternalDistributedMember("localhost", 5),
              member2, 10f);
          fail("Should have received an exception");
        } catch (IllegalStateException expected) {
          System.err.println(expected);
        }
        assertHasMembers(PartitionRegionHelper.getAllMembersForKey(region, 1), member0, member1);

        // Try to move the bucket to the same member.
        try {
          PartitionRegionHelper.moveData(region, member0, member0, 10f);
          fail("Should have received an exception");
        } catch (IllegalStateException expected) {
          System.err.println(expected);
        }
        assertHasMembers(PartitionRegionHelper.getAllMembersForKey(region, 1), member0, member1);

        // Try to move a really small percentage of data.
        assertEquals(0, PartitionRegionHelper.moveData(region, member0, member2, .001f)
            .getTotalBucketTransfersCompleted());

        // Try to move data between members that have the same buckets
        // There should be nothing to move
        assertEquals(0, PartitionRegionHelper.moveData(region, member0, member1, 100f)
            .getTotalBucketTransfersCompleted());

        // Move all of the data
        assertEquals(8, PartitionRegionHelper.moveData(region, member0, member2, 100f)
            .getTotalBucketTransfersCompleted());
        // See if the buckets have moved.
        assertHasMembers(PartitionRegionHelper.getAllMembersForKey(region, 1), member1, member2);
        assertHasMembers(PartitionRegionHelper.getAllMembersForKey(region, 2), member1, member2);
        assertHasMembers(PartitionRegionHelper.getAllMembersForKey(region, 3), member1, member2);
        assertHasMembers(PartitionRegionHelper.getAllMembersForKey(region, 4), member1, member2);
        assertHasMembers(PartitionRegionHelper.getAllMembersForKey(region, 5), member1, member2);
        assertHasMembers(PartitionRegionHelper.getAllMembersForKey(region, 6), member1, member2);
        assertHasMembers(PartitionRegionHelper.getAllMembersForKey(region, 7), member1, member2);
        // Retry the move, to show there's nothing left
        assertEquals(0, PartitionRegionHelper.moveData(region, member0, member2, 50f)
            .getTotalBucketTransfersCompleted());

        // Move data in chunks
        assertEquals(2, PartitionRegionHelper.moveData(region, member1, member0, 25f)
            .getTotalBucketTransfersCompleted());
        assertEquals(2, PartitionRegionHelper.moveData(region, member1, member0, 34f)
            .getTotalBucketTransfersCompleted());
        assertEquals(2, PartitionRegionHelper.moveData(region, member1, member0, 50f)
            .getTotalBucketTransfersCompleted());
        assertEquals(2, PartitionRegionHelper.moveData(region, member1, member0, 100f)
            .getTotalBucketTransfersCompleted());
        // Should be nothing left to move
        assertEquals(0, PartitionRegionHelper.moveData(region, member1, member0, 100)
            .getTotalBucketTransfersCompleted());

        assertHasMembers(PartitionRegionHelper.getAllMembersForKey(region, 1), member0, member2);
        assertHasMembers(PartitionRegionHelper.getAllMembersForKey(region, 2), member0, member2);
        assertHasMembers(PartitionRegionHelper.getAllMembersForKey(region, 3), member0, member2);
        assertHasMembers(PartitionRegionHelper.getAllMembersForKey(region, 4), member0, member2);
        assertHasMembers(PartitionRegionHelper.getAllMembersForKey(region, 5), member0, member2);
        assertHasMembers(PartitionRegionHelper.getAllMembersForKey(region, 6), member0, member2);
        assertHasMembers(PartitionRegionHelper.getAllMembersForKey(region, 7), member0, member2);
      }
    });
  }

  public void assertHasMembers(Set<DistributedMember> got, DistributedMember... expected) {
    var expectedSet = new HashSet(Arrays.asList(expected));
    assertEquals(expectedSet, got);
  }

  public enum Months_Accessor {
    JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP
  }
}
