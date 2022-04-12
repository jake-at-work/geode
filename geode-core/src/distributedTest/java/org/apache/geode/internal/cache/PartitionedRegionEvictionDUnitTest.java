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

import static java.lang.System.getProperties;
import static java.util.Map.Entry;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;

import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.InterestPolicy;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.SubscriptionAttributes;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.internal.cache.control.HeapMemoryMonitor;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceType;
import org.apache.geode.internal.cache.eviction.HeapEvictor;
import org.apache.geode.internal.cache.eviction.HeapLRUController;
import org.apache.geode.logging.internal.OSProcess;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;


public class PartitionedRegionEvictionDUnitTest extends JUnit4CacheTestCase {

  @Test
  public void testHeapLRUWithOverflowToDisk() {
    // Ignore this excetion as this can happen if pool is shutting down
    IgnoredException
        .addIgnoredException(java.util.concurrent.RejectedExecutionException.class.getName());
    final var host = Host.getHost(0);
    final var vm2 = host.getVM(2);
    final var vm3 = host.getVM(3);
    final var uniqName = getUniqueName();
    final var heapPercentage = 50.9f;
    final var redundantCopies = 1;
    final var evictorInterval = 100;
    final var name = uniqName + "-PR";

    final var create =
        new CacheSerializableRunnable("Create Heap LRU with Overflow to disk partitioned Region") {
          @Override
          public void run2() {
            System.setProperty(HeapLRUController.TOP_UP_HEAP_EVICTION_PERCENTAGE_PROPERTY,
                Float.toString(0));
            setEvictionPercentage(heapPercentage);
            final var sp = System.getProperties();
            final var factory = new AttributesFactory();
            factory.setOffHeap(isOffHeap());
            factory.setPartitionAttributes(
                new PartitionAttributesFactory().setRedundantCopies(redundantCopies).create());
            factory.setEvictionAttributes(
                EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK));
            factory.setDiskSynchronous(true);
            var dsf = getCache().createDiskStoreFactory();
            final var diskDirs = new File[1];
            diskDirs[0] = new File("overflowDir/" + uniqName + "_" + OSProcess.getId());
            diskDirs[0].mkdirs();
            dsf.setDiskDirs(diskDirs);
            var ds = dsf.create(name);
            factory.setDiskStoreName(ds.getName());
            final var pr =
                (PartitionedRegion) createRootRegion(name, factory.create());
            assertNotNull(pr);
          }
        };
    vm3.invoke(create);
    vm2.invoke(create);

    final var bucketsToCreate = 3;
    final var createBuckets = new SerializableRunnable("Create Buckets") {
      @Override
      public void run() {
        setEvictionPercentage(heapPercentage);
        final var pr = (PartitionedRegion) getRootRegion(name);
        assertNotNull(pr);
        // Create three buckets, add enough stuff in each to force two
        // overflow
        // ops in each
        for (var i = 0; i < bucketsToCreate; i++) {
          // assume mod-based hashing for bucket creation
          pr.put(i, "value0");
          pr.put(i + pr.getPartitionAttributes().getTotalNumBuckets(), "value1");
          pr.put(i + (pr.getPartitionAttributes().getTotalNumBuckets()) * 2, "value2");
        }
      }
    };
    vm3.invoke(createBuckets);

    final var assertBucketAttributesAndEviction =
        new SerializableCallable("Assert bucket attributes and eviction") {
          @Override
          public Object call() throws Exception {

            try {
              setEvictionPercentage(heapPercentage);
              final var sp = getProperties();
              final var expectedInvocations = 10;
              final var maximumWaitSeconds = 60; // seconds
              final var pollWaitMillis = evictorInterval * 2;
              assertTrue(pollWaitMillis < (SECONDS.toMillis(maximumWaitSeconds) * 4));
              final var pr = (PartitionedRegion) getRootRegion(name);
              assertNotNull(pr);
              for (final var integerBucketRegionEntry : pr.getDataStore()
                  .getAllLocalBuckets()) {
                final var entry = (Entry) integerBucketRegionEntry;
                final var bucketRegion = (BucketRegion) entry.getValue();
                if (bucketRegion == null) {
                  continue;
                }
                assertTrue(bucketRegion.getAttributes().getEvictionAttributes().getAlgorithm()
                    .isLRUHeap());
                assertTrue(bucketRegion.getAttributes().getEvictionAttributes().getAction()
                    .isOverflowToDisk());
              }
              raiseFakeNotification();
              var wc = new WaitCriterion() {
                String excuse;

                @Override
                public boolean done() {
                  // we have a primary
                  return pr.getDiskRegionStats().getNumOverflowOnDisk() == 9;
                }

                @Override
                public String description() {
                  return excuse;
                }
              };
              GeodeAwaitility.await().untilAsserted(wc);

              var entriesEvicted = 0;

              entriesEvicted += pr.getDiskRegionStats().getNumOverflowOnDisk();
              return entriesEvicted;
            } finally {
              cleanUpAfterFakeNotification();
              // Clean up the observer for future tests
              // final Properties sp = System.getProperties();
              /*
               * assertNotNull(sp .remove(HeapLRUCapacityController.FREE_MEMORY_OBSERVER));
               */
            }
          }
        };
    final var v2i = (Integer) vm2.invoke(assertBucketAttributesAndEviction);
    final var v3i = (Integer) vm3.invoke(assertBucketAttributesAndEviction);
    final var totalEvicted = v2i + v3i;
    // assume all three entries in each bucket were evicted to disk
    assertEquals((3 * bucketsToCreate * (redundantCopies + 1)), totalEvicted);
  }

  protected void raiseFakeNotification() {
    getCache().getHeapEvictor().setTestAbortAfterLoopCount(1);
    HeapMemoryMonitor.setTestDisableMemoryUpdates(true);

    setEvictionPercentage(85);
    var hmm =
        getCache().getInternalResourceManager().getHeapMonitor();
    hmm.setTestMaxMemoryBytes(100);

    hmm.updateStateAndSendEvent(90, "test");
  }

  protected void cleanUpAfterFakeNotification() {
    getCache().getHeapEvictor().setTestAbortAfterLoopCount(Integer.MAX_VALUE);
    HeapMemoryMonitor.setTestDisableMemoryUpdates(false);
  }

  @Test
  public void testHeapLRUWithLocalDestroy() {
    // Ignore this excetion as this can happen if pool is shutting down
    IgnoredException
        .addIgnoredException(java.util.concurrent.RejectedExecutionException.class.getName());
    final var host = Host.getHost(0);
    final var vm2 = host.getVM(2);
    final var vm3 = host.getVM(3);
    final var uniqName = getUniqueName();
    final var heapPercentage = 50.9f;
    final var redundantCopies = 1;
    final var evictorInterval = 100;
    final var name = uniqName + "-PR";

    final SerializableRunnable create = new CacheSerializableRunnable(
        "Create Heap LRU with local destroy on a partitioned Region") {
      @Override
      public void run2() {
        System.setProperty(HeapLRUController.TOP_UP_HEAP_EVICTION_PERCENTAGE_PROPERTY,
            Float.toString(0));
        setEvictionPercentage(heapPercentage);
        final var sp = System.getProperties();
        final var factory = new AttributesFactory();
        factory.setOffHeap(isOffHeap());
        factory.setPartitionAttributes(
            new PartitionAttributesFactory().setRedundantCopies(redundantCopies).create());
        factory.setEvictionAttributes(EvictionAttributes.createLRUHeapAttributes());
        final var pr = (PartitionedRegion) createRootRegion(name, factory.create());
        assertNotNull(pr);
      }
    };
    vm3.invoke(create);
    vm2.invoke(create);

    final var bucketsToCreate = 3;
    final var createBuckets = new SerializableRunnable("Create Buckets") {
      @Override
      public void run() {
        final var pr = (PartitionedRegion) getRootRegion(name);
        assertNotNull(pr);
        // Create three buckets, add enough stuff in each to force two
        // overflow
        // ops in each
        for (var i = 0; i < bucketsToCreate; i++) {
          // assume mod-based hashing for bucket creation
          pr.put(i, "value0");
          pr.put(i + pr.getPartitionAttributes().getTotalNumBuckets(), "value1");
          pr.put(i + (pr.getPartitionAttributes().getTotalNumBuckets()) * 2, "value2");
        }
      }
    };
    vm3.invoke(createBuckets);

    final var assertBucketAttributesAndEviction =
        new SerializableCallable("Assert bucket attributes and eviction") {
          @Override
          public Object call() throws Exception {
            try {
              final var sp = getProperties();
              final var expectedInvocations = 10;
              final var maximumWaitSeconds = 60; // seconds
              final var pollWaitMillis = evictorInterval * 2;
              assertTrue(pollWaitMillis < (SECONDS.toMillis(maximumWaitSeconds) * 4));
              final var pr = (PartitionedRegion) getRootRegion(name);
              assertNotNull(pr);

              long entriesEvicted = 0;
              for (final var integerBucketRegionEntry : pr.getDataStore()
                  .getAllLocalBuckets()) {
                final var entry = (Entry) integerBucketRegionEntry;

                final var bucketRegion = (BucketRegion) entry.getValue();
                if (bucketRegion == null) {
                  continue;
                }
                assertTrue(bucketRegion.getAttributes().getEvictionAttributes().getAlgorithm()
                    .isLRUHeap());
                assertTrue(bucketRegion.getAttributes().getEvictionAttributes().getAction()
                    .isLocalDestroy());
              }
              raiseFakeNotification();
              var wc = new WaitCriterion() {
                String excuse;

                @Override
                public boolean done() {
                  // we have a primary
                  return pr.getTotalEvictions() == 9;
                }

                @Override
                public String description() {
                  return excuse;
                }
              };
              GeodeAwaitility.await().untilAsserted(wc);

              entriesEvicted = pr.getTotalEvictions();
              return entriesEvicted;
            } finally {
              cleanUpAfterFakeNotification();
              // Clean up the observer for future tests
              // final Properties sp = System.getProperties();
              /*
               * assertNotNull(sp .remove(HeapLRUCapacityController.FREE_MEMORY_OBSERVER));
               */
            }
          }
        };
    final var v2i = (Long) vm2.invoke(assertBucketAttributesAndEviction);
    final var v3i = (Long) vm3.invoke(assertBucketAttributesAndEviction);
    final var totalEvicted = v2i.intValue() + v3i.intValue();
    // assume all three entries in each bucket were evicted to disk
    assertEquals((3 * bucketsToCreate * (redundantCopies + 1)), totalEvicted);
  }

  @Test
  public void testMemoryLRUWithOverflowToDisk() {
    final var host = Host.getHost(0);
    final var vm2 = host.getVM(2);
    final var vm3 = host.getVM(3);
    final var vm0 = host.getVM(0);
    final var uniqName = getUniqueName();
    final var redundantCopies = 1;
    final var maxBuckets = 8;
    final var localMaxMem = 16;
    final var halfKb = 512;
    final var justShyOfTwoMb = (2 * 1024 * 1024) - halfKb;

    final var name = uniqName + "-PR";
    final var create =
        new SerializableRunnable("Create Memory LRU with Overflow to disk partitioned Region") {
          @Override
          public void run() {
            try {
              final var factory = new AttributesFactory();
              factory.setOffHeap(isOffHeap());
              factory.setPartitionAttributes(
                  new PartitionAttributesFactory().setRedundantCopies(redundantCopies)
                      .setLocalMaxMemory(localMaxMem).setTotalNumBuckets(maxBuckets).create());

              factory.setEvictionAttributes(EvictionAttributes
                  .createLRUMemoryAttributes(ObjectSizer.DEFAULT, EvictionAction.OVERFLOW_TO_DISK));
              factory.setDiskSynchronous(true);
              var dsf = getCache().createDiskStoreFactory();
              final var diskDirs = new File[1];
              diskDirs[0] = new File("overflowDir/" + uniqName + "_" + OSProcess.getId());
              diskDirs[0].mkdirs();
              dsf.setDiskDirs(diskDirs);
              var ds = dsf.create(name);
              factory.setDiskStoreName(ds.getName());
              final var pr =
                  (PartitionedRegion) createRootRegion(name, factory.create());
              assertNotNull(pr);
            } catch (final CacheException ex) {
              Assert.fail("While creating Partitioned region", ex);
            }
          }
        };
    vm3.invoke(create);
    vm2.invoke(create);
    final var extraEntries = 4;
    final var createBuckets = new SerializableRunnable("Create Buckets") {
      @Override
      public void run() {
        final var pr = (PartitionedRegion) getRootRegion(name);
        assertNotNull(pr);
        for (var counter = 1; counter <= localMaxMem + extraEntries; counter++) {
          pr.put(counter, new byte[1 * 1024 * 1024]);
        }
      }
    };
    vm3.invoke(createBuckets);

    final var assertBucketAttributesAndEviction =
        new SerializableCallable("Assert bucket attributes and eviction") {
          @Override
          public Object call() throws Exception {
            final var pr = (PartitionedRegion) getRootRegion(name);
            assertNotNull(pr);
            assertNull(pr.getDiskRegion());

            // assert over-flow behavior in local buckets and number of
            // entries
            // overflowed
            long entriesEvicted = 0;
            for (final var integerBucketRegionEntry : pr.getDataStore()
                .getAllLocalBuckets()) {
              final var entry = (Entry) integerBucketRegionEntry;

              final var bucketRegion = (BucketRegion) entry.getValue();
              if (bucketRegion == null) {
                continue;
              }
              assertTrue(bucketRegion.getAttributes().getEvictionAttributes().getAlgorithm()
                  .isLRUMemory());
              assertTrue(bucketRegion.getAttributes().getEvictionAttributes().getAction()
                  .isOverflowToDisk());

            }
            entriesEvicted += pr.getDiskRegionStats().getNumOverflowOnDisk();
            return entriesEvicted;
          }
        };
    final var vm2i = (Long) vm2.invoke(assertBucketAttributesAndEviction);
    final var vm3i = (Long) vm3.invoke(assertBucketAttributesAndEviction);
    final var totalEvicted = vm2i.intValue() + vm3i.intValue();
    assertTrue(2 * extraEntries <= totalEvicted);

    // Test for bug 42056 - make sure we get the faulted out entries.
    vm0.invoke(create);

    vm0.invoke(new SerializableRunnable("Test to see that we can get keys") {
      @Override
      public void run() {
        final var pr = (PartitionedRegion) getRootRegion(name);
        // Make sure we can get all of the entries
        for (var counter = 1; counter <= localMaxMem + extraEntries; counter++) {
          assertNotNull(pr.get(counter));
        }
      }
    });
  }

  @Test
  public void testMemoryLRUWithLocalDestroy() {
    final var host = Host.getHost(0);
    final var vm2 = host.getVM(2);
    final var vm3 = host.getVM(3);
    final var uniqName = getUniqueName();
    final var redundantCopies = 1;
    final var maxBuckets = 8;
    final var localMaxMem = 16;
    final var halfKb = 512;
    final var justShyOfTwoMb = (2 * 1024 * 1024) - halfKb;
    final var name = uniqName + "-PR";

    final SerializableRunnable create = new CacheSerializableRunnable(
        "Create Memory LRU with local destroy on a partitioned Region") {
      @Override
      public void run2() {
        final var factory = new AttributesFactory();
        factory.setOffHeap(isOffHeap());
        factory.setPartitionAttributes(
            new PartitionAttributesFactory().setRedundantCopies(redundantCopies)
                .setLocalMaxMemory(16).setTotalNumBuckets(maxBuckets).create());
        factory.setEvictionAttributes(EvictionAttributes
            .createLRUMemoryAttributes(ObjectSizer.DEFAULT, EvictionAction.LOCAL_DESTROY));
        final var pr = (PartitionedRegion) createRootRegion(name, factory.create());
        assertNotNull(pr);
      }
    };
    vm3.invoke(create);
    vm2.invoke(create);

    final var extraEntries = 4;
    final var createBuckets = new SerializableRunnable("Create Buckets") {
      @Override
      public void run() {
        final var pr = (PartitionedRegion) getRootRegion(name);
        assertNotNull(pr);
        for (var counter = 1; counter <= localMaxMem + extraEntries; counter++) {
          pr.put(counter, new byte[1 * 1024 * 1024]);
        }
      }
    };
    vm3.invoke(createBuckets);

    final var assertBucketAttributesAndEviction =
        new SerializableCallable("Assert bucket attributes and eviction") {
          @Override
          public Object call() throws Exception {
            try {
              final var pr = (PartitionedRegion) getRootRegion(name);
              assertNotNull(pr);
              long entriesEvicted = 0;
              for (final var integerBucketRegionEntry : pr.getDataStore()
                  .getAllLocalBuckets()) {
                final var entry = (Entry) integerBucketRegionEntry;

                final var bucketRegion = (BucketRegion) entry.getValue();
                if (bucketRegion == null) {
                  continue;
                }
                assertTrue(bucketRegion.getAttributes().getEvictionAttributes().getAlgorithm()
                    .isLRUMemory());
                assertTrue(bucketRegion.getAttributes().getEvictionAttributes().getAction()
                    .isLocalDestroy());
              }
              entriesEvicted = pr.getTotalEvictions();
              return entriesEvicted;
            } finally {
            }
          }
        };
    final var v2i = (Long) vm2.invoke(assertBucketAttributesAndEviction);
    final var v3i = (Long) vm3.invoke(assertBucketAttributesAndEviction);
    final var totalEvicted = v2i.intValue() + v3i.intValue();
    assertTrue(2 * extraEntries <= totalEvicted);
  }

  @Test
  public void testEntryLRUWithOverflowToDisk() {
    final var host = Host.getHost(0);
    final var vm2 = host.getVM(2);
    final var vm3 = host.getVM(3);
    final var uniqName = getUniqueName();
    final var redundantCopies = 1;
    final var maxBuckets = 8;
    final var maxEntries = 16;
    final var name = uniqName + "-PR";
    final var create =
        new SerializableRunnable("Create Entry LRU with Overflow to disk partitioned Region") {
          @Override
          public void run() {
            try {
              final var factory = new AttributesFactory();
              factory.setOffHeap(isOffHeap());
              factory.setPartitionAttributes(new PartitionAttributesFactory()
                  .setRedundantCopies(redundantCopies).setTotalNumBuckets(maxBuckets).create());
              factory.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(maxEntries,
                  EvictionAction.OVERFLOW_TO_DISK));
              factory.setDiskSynchronous(true);
              var dsf = getCache().createDiskStoreFactory();
              final var diskDirs = new File[1];
              diskDirs[0] = new File("overflowDir/" + uniqName + "_" + OSProcess.getId());
              diskDirs[0].mkdirs();
              dsf.setDiskDirs(diskDirs);
              var ds = dsf.create(name);
              factory.setDiskStoreName(ds.getName());
              final var pr =
                  (PartitionedRegion) createRootRegion(name, factory.create());
              assertNotNull(pr);
            } catch (final CacheException ex) {
              Assert.fail("While creating Partitioned region", ex);
            }
          }
        };
    vm3.invoke(create);
    vm2.invoke(create);
    final var extraEntries = 4;
    final var createBuckets = new SerializableRunnable("Create Buckets") {
      @Override
      public void run() {
        final var pr = (PartitionedRegion) getRootRegion(name);
        assertNotNull(pr);
        for (var counter = 1; counter <= maxEntries + extraEntries; counter++) {
          pr.put(counter, new byte[1 * 1024 * 1024]);
        }
      }
    };
    vm3.invoke(createBuckets);

    final var assertBucketAttributesAndEviction =
        new SerializableCallable("Assert bucket attributes and eviction") {
          @Override
          public Object call() throws Exception {
            final var pr = (PartitionedRegion) getRootRegion(name);
            assertNotNull(pr);

            // assert over-flow behavior in local buckets and number of
            // entries
            // overflowed
            var entriesEvicted = 0;
            for (final var integerBucketRegionEntry : pr.getDataStore()
                .getAllLocalBuckets()) {
              final var entry = (Entry) integerBucketRegionEntry;

              final var bucketRegion = (BucketRegion) entry.getValue();
              if (bucketRegion == null) {
                continue;
              }
              assertTrue(
                  bucketRegion.getAttributes().getEvictionAttributes().getAlgorithm().isLRUEntry());
              assertTrue(bucketRegion.getAttributes().getEvictionAttributes().getAction()
                  .isOverflowToDisk());
            }
            entriesEvicted += pr.getDiskRegionStats().getNumOverflowOnDisk();
            return entriesEvicted;

          }
        };
    final var vm2i = (Integer) vm2.invoke(assertBucketAttributesAndEviction);
    final var vm3i = (Integer) vm3.invoke(assertBucketAttributesAndEviction);
    final var totalEvicted = vm2i + vm3i;
    assertEquals(extraEntries * 2, totalEvicted);
  }

  private static class VerifiableCacheListener extends CacheListenerAdapter
      implements java.io.Serializable {
    public boolean verify(long expectedEvictions) {
      return false;
    }
  }

  @Test
  public void testEntryLRUWithLocalDestroy() {
    final var host = Host.getHost(0);
    final var vm2 = host.getVM(2);
    final var vm3 = host.getVM(3);
    final var uniqName = getUniqueName();
    final var redundantCopies = 1;
    final var maxBuckets = 8;
    final var maxEntries = 16;
    final var name = uniqName + "-PR";
    final var extraEntries = 4;

    // final int heapPercentage = 66;
    // final int evictorInterval = 100;

    final SerializableRunnable create = new CacheSerializableRunnable(
        "Create Entry LRU with local destroy on a partitioned Region") {
      @Override
      public void run2() {
        final var factory = new AttributesFactory();
        factory.setOffHeap(isOffHeap());
        factory.setPartitionAttributes(new PartitionAttributesFactory()
            .setRedundantCopies(redundantCopies).setTotalNumBuckets(maxBuckets).create());
        factory.setEvictionAttributes(
            EvictionAttributes.createLRUEntryAttributes(maxEntries, EvictionAction.LOCAL_DESTROY));
        factory.addCacheListener(new VerifiableCacheListener() {
          private long evictionDestroyEvents = 0;

          @Override
          public void afterDestroy(EntryEvent e) {
            System.out.println("EEEEEEEEEEEEEE key:" + e.getKey());
            var eei = (EntryEventImpl) e;
            if (Operation.EVICT_DESTROY.equals(eei.getOperation())) {
              evictionDestroyEvents++;
            }
          }

          @Override
          public boolean verify(long expectedEntries) {
            return expectedEntries == evictionDestroyEvents;
          }
        });
        final var pr = (PartitionedRegion) createRootRegion(name, factory.create());
        assertNotNull(pr);
      }
    };
    vm3.invoke(create);

    final var create2 =
        new SerializableRunnable("Create Entry LRU with local destroy on a partitioned Region") {
          @Override
          public void run() {
            try {
              final var factory = new AttributesFactory();
              factory.setOffHeap(isOffHeap());
              factory.setPartitionAttributes(new PartitionAttributesFactory()
                  .setRedundantCopies(redundantCopies).setTotalNumBuckets(8).create());
              factory
                  .setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(maxEntries));
              final var pr =
                  (PartitionedRegion) createRootRegion(name, factory.create());
              assertNotNull(pr);
            } catch (final CacheException ex) {
              Assert.fail("While creating Partitioned region", ex);
            }
          }
        };
    vm2.invoke(create2);

    final var createBuckets = new SerializableRunnable("Create Buckets") {
      @Override
      public void run() {
        final var pr = (PartitionedRegion) getRootRegion(name);
        assertNotNull(pr);
        for (var counter = 1; counter <= maxEntries + extraEntries; counter++) {
          pr.put(counter, new byte[1 * 1024 * 1024]);
        }
      }
    };
    vm3.invoke(createBuckets);

    final var assertBucketAttributesAndEviction =
        new SerializableCallable("Assert bucket attributes and eviction") {
          @Override
          public Object call() throws Exception {
            try {
              final var pr = (PartitionedRegion) getRootRegion(name);
              assertNotNull(pr);
              for (final var integerBucketRegionEntry : pr.getDataStore()
                  .getAllLocalBuckets()) {
                final var entry = (Entry) integerBucketRegionEntry;

                final var bucketRegion = (BucketRegion) entry.getValue();
                if (bucketRegion == null) {
                  continue;
                }
                assertTrue(bucketRegion.getAttributes().getEvictionAttributes().getAlgorithm()
                    .isLRUEntry());
                assertTrue(bucketRegion.getAttributes().getEvictionAttributes().getAction()
                    .isLocalDestroy());
              }
              return pr.getTotalEvictions();
            } finally {
            }
          }
        };

    final var v2i = (Long) vm2.invoke(assertBucketAttributesAndEviction);
    final var v3i = (Long) vm3.invoke(assertBucketAttributesAndEviction);
    final var totalEvicted = v2i.intValue() + v3i.intValue();
    assertEquals(2 * extraEntries, totalEvicted);

    final var assertListenerCount = new SerializableCallable(
        "Assert that the number of listener invocations matches the expected total") {
      @Override
      public Object call() throws Exception {
        final var pr = (PartitionedRegion) getRootRegion(name);
        assertNotNull(pr);
        var attrs = pr.getAttributes();
        assertNotNull(attrs);
        var entriesEvicted = pr.getTotalEvictions();
        VerifiableCacheListener verifyMe = null;
        for (var listener : attrs.getCacheListeners()) {
          if (listener instanceof VerifiableCacheListener) {
            verifyMe = ((VerifiableCacheListener) listener);
          }
        }
        assertNotNull(verifyMe); // assert if unable to find the expected listener
        return verifyMe.verify(entriesEvicted);
      }
    };
    assertTrue((Boolean) vm3.invoke(assertListenerCount));
  }

  // Test to validate the Eviction Attribute : LRU Check
  @Test
  public void testEvictionValidationForLRUEntry() {
    final var host = Host.getHost(0);
    final var testAccessor = host.getVM(1);
    final var testDatastore = host.getVM(2);
    final var firstDatastore = host.getVM(3);
    final var uniqName = getUniqueName();
    final var redundantCopies = 1;
    final var maxEntries = 226;
    final var name = uniqName + "-PR";

    // final int evictionSizeInMB = 200;
    final var firstEvictionAttrs =
        EvictionAttributes.createLRUEntryAttributes(maxEntries, EvictionAction.LOCAL_DESTROY);

    // Creating LRU Entry Count Eviction Attribute
    final SerializableRunnable create = new CacheSerializableRunnable(
        "Create Entry LRU with local destroy on a partitioned Region") {
      @Override
      public void run2() {

        // Assert that LRUEntry maximum can be less than 1 entry per
        // bucket
        // DUnit not required for this test, but its a convenient place
        // to put it.
        {
          final var buks = 11;
          final var factory = new AttributesFactory();
          factory.setOffHeap(isOffHeap());
          factory.setPartitionAttributes(new PartitionAttributesFactory().setTotalNumBuckets(buks)
              .setRedundantCopies(0).create());
          factory.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes((buks / 2)));
          final var pr = (PartitionedRegion) createRootRegion(name, factory.create());
          final Integer key = 1;
          pr.put(key, "testval");
          final BucketRegion b;
          try {
            b = pr.getDataStore().getInitializedBucketForId(key,
                PartitionedRegionHelper.getHashKey(pr, null, key, null, null));
          } catch (final ForceReattemptException e) {
            fail();
          }
          pr.destroyRegion();
        }

        final var factory = new AttributesFactory();
        factory.setOffHeap(isOffHeap());
        factory.setPartitionAttributes(
            new PartitionAttributesFactory().setRedundantCopies(redundantCopies).create());
        factory.setEvictionAttributes(firstEvictionAttrs);
        final var pr = createRootRegion(name, factory.create());
        assertNotNull(pr);
      }
    };
    firstDatastore.invoke(create);

    final var create2 =
        new SerializableRunnable("Create Entry LRU with Overflow to disk partitioned Region") {
          @Override
          public void run() {
            final var factory = new AttributesFactory();
            factory.setOffHeap(isOffHeap());
            factory.setPartitionAttributes(
                new PartitionAttributesFactory().setRedundantCopies(redundantCopies).create());
            // Assert a different algo is invalid
            try {
              getCache().getLogger().info(
                  "<ExpectedException action=add>" + "IllegalStateException</ExpectedException>");
              assertTrue(!firstEvictionAttrs.getAlgorithm().isLRUHeap());
              final var illegalEa =
                  EvictionAttributes.createLRUHeapAttributes(null, firstEvictionAttrs.getAction());
              assertTrue(!firstEvictionAttrs.equals(illegalEa));
              factory.setEvictionAttributes(illegalEa);
              setEvictionPercentage(50);
              createRootRegion(name, factory.create());
              fail("Creating LRU Entry Count Eviction Attribute");
            } catch (final IllegalStateException expected) {
              assertTrue(expected.getMessage().contains(
                  PartitionRegionConfigValidator.EVICTION_ATTRIBUTES_ARE_INCOMPATIBLE_MESSAGE));
            } finally {
              getCache().getLogger().info("<ExpectedException action=remove>"
                  + "IllegalStateException</ExpectedException>");
            }

            // Assert a different action is invalid
            try {
              getCache().getLogger().info(
                  "<ExpectedException action=add>" + "IllegalStateException</ExpectedException>");
              assertTrue(firstEvictionAttrs.getAlgorithm().isLRUEntry());
              assertTrue(!firstEvictionAttrs.getAction().isOverflowToDisk());
              final var illegalEa = EvictionAttributes.createLRUEntryAttributes(
                  firstEvictionAttrs.getMaximum(), EvictionAction.OVERFLOW_TO_DISK);
              assertTrue(!firstEvictionAttrs.equals(illegalEa));
              factory.setEvictionAttributes(illegalEa);
              createRootRegion(name, factory.create());
              fail("Creating LRU Entry Count Eviction Attribute");
            } catch (final IllegalStateException expected) {
              assertTrue(expected.getMessage().contains(
                  PartitionRegionConfigValidator.EVICTION_ATTRIBUTES_ARE_INCOMPATIBLE_MESSAGE));
            } finally {
              getCache().getLogger().info("<ExpectedException action=remove>"
                  + "IllegalStateException</ExpectedException>");
            }


            assertTrue(firstEvictionAttrs.getAlgorithm().isLRUEntry());
            final var brokenEa = EvictionAttributes.createLRUEntryAttributes(
                firstEvictionAttrs.getMaximum() + 1, firstEvictionAttrs.getAction());
            assertTrue(!firstEvictionAttrs.equals(brokenEa));
            factory.setEvictionAttributes(brokenEa);
            createRootRegion(name, factory.create());
          }
        };
    testDatastore.invoke(create2);

    testAccessor.invoke(
        new CacheSerializableRunnable("Create an Accessor with and without eviction attributes") {
          @Override
          public void run2() throws CacheException {
            final var pra = new PartitionAttributesFactory()
                .setRedundantCopies(redundantCopies).setLocalMaxMemory(0).create();
            var factory = new AttributesFactory();
            factory.setOffHeap(isOffHeap());
            factory.setPartitionAttributes(pra);
            factory.setEvictionAttributes(firstEvictionAttrs);
            setEvictionPercentage(50);
            var r1 = createRootRegion(name, factory.create());
            assertNotNull(r1);
            assertEquals(firstEvictionAttrs, r1.getAttributes().getEvictionAttributes());
          }
        });
  }

  // Test to validate the Eviction Attribute : LRU Action
  @Test
  public void testEvictionValidationForLRUAction() {
    final var host = Host.getHost(0);
    final var testDatastore = host.getVM(2);
    final var firstDatastore = host.getVM(3);
    final var uniqName = getUniqueName();
    final var redundantCopies = 1;
    final var maxEntries = 226;
    final var name = uniqName + "-PR";
    final var firstEa =
        EvictionAttributes.createLRUEntryAttributes(maxEntries, EvictionAction.LOCAL_DESTROY);

    // Creating LRU Entry Count Eviction Attribute : Algorithm :
    // LOCAL_DESTROY
    final SerializableRunnable create = new CacheSerializableRunnable(
        "Create Entry LRU with local destroy on a partitioned Region") {
      @Override
      public void run2() {
        final var factory = new AttributesFactory();
        factory.setOffHeap(isOffHeap());
        factory.setPartitionAttributes(
            new PartitionAttributesFactory().setRedundantCopies(redundantCopies).create());
        factory.setEvictionAttributes(firstEa);
        final var pr = createRootRegion(name, factory.create());
        assertNotNull(pr);
        assertEquals(firstEa, pr.getAttributes().getEvictionAttributes());
      }
    };
    firstDatastore.invoke(create);

    final var create2 =
        new SerializableRunnable("Create Entry LRU with Overflow to disk partitioned Region") {
          @Override
          public void run() {
            final var factory = new AttributesFactory();
            factory.setOffHeap(isOffHeap());
            factory.setPartitionAttributes(
                new PartitionAttributesFactory().setRedundantCopies(redundantCopies).create());
            // Creating LRU Entry Count Eviction Attribute : Action :
            // OVERFLOW_TO_DISK // Exception should occur
            try {
              getCache().getLogger().info(
                  "<ExpectedException action=add>" + "IllegalStateException</ExpectedException>");
              assertTrue(firstEa.getAlgorithm().isLRUEntry());
              assertTrue(!firstEa.getAction().isOverflowToDisk());
              factory.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(maxEntries,
                  EvictionAction.OVERFLOW_TO_DISK));
              createRootRegion(name, factory.create());
              fail("Test to validate the Eviction Attribute : LRU Action");
            } catch (final IllegalStateException expected) {
              assertTrue(expected.getMessage().contains(
                  PartitionRegionConfigValidator.EVICTION_ATTRIBUTES_ARE_INCOMPATIBLE_MESSAGE));
            } finally {
              getCache().getLogger().info("<ExpectedException action=remove>"
                  + "IllegalStateException</ExpectedException>");
            }
            // Creating LRU Entry Count Eviction Attribute : Action : NONE
            // // Exception should occur
            try {
              getCache().getLogger().info(
                  "<ExpectedException action=add>" + "IllegalStateException</ExpectedException>");
              assertTrue(firstEa.getAlgorithm().isLRUEntry());
              assertTrue(!firstEa.getAction().isNone());
              factory.setEvictionAttributes(
                  EvictionAttributes.createLRUEntryAttributes(maxEntries, EvictionAction.NONE));
              createRootRegion(name, factory.create());
              fail("Test to validate the Eviction Attribute : LRU Action");
            } catch (final IllegalStateException expected) {
              assertTrue(expected.getMessage().contains(
                  PartitionRegionConfigValidator.EVICTION_ATTRIBUTES_ARE_INCOMPATIBLE_MESSAGE));
            } finally {
              getCache().getLogger().info("<ExpectedException action=remove>"
                  + "IllegalStateException</ExpectedException>");
            }
            // Creating LRU Entry Count Eviction Attribute : Action :
            // LOCAL_DESTROY // Exception should not occur
            factory.setEvictionAttributes(firstEa);
            final var pr = createRootRegion(name, factory.create());
            assertNotNull(pr);
            assertEquals(firstEa, pr.getAttributes().getEvictionAttributes());
          }
        };
    testDatastore.invoke(create2);
  }

  // Test to validate the Eviction Attribute : LRU Maximum
  @Test
  public void testEvictionValidationForLRUMaximum() {
    final var host = Host.getHost(0);
    final var testDatastore = host.getVM(2);
    final var firstDatastore = host.getVM(3);
    final var uniqName = getUniqueName();
    final var redundantCopies = 1;
    final var maxEntries = 226;
    final var name = uniqName + "-PR";
    final var firstEvictionAttributes =
        EvictionAttributes.createLRUEntryAttributes(maxEntries, EvictionAction.LOCAL_DESTROY);

    // Creating LRU Entry Count Eviction Attribute : maxentries : 2
    final SerializableRunnable create = new CacheSerializableRunnable(
        "Create Entry LRU with local destroy on a partitioned Region") {
      @Override
      public void run2() {
        final var factory = new AttributesFactory();
        factory.setOffHeap(isOffHeap());
        factory.setPartitionAttributes(
            new PartitionAttributesFactory().setRedundantCopies(redundantCopies).create());
        factory.setEvictionAttributes(firstEvictionAttributes);
        final var pr = createRootRegion(name, factory.create());
        assertNotNull(pr);
        assertEquals(firstEvictionAttributes, pr.getAttributes().getEvictionAttributes());
      }
    };
    firstDatastore.invoke(create);

    final var create2 =
        new SerializableRunnable("Create Entry LRU with Overflow to disk partitioned Region") {
          @Override
          public void run() {
            final var factory = new AttributesFactory();
            factory.setOffHeap(isOffHeap());
            factory.setPartitionAttributes(
                new PartitionAttributesFactory().setRedundantCopies(redundantCopies).create());

            final var ea = EvictionAttributes.createLRUEntryAttributes(
                firstEvictionAttributes.getMaximum() + 10, firstEvictionAttributes.getAction());
            factory.setEvictionAttributes(ea);
            final var pr = createRootRegion(name, factory.create());
            assertNotNull(pr);
          }
        };
    testDatastore.invoke(create2);
  }

  // Test to validate the Eviction Attribute for LRUHeap
  @Test
  public void testEvictionValidationForLRUHeap() {
    final var host = Host.getHost(0);
    final var testDatastore = host.getVM(2);
    final var firstDatastore = host.getVM(3);
    final var uniqName = getUniqueName();
    final var redundantCopies = 1;
    final var name = uniqName + "-PR";
    final var heapPercentage = 66;
    final var evictorInterval = 100;

    final var firstEvictionAttributes = EvictionAttributes.createLRUHeapAttributes();
    // Creating Heap LRU Eviction Attribute : evictorInterval : 100
    final SerializableRunnable create = new CacheSerializableRunnable(
        "Create Entry LRU with local destroy on a partitioned Region") {
      @Override
      public void run2() {
        getCache().getResourceManager().setEvictionHeapPercentage(heapPercentage);
        final var factory = new AttributesFactory();
        factory.setOffHeap(isOffHeap());
        factory.setPartitionAttributes(
            new PartitionAttributesFactory().setRedundantCopies(redundantCopies).create());
        factory.setEvictionAttributes(firstEvictionAttributes);
        final var pr = createRootRegion(name, factory.create());
        assertNotNull(pr);
        assertEquals(firstEvictionAttributes, pr.getAttributes().getEvictionAttributes());
      }
    };
    firstDatastore.invoke(create);

    final var create2 =
        new SerializableRunnable("Create Entry LRU with Overflow to disk partitioned Region") {
          @Override
          public void run() {
            setEvictionPercentage(heapPercentage);
            final var factory = new AttributesFactory();
            factory.setOffHeap(isOffHeap());
            factory.setPartitionAttributes(
                new PartitionAttributesFactory().setRedundantCopies(redundantCopies).create());
            // Assert that a different algo is invalid
            try {
              getCache().getLogger().info(
                  "<ExpectedException action=add>" + "IllegalStateException</ExpectedException>");
              assertTrue(!firstEvictionAttributes.getAlgorithm().isLRUEntry());
              final var invalidEa = EvictionAttributes.createLRUEntryAttributes();
              assertTrue(!invalidEa.equals(firstEvictionAttributes));
              factory.setEvictionAttributes(invalidEa);
              createRootRegion(name, factory.create());
              fail("Expected an IllegalStateException");
            } catch (final IllegalStateException expected) {
              assertTrue(expected.getMessage().contains(
                  PartitionRegionConfigValidator.EVICTION_ATTRIBUTES_ARE_INCOMPATIBLE_MESSAGE));
            } finally {
              getCache().getLogger().info("<ExpectedException action=remove>"
                  + "IllegalStateException</ExpectedException>");
            }

            // Assert that a different action is invalid
            try {
              getCache().getLogger().info(
                  "<ExpectedException action=add>" + "IllegalStateException</ExpectedException>");
              assertTrue(firstEvictionAttributes.getAlgorithm().isLRUHeap());
              assertTrue(!firstEvictionAttributes.getAction().isOverflowToDisk());
              final var invalidEa =
                  EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK);
              assertTrue(!invalidEa.equals(firstEvictionAttributes));
              factory.setEvictionAttributes(invalidEa);
              createRootRegion(name, factory.create());
              fail("Expected an IllegalStateException");
            } catch (final IllegalStateException expected) {
              assertTrue(expected.getMessage().contains(
                  PartitionRegionConfigValidator.EVICTION_ATTRIBUTES_ARE_INCOMPATIBLE_MESSAGE));
            } finally {
              getCache().getLogger().info("<ExpectedException action=remove>"
                  + "IllegalStateException</ExpectedException>");
            }


            // Assert that a different interval is valid
            // {
            // assertTrue(firstEvictionAttributes.getAlgorithm().isLRUHeap());
            // final EvictionAttributes okHeapLRUea = EvictionAttributes
            // .createLRUHeapAttributes((int)(firstEvictionAttributes.getInterval() + 100),
            // firstEvictionAttributes.getAction());
            // factory.setEvictionAttributes(okHeapLRUea);
            // final Region pr = createRootRegion(name, factory.create());
            // assertNotNull(pr);
            // assertIndexDetailsEquals(okHeapLRUea, pr.getAttributes().getEvictionAttributes());
            // pr.localDestroyRegion();
            // }

            // Assert that a different maximum is valid
            // {
            // assertTrue(firstEvictionAttributes.getAlgorithm().isLRUHeap());
            // final EvictionAttributes okHeapLRUea = EvictionAttributes
            // .createLRUHeapAttributes(
            // (int)firstEvictionAttributes.getInterval(),
            // firstEvictionAttributes.getAction());
            // factory.setEvictionAttributes(okHeapLRUea);
            // final Region pr = createRootRegion(name, factory.create());
            // assertNotNull(pr);
            // assertIndexDetailsEquals(okHeapLRUea, pr.getAttributes().getEvictionAttributes());
            // pr.localDestroyRegion();
            // }

            // Assert that all attributes can be the same
            {
              factory.setEvictionAttributes(firstEvictionAttributes);
              final var pr = createRootRegion(name, factory.create());
              assertNotNull(pr);
              assertEquals(firstEvictionAttributes, pr.getAttributes().getEvictionAttributes());
            }
          }
        };
    testDatastore.invoke(create2);
  }

  // Test to validate an accessor can set the initial attributes
  @Test
  public void testEvictionValidationWhenInitializedByAccessor() {
    final var host = Host.getHost(0);
    final var testDatastore = host.getVM(2);
    final var accessor = host.getVM(3);
    final var uniqName = getUniqueName();
    final var redundantCopies = 1;
    final var name = uniqName;

    final var firstEvictionAttributes = EvictionAttributes
        .createLRUMemoryAttributes(PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_DEFAULT);
    accessor.invoke(
        new CacheSerializableRunnable("Create an Accessor which sets the first PR eviction attrs") {
          @Override
          public void run2() {
            final var factory = new AttributesFactory();
            factory.setOffHeap(isOffHeap());
            factory.setPartitionAttributes(new PartitionAttributesFactory().setLocalMaxMemory(0)
                .setRedundantCopies(redundantCopies).create());
            factory.setEvictionAttributes(firstEvictionAttributes);
            final var pr = createRootRegion(name, factory.create());
            assertNotNull(pr);
            assertNotSame(firstEvictionAttributes, pr.getAttributes().getEvictionAttributes());
            assertEquals(firstEvictionAttributes, pr.getAttributes().getEvictionAttributes());
            assertEquals(PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_DEFAULT,
                pr.getAttributes().getEvictionAttributes().getMaximum());
          }
        });

    testDatastore.invoke(
        new SerializableRunnable("Create a datastore to test existing eviction attributes") {
          @Override
          public void run() {
            final var factory = new AttributesFactory();
            factory.setOffHeap(isOffHeap());
            factory.setPartitionAttributes(
                new PartitionAttributesFactory().setRedundantCopies(redundantCopies).create());
            // Assert that the same attrs is valid
            factory.setEvictionAttributes(firstEvictionAttributes);
            final var pr = createRootRegion(name, factory.create());
            assertNotNull(pr);
            assertNotSame(firstEvictionAttributes, pr.getAttributes().getEvictionAttributes());
            assertNotSame(PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_DEFAULT,
                pr.getAttributes().getEvictionAttributes().getMaximum());
            assertEquals(pr.getAttributes().getPartitionAttributes().getLocalMaxMemory(),
                pr.getAttributes().getEvictionAttributes().getMaximum());
          }
        });
  }

  // Test to validate the Eviction Attribute : HeapLRU Interval
  @Test
  public void testEvictionValidationForLRUMemory() {
    final var host = Host.getHost(0);
    final var firstDatastore = host.getVM(1);
    final var testDatastore = host.getVM(2);
    final var testAccessor = host.getVM(3);
    final var uniqName = getUniqueName();
    final var redundantCopies = 1;
    final var name = uniqName + "-PR";
    final var firstEvictionAttributes = EvictionAttributes
        .createLRUMemoryAttributes(PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_DEFAULT * 2);

    firstDatastore
        .invoke(new CacheSerializableRunnable("First datastore setting LRU memory eviction attrs") {
          @Override
          public void run2() {
            // Assert that LRUMemory attributes may be lesser than 1 Mb per
            // bucket
            // DUnit not required for this test, but its a convenient place
            // to put it.
            {
              final var totalNumBuckets = 11;
              final var factory = new AttributesFactory();
              factory.setOffHeap(isOffHeap());
              factory.setPartitionAttributes(new PartitionAttributesFactory()
                  .setTotalNumBuckets(totalNumBuckets).setRedundantCopies(0).create());
              factory.setEvictionAttributes(
                  EvictionAttributes.createLRUMemoryAttributes((totalNumBuckets / 2)));
              final var pr =
                  (PartitionedRegion) createRootRegion(name, factory.create());
              final Integer key = 1;
              pr.put(key, "testval");
              final BucketRegion b;
              try {
                b = pr.getDataStore().getInitializedBucketForId(key,
                    PartitionedRegionHelper.getHashKey(pr, null, key, null, null));
              } catch (final ForceReattemptException e) {
                fail();
              }
              pr.destroyRegion();
            }

            // Set the "global" eviction attributes
            final var factory = new AttributesFactory();
            factory.setOffHeap(isOffHeap());
            factory.setPartitionAttributes(
                new PartitionAttributesFactory().setRedundantCopies(redundantCopies).create());
            factory.setEvictionAttributes(firstEvictionAttributes);
            final var pr = createRootRegion(name, factory.create());
            assertNotNull(pr);
            assertNotSame(firstEvictionAttributes, pr.getAttributes().getEvictionAttributes());
            assertNotSame(PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_DEFAULT,
                pr.getAttributes().getEvictionAttributes().getMaximum());
            assertEquals(pr.getAttributes().getPartitionAttributes().getLocalMaxMemory(),
                pr.getAttributes().getEvictionAttributes().getMaximum());
          }
        });

    class PRLRUMemoryRunnable extends CacheSerializableRunnable {
      private static final long serialVersionUID = 1L;

      final boolean isAccessor;

      public PRLRUMemoryRunnable(final boolean accessor) {
        super("Test LRU memory eviction attrs on a " + (accessor ? " accessor" : " datastore"));
        isAccessor = accessor;
      }

      public PartitionAttributes createPartitionAttributes(final int localMaxMemory) {
        if (isAccessor) {
          return new PartitionAttributesFactory().setLocalMaxMemory(0)
              .setRedundantCopies(redundantCopies).create();
        } else {
          return new PartitionAttributesFactory().setLocalMaxMemory(localMaxMemory)
              .setRedundantCopies(redundantCopies).create();
        }
      }

      @Override
      public void run2() throws CacheException {
        // Configure a PR with the same local max mem as the eviction
        // attributes... and do these tests
        var pra = createPartitionAttributes(firstEvictionAttributes.getMaximum());
        final var factory = new AttributesFactory();
        factory.setOffHeap(isOffHeap());
        factory.setPartitionAttributes(pra);

        // Assert that no eviction attributes is invalid
        try {
          getCache().getLogger()
              .info("<ExpectedException action=add>" + "IllegalStateException</ExpectedException>");
          final var attrs = factory.create();
          createRootRegion(name, attrs);
          fail("Expected a IllegalStateException to be thrown");
        } catch (final IllegalStateException expected) {
          assertTrue(expected.getMessage().contains(
              PartitionRegionConfigValidator.EVICTION_ATTRIBUTES_ARE_INCOMPATIBLE_MESSAGE));
        } finally {
          getCache().getLogger().info(
              "<ExpectedException action=remove>" + "IllegalStateException</ExpectedException>");
        }

        // Assert different algo is invalid
        try {
          getCache().getLogger()
              .info("<ExpectedException action=add>" + "IllegalStateException</ExpectedException>");
          assertTrue(!((EvictionAttributesImpl) firstEvictionAttributes).isLIFOMemory());
          final var badEa =
              EvictionAttributesImpl.createLIFOMemoryAttributes(100, EvictionAction.LOCAL_DESTROY);
          assertTrue(!badEa.equals(firstEvictionAttributes));
          factory.setEvictionAttributes(badEa);
          createRootRegion(name, factory.create());
          fail("Test to validate the Eviction Attribute : HeapLRU Interval");
        } catch (final IllegalStateException expected) {
          assertTrue(expected.getMessage().contains(
              PartitionRegionConfigValidator.EVICTION_ATTRIBUTES_ARE_INCOMPATIBLE_MESSAGE));
        } finally {
          getCache().getLogger().info(
              "<ExpectedException action=remove>" + "IllegalStateException</ExpectedException>");
        }

        // Assert different action is invalid
        try {
          getCache().getLogger()
              .info("<ExpectedException action=add>" + "IllegalStateException</ExpectedException>");
          assertTrue(!firstEvictionAttributes.getAction().isOverflowToDisk());
          assertTrue(firstEvictionAttributes.getAlgorithm().isLRUMemory());
          final var badEa =
              EvictionAttributes.createLRUMemoryAttributes(firstEvictionAttributes.getMaximum(),
                  firstEvictionAttributes.getObjectSizer(), EvictionAction.OVERFLOW_TO_DISK);
          assertTrue(!badEa.equals(firstEvictionAttributes));
          factory.setEvictionAttributes(badEa);
          createRootRegion(name, factory.create());
          fail("Test to validate the Eviction Attribute : HeapLRU Interval");
        } catch (final IllegalStateException expected) {
          assertTrue(expected.getMessage().contains(
              PartitionRegionConfigValidator.EVICTION_ATTRIBUTES_ARE_INCOMPATIBLE_MESSAGE));
        } finally {
          getCache().getLogger().info(
              "<ExpectedException action=remove>" + "IllegalStateException</ExpectedException>");
        }

        // Assert a smaller eviction maximum than the global is not valid (bug 40419)
        {
          assertTrue(firstEvictionAttributes.getAlgorithm().isLRUMemory());
          final var okEa =
              EvictionAttributes.createLRUMemoryAttributes(firstEvictionAttributes.getMaximum() - 1,
                  firstEvictionAttributes.getObjectSizer(), firstEvictionAttributes.getAction());
          if (!isAccessor) {
            assertTrue(okEa.getMaximum() < pra.getLocalMaxMemory());
          }
          assertTrue(!okEa.equals(firstEvictionAttributes));
          factory.setEvictionAttributes(okEa);
          final var attrs = factory.create();
          final var pr = createRootRegion(name, attrs);
          assertNotNull(pr);
          assertNotSame(okEa, pr.getAttributes().getEvictionAttributes());
          assertEquals(firstEvictionAttributes.getMaximum(),
              pr.getAttributes().getEvictionAttributes().getMaximum());
          pr.localDestroyRegion();
        }

        // Assert that a larger eviction maximum than the global is not
        // valid (bug 40419)
        {
          assertTrue(firstEvictionAttributes.getAlgorithm().isLRUMemory());
          final var okEa =
              EvictionAttributes.createLRUMemoryAttributes(firstEvictionAttributes.getMaximum() + 1,
                  firstEvictionAttributes.getObjectSizer(), firstEvictionAttributes.getAction());
          assertTrue(!okEa.equals(firstEvictionAttributes));
          if (!isAccessor) {
            assertEquals(okEa.getMaximum(), pra.getLocalMaxMemory() + 1);
          }
          factory.setEvictionAttributes(okEa);
          final var attrs = factory.create();
          final var pr = createRootRegion(name, attrs);
          assertNotNull(pr);
          assertNotSame(okEa, pr.getAttributes().getEvictionAttributes());
          assertEquals(firstEvictionAttributes.getMaximum(),
              pr.getAttributes().getEvictionAttributes().getMaximum());
          pr.localDestroyRegion();
        }

        // Assert that an eviction maximum larger than the
        // localMaxMemroy is allowed
        // and that the local eviction maximum is re-set to the
        // localMaxMemory value
        {
          // lower the localMaxMemory
          pra = createPartitionAttributes(firstEvictionAttributes.getMaximum() - 1);
          factory.setPartitionAttributes(pra);
          if (!isAccessor) {
            assertTrue(firstEvictionAttributes.getMaximum() > pra.getLocalMaxMemory());
          }
          factory.setEvictionAttributes(firstEvictionAttributes);
          final var attrs = factory.create();
          final var pr = createRootRegion(name, attrs);
          assertNotNull(pr);
          if (!isAccessor) {
            assertEquals(pr.getAttributes().getPartitionAttributes().getLocalMaxMemory(),
                pr.getAttributes().getEvictionAttributes().getMaximum());
          }
          pr.localDestroyRegion();
        }

        // Assert exact same attributes is valid
        {
          factory.setEvictionAttributes(firstEvictionAttributes);
          final var pr = createRootRegion(name, factory.create());
          assertNotNull(pr);
          assertNotSame(firstEvictionAttributes, pr.getAttributes().getEvictionAttributes());
          assertEquals(pr.getAttributes().getPartitionAttributes().getLocalMaxMemory(),
              pr.getAttributes().getEvictionAttributes().getMaximum());
          pr.localDestroyRegion();
        }
      }
    }

    testDatastore.invoke(new PRLRUMemoryRunnable(false));
    // testAccessor.invoke(new PRLRUMemoryRunnable(true));
  }


  @Test
  public void testEvictionValidationForLRUEntry_AccessorFirst() {
    final var host = Host.getHost(0);
    final var firstAccessor = host.getVM(0);
    final var testAccessor = host.getVM(1);
    final var testDatastore = host.getVM(2);
    final var firstDatastore = host.getVM(3);
    final var uniqName = getUniqueName();
    final var redundantCopies = 1;
    final var maxEntries = 226;
    final var name = uniqName + "-PR";

    final var firstEvictionAttrs =
        EvictionAttributes.createLRUEntryAttributes(maxEntries, EvictionAction.LOCAL_DESTROY);

    final var secondEvictionAttrs =
        EvictionAttributes.createLRUEntryAttributes(maxEntries, EvictionAction.OVERFLOW_TO_DISK);

    final SerializableRunnable createFirstAccessor =
        new CacheSerializableRunnable("Create an accessor without eviction attributes") {
          @Override
          public void run2() {
            final var pra = new PartitionAttributesFactory()
                .setRedundantCopies(redundantCopies).setLocalMaxMemory(0).create();
            var factory = new AttributesFactory();
            factory.setOffHeap(isOffHeap());
            factory.setPartitionAttributes(pra);
            final var pr = createRootRegion(name, factory.create());
            assertNotNull(pr);
          }
        };
    final SerializableRunnable createFirstDataStore =
        new CacheSerializableRunnable("Create a data store with eviction attributes") {
          @Override
          public void run2() {
            final var pra = new PartitionAttributesFactory()
                .setRedundantCopies(redundantCopies).setLocalMaxMemory(0).create();
            var factory = new AttributesFactory();
            factory.setOffHeap(isOffHeap());
            factory.setPartitionAttributes(pra);
            factory.setEvictionAttributes(firstEvictionAttrs);
            final var pr = createRootRegion(name, factory.create());
            assertNotNull(pr);
          }
        };

    final SerializableRunnable createSecondAccessor =
        new CacheSerializableRunnable("Create an accessor with incorrect eviction attributes") {
          @Override
          public void run2() {
            final var pra = new PartitionAttributesFactory()
                .setRedundantCopies(redundantCopies).setLocalMaxMemory(0).create();
            var factory = new AttributesFactory();
            factory.setOffHeap(isOffHeap());
            factory.setPartitionAttributes(pra);
            factory.setEvictionAttributes(secondEvictionAttrs);
            final var pr = createRootRegion(name, factory.create());
            assertNotNull(pr);
          }
        };
    final SerializableRunnable createSecondDataStore =
        new CacheSerializableRunnable("Create a data store with eviction attributes") {
          @Override
          public void run2() {
            final var pra = new PartitionAttributesFactory()
                .setRedundantCopies(redundantCopies).setLocalMaxMemory(0).create();
            var factory = new AttributesFactory();
            factory.setOffHeap(isOffHeap());
            factory.setPartitionAttributes(pra);
            factory.setEvictionAttributes(firstEvictionAttrs);
            final var pr = createRootRegion(name, factory.create());
            assertNotNull(pr);
          }
        };
    firstAccessor.invoke(createFirstAccessor);
    firstDatastore.invoke(createFirstDataStore);
    testAccessor.invoke(createSecondAccessor);
    testDatastore.invoke(createSecondDataStore);
  }

  @Test
  public void testEvictionValidationForLRUEntry_DatastoreFirst() {
    final var host = Host.getHost(0);
    final var firstAccessor = host.getVM(0);
    final var testAccessor = host.getVM(1);
    final var testDatastore = host.getVM(2);
    final var firstDatastore = host.getVM(3);
    final var uniqName = getUniqueName();
    final var redundantCopies = 1;
    final var maxEntries = 226;
    final var name = uniqName + "-PR";

    final var firstEvictionAttrs =
        EvictionAttributes.createLRUEntryAttributes(maxEntries, EvictionAction.LOCAL_DESTROY);

    final var secondEvictionAttrs =
        EvictionAttributes.createLRUEntryAttributes(maxEntries, EvictionAction.OVERFLOW_TO_DISK);

    final SerializableRunnable createFirstAccessor =
        new CacheSerializableRunnable("Create an accessor without eviction attributes") {
          @Override
          public void run2() {
            final var pra = new PartitionAttributesFactory()
                .setRedundantCopies(redundantCopies).setLocalMaxMemory(0).create();
            var factory = new AttributesFactory();
            factory.setOffHeap(isOffHeap());
            factory.setPartitionAttributes(pra);
            final var pr = createRootRegion(name, factory.create());
            assertNotNull(pr);
          }
        };
    final SerializableRunnable createFirstDataStore =
        new CacheSerializableRunnable("Create a data store with eviction attributes") {
          @Override
          public void run2() {
            final var pra = new PartitionAttributesFactory()
                .setRedundantCopies(redundantCopies).setLocalMaxMemory(0).create();
            var factory = new AttributesFactory();
            factory.setOffHeap(isOffHeap());
            factory.setPartitionAttributes(pra);
            factory.setEvictionAttributes(firstEvictionAttrs);
            final var pr = createRootRegion(name, factory.create());
            assertNotNull(pr);
          }
        };

    final SerializableRunnable createSecondAccessor =
        new CacheSerializableRunnable("Create an accessor with incorrect eviction attributes") {
          @Override
          public void run2() {
            final var pra = new PartitionAttributesFactory()
                .setRedundantCopies(redundantCopies).setLocalMaxMemory(0).create();
            var factory = new AttributesFactory();
            factory.setOffHeap(isOffHeap());
            factory.setPartitionAttributes(pra);
            factory.setEvictionAttributes(secondEvictionAttrs);
            final var pr = createRootRegion(name, factory.create());
            assertNotNull(pr);
          }
        };
    final SerializableRunnable createSecondDataStore =
        new CacheSerializableRunnable("Create a data store with eviction attributes") {
          @Override
          public void run2() {
            final var pra = new PartitionAttributesFactory()
                .setRedundantCopies(redundantCopies).setLocalMaxMemory(0).create();
            var factory = new AttributesFactory();
            factory.setOffHeap(isOffHeap());
            factory.setPartitionAttributes(pra);
            factory.setEvictionAttributes(firstEvictionAttrs);
            final var pr = createRootRegion(name, factory.create());
            assertNotNull(pr);
          }
        };
    firstDatastore.invoke(createFirstDataStore);
    firstAccessor.invoke(createFirstAccessor);
    testDatastore.invoke(createSecondDataStore);
    testAccessor.invoke(createSecondAccessor);
  }

  @Test
  public void testEvictionValidationForLRUEntry_TwoAccessors() {
    final var host = Host.getHost(0);
    final var firstAccessor = host.getVM(0);
    final var testAccessor = host.getVM(1);
    final var testDatastore = host.getVM(2);
    final var firstDatastore = host.getVM(3);
    final var uniqName = getUniqueName();
    final var redundantCopies = 1;
    final var maxEntries = 226;
    final var name = uniqName + "-PR";

    final var firstEvictionAttrs =
        EvictionAttributes.createLRUEntryAttributes(maxEntries, EvictionAction.LOCAL_DESTROY);

    final SerializableRunnable createFirstAccessor =
        new CacheSerializableRunnable("Create an accessor without eviction attributes") {
          @Override
          public void run2() {
            final var pra = new PartitionAttributesFactory()
                .setRedundantCopies(redundantCopies).setLocalMaxMemory(0).create();
            var factory = new AttributesFactory();
            factory.setOffHeap(isOffHeap());
            factory.setPartitionAttributes(pra);
            final var pr = createRootRegion(name, factory.create());
            assertNotNull(pr);
          }
        };
    final SerializableRunnable createFirstDataStore =
        new CacheSerializableRunnable("Create a data store with eviction attributes") {
          @Override
          public void run2() {
            final var pra = new PartitionAttributesFactory()
                .setRedundantCopies(redundantCopies).setLocalMaxMemory(0).create();
            var factory = new AttributesFactory();
            factory.setOffHeap(isOffHeap());
            factory.setPartitionAttributes(pra);
            factory.setEvictionAttributes(firstEvictionAttrs);
            final var pr = createRootRegion(name, factory.create());
            assertNotNull(pr);
          }
        };

    final SerializableRunnable createSecondAccessor =
        new CacheSerializableRunnable("Create an accessor with correct eviction attributes") {
          @Override
          public void run2() {
            final var pra = new PartitionAttributesFactory()
                .setRedundantCopies(redundantCopies).setLocalMaxMemory(0).create();
            var factory = new AttributesFactory();
            factory.setOffHeap(isOffHeap());
            factory.setPartitionAttributes(pra);
            factory.setEvictionAttributes(firstEvictionAttrs);
            final var pr = createRootRegion(name, factory.create());
            assertNotNull(pr);

          }
        };
    final SerializableRunnable createSecondDataStore =
        new CacheSerializableRunnable("Create a data store with eviction attributes") {
          @Override
          public void run2() {
            final var pra = new PartitionAttributesFactory()
                .setRedundantCopies(redundantCopies).setLocalMaxMemory(0).create();
            var factory = new AttributesFactory();
            factory.setOffHeap(isOffHeap());
            factory.setPartitionAttributes(pra);
            factory.setEvictionAttributes(firstEvictionAttrs);
            final var pr = createRootRegion(name, factory.create());
            assertNotNull(pr);
          }
        };
    firstAccessor.invoke(createFirstAccessor);
    testAccessor.invoke(createSecondAccessor);

    firstDatastore.invoke(createFirstDataStore);
    testDatastore.invoke(createSecondDataStore);
  }

  /**
   * Test that gets do not need to acquire a lock on the region entry when LRU is enabled. This is
   * bug 42265
   */
  @Test
  public void testEntryLRUDeadlock() {
    final var host = Host.getHost(0);
    final var vm0 = host.getVM(0);
    final var vm1 = host.getVM(1);
    final var uniqName = getUniqueName();
    final var redundantCopies = 0;
    final var maxBuckets = 8;
    final var maxEntries = 16;
    final var name = uniqName + "-PR";
    final var extraEntries = 4;

    // final int heapPercentage = 66;
    // final int evictorInterval = 100;

    final SerializableRunnable create = new CacheSerializableRunnable(
        "Create Entry LRU with local destroy on a partitioned Region") {
      @Override
      public void run2() {
        final var factory = new AttributesFactory();
        factory.setOffHeap(isOffHeap());
        factory.setPartitionAttributes(new PartitionAttributesFactory()
            .setRedundantCopies(redundantCopies).setTotalNumBuckets(maxBuckets).create());
        factory.setEvictionAttributes(
            EvictionAttributes.createLRUEntryAttributes(maxEntries, EvictionAction.LOCAL_DESTROY));
        final var pr = (PartitionedRegion) createRootRegion(name, factory.create());
        assertNotNull(pr);
      }
    };
    vm0.invoke(create);

    final var create2 =
        new SerializableRunnable("Create Entry LRU with local destroy on a partitioned Region") {
          @Override
          public void run() {
            try {
              final var factory = new AttributesFactory();
              factory.setOffHeap(isOffHeap());
              factory.setPartitionAttributes(
                  new PartitionAttributesFactory().setRedundantCopies(redundantCopies)
                      .setLocalMaxMemory(0).setTotalNumBuckets(maxBuckets).create());
              factory
                  .setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(maxEntries));
              factory.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
              // this listener will cause a deadlock if the get ends up
              // locking the entry.
              factory.addCacheListener(new CacheListenerAdapter() {

                @Override
                public void afterCreate(EntryEvent event) {
                  var region = event.getRegion();
                  var key = event.getKey();
                  region.get(key);
                }

              });
              final var pr =
                  (PartitionedRegion) createRootRegion(name, factory.create());
              assertNotNull(pr);
            } catch (final CacheException ex) {
              Assert.fail("While creating Partitioned region", ex);
            }
          }
        };
    vm1.invoke(create2);

    final var doPuts = new SerializableRunnable("Do Puts") {
      @Override
      public void run() {
        final var pr = (PartitionedRegion) getRootRegion(name);
        assertNotNull(pr);
        for (var counter = 0; counter <= maxEntries + extraEntries; counter++) {
          pr.put(counter, "value");
        }
      }
    };
    vm0.invoke(doPuts);
  }

  protected void setEvictionPercentage(float percentage) {
    getCache().getResourceManager().setEvictionHeapPercentage(percentage);
  }

  protected boolean isOffHeap() {
    return false;
  }

  protected ResourceType getMemoryType() {
    return ResourceType.HEAP_MEMORY;
  }

  protected HeapEvictor getEvictor(Region region) {
    return ((GemFireCacheImpl) region.getRegionService()).getHeapEvictor();
  }
}
