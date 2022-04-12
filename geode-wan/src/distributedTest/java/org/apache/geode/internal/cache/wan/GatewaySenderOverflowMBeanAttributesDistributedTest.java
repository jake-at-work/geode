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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import junitparams.Parameters;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.wan.parallel.ParallelGatewaySenderQueue;
import org.apache.geode.management.ManagementService;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@Category({WanTest.class})
@RunWith(GeodeParamsRunner.class)
public class GatewaySenderOverflowMBeanAttributesDistributedTest extends WANTestBase {

  @Test
  @Parameters({"true", "false"})
  public void testParallelGatewaySenderOverflowMBeanAttributes(boolean createSenderFirst)
      throws Exception {
    // Start the locators
    var lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    var nyPort = vm1.invoke(() -> createFirstRemoteLocator(2, lnPort));

    // Create the cache
    vm4.invoke(() -> createCache(lnPort));

    var senderId = "ln";
    if (createSenderFirst) {
      // Create a gateway sender then a region (normal xml order)

      // Create a gateway sender in paused state so it creates the queue, but doesn't read any
      // events from disk
      vm4.invoke(() -> createSender(senderId, 2, true, 1, 10, false, false, null, false));
      vm4.invoke(() -> pauseSender(senderId));

      // Create a partitioned region attached to the gateway sender
      vm4.invoke(() -> createPartitionedRegion(getTestMethodName(), senderId, 1, 100, isOffHeap()));
    } else {
      // Create a partitioned region then a gateway sender

      // Create a partitioned region attached to the gateway sender
      vm4.invoke(() -> createPartitionedRegion(getTestMethodName(), senderId, 1, 100, isOffHeap()));

      // Create a gateway sender in paused state so it creates the queue, but doesn't read any
      // events from disk
      vm4.invoke(() -> createSender(senderId, 2, true, 1, 10, false, false, null, false));
      vm4.invoke(() -> pauseSender(senderId));
    }

    // Do some puts to cause overflow
    var numPuts = 10;
    vm4.invoke(() -> doHeavyPuts(getTestMethodName(), numPuts));

    // Compare overflow stats to mbean attributes
    vm4.invoke(() -> compareParallelOverflowStatsToMBeanAttributes(senderId));

    // Start a gateway receiver with partitioned region
    vm2.invoke(() -> createCache(nyPort));
    vm2.invoke(WANTestBase::createReceiver);
    vm2.invoke(() -> createPartitionedRegion(getTestMethodName(), null, 1, 100, isOffHeap()));

    // Resume gateway sender
    vm4.invoke(() -> resumeSender(senderId));

    // Wait for queue to drain
    vm4.invoke(() -> checkQueueSize(senderId, 0));

    // Compare overflow stats to mbean attributes
    vm4.invoke(() -> compareParallelOverflowStatsToMBeanAttributes(senderId));
  }

  @Test
  @Parameters({"true", "false"})
  public void testSerialGatewaySenderOverflowMBeanAttributes(boolean createSenderFirst)
      throws Exception {
    // Start the locators
    var lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    var nyPort = vm1.invoke(() -> createFirstRemoteLocator(2, lnPort));

    // Create the cache
    vm4.invoke(() -> createCache(lnPort));

    var senderId = "ln";
    if (createSenderFirst) {
      // Create a gateway sender then a region (normal xml order)

      // Create a gateway sender in paused state so it creates the queue, but doesn't read any
      // events from disk
      vm4.invoke(() -> createConcurrentSender(senderId, 2, false, 10, 10, false, false, null, false,
          5, GatewaySender.OrderPolicy.KEY));
      vm4.invoke(() -> pauseSender(senderId));

      // Create a partitioned region attached to the gateway sender
      vm4.invoke(() -> createPartitionedRegion(getTestMethodName(), senderId, 1, 100, isOffHeap()));
    } else {
      // Create a partitioned region then a gateway sender

      // Create a partitioned region attached to the gateway sender
      vm4.invoke(() -> createPartitionedRegion(getTestMethodName(), senderId, 1, 100, isOffHeap()));

      // Create a gateway sender in paused state so it creates the queue, but doesn't read any
      // events from disk
      vm4.invoke(() -> createConcurrentSender(senderId, 2, false, 10, 10, false, false, null, false,
          5, GatewaySender.OrderPolicy.KEY));
      vm4.invoke(() -> pauseSender(senderId));
    }

    // Do some puts to cause overflow
    var numPuts = 20;
    vm4.invoke(() -> doHeavyPuts(getTestMethodName(), numPuts));

    // Compare overflow stats to mbean attributes
    vm4.invoke(() -> compareSerialOverflowStatsToMBeanAttributes(senderId));

    // Start a gateway receiver with partitioned region
    vm2.invoke(() -> createCache(nyPort));
    vm2.invoke(WANTestBase::createReceiver);
    vm2.invoke(() -> createPartitionedRegion(getTestMethodName(), null, 1, 100, isOffHeap()));

    // Resume the gateway sender
    vm4.invoke(() -> resumeSender(senderId));

    // Wait for queue to drain
    vm4.invoke(() -> checkQueueSize(senderId, 0));

    // Compare disk region stats to mbean attributes
    vm4.invoke(() -> compareSerialOverflowStatsToMBeanAttributes(senderId));
  }

  @Test
  @Parameters({"true", "false"})
  public void testParallelGatewaySenderOverflowMBeanAttributesClear(boolean createSenderFirst)
      throws Exception {
    // Start the locators
    var lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    var nyPort = vm1.invoke(() -> createFirstRemoteLocator(2, lnPort));

    // Create the cache
    vm4.invoke(() -> createCache(lnPort));

    var senderId = "ln";
    if (createSenderFirst) {
      // Create a gateway sender then a region (normal xml order)

      // Create a gateway sender in paused state so it creates the queue, but doesn't read any
      // events from disk
      vm4.invoke(() -> createSender(senderId, 2, true, 1, 10, false, false, null, false));
      vm4.invoke(() -> pauseSender(senderId));

      // Create a partitioned region attached to the gateway sender
      vm4.invoke(() -> createPartitionedRegion(getTestMethodName(), senderId, 1, 100, isOffHeap()));
    } else {
      // Create a partitioned region then a gateway sender

      // Create a partitioned region attached to the gateway sender
      vm4.invoke(() -> createPartitionedRegion(getTestMethodName(), senderId, 1, 100, isOffHeap()));

      // Create a gateway sender in paused state so it creates the queue, but doesn't read any
      // events from disk
      vm4.invoke(() -> createSender(senderId, 2, true, 1, 10, false, false, null, false));
      vm4.invoke(() -> pauseSender(senderId));
    }

    // Do some puts to cause overflow
    var numPuts = 10;
    vm4.invoke(() -> doHeavyPuts(getTestMethodName(), numPuts));

    // Compare overflow stats to mbean attributes
    vm4.invoke(() -> compareParallelOverflowStatsToMBeanAttributes(senderId));

    vm4.invoke(() -> stopSender(senderId));
    vm4.invoke(() -> startSenderwithCleanQueues(senderId));

    vm4.invoke(() -> checkParallelOverflowStatsAreZero(senderId));

    // Check queue is clear
    vm4.invoke(() -> checkQueueSize(senderId, 0));

    // Compare overflow stats to mbean attributes
    vm4.invoke(() -> compareParallelOverflowStatsToMBeanAttributes(senderId));
  }

  private void compareParallelOverflowStatsToMBeanAttributes(String senderId) throws Exception {
    // Get disk region stats associated with the queue region
    var region =
        (PartitionedRegion) cache.getRegion(senderId + ParallelGatewaySenderQueue.QSTRING);
    var drs = region.getDiskRegionStats();
    assertThat(drs).isNotNull();

    // Get gateway sender mbean
    var service = ManagementService.getManagementService(cache);
    var bean = service.getLocalGatewaySenderMXBean(senderId);
    assertThat(bean).isNotNull();

    // Wait for the sampler to take a few samples
    waitForSamplerToSample(5);

    // Verify the bean attributes match the stat values
    await().untilAsserted(() -> {
      assertThat(bean.getEntriesOverflowedToDisk()).isEqualTo(drs.getNumOverflowOnDisk());
      assertThat(bean.getBytesOverflowedToDisk()).isEqualTo(drs.getNumOverflowBytesOnDisk());
    });
  }

  private void compareSerialOverflowStatsToMBeanAttributes(String senderId) throws Exception {
    // Get the sender
    var sender = (AbstractGatewaySender) cache.getGatewaySender(senderId);

    // Get the sender's queue regions
    var queues = sender.getQueues();

    // Get gateway sender mbean
    var service = ManagementService.getManagementService(cache);
    var bean = service.getLocalGatewaySenderMXBean(senderId);
    assertThat(bean).isNotNull();

    // Wait for the sampler to take a few samples
    waitForSamplerToSample(5);

    // Verify the bean attributes match the stat values
    await().untilAsserted(() -> {
      // Calculate the total entries and bytes overflowed to disk
      var entriesOverflowedToDisk = 0;
      var bytesOverflowedToDisk = 0l;
      for (var queue : queues) {
        var lr = (LocalRegion) queue.getRegion();
        var drs = lr.getDiskRegion().getStats();
        entriesOverflowedToDisk += drs.getNumOverflowOnDisk();
        bytesOverflowedToDisk += drs.getNumOverflowBytesOnDisk();
      }

      // Verify the bean attributes match the stat values
      assertThat(bean.getEntriesOverflowedToDisk()).isEqualTo(entriesOverflowedToDisk);
      assertThat(bean.getBytesOverflowedToDisk()).isEqualTo(bytesOverflowedToDisk);
    });
  }

  private void waitForSamplerToSample(int numTimesToSample) throws Exception {
    var ids = (InternalDistributedSystem) cache.getDistributedSystem();
    assertThat(ids.getStatSampler().waitForSampleCollector(60000)).isNotNull();
    for (var i = 0; i < numTimesToSample; i++) {
      assertThat(ids.getStatSampler().waitForSample((60000))).isTrue();
    }
  }

  private void checkParallelOverflowStatsAreZero(String senderId) throws Exception {

    // Get gateway sender mbean
    var service = ManagementService.getManagementService(cache);
    var bean = service.getLocalGatewaySenderMXBean(senderId);
    assertThat(bean).isNotNull();

    // Wait for the sampler to take a few samples
    waitForSamplerToSample(5);

    // Verify the bean attributes match the stat values
    await().untilAsserted(() -> {
      assertThat(bean.getEntriesOverflowedToDisk()).isEqualTo(0);
      assertThat(bean.getBytesOverflowedToDisk()).isEqualTo(0);
      assertThat(bean.getTotalQueueSizeBytesInUse()).isEqualTo(0);
    });
  }
}
