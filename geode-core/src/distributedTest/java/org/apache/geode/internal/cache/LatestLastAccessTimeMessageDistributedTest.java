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

import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import java.io.Serializable;
import java.util.Objects;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

public class LatestLastAccessTimeMessageDistributedTest implements Serializable {

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Test
  public void testSendLatestLastAccessTimeMessageToMemberWithNoRegion() {
    // Start Locator
    var locator = cluster.startLocatorVM(0);

    // Start servers
    var locatorPort = locator.getPort();
    var server1 =
        cluster.startServerVM(1, s -> s.withConnectionToLocator(locatorPort).withRegion(
            RegionShortcut.PARTITION_REDUNDANT, testName.getMethodName()));
    cluster.startServerVM(2, s -> s.withConnectionToLocator(locatorPort));

    // Assign buckets to create the BucketRegions
    server1.invoke(this::assignBucketsToPartitions);

    // Send LastAccessTimeMessage from server1 to server2
    server1.invoke(this::sendLastAccessTimeMessage);
  }

  private void assignBucketsToPartitions() {
    Cache cache = Objects.requireNonNull(ClusterStartupRule.getCache());
    var pr = (PartitionedRegion) cache.getRegion(testName.getMethodName());
    PartitionRegionHelper.assignBucketsToPartitions(pr);
  }

  private void sendLastAccessTimeMessage() throws InterruptedException {
    // Get a BucketRegion
    Cache cache = Objects.requireNonNull(ClusterStartupRule.getCache());
    var pr = (PartitionedRegion) cache.getRegion(testName.getMethodName());
    var br = pr.getBucketRegion(0);

    // Get the recipients
    var dm = br.getDistributionManager();
    var recipients = dm.getOtherNormalDistributionManagerIds();

    // Create and sent the LatestLastAccessTimeMessage
    var replyProcessor =
        new LatestLastAccessTimeReplyProcessor(dm, recipients);
    dm.putOutgoing(new LatestLastAccessTimeMessage<>(replyProcessor, recipients, br, (Object) 0));

    // Wait for the reply. Timeout if no reply is received.
    var success = replyProcessor.waitForReplies(getTimeout().toMillis());

    // Assert the wait was successful
    assertThat(success).isTrue();

    // Assert the latest last accessed time is 0
    assertThat(replyProcessor.getLatestLastAccessTime()).isEqualTo(0L);
  }
}
