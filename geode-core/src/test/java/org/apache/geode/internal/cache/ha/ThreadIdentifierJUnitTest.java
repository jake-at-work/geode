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
package org.apache.geode.internal.cache.ha;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.InetAddress;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.ha.ThreadIdentifier.WanType;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class ThreadIdentifierJUnitTest {

  @Test
  public void testEqualsIgnoresUUIDBytes() throws Exception {
    var id = new InternalDistributedMember(InetAddress.getLocalHost(), 1234);
    id.setVersionForTest(KnownVersion.GFE_90);
    var memberIdBytes = EventID.getMembershipId(new ClientProxyMembershipID(id));
    var memberIdBytesWithoutUUID = new byte[memberIdBytes.length - (2 * 8 + 1)];// UUID bytes +
                                                                                // weight byte
    System.arraycopy(memberIdBytes, 0, memberIdBytesWithoutUUID, 0,
        memberIdBytesWithoutUUID.length);
    var threadIdWithUUID = new ThreadIdentifier(memberIdBytes, 1);
    var threadIdWithoutUUID = new ThreadIdentifier(memberIdBytesWithoutUUID, 1);
    assertEquals(threadIdWithoutUUID, threadIdWithUUID);
    assertEquals(threadIdWithUUID, threadIdWithoutUUID);
    assertEquals(threadIdWithoutUUID.hashCode(), threadIdWithUUID.hashCode());

    var eventIDWithUUID = new EventID(memberIdBytes, 1, 1);
    var eventIDWithoutUUID = new EventID(memberIdBytesWithoutUUID, 1, 1);
    assertEquals(eventIDWithUUID, eventIDWithoutUUID);
    assertEquals(eventIDWithoutUUID, eventIDWithUUID);
    assertEquals(eventIDWithoutUUID.hashCode(), eventIDWithUUID.hashCode());
  }

  @Test
  public void testPutAllId() {
    var id = 42;
    var bucketNumber = 113;

    var putAll = ThreadIdentifier.createFakeThreadIDForBulkOp(bucketNumber, id);

    assertTrue(ThreadIdentifier.isPutAllFakeThreadID(putAll));
    assertEquals(42, ThreadIdentifier.getRealThreadID(putAll));
  }

  @Test
  public void testWanId() {
    var id = 42;

    var wan1 = ThreadIdentifier.createFakeThreadIDForParallelGSPrimaryBucket(1, id, 0);
    assertEquals(42, ThreadIdentifier.getRealThreadID(wan1));
    assertTrue(ThreadIdentifier.isParallelWANThreadID(wan1));
    {
      var real_tid_with_wan = ThreadIdentifier.getRealThreadIDIncludingWan(wan1);
      assertEquals(42, ThreadIdentifier.getRealThreadID(real_tid_with_wan));
      assertTrue(ThreadIdentifier.isParallelWANThreadID(real_tid_with_wan));
      assertTrue(WanType.matches(real_tid_with_wan));
    }

    var wan2 = ThreadIdentifier.createFakeThreadIDForParallelGSSecondaryBucket(1, id, 0);
    assertEquals(42, ThreadIdentifier.getRealThreadID(wan2));
    assertTrue(ThreadIdentifier.isParallelWANThreadID(wan2));
    {
      var real_tid_with_wan = ThreadIdentifier.getRealThreadIDIncludingWan(wan2);
      assertEquals(42, ThreadIdentifier.getRealThreadID(real_tid_with_wan));
      assertTrue(ThreadIdentifier.isParallelWANThreadID(real_tid_with_wan));
      assertTrue(WanType.matches(real_tid_with_wan));
    }

    var wan3 = ThreadIdentifier.createFakeThreadIDForParallelGateway(1, id, 0);
    assertEquals(42, ThreadIdentifier.getRealThreadID(wan3));
    assertTrue(ThreadIdentifier.isParallelWANThreadID(wan3));
    {
      var real_tid_with_wan = ThreadIdentifier.getRealThreadIDIncludingWan(wan3);
      assertEquals(42, ThreadIdentifier.getRealThreadID(real_tid_with_wan));
      assertTrue(ThreadIdentifier.isParallelWANThreadID(real_tid_with_wan));
      assertTrue(WanType.matches(real_tid_with_wan));
    }
  }

  @Test
  public void testWanAndPutAllId() {
    var id = 42;
    var bucketNumber = 113;

    var putAll = ThreadIdentifier.createFakeThreadIDForBulkOp(bucketNumber, id);

    var wan1 = ThreadIdentifier.createFakeThreadIDForParallelGSPrimaryBucket(1, putAll, 0);
    assertEquals(42, ThreadIdentifier.getRealThreadID(wan1));
    assertTrue(ThreadIdentifier.isParallelWANThreadID(wan1));
    assertTrue(ThreadIdentifier.isPutAllFakeThreadID(wan1));
    {
      var real_tid_with_wan = ThreadIdentifier.getRealThreadIDIncludingWan(wan1);
      assertEquals(42, ThreadIdentifier.getRealThreadID(real_tid_with_wan));
      assertTrue(ThreadIdentifier.isParallelWANThreadID(real_tid_with_wan));
      assertTrue(WanType.matches(real_tid_with_wan));
    }

    var wan2 = ThreadIdentifier.createFakeThreadIDForParallelGSSecondaryBucket(1, putAll, 0);
    assertEquals(42, ThreadIdentifier.getRealThreadID(wan2));
    assertTrue(ThreadIdentifier.isParallelWANThreadID(wan2));
    assertTrue(ThreadIdentifier.isPutAllFakeThreadID(wan2));
    {
      var real_tid_with_wan = ThreadIdentifier.getRealThreadIDIncludingWan(wan2);
      assertEquals(42, ThreadIdentifier.getRealThreadID(real_tid_with_wan));
      assertTrue(ThreadIdentifier.isParallelWANThreadID(real_tid_with_wan));
      assertTrue(WanType.matches(real_tid_with_wan));
    }

    var wan3 = ThreadIdentifier.createFakeThreadIDForParallelGateway(1, putAll, 0);
    assertEquals(42, ThreadIdentifier.getRealThreadID(wan3));
    assertTrue(ThreadIdentifier.isParallelWANThreadID(wan3));
    assertTrue(ThreadIdentifier.isPutAllFakeThreadID(wan3));
    {
      var real_tid_with_wan = ThreadIdentifier.getRealThreadIDIncludingWan(wan3);
      assertEquals(42, ThreadIdentifier.getRealThreadID(real_tid_with_wan));
      assertTrue(ThreadIdentifier.isParallelWANThreadID(real_tid_with_wan));
      assertTrue(WanType.matches(real_tid_with_wan));
    }

    var tid = 4054000001L;
    assertTrue(ThreadIdentifier.isParallelWANThreadID(tid));
    assertFalse(ThreadIdentifier.isParallelWANThreadID(putAll));
  }
}
