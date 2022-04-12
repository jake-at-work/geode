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

package org.apache.geode.distributed.internal.membership.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.UnknownHostException;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.serialization.KnownVersion;

public class MemberIdentifierFactoryImplTest {

  private MemberIdentifierFactoryImpl factory;

  @Before
  public void setup() {
    factory = new MemberIdentifierFactoryImpl();
  }

  @Test
  public void testRemoteHost() throws UnknownHostException {
    var localhost = LocalHostUtil.getLocalHost();
    var memberData =
        MemberDataBuilder.newBuilder(localhost, localhost.getHostName()).build();
    var data = factory.create(memberData);
    assertThat(data.getInetAddress()).isEqualTo(localhost);
    assertThat(data.getHostName()).isEqualTo(localhost.getHostName());
  }

  @Test
  public void testNewBuilderForLocalHost() throws UnknownHostException {
    var localhost = LocalHostUtil.getLocalHost();
    var memberData = MemberDataBuilder.newBuilderForLocalHost("hostname").build();
    var data = factory.create(memberData);
    assertThat(data.getInetAddress()).isEqualTo(localhost);
    assertThat(data.getHostName()).isEqualTo("hostname");
  }

  @Test
  public void testSetMembershipPort() {
    var memberData = MemberDataBuilder.newBuilderForLocalHost("hostname")
        .setMembershipPort(1234).build();
    var data = factory.create(memberData);
    assertThat(data.getMembershipPort()).isEqualTo(1234);
  }

  @Test
  public void testSetVmKind() {
    var memberData = MemberDataBuilder.newBuilderForLocalHost("hostname")
        .setVmKind(12).build();
    var data = factory.create(memberData);
    assertThat(data.getVmKind()).isEqualTo((byte) 12);
  }

  @Test
  public void testSetVmViewId() {
    var memberData = MemberDataBuilder.newBuilderForLocalHost("hostname")
        .setVmViewId(1234).build();
    var data = factory.create(memberData);
    assertThat(data.getVmViewId()).isEqualTo(1234);
  }

  @Test
  public void testSetGroups() {
    var groups = new String[] {"group1", "group2"};
    var memberData = MemberDataBuilder.newBuilderForLocalHost("hostname")
        .setGroups(groups).build();
    var data = factory.create(memberData);
    assertThat(data.getGroups()).isEqualTo(Arrays.asList(groups));
  }

  @Test
  public void testSetDurableId() {
    var memberData = MemberDataBuilder.newBuilderForLocalHost("hostname")
        .setDurableId("myId").build();
    var data = factory.create(memberData);
    assertThat(data.getDurableId()).isEqualTo("myId");
  }

  @Test
  public void testSetVersionOrdinal() {
    var memberData = MemberDataBuilder.newBuilderForLocalHost("hostname")
        .setVersionOrdinal(KnownVersion.CURRENT_ORDINAL).build();
    var data = factory.create(memberData);
    assertThat(data.getVersionOrdinal()).isEqualTo(KnownVersion.CURRENT_ORDINAL);
  }

  @Test
  public void membersAreEqual() {
    var memberData = MemberDataBuilder.newBuilderForLocalHost("hostname")
        .setMembershipPort(1).build();
    var member1 = factory.create(memberData);
    memberData = MemberDataBuilder.newBuilderForLocalHost("hostname")
        .setMembershipPort(1).build();
    var member2 = factory.create(memberData);
    assertThat(factory.getComparator().compare(member1, member2)).isZero();
  }

  @Test
  public void membersAreNotEqual() {
    var memberData = MemberDataBuilder.newBuilderForLocalHost("hostname")
        .setMembershipPort(1).build();
    var member1 = factory.create(memberData);
    memberData = MemberDataBuilder.newBuilderForLocalHost("hostname")
        .setMembershipPort(2).build();
    var member2 = factory.create(memberData);
    assertThat(factory.getComparator().compare(member1, member2)).isLessThan(0);
    assertThat(factory.getComparator().compare(member2, member1)).isGreaterThan(0);
  }
}
