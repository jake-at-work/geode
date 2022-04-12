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
package org.apache.geode.distributed.internal.membership.gms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.net.InetAddress;

import org.jgroups.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.serialization.BufferDataOutputStream;
import org.apache.geode.internal.serialization.DSFIDSerializer;
import org.apache.geode.internal.serialization.DSFIDSerializerFactory;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.VersionedDataInputStream;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({SecurityTest.class})
public class GMSMemberDataJUnitTest {

  private DSFIDSerializer dsfidSerializer;

  @Before
  public void setup() {
    dsfidSerializer = new DSFIDSerializerFactory().create();
  }

  @Test
  public void testEqualsNotSameType() {
    var member = new GMSMemberData();
    assertThat(member).isNotEqualTo("Not a GMSMemberData");
  }

  @Test
  public void testEqualsIsSame() {
    var member = new GMSMemberData();
    assertThat(member).isEqualTo(member);
  }

  @Test
  public void testCompareToIsSame() {
    var member = new GMSMemberData();
    var uuid = new UUID(0, 0);
    member.setUUID(uuid);
    assertThat(member.compareTo(member)).isZero();
  }

  private GMSMemberData createGMSMember(byte[] inetAddress, int viewId, long msb, long lsb) {
    var member = new GMSMemberData();
    var addr1 = mock(InetAddress.class);
    when(addr1.getAddress()).thenReturn(inetAddress);
    member.setInetAddr(addr1);
    member.setVmViewId(viewId);
    member.setUUID(new UUID(msb, lsb));
    return member;
  }

  @Test
  public void testCompareToInetAddressIsLongerThan() {
    var member1 = createGMSMember(new byte[] {1, 1, 1, 1, 1}, 1, 1, 1);
    var member2 = createGMSMember(new byte[] {1, 1, 1, 1}, 1, 1, 1);
    assertThat(member1.compareTo(member2)).isGreaterThan(0);
  }

  @Test
  public void testShallowMemberEquals() {
    var member1 = createGMSMember(new byte[] {1, 1, 1, 1, 1}, 1, 1, 1);
    var member2 =
        new GMSMemberData(member1.getInetAddress(), member1.getMembershipPort(),
            member1.getVersionOrdinal(),
            member1.getUuidMostSignificantBits(), member1.getUuidLeastSignificantBits(),
            member1.getVmViewId());
    assertThat(member1.compareTo(member2)).isZero();
  }

  @Test
  public void testShallowMemberNotEquals() {
    var member1 = createGMSMember(new byte[] {1, 1, 1, 1, 1}, 1, 1, 1);
    var member2 = new GMSMemberData(member1.getInetAddress(), member1.getMembershipPort(),
        member1.getVersionOrdinal(), member1.getUuidMostSignificantBits(),
        member1.getUuidLeastSignificantBits(), 100);
    assertThat(member1).isNotEqualTo(member2);
  }

  @Test
  public void testCompareToInetAddressIsShorterThan() {
    var member1 = createGMSMember(new byte[] {1, 1, 1, 1}, 1, 1, 1);
    var member2 = createGMSMember(new byte[] {1, 1, 1, 1, 1}, 1, 1, 1);
    assertThat(member1.compareTo(member2)).isLessThan(0);
  }

  @Test
  public void testCompareToInetAddressIsGreater() {
    var member1 = createGMSMember(new byte[] {1, 2, 1, 1, 1}, 1, 1, 1);
    var member2 = createGMSMember(new byte[] {1, 1, 1, 1, 1}, 1, 1, 1);
    assertThat(member1.compareTo(member2)).isGreaterThan(0);
  }

  @Test
  public void testCompareToInetAddressIsLessThan() {
    var member1 = createGMSMember(new byte[] {1, 1, 1, 1, 1}, 1, 1, 1);
    var member2 = createGMSMember(new byte[] {1, 2, 1, 1, 1}, 1, 1, 1);
    assertThat(member1.compareTo(member2)).isLessThan(0);
  }

  @Test
  public void testCompareToMyViewIdLarger() {
    var member1 = createGMSMember(new byte[] {1}, 2, 1, 1);
    var member2 = createGMSMember(new byte[] {1}, 1, 1, 1);
    assertThat(member1.compareTo(member2)).isGreaterThan(0);
  }

  @Test
  public void testCompareToTheirViewIdLarger() {
    var member1 = createGMSMember(new byte[] {1}, 1, 1, 1);
    var member2 = createGMSMember(new byte[] {1}, 2, 1, 1);
    assertThat(member1.compareTo(member2)).isLessThan(0);
  }

  @Test
  public void testCompareToMyMSBLarger() {
    var member1 = createGMSMember(new byte[] {1}, 1, 2, 1);
    var member2 = createGMSMember(new byte[] {1}, 1, 1, 1);
    assertThat(member1.compareTo(member2)).isGreaterThan(0);
  }

  @Test
  public void testCompareToTheirMSBLarger() {
    var member1 = createGMSMember(new byte[] {1}, 1, 1, 1);
    var member2 = createGMSMember(new byte[] {1}, 1, 2, 1);
    assertThat(member1.compareTo(member2)).isLessThan(0);
  }

  @Test
  public void testCompareToMyLSBLarger() {
    var member1 = createGMSMember(new byte[] {1}, 1, 1, 2);
    var member2 = createGMSMember(new byte[] {1}, 1, 1, 1);
    assertThat(member1.compareTo(member2)).isGreaterThan(0);
  }

  @Test
  public void testCompareToTheirLSBLarger() {
    var member1 = createGMSMember(new byte[] {1}, 1, 1, 1);
    var member2 = createGMSMember(new byte[] {1}, 1, 1, 2);
    assertThat(member1.compareTo(member2)).isLessThan(0);
  }

  @Test
  public void testGetUUIDReturnsNullWhenUUIDIs0() {
    var member = new GMSMemberData();
    var uuid = new UUID(0, 0);
    member.setUUID(uuid);
    assertThat(member.getUUID()).isNull();
  }

  @Test
  public void testGetUUID() {
    var member = new GMSMemberData();
    var uuid = new UUID(1, 1);
    member.setUUID(uuid);
    assertThat(member.getUUID()).isNotNull();
  }

  /**
   * <p>
   * GEODE-2875 - adds vmKind to on-wire form of GMSMemberData.writeEssentialData
   * </p>
   * <p>
   * This must be backward-compatible with Geode 1.0 (Version.GFE_90)
   * </p>
   *
   */
  @Test
  public void testGMSMemberBackwardCompatibility() throws Exception {
    var baos = new ByteArrayOutputStream();
    var member = new GMSMemberData();
    member.setInetAddr(LocalHostUtil.getLocalHost());
    DataOutput dataOutput = new DataOutputStream(baos);
    var serializationContext = dsfidSerializer
        .createSerializationContext(dataOutput);
    member.writeEssentialData(dataOutput, serializationContext);

    // vmKind should be transmitted to a member with the current version
    var bais = new ByteArrayInputStream(baos.toByteArray());
    DataInput dataInput = new DataInputStream(bais);
    var deserializationContext = dsfidSerializer
        .createDeserializationContext(dataInput);
    var newMember = new GMSMemberData();
    newMember.readEssentialData(dataInput, deserializationContext);
    assertThat(newMember.getVmKind()).isEqualTo(member.getVmKind());
    assertThat(newMember.getInetAddress()).isNotNull();
    assertThat(newMember.getInetAddress().getHostAddress()).isEqualTo(newMember.getHostName());


    // vmKind should not be transmitted to a member with version GFE_90 or earlier
    dataOutput = new BufferDataOutputStream(KnownVersion.GFE_90);
    member.writeEssentialData(dataOutput, serializationContext);
    bais = new ByteArrayInputStream(baos.toByteArray());
    var stream = new DataInputStream(bais);
    deserializationContext = dsfidSerializer.createDeserializationContext(stream);
    dataInput = new VersionedDataInputStream(stream, KnownVersion.GFE_90);
    newMember = new GMSMemberData();
    newMember.readEssentialData(dataInput, deserializationContext);
    assertThat(newMember.getVmKind()).isZero();
  }


}
