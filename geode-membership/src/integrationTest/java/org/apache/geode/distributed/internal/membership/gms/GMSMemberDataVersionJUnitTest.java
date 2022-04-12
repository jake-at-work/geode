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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;

import org.assertj.core.api.AbstractShortAssert;
import org.junit.Test;

import org.apache.geode.distributed.internal.membership.api.MemberData;
import org.apache.geode.internal.serialization.DSFIDSerializerFactory;
import org.apache.geode.internal.serialization.KnownVersion;

/**
 * MemberData has to be able to hold an unknown version ordinal since, during a rolling upgrade,
 * we may receive a MemberData from a member running a future version of the product.
 */
public class GMSMemberDataVersionJUnitTest {

  private final short unknownVersionOrdinal =
      (short) (KnownVersion.CURRENT_ORDINAL + 1);

  @Test
  public void testConstructor1() {
    final var builder = MemberDataBuilderImpl.newBuilder(null, null);
    builder.setVersionOrdinal(unknownVersionOrdinal);
    validate(builder.build());
  }

  @Test
  public void testConstructor2() {
    final var memberData =
        new GMSMemberData(mock(InetAddress.class), 0, unknownVersionOrdinal, 0, 0, 0);
    validate(memberData);
  }

  @Test
  public void testReadEssentialData() throws IOException, ClassNotFoundException {

    final var builder = MemberDataBuilderImpl.newBuilder(null, null);
    builder.setVersionOrdinal(unknownVersionOrdinal);
    final var member = builder.build();

    final var baos = new ByteArrayOutputStream();
    final DataOutput dataOutput = new DataOutputStream(baos);
    final var dsfidSerializer = new DSFIDSerializerFactory().create();
    final var serializationContext =
        dsfidSerializer.createSerializationContext(dataOutput);
    member.writeEssentialData(dataOutput, serializationContext);

    final var bais = new ByteArrayInputStream(baos.toByteArray());
    final var stream = new DataInputStream(bais);
    final var deserializationContext =
        dsfidSerializer.createDeserializationContext(stream);
    final DataInput dataInput = new DataInputStream(bais);
    final var newMember = new GMSMemberData();
    newMember.readEssentialData(dataInput, deserializationContext);

    validate(newMember);
  }

  private AbstractShortAssert<?> validate(final MemberData memberData) {
    return assertThat(memberData.getVersionOrdinal()).isEqualTo(unknownVersionOrdinal);
  }

}
