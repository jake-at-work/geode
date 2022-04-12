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
package org.apache.geode.pdx.internal;

import static org.apache.geode.internal.serialization.DataSerializableFixedID.ENUM_INFO;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.junit.Test;

import org.apache.geode.internal.InternalDataSerializer;


public class EnumInfoTest {
  enum TestEnum {
    ZERO(0),
    ONE(1),
    TWO(2),
    THREE(3),
    FOUR(4);

    private final int intValue;

    TestEnum(int intValue) {
      this.intValue = intValue;
    }

    public int intValue() {
      return intValue;
    }
  }

  @Test
  public void testNoArgConstructor() {
    final var enumInfo = new EnumInfo();
    assertNull(enumInfo.getClassName());
    assertEquals(0, enumInfo.getOrdinal());
  }

  @Test
  public void testThreeArgConstructor() {
    final var enumInfo = new EnumInfo("clazz", "name", 37);
    assertEquals("clazz", enumInfo.getClassName());
    assertEquals(37, enumInfo.getOrdinal());
  }

  @Test
  public void testOneArgConstructor() {
    final var enumInfo = new EnumInfo(TestEnum.ONE);
    assertEquals("org.apache.geode.pdx.internal.EnumInfoTest$TestEnum", enumInfo.getClassName());
    assertEquals(1, enumInfo.getOrdinal());
  }

  @Test
  public void testGetDSFID() {
    final var enumInfo = new EnumInfo(TestEnum.ONE);
    assertEquals(ENUM_INFO, enumInfo.getDSFID());
  }

  @Test
  public void testGetSerializationVersions() {
    final var enumInfo = new EnumInfo(TestEnum.ONE);
    assertNull(enumInfo.getSerializationVersions());
  }

  @Test
  public void testHashCode() {
    final var enumInfo = new EnumInfo(TestEnum.ONE);
    assertEquals(enumInfo.hashCode(), enumInfo.hashCode());

    final var sameClazzAndSameName =
        new EnumInfo("org.apache.geode.pdx.internal.EnumInfoTest$TestEnum", "ONE", 1);
    assertEquals(enumInfo.hashCode(), sameClazzAndSameName.hashCode());

    final var differentClazzAndSameName =
        new EnumInfo("Not " + "org.apache.geode.pdx.internal.EnumInfoTest$TestEnum", "ONE", 1);
    assertNotEquals(enumInfo.hashCode(), differentClazzAndSameName.hashCode());

    final var sameClazzAndDifferentName =
        new EnumInfo("org.apache.geode.pdx.internal.EnumInfoTest$TestEnum", "Not " + "ONE", 1);
    assertNotEquals(enumInfo.hashCode(), sameClazzAndDifferentName.hashCode());
  }

  @Test
  public void testEquals() {
    final var enumInfo = new EnumInfo(TestEnum.ONE);
    assertTrue(enumInfo.equals(enumInfo));
    assertFalse(enumInfo.equals(null));
    assertFalse(enumInfo.equals(new Object()));

    final var sameClazzSameNameAndSameOrdinal =
        new EnumInfo("org.apache.geode.pdx.internal.EnumInfoTest$TestEnum", "ONE", 1);
    assertTrue(enumInfo.equals(sameClazzSameNameAndSameOrdinal));

    final var differentClazzSameNameAndSameOrdinal =
        new EnumInfo("Not " + "org.apache.geode.pdx.internal.EnumInfoTest$TestEnum", "ONE", 1);
    assertFalse(enumInfo.equals(differentClazzSameNameAndSameOrdinal));

    final var sameClazzDifferentNameAndSameOrdinal =
        new EnumInfo("org.apache.geode.pdx.internal.EnumInfoTest$TestEnum", "Not " + "ONE", 1);
    assertFalse(enumInfo.equals(sameClazzDifferentNameAndSameOrdinal));

    final var sameClazzDifferentNameAndDifferentOrdinal =
        new EnumInfo("org.apache.geode.pdx.internal.EnumInfoTest$TestEnum", "Not " + "ONE", 1 + 1);
    assertFalse(enumInfo.equals(sameClazzDifferentNameAndDifferentOrdinal));
  }

  @Test
  public void testToFormattedString() {
    final var enumInfo = new EnumInfo(TestEnum.ONE);
    final var str = enumInfo.toFormattedString();
    assertEquals(0, str.indexOf("EnumInfo"));
    assertNotEquals(-1, str.indexOf("org.apache.geode.pdx.internal.EnumInfoTest$TestEnum"));
    assertNotEquals(-1, str.indexOf("ONE"));
  }

  @Test
  public void testToString() {
    final var enumInfo = new EnumInfo(TestEnum.ONE);
    final var str = enumInfo.toString();
    assertNotEquals(-1, str.indexOf("org.apache.geode.pdx.internal.EnumInfoTest$TestEnum"));
    assertNotEquals(-1, str.indexOf("ONE"));
  }

  @Test
  public void testToStream() {
    final var enumInfo = new EnumInfo(TestEnum.ONE);
    var byteArrayOutputStream = new ByteArrayOutputStream();
    enumInfo.toStream(new PrintStream(byteArrayOutputStream));
    final var str = byteArrayOutputStream.toString();
    assertNotEquals(-1, str.indexOf("org.apache.geode.pdx.internal.EnumInfoTest$TestEnum"));
    assertNotEquals(-1, str.indexOf("ONE"));
  }

  @Test
  public void testToDataAndFromData() throws IOException, ClassNotFoundException {
    final var before = new EnumInfo(TestEnum.ONE);
    var byteArrayOutputStream = new ByteArrayOutputStream(1024);
    var dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    before.toData(dataOutputStream,
        InternalDataSerializer.createSerializationContext(dataOutputStream));
    dataOutputStream.close();

    final var after = new EnumInfo();
    var byteArrayInputStream =
        new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
    var dataInputStream = new DataInputStream(byteArrayInputStream);
    after.fromData(dataInputStream,
        InternalDataSerializer.createDeserializationContext(dataInputStream));

    assertEquals(before.getClassName(), after.getClassName());
    assertEquals(before.getOrdinal(), after.getOrdinal());
  }
}
