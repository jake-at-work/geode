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
package org.apache.geode.internal.offheap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Random;
import java.util.Stack;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.Instantiator;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.internal.DataSerializableJUnitTest.DataSerializableImpl;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.InternalInstantiator;
import org.apache.geode.internal.admin.remote.ShutdownAllResponse;
import org.apache.geode.internal.cache.execute.data.CustId;
import org.apache.geode.internal.serialization.DSCODE;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.internal.DSFIDSerializerImpl;

/**
 * Tests the DataType support for off-heap MemoryInspector.
 */
public class DataTypeJUnitTest {
  @BeforeClass
  public static void beforeClass() {
    Instantiator.register(new Instantiator(CustId.class, (short) 1) {
      @Override
      public DataSerializable newInstance() {
        return new CustId();
      }
    });
  }

  @AfterClass
  public static void afterClass() {
    InternalInstantiator.unregister(CustId.class, (short) 1);
  }

  @Test
  public void testDataSerializableFixedIDByte() throws IOException {
    DataSerializableFixedID value = new ReplyMessage();
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    InternalDataSerializer.writeDSFID(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals(
        "org.apache.geode.internal.serialization.DataSerializableFixedID:"
            + ReplyMessage.class.getName(),
        type);
  }

  @Test
  public void testDataSerializableFixedIDShort() throws IOException {
    DataSerializableFixedID value = new ShutdownAllResponse();
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    InternalDataSerializer.writeDSFID(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals(
        "org.apache.geode.internal.serialization.DataSerializableFixedID:"
            + ShutdownAllResponse.class
                .getName(),
        type);
  }

  @Test
  public void testDataSerializableFixedIDInt() throws IOException, ClassNotFoundException {
    assertFalse(
        ((DSFIDSerializerImpl) InternalDataSerializer.getDSFIDSerializer())
            .getDsfidmap2()
            .containsKey(DummyIntDataSerializableFixedID.INT_SIZE_id));
    InternalDataSerializer.getDSFIDSerializer().register(
        DummyIntDataSerializableFixedID.INT_SIZE_id,
        DummyIntDataSerializableFixedID.class);
    assertTrue(
        ((DSFIDSerializerImpl) InternalDataSerializer.getDSFIDSerializer())
            .getDsfidmap2()
            .containsKey(DummyIntDataSerializableFixedID.INT_SIZE_id));

    try {
      var dummyObj = new DummyIntDataSerializableFixedID();

      var baos = new ByteArrayOutputStream();
      var out = new DataOutputStream(baos);
      DataSerializer.writeObject(dummyObj, out);
      var bytes = baos.toByteArray();

      var type = DataType.getDataType(bytes);
      assertEquals("org.apache.geode.internal.serialization.DataSerializableFixedID:"
          + DummyIntDataSerializableFixedID.class.getName(),
          type);
    } finally {
      ((DSFIDSerializerImpl) InternalDataSerializer.getDSFIDSerializer())
          .getDsfidmap2()
          .remove(DummyIntDataSerializableFixedID.INT_SIZE_id);
      assertFalse(
          ((DSFIDSerializerImpl) InternalDataSerializer.getDSFIDSerializer())
              .getDsfidmap2()
              .containsKey(DummyIntDataSerializableFixedID.INT_SIZE_id));
    }
  }

  @Test
  public void testDataSerializableFixedIDClass() throws IOException {
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeByte(DSCODE.DS_NO_FIXED_ID.toByte(), out);
    DataSerializer.writeClass(Integer.class, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("org.apache.geode.internal.serialization.DataSerializableFixedID:"
        + Integer.class.getName(),
        type);
  }

  @Test
  public void testNull() throws IOException {
    Object value = null;
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeObject(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("null", type);
  }

  @Test
  public void testString() throws IOException {
    var value = "this is a string";
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeObject(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("java.lang.String", type);
  }

  @Test
  public void testNullString() throws IOException {
    String value = null;
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeString(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("java.lang.String", type);
  }

  @Test
  public void testClass() throws IOException {
    Class<?> value = String.class;
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeObject(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("java.lang.Class", type);
  }

  @Test
  public void testDate() throws IOException {
    var value = new Date();
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeObject(value, out); // NOT writeDate
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("java.util.Date", type);
  }

  @Test
  public void testFile() throws IOException {
    var value = new File("tmp");
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeObject(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("java.io.File", type);
  }

  @Test
  public void testInetAddress() throws IOException {
    var value = InetAddress.getLocalHost();
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeObject(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("java.net.InetAddress", type);
  }

  @Test
  public void testBoolean() throws IOException {
    var value = Boolean.TRUE;
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeObject(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("java.lang.Boolean", type);
  }

  @Test
  public void testCharacter() throws IOException {
    Character value = 'c';
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeObject(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("java.lang.Character", type);
  }

  @Test
  public void testByte() throws IOException {
    Byte value = (byte) 0;
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeObject(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("java.lang.Byte", type);
  }

  @Test
  public void testShort() throws IOException {
    Short value = (short) 1;
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeObject(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("java.lang.Short", type);
  }

  @Test
  public void testInteger() throws IOException {
    Integer value = 1;
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeObject(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("java.lang.Integer", type);
  }

  @Test
  public void testLong() throws IOException {
    Long value = 1L;
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeObject(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("java.lang.Long", type);
  }

  @Test
  public void testFloat() throws IOException {
    Float value = (float) 1.0;
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeObject(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("java.lang.Float", type);
  }

  @Test
  public void testDouble() throws IOException {
    Double value = 1.0;
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeObject(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("java.lang.Double", type);
  }

  @Test
  public void testByteArray() throws IOException {
    var value = new byte[10];
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeObject(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("byte[]", type);
  }

  @Test
  public void testByteArrays() throws IOException {
    var value = new byte[1][1];
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeObject(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("byte[][]", type);
  }

  @Test
  public void testShortArray() throws IOException {
    var value = new short[1];
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeObject(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("short[]", type);
  }

  @Test
  public void testStringArray() throws IOException {
    var value = new String[1];
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeObject(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("java.lang.String[]", type);
  }

  @Test
  public void testIntArray() throws IOException {
    var value = new int[1];
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeObject(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("int[]", type);
  }

  @Test
  public void testFloatArray() throws IOException {
    var value = new float[1];
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeObject(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("float[]", type);
  }

  @Test
  public void testLongArray() throws IOException {
    var value = new long[1];
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeObject(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("long[]", type);
  }

  @Test
  public void testDoubleArray() throws IOException {
    var value = new double[1];
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeObject(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("double[]", type);
  }

  @Test
  public void testBooleanArray() throws IOException {
    var value = new boolean[1];
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeObject(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("boolean[]", type);
  }

  @Test
  public void testCharArray() throws IOException {
    var value = new char[1];
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeObject(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("char[]", type);
  }

  @Test
  public void testObjectArray() throws IOException {
    var value = new Object[1];
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeObject(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("java.lang.Object[]", type);
  }

  @Test
  public void testArrayList() throws IOException {
    var value = new ArrayList<Object>();
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeObject(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("java.util.ArrayList", type);
  }

  @Test
  public void testLinkedList() throws IOException {
    var value = new LinkedList<Object>();
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeObject(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("java.util.LinkedList", type);
  }

  @Test
  public void testHashSet() throws IOException {
    var value = new HashSet<Object>();
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeObject(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("java.util.HashSet", type);
  }

  @Test
  public void testLinkedHashSet() throws IOException {
    var value = new LinkedHashSet<Object>();
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeObject(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("java.util.LinkedHashSet", type);
  }

  @Test
  public void testHashMap() throws IOException {
    var value = new HashMap<Object, Object>();
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeObject(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("java.util.HashMap", type);
  }

  @Test
  public void testIdentityHashMap() throws IOException {
    var value = new IdentityHashMap<Object, Object>();
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeObject(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("java.util.IdentityHashMap", type);
  }

  @Test
  public void testHashtable() throws IOException {
    var value = new Hashtable<Object, Object>();
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeObject(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("java.util.Hashtable", type);
  }

  @Test
  public void testConcurrentHashMap() throws IOException { // java.io.Serializable (broken)
    var value = new ConcurrentHashMap<Object, Object>();
    value.put("key1", "value1");
    value.put("key2", "value2");
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    // DataSerializer.writeConcurrentHashMap(value, out);
    DataSerializer.writeObject(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("java.io.Serializable:java.util.concurrent.ConcurrentHashMap", type);
  }

  @Test
  public void testProperties() throws IOException {
    var value = new Properties();
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeObject(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("java.util.Properties", type);
  }

  @Test
  public void testTimeUnit() throws IOException {
    final var optimizedTimeUnits =
        EnumSet.range(TimeUnit.NANOSECONDS, TimeUnit.SECONDS);
    for (var v : TimeUnit.values()) {
      var baos = new ByteArrayOutputStream();
      var out = new DataOutputStream(baos);
      DataSerializer.writeObject(v, out);
      var bytes = baos.toByteArray();
      var type = DataType.getDataType(bytes); // 4?
      if (optimizedTimeUnits.contains(v)) {
        assertEquals("for enum " + v, "java.util.concurrent.TimeUnit", type);
      } else {
        assertEquals("for enum " + v, "java.lang.Enum:java.util.concurrent.TimeUnit", type);
      }
    }
  }

  @Test
  public void testVector() throws IOException {
    var value = new Vector<Object>();
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeObject(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("java.util.Vector", type);
  }

  @Test
  public void testStack() throws IOException {
    var value = new Stack<Object>();
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeObject(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("java.util.Stack", type);
  }

  @Test
  public void testTreeMap() throws IOException {
    var value = new TreeMap<Object, Object>();
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeObject(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("java.util.TreeMap", type);
  }

  @Test
  public void testTreeSet() throws IOException {
    var value = new TreeSet<Object>();
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeObject(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("java.util.TreeSet", type);
  }

  @Test
  public void testBooleanType() throws IOException {
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    out.writeByte(DSCODE.BOOLEAN_TYPE.toByte());
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("java.lang.Boolean.class", type);
  }

  @Test
  public void testCharacterType() throws IOException {
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    out.writeByte(DSCODE.CHARACTER_TYPE.toByte());
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("java.lang.Character.class", type);
  }

  @Test
  public void testByteType() throws IOException {
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    out.writeByte(DSCODE.BYTE_TYPE.toByte());
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("java.lang.Byte.class", type);
  }

  @Test
  public void testShortType() throws IOException {
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    out.writeByte(DSCODE.SHORT_TYPE.toByte());
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("java.lang.Short.class", type);
  }

  @Test
  public void testIntegerType() throws IOException {
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    out.writeByte(DSCODE.INTEGER_TYPE.toByte());
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("java.lang.Integer.class", type);
  }

  @Test
  public void testLongType() throws IOException {
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    out.writeByte(DSCODE.LONG_TYPE.toByte());
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("java.lang.Long.class", type);
  }

  @Test
  public void testFloatType() throws IOException {
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    out.writeByte(DSCODE.FLOAT_TYPE.toByte());
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("java.lang.Float.class", type);
  }

  @Test
  public void testDoubleType() throws IOException {
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    out.writeByte(DSCODE.DOUBLE_TYPE.toByte());
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("java.lang.Double.class", type);
  }

  @Test
  public void testVoidType() throws IOException {
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    out.writeByte(DSCODE.VOID_TYPE.toByte());
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("java.lang.Void.class", type);
  }

  // TODO: these tests have to corrected once USER_CLASS, USER_CLASS_2, USER_CLASS_4 are
  // implemented.
  @Test
  public void getDataTypeShouldReturnUserClass() throws IOException {
    byte someUserClassId = 1;
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    out.writeByte(DSCODE.USER_CLASS.toByte());
    out.writeByte(someUserClassId);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertThat(type).isEqualTo("DataSerializer: with Id:" + someUserClassId);
  }

  @Test
  public void getDataTypeShouldReturnUserClass2() throws IOException {
    short someUserClass2Id = 1;
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    out.writeByte(DSCODE.USER_CLASS_2.toByte());
    out.writeShort(someUserClass2Id);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertThat(type).isEqualTo("DataSerializer: with Id:" + someUserClass2Id);
  }

  @Test
  public void getDataTypeShouldReturnUserClass4() throws IOException {
    var someUserClass4Id = 1;
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    out.writeByte(DSCODE.USER_CLASS_4.toByte());
    out.writeInt(someUserClass4Id);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertThat(type).isEqualTo("DataSerializer: with Id:" + someUserClass4Id);
  }

  @Test
  public void getDataTypeShouldReturnUserDataSeriazliable() throws IOException {
    var someClassId = 1;

    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    out.writeByte(DSCODE.USER_DATA_SERIALIZABLE.toByte());
    out.writeByte(someClassId);

    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);

    assertThat(type).isEqualTo(
        "org.apache.geode.Instantiator:org.apache.geode.internal.cache.execute.data.CustId");
  }

  @Test
  public void getDataTypeShouldReturnUserDataSeriazliable2() throws IOException {
    short someClassId = 1;

    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    out.writeByte(DSCODE.USER_DATA_SERIALIZABLE_2.toByte());
    out.writeShort(someClassId);

    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);

    assertThat(type).isEqualTo(
        "org.apache.geode.Instantiator:org.apache.geode.internal.cache.execute.data.CustId");
  }

  @Test
  public void getDataTypeShouldReturnUserDataSeriazliable4() throws IOException {
    var someClassId = 1;

    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    out.writeByte(DSCODE.USER_DATA_SERIALIZABLE_4.toByte());
    out.writeInt(someClassId);

    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);

    assertThat(type).isEqualTo(
        "org.apache.geode.Instantiator:org.apache.geode.internal.cache.execute.data.CustId");
  }

  @Test
  public void testDataSerializable() throws IOException {
    var value = new DataSerializableImpl(new Random());
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeObject(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("org.apache.geode.DataSerializable:" + DataSerializableImpl.class.getName(), type);
  }

  @Test
  public void testSerializable() throws IOException {
    var value = new SerializableClass();
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeObject(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("java.io.Serializable:" + SerializableClass.class.getName(), type);
  }

  @SuppressWarnings("serial")
  public static class SerializableClass implements Serializable {
  }

  @Test
  public void getDataTypeShouldReturnPDXType() throws IOException {
    var somePdxTypeInt = 1;
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    out.writeByte(DSCODE.PDX.toByte());
    out.writeInt(somePdxTypeInt);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);

    assertThat(type).isEqualTo("pdxType:1");
  }

  @Test
  public void getDataTypeShouldReturnPDXEnumType() throws IOException {
    var somePdxEnumId = 1;
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    InternalDataSerializer.writePdxEnumId(somePdxEnumId, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);

    assertThat(type).isEqualTo("pdxEnum:1");
  }

  @Test
  public void getDataTypeShouldReturnGemfireEnum() throws IOException {
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    out.writeByte(DSCODE.GEMFIRE_ENUM.toByte());
    DataSerializer.writeString(DSCODE.GEMFIRE_ENUM.name(), out);

    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);

    assertThat(type).isEqualTo("java.lang.Enum:GEMFIRE_ENUM");
  }

  @Test
  public void getDataTypeShouldReturnPdxInlineEnum() throws IOException {
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    out.writeByte(DSCODE.PDX_INLINE_ENUM.toByte());
    DataSerializer.writeString(DSCODE.PDX_INLINE_ENUM.name(), out);

    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);

    assertThat(type).isEqualTo("java.lang.Enum:PDX_INLINE_ENUM");
  }

  @Test
  public void testBigInteger() throws IOException {
    var value = BigInteger.ZERO;
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeObject(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("java.math.BigInteger", type);
  }

  @Test
  public void testBigDecimal() throws IOException {
    var value = BigDecimal.ZERO;
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeObject(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("java.math.BigDecimal", type);
  }

  @Test
  public void testUUID() throws IOException {
    var value = new UUID(Long.MAX_VALUE, Long.MIN_VALUE);
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    DataSerializer.writeObject(value, out);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("java.util.UUID", type);
  }

  @Test
  public void testSQLTimestamp() throws IOException {
    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    out.writeByte(DSCODE.TIMESTAMP.toByte());
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertEquals("java.sql.Timestamp", type);
  }

  @Test
  public void testUnknownHeaderType() throws IOException {
    byte unknownType = 0;

    var baos = new ByteArrayOutputStream();
    var out = new DataOutputStream(baos);
    out.writeByte(unknownType);
    var bytes = baos.toByteArray();
    var type = DataType.getDataType(bytes);
    assertThat(type).isEqualTo("Unknown header byte: " + unknownType);
  }

  public static class DummyIntDataSerializableFixedID implements DataSerializableFixedID {
    public DummyIntDataSerializableFixedID() {}

    static final int INT_SIZE_id = 66000;

    @Override
    public int getDSFID() {
      return INT_SIZE_id;
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {

    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {

    }

    @Override
    public KnownVersion[] getSerializationVersions() {
      return null;
    }
  }
}
