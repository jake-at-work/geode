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
package org.apache.geode.cache.operations.internal;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.operations.PutOperationContextJUnitTest;
import org.apache.geode.pdx.PdxInstance;

public class GetOperationContextImplJUnitTest {

  @Test
  public void testGetSerializedValue() throws IOException {
    {
      var byteArrayValue = new byte[] {1, 2, 3, 4};
      var poc = new GetOperationContextImpl("key", true);
      poc.setObject(byteArrayValue, false);
      Assert.assertFalse(poc.isObject());
      Assert.assertNull("value is an actual byte array which is not a serialized blob",
          poc.getSerializedValue());
    }

    {
      var poc = new GetOperationContextImpl("key", true);
      poc.setObject(null, true);
      Assert.assertTrue(poc.isObject());
      Assert.assertNull("value is null which is not a serialized blob", poc.getSerializedValue());
    }

    {
      var poc = new GetOperationContextImpl("key", true);
      poc.setObject("value", true);
      Assert.assertTrue(poc.isObject());
      Assert.assertNull("value is a String which is not a serialized blob",
          poc.getSerializedValue());
    }

    {
      var baos = new ByteArrayOutputStream();
      var dos = new DataOutputStream(baos);
      DataSerializer.writeObject("value", dos);
      dos.close();
      var blob = baos.toByteArray();
      var poc = new GetOperationContextImpl("key", true);
      poc.setObject(blob, true);
      Assert.assertTrue(poc.isObject());
      Assert.assertArrayEquals(blob, poc.getSerializedValue());
    }

    {
      // create a loner cache so that pdx serialization will work
      var c = (new CacheFactory()).set(LOCATORS, "").set(MCAST_PORT, "0")
          .setPdxReadSerialized(true).create();
      try {
        var baos = new ByteArrayOutputStream();
        var dos = new DataOutputStream(baos);
        DataSerializer.writeObject(new PutOperationContextJUnitTest.PdxValue("value"), dos);
        dos.close();
        var blob = baos.toByteArray();
        var poc = new GetOperationContextImpl("key", true);
        poc.setObject(blob, true);
        Assert.assertTrue(poc.isObject());
        Assert.assertArrayEquals(blob, poc.getSerializedValue());
      } finally {
        c.close();
      }
    }
  }

  @Test
  public void testGetDeserializedValue() throws IOException {
    {
      var byteArrayValue = new byte[] {1, 2, 3, 4};
      var poc = new GetOperationContextImpl("key", true);
      poc.setObject(byteArrayValue, false);
      Assert.assertFalse(poc.isObject());
      Assert.assertArrayEquals(byteArrayValue, (byte[]) poc.getDeserializedValue());
    }

    {
      var poc = new GetOperationContextImpl("key", true);
      poc.setObject(null, true);
      Assert.assertTrue(poc.isObject());
      Assert.assertEquals(null, poc.getDeserializedValue());
    }

    {
      var poc = new GetOperationContextImpl("key", true);
      poc.setObject("value", true);
      Assert.assertTrue(poc.isObject());
      Assert.assertEquals("value", poc.getDeserializedValue());
    }

    {
      var baos = new ByteArrayOutputStream();
      var dos = new DataOutputStream(baos);
      DataSerializer.writeObject("value", dos);
      dos.close();
      var blob = baos.toByteArray();
      var poc = new GetOperationContextImpl("key", true);
      poc.setObject(blob, true);
      Assert.assertTrue(poc.isObject());
      Assert.assertEquals("value", poc.getDeserializedValue());
    }

    {
      // create a loner cache so that pdx serialization will work
      var c = (new CacheFactory()).set(LOCATORS, "").set(MCAST_PORT, "0")
          .setPdxReadSerialized(true).create();
      try {
        var baos = new ByteArrayOutputStream();
        var dos = new DataOutputStream(baos);
        DataSerializer.writeObject(new PutOperationContextJUnitTest.PdxValue("value"), dos);
        dos.close();
        var blob = baos.toByteArray();
        var poc = new GetOperationContextImpl("key", true);
        poc.setObject(blob, true);
        Assert.assertTrue(poc.isObject());
        var pi = (PdxInstance) poc.getDeserializedValue();
        Assert.assertEquals("value", pi.getField("v"));
      } finally {
        c.close();
      }
    }
  }

  @Test
  public void testGetValue() throws IOException {
    {
      var byteArrayValue = new byte[] {1, 2, 3, 4};
      var poc = new GetOperationContextImpl("key", true);
      poc.setObject(byteArrayValue, false);
      Assert.assertFalse(poc.isObject());
      Assert.assertArrayEquals(byteArrayValue, (byte[]) poc.getValue());
    }

    {
      var poc = new GetOperationContextImpl("key", true);
      poc.setObject(null, true);
      Assert.assertTrue(poc.isObject());
      Assert.assertEquals(null, poc.getValue());
    }

    {
      var poc = new GetOperationContextImpl("key", true);
      poc.setObject("value", true);
      Assert.assertTrue(poc.isObject());
      Assert.assertEquals("value", poc.getValue());
    }

    {
      var baos = new ByteArrayOutputStream();
      var dos = new DataOutputStream(baos);
      DataSerializer.writeObject("value", dos);
      dos.close();
      var blob = baos.toByteArray();
      var poc = new GetOperationContextImpl("key", true);
      poc.setObject(blob, true);
      Assert.assertTrue(poc.isObject());
      Assert.assertArrayEquals(blob, (byte[]) poc.getValue());
    }

    {
      // create a loner cache so that pdx serialization will work
      var c = (new CacheFactory()).set(LOCATORS, "").set(MCAST_PORT, "0")
          .setPdxReadSerialized(true).create();
      try {
        var baos = new ByteArrayOutputStream();
        var dos = new DataOutputStream(baos);
        DataSerializer.writeObject(new PutOperationContextJUnitTest.PdxValue("value"), dos);
        dos.close();
        var blob = baos.toByteArray();
        var poc = new GetOperationContextImpl("key", true);
        poc.setObject(blob, true);
        Assert.assertTrue(poc.isObject());
        Assert.assertArrayEquals(blob, (byte[]) poc.getValue());
      } finally {
        c.close();
      }
    }
  }

  @Test
  public void testGetObject() throws IOException {
    {
      var byteArrayValue = new byte[] {1, 2, 3, 4};
      var poc = new GetOperationContextImpl("key", true);
      poc.setObject(byteArrayValue, false);
      Assert.assertFalse(poc.isObject());
      Assert.assertArrayEquals(byteArrayValue, (byte[]) poc.getObject());
    }

    {
      var poc = new GetOperationContextImpl("key", true);
      poc.setObject(null, true);
      Assert.assertTrue(poc.isObject());
      Assert.assertEquals(null, poc.getObject());
    }

    {
      var poc = new GetOperationContextImpl("key", true);
      poc.setObject("value", true);
      Assert.assertTrue(poc.isObject());
      Assert.assertEquals("value", poc.getObject());
    }

    {
      var baos = new ByteArrayOutputStream();
      var dos = new DataOutputStream(baos);
      DataSerializer.writeObject("value", dos);
      dos.close();
      var blob = baos.toByteArray();
      var poc = new GetOperationContextImpl("key", true);
      poc.setObject(blob, true);
      Assert.assertTrue(poc.isObject());
      Assert.assertNull("value is a serialized blob which is not an Object", poc.getObject());
    }

    {
      // create a loner cache so that pdx serialization will work
      var c = (new CacheFactory()).set(LOCATORS, "").set(MCAST_PORT, "0")
          .setPdxReadSerialized(true).create();
      try {
        var baos = new ByteArrayOutputStream();
        var dos = new DataOutputStream(baos);
        DataSerializer.writeObject(new PutOperationContextJUnitTest.PdxValue("value"), dos);
        dos.close();
        var blob = baos.toByteArray();
        var poc = new GetOperationContextImpl("key", true);
        poc.setObject(blob, true);
        Assert.assertTrue(poc.isObject());
        Assert.assertNull("value is a serialized blob which is not an Object", poc.getObject());
      } finally {
        c.close();
      }
    }
  }
}
