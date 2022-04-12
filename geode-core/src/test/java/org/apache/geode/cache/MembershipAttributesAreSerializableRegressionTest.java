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
package org.apache.geode.cache;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.MembershipTest;

/**
 * Tests MembershipAttributes and SubscriptionAttributes to make sure they are Serializable
 */
@Category({MembershipTest.class})
public class MembershipAttributesAreSerializableRegressionTest {

  /**
   * Assert that MembershipAttributes are serializable.
   */
  @Test
  public void testMembershipAttributesAreSerializable() throws Exception {
    var roles = new String[] {"a", "b", "c"};
    var outMA = new MembershipAttributes(roles);

    var baos = new ByteArrayOutputStream(1000);
    try (var oos = new ObjectOutputStream(baos)) {
      oos.writeObject(outMA);
    }

    var data = baos.toByteArray();

    var bais = new ByteArrayInputStream(data);
    try (var ois = new ObjectInputStream(bais)) {
      var inMA = (MembershipAttributes) ois.readObject();
      assertEquals(outMA, inMA);
    }
  }

  /**
   * Assert that SubscriptionAttributes are serializable.
   */
  @Test
  public void testSubscriptionAttributesAreSerializable() throws Exception {
    var outSA = new SubscriptionAttributes();

    var baos = new ByteArrayOutputStream(1000);
    try (var oos = new ObjectOutputStream(baos)) {
      oos.writeObject(outSA);
    }

    var data = baos.toByteArray();

    var bais = new ByteArrayInputStream(data);
    try (var ois = new ObjectInputStream(bais)) {
      var inSA = (SubscriptionAttributes) ois.readObject();
      assertEquals(outSA, inSA);
    }
  }
}
