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
package org.apache.geode.distributed.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.DataInput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.ByteArrayData;
import org.apache.geode.internal.admin.remote.DistributionLocatorId;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.test.junit.categories.MembershipTest;

/**
 * Tests {@link StartupMessageData}.
 *
 * @since GemFire 7.0
 */
@Category({MembershipTest.class})
public class StartupMessageDataJUnitTest {

  @Test
  public void testWriteHostedLocatorsWithEmpty() throws Exception {
    Collection<String> hostedLocators = new ArrayList<>();
    var data = new StartupMessageData();
    data.writeHostedLocators(hostedLocators);
    assertTrue(data.getOptionalFields().isEmpty());
  }

  @Test
  public void testWriteHostedLocatorsWithNull() throws Exception {
    Collection<String> hostedLocators = null;
    var data = new StartupMessageData();
    data.writeHostedLocators(hostedLocators);
    assertTrue(data.getOptionalFields().isEmpty());
  }

  @Test
  public void testWriteHostedLocatorsWithOne() throws Exception {
    var locatorString = createOneLocatorString();

    List<String> hostedLocators = new ArrayList<>();
    hostedLocators.add(locatorString);

    var data = new StartupMessageData();
    data.writeHostedLocators(hostedLocators);
    assertEquals(1, data.getOptionalFields().size());
    assertEquals(locatorString, data.getOptionalFields().get(StartupMessageData.HOSTED_LOCATORS));
  }

  @Test
  public void testWriteHostedLocatorsWithThree() throws Exception {
    var locatorStrings = createManyLocatorStrings(3);
    List<String> hostedLocators = new ArrayList<>();
    for (var i = 0; i < 3; i++) {
      hostedLocators.add(locatorStrings[i]);
    }

    var data = new StartupMessageData();
    data.writeHostedLocators(hostedLocators);
    assertEquals(1, data.getOptionalFields().size());

    var hostedLocatorsField =
        data.getOptionalFields().getProperty(StartupMessageData.HOSTED_LOCATORS);

    var st =
        new StringTokenizer(hostedLocatorsField, StartupMessageData.COMMA_DELIMITER);
    for (var i = 0; st.hasMoreTokens(); i++) {
      assertEquals(locatorStrings[i], st.nextToken());
    }
  }

  @Test
  public void testReadHostedLocatorsWithThree() throws Exception {
    // set up the data
    var locatorStrings = createManyLocatorStrings(3);
    List<String> hostedLocators = new ArrayList<>();
    for (var i = 0; i < 3; i++) {
      hostedLocators.add(locatorStrings[i]);
    }

    var data = new StartupMessageData();
    data.writeHostedLocators(hostedLocators);
    assertEquals(1, data.getOptionalFields().size());

    // test readHostedLocators
    var i = 0;
    var readLocatorStrings = data.readHostedLocators();
    assertEquals(3, readLocatorStrings.size());
    for (var readLocatorString : readLocatorStrings) {
      assertEquals(locatorStrings[i], readLocatorString);
      i++;
    }
  }

  @Test
  public void testToDataWithEmptyHostedLocators() throws Exception {
    Collection<String> hostedLocators = new ArrayList<>();
    var data = new StartupMessageData();
    data.writeHostedLocators(hostedLocators);

    var testStream = new ByteArrayData();
    assertTrue(testStream.isEmpty());

    var out = testStream.getDataOutput();
    data.writeTo(out);
    assertTrue(testStream.size() > 0);

    var in = testStream.getDataInput();
    Properties props = DataSerializer.readObject(in);
    assertNull(props);
  }

  @Test
  public void testToDataWithNullHostedLocators() throws Exception {
    Collection<String> hostedLocators = null;
    var data = new StartupMessageData();
    data.writeHostedLocators(hostedLocators);

    var testStream = new ByteArrayData();
    assertTrue(testStream.isEmpty());

    var out = testStream.getDataOutput();
    data.writeTo(out);
    assertTrue(testStream.size() > 0);

    var in = testStream.getDataInput();
    Properties props = DataSerializer.readObject(in);
    assertNull(props);
  }

  @Test
  public void testToDataWithOneHostedLocator() throws Exception {
    var locatorString = createOneLocatorString();

    List<String> hostedLocators = new ArrayList<>();
    hostedLocators.add(locatorString);

    var data = new StartupMessageData();
    data.writeHostedLocators(hostedLocators);

    var testStream = new ByteArrayData();
    assertTrue(testStream.isEmpty());

    var out = testStream.getDataOutput();
    data.writeTo(out);
    assertTrue(testStream.size() > 0);

    var in = testStream.getDataInput();
    Properties props = DataSerializer.readObject(in);
    assertNotNull(props);

    var hostedLocatorsString = props.getProperty(StartupMessageData.HOSTED_LOCATORS);
    assertNotNull(hostedLocatorsString);
    assertEquals(locatorString, hostedLocatorsString);
  }

  @Test
  public void testToDataWithThreeHostedLocators() throws Exception {
    var locatorStrings = createManyLocatorStrings(3);
    List<String> hostedLocators = new ArrayList<>();
    for (var i = 0; i < 3; i++) {
      hostedLocators.add(locatorStrings[i]);
    }

    var data = new StartupMessageData();
    data.writeHostedLocators(hostedLocators);

    var testStream = new ByteArrayData();
    assertTrue(testStream.isEmpty());

    var out = testStream.getDataOutput();
    data.writeTo(out);
    assertTrue(testStream.size() > 0);

    var in = testStream.getDataInput();
    Properties props = DataSerializer.readObject(in);
    assertNotNull(props);

    var hostedLocatorsString = props.getProperty(StartupMessageData.HOSTED_LOCATORS);
    assertNotNull(hostedLocatorsString);

    Collection<String> actualLocatorStrings = new ArrayList<>(1);
    var st =
        new StringTokenizer(hostedLocatorsString, StartupMessageData.COMMA_DELIMITER);
    while (st.hasMoreTokens()) {
      actualLocatorStrings.add(st.nextToken());
    }
    assertEquals(3, actualLocatorStrings.size());

    var i = 0;
    for (var actualLocatorString : actualLocatorStrings) {
      assertEquals(locatorStrings[i], actualLocatorString);
      i++;
    }
  }

  @Test
  public void testNullHostedLocator() throws Exception {
    String locatorString = null;
    var in = getDataInputWithOneHostedLocator(locatorString);
    var dataToRead = new StartupMessageData();
    dataToRead.readFrom(in);
    var readHostedLocators = dataToRead.readHostedLocators();
    assertNull(readHostedLocators);
  }

  @Test
  public void testEmptyHostedLocator() throws Exception {
    var locatorString = "";
    var in = getDataInputWithOneHostedLocator(locatorString);
    var dataToRead = new StartupMessageData();
    dataToRead.readFrom(in);
    var readHostedLocators = dataToRead.readHostedLocators();
    assertNull(readHostedLocators);
  }

  @Test
  public void testOneHostedLocator() throws Exception {
    var locatorString = createOneLocatorString();
    var in = getDataInputWithOneHostedLocator(locatorString);
    var dataToRead = new StartupMessageData();
    dataToRead.readFrom(in);
    var readHostedLocators = dataToRead.readHostedLocators();
    assertNotNull(readHostedLocators);
    assertEquals(1, readHostedLocators.size());
    assertEquals(locatorString, readHostedLocators.iterator().next());
  }

  private String createOneLocatorString() throws Exception {
    var locatorId =
        new DistributionLocatorId(LocalHostUtil.getLocalHost(), 44556, "111.222.333.444", null);
    var locatorString = locatorId.marshal();
    assertEquals("" + locatorId.getHost().getAddress().getHostAddress() + ":111.222.333.444[44556]",
        locatorString);
    return locatorString;
  }

  private String[] createManyLocatorStrings(int n) throws Exception {
    var locatorStrings = new String[3];
    for (var i = 0; i < 3; i++) {
      var j = i + 1;
      var k = j + 1;
      var l = k + 1;
      var locatorId = new DistributionLocatorId(LocalHostUtil.getLocalHost(),
          445566, "" + i + "" + i + "" + i + "." + j + "" + j + "" + j + "." + k + "" + k + "" + k
              + "." + l + "" + l + "" + l,
          null);
      locatorStrings[i] = locatorId.marshal();
    }
    return locatorStrings;
  }

  private DataInput getDataInputWithOneHostedLocator(String locatorString) throws Exception {
    List<String> hostedLocators = new ArrayList<>();
    if (locatorString != null) {
      hostedLocators.add(locatorString);
    }

    var dataToWrite = new StartupMessageData();
    dataToWrite.writeHostedLocators(hostedLocators);

    var testStream = new ByteArrayData();
    assertTrue(testStream.isEmpty());

    var out = testStream.getDataOutput();
    dataToWrite.writeTo(out);
    assertTrue(testStream.size() > 0);

    var in = testStream.getDataInput();
    assertNotNull(in);
    return in;
  }
}
