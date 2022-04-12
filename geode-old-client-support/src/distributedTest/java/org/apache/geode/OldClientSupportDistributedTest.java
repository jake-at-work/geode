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
package org.apache.geode;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.tier.sockets.OldClientSupportService;
import org.apache.geode.internal.serialization.ByteArrayDataInput;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.VersionedDataInputStream;
import org.apache.geode.internal.serialization.VersionedDataOutputStream;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.SerializationTest;

import com.gemstone.gemfire.OldClientSupportProvider;

@SuppressWarnings("serial")
@Category({SerializationTest.class})
public class OldClientSupportDistributedTest implements Serializable {

  private static final List<String> allGeodeThrowableClasses =
      singletonList("org.apache.geode.cache.execute.EmptyRegionFunctionException");

  private static final List<String> newArrayClassNames = asList("[Lorg.apache.geode.class1",
      "[[Lorg.apache.geode.class1", "[[[Lorg.apache.geode.class1");

  private static final List<String> oldArrayClassNames =
      asList("[Lcom.gemstone.gemfire.class1", "[[Lcom.gemstone.gemfire.class1",
          "[[[Lcom.gemstone.gemfire.class1");

  private static final List<String> allNonconformingArrayClassNames = asList(
      "[Lmypackage.org.apache.geode.class2", "[[Lmypackage.org.apache.geode.class2");

  private static MemberVM server;

  @ClassRule
  public static ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  @BeforeClass
  public static void setUp() {
    var locator = clusterStartupRule.startLocatorVM(0);
    server =
        clusterStartupRule.startServerVM(1,
            s -> s.withProperty(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
                "org.apache.geode.ClientSerializableObject")
                .withConnectionToLocator(locator.getPort()));
  }

  @Test
  public void cacheInstallsOldClientSupportServiceProvider() {
    server
        .invoke(() -> {
          assertThat((Objects.requireNonNull(ClusterStartupRule.getCache()))
              .getService(OldClientSupportService.class)).isNotNull();
        });
  }

  /**
   * This test can be vastly simplified if clients have the ability to translate org.apache.geode
   * package prefixes to com.gemstone.gemfire. In that case we will only need to test translation of
   * EmtpyRegionFunctionException.
   */
  @Test
  public void testConversionOfThrowablesForOldClients() {
    server.invoke(() -> {
      List<Throwable> problems = new LinkedList<>();

      for (var geodeClassName : allGeodeThrowableClasses) {
        try {
          convertThrowable(geodeClassName);
        } catch (Exception e) {
          System.out.println("-- failed");
          var failure =
              new Exception("Failed processing " + geodeClassName + ": " + e, e);
          problems.add(failure);
        }
      }

      if (!problems.isEmpty()) {
        fail(problems.toString());
      }
    });
  }

  @Test
  public void testConversionOfArrayTypes() {
    server.invoke(() -> {
      var oldClientSupport =
          OldClientSupportProvider.getService(ClusterStartupRule.getCache());

      var oldClientVersion = KnownVersion.GFE_81;
      var dout = new VersionedDataOutputStream(
          new HeapDataOutputStream(10, oldClientVersion), oldClientVersion);

      for (var geodeClassName : newArrayClassNames) {
        var newName = oldClientSupport.processOutgoingClassName(geodeClassName, dout);
        assertThat(geodeClassName).isNotEqualTo(newName);
      }

      for (var className : allNonconformingArrayClassNames) {
        var newName = oldClientSupport.processOutgoingClassName(className, dout);
        assertThat(className).isEqualTo(newName);
      }

      var din = new VersionedDataInputStream(
          new DataInputStream(new ByteArrayInputStream(new byte[10])), oldClientVersion);

      for (var oldClassName : oldArrayClassNames) {
        var newName = oldClientSupport.processIncomingClassName(oldClassName, din);
        assertThat(oldClassName).isNotEqualTo(newName);
      }
    });

  }

  private void convertThrowable(String geodeClassName) throws Exception {
    var oldClientVersion = KnownVersion.GFE_81;
    final var comGemstoneGemFire = "com.gemstone.gemfire";
    final var comGemstoneGemFireLength = comGemstoneGemFire.length();

    var oldClientSupport =
        OldClientSupportProvider.getService(ClusterStartupRule.getCache());

    System.out.println("checking " + geodeClassName);
    var geodeClass = Class.forName(geodeClassName);
    var geodeObject = instantiate(geodeClass);
    if (geodeObject instanceof Throwable) {
      var geodeThrowable = (Throwable) instantiate(geodeClass);
      var gemfireThrowable = oldClientSupport.getThrowable(geodeThrowable, oldClientVersion);
      assertThat(comGemstoneGemFire)
          .withFailMessage("Failed to convert " + geodeClassName + ". Throwable class is "
              + gemfireThrowable.getClass().getName())
          .isEqualTo(gemfireThrowable.getClass().getName().substring(0, comGemstoneGemFireLength));
    }
  }

  private Object instantiate(Class<?> aClass) throws Exception {
    Constructor<?> c;
    try {
      c = aClass.getConstructor();
      return c.newInstance();
    } catch (NoSuchMethodException e1) {
      try {
        c = aClass.getConstructor(String.class);
        return c.newInstance("test instance");
      } catch (NoSuchMethodException e2) {
        try {
          c = aClass.getConstructor(String.class, Throwable.class);
          return c.newInstance("test instance", null);
        } catch (NoSuchMethodException e3) {
          throw new RuntimeException("unable to find an instantiator for " + aClass.getName());
        }
      }
    }
  }

  /**
   * com.gemstone.gemfire objects received from a client should translate to org.apache.geode
   * objects. Here we perform a simple unit test on a com.gemstone.gemfire object to ensure that
   * this is happening correctly for Java-serialized objects
   */
  @Test
  public void oldClientObjectTranslatesToGeodeObject_javaSerialization() {
    server.invoke(() -> {
      var gemfireObject =
          new com.gemstone.gemfire.ClientSerializableObject();
      var subObject =
          new com.gemstone.gemfire.ClientSerializableObject();
      gemfireObject.setSubObject(subObject);

      var byteStream = new ByteArrayOutputStream(500);
      var dataOut = new DataOutputStream(byteStream);
      DataSerializer.writeObject(gemfireObject, dataOut);
      dataOut.flush();
      var serializedForm = byteStream.toByteArray();

      var byteDataInput = new ByteArrayDataInput();
      byteDataInput.initialize(serializedForm, KnownVersion.GFE_81);
      ClientSerializableObject result = DataSerializer.readObject(byteDataInput);
      assertThat(
          result.getClass().getName().substring(0, "org.apache.geode".length()))
              .withFailMessage("Expected an org.apache.geode exception but found " + result)
              .isEqualTo("org.apache.geode");
      var newSubObject = result.getSubObject();
      assertThat(newSubObject).isNotNull();
    });
  }


  /**
   * com.gemstone.gemfire objects received from a client should translate to org.apache.geode
   * objects. Here we perform a simple unit test on a com.gemstone.gemfire object to ensure that
   * this is happening correctly for data-serialized objects
   */
  @Test
  public void oldClientObjectTranslatesToGeodeObject_dataSerialization() {
    server.invoke(() -> {
      var gemfireObject =
          new com.gemstone.gemfire.ClientDataSerializableObject();

      validateObjectTranslation(gemfireObject);
    });
  }

  private void validateObjectTranslation(Object gemfireObject)
      throws IOException, ClassNotFoundException {
    var byteStream = new ByteArrayOutputStream(500);
    var dataOut = new DataOutputStream(byteStream);
    // use an internal API to ensure that java serialization isn't used
    InternalDataSerializer.writeObject(gemfireObject, dataOut, false);
    dataOut.flush();
    var serializedForm = byteStream.toByteArray();

    var byteDataInput = new ByteArrayDataInput();
    byteDataInput.initialize(serializedForm, KnownVersion.GFE_81);
    var result = DataSerializer.readObject(byteDataInput);
    assertThat(
        result.getClass().getName().substring(0, "org.apache.geode".length()))
            .withFailMessage("Expected an org.apache.geode object but found " + result)
            .isEqualTo("org.apache.geode");
  }

  /**
   * com.gemstone.gemfire objects received from a client should translate to org.apache.geode
   * objects. Here we perform a simple unit test on a com.gemstone.gemfire object to ensure that
   * this is happening correctly for PDX-serialized objects
   */
  @Test
  public void oldClientObjectTranslatesToGeodeObject_pdxSerialization() {
    server.invoke(() -> {
      var gemfireObject =
          new com.gemstone.gemfire.ClientPDXSerializableObject();

      validateObjectTranslation(gemfireObject);
    });
  }
}
