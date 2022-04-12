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
package org.apache.geode.internal;

import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.START_LOCATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;
import org.apache.geode.pdx.internal.PdxUnreadData;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.SerializationTest;

@Category({SerializationTest.class})
public class PdxDeleteFieldDUnitTest extends JUnit4CacheTestCase {

  final List<String> filesToBeDeleted = new CopyOnWriteArrayList<>();

  @Test
  public void testPdxDeleteFieldVersioning() throws Exception {
    final var DS_NAME = "PdxDeleteFieldDUnitTestDiskStore";
    final var DS_NAME2 = "PdxDeleteFieldDUnitTestDiskStore2";

    final var props = new Properties();
    final var locatorPorts = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS,
        "localhost[" + locatorPorts[0] + "],localhost[" + locatorPorts[1] + "]");
    props.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");

    final var f = new File(DS_NAME);
    f.mkdir();
    final var f2 = new File(DS_NAME2);
    f2.mkdir();
    filesToBeDeleted.add(DS_NAME);
    filesToBeDeleted.add(DS_NAME2);

    var host = Host.getHost(0);
    var vm1 = host.getVM(0);
    var vm2 = host.getVM(1);

    vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        disconnectFromDS();
        props.setProperty(START_LOCATOR, "localhost[" + locatorPorts[0] + "]");
        final var cache =
            (new CacheFactory(props)).setPdxPersistent(true).setPdxDiskStore(DS_NAME).create();
        var dsf = cache.createDiskStoreFactory();
        dsf.setDiskDirs(new File[] {f});
        dsf.create(DS_NAME);
        RegionFactory<String, PdxValue> rf1 =
            cache.createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT);
        rf1.setDiskStoreName(DS_NAME);
        var region1 = rf1.create("region1");
        region1.put("key1", new PdxValue(1, 2L));
        return null;
      }
    });

    vm2.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        disconnectFromDS();
        props.setProperty(START_LOCATOR, "localhost[" + locatorPorts[1] + "]");
        final var cache = (new CacheFactory(props)).setPdxReadSerialized(true)
            .setPdxPersistent(true).setPdxDiskStore(DS_NAME2).create();
        var dsf = cache.createDiskStoreFactory();
        dsf.setDiskDirs(new File[] {f2});
        dsf.create(DS_NAME2);
        RegionFactory rf1 = cache.createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT);
        rf1.setDiskStoreName(DS_NAME2);
        var region1 = rf1.create("region1");
        var v = region1.get("key1");
        assertNotNull(v);
        cache.close();
        return null;
      }
    });

    vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        var cache = CacheFactory.getAnyInstance();
        if (cache != null && !cache.isClosed()) {
          cache.close();
        }
        return null;
      }
    });

    vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        var types = DiskStoreImpl.pdxDeleteField(DS_NAME, new File[] {f},
            PdxValue.class.getName(), "fieldToDelete");
        assertEquals(1, types.size());
        var pt = types.iterator().next();
        assertEquals(PdxValue.class.getName(), pt.getClassName());
        assertEquals(null, pt.getPdxField("fieldToDelete"));
        return null;
      }
    });

    vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        props.setProperty(START_LOCATOR, "localhost[" + locatorPorts[0] + "]");
        final var cache =
            (new CacheFactory(props)).setPdxPersistent(true).setPdxDiskStore(DS_NAME).create();
        var dsf = cache.createDiskStoreFactory();
        dsf.setDiskDirs(new File[] {f});
        dsf.create(DS_NAME);
        RegionFactory<String, PdxValue> rf1 =
            cache.createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT);
        rf1.setDiskStoreName(DS_NAME);
        var region1 = rf1.create("region1");
        return null;
      }
    });

    vm2.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        props.setProperty(START_LOCATOR, "localhost[" + locatorPorts[1] + "]");
        final var cache = (new CacheFactory(props)).setPdxReadSerialized(true)
            .setPdxPersistent(true).setPdxDiskStore(DS_NAME2).create();

        var dsf = cache.createDiskStoreFactory();
        dsf.setDiskDirs(new File[] {f2});
        dsf.create(DS_NAME2);
        RegionFactory rf1 = cache.createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT);
        rf1.setDiskStoreName(DS_NAME2);
        var region1 = rf1.create("region1");
        var v = (PdxInstance) region1.get("key1");
        assertNotNull(v);
        assertEquals(1, v.getField("value"));
        assertEquals(null, v.getField("fieldToDelete"));
        cache.close();
        return null;
      }
    });

    vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        var cache = CacheFactory.getAnyInstance();
        if (cache != null && !cache.isClosed()) {
          cache.close();
        }
        return null;
      }
    });
  }

  @Override
  public void preTearDownCacheTestCase() throws Exception {
    for (var path : filesToBeDeleted) {
      try {
        Files.delete(new File(path).toPath());
      } catch (IOException e) {
        LogWriterUtils.getLogWriter().error("Unable to delete file", e);
      }
    }
    filesToBeDeleted.clear();
  }

  public static class PdxValue implements PdxSerializable {

    public int value;
    public long fieldToDelete = -1L;

    public PdxValue() {} // for deserialization

    public PdxValue(int v, long lv) {
      value = v;
      fieldToDelete = lv;
    }

    @Override
    public void toData(PdxWriter writer) {
      writer.writeInt("value", value);
      writer.writeLong("fieldToDelete", fieldToDelete);
    }

    @Override
    public void fromData(PdxReader reader) {
      value = reader.readInt("value");
      if (reader.hasField("fieldToDelete")) {
        fieldToDelete = reader.readLong("fieldToDelete");
      } else {
        fieldToDelete = 0L;
        var unread = (PdxUnreadData) reader.readUnreadFields();
        assertEquals(true, unread.isEmpty());
      }
    }
  }
}
