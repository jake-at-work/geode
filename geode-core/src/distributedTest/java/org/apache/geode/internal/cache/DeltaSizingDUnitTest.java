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
package org.apache.geode.internal.cache;

import static org.junit.Assert.assertEquals;

import java.io.Serializable;

import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;


public class DeltaSizingDUnitTest extends JUnit4CacheTestCase {

  public DeltaSizingDUnitTest() {
    super();
  }

  @Test
  public void testPeerWithoutCloning() throws Exception {
    doPeerTest(false, false);
  }

  @Test
  public void testPeerWithCloning() throws Exception {
    doPeerTest(true, false);
  }

  @Test
  public void testPeerWithCopyOnRead() throws Exception {
    doPeerTest(false, true);
  }

  @Test
  public void testPeerWithCopyOnAndClone() throws Exception {
    doPeerTest(true, true);
  }

  @Test
  public void testClientWithoutCloning() throws Exception {
    doClientTest(false, false);
  }

  @Test
  public void testClientWithCloning() throws Exception {
    doClientTest(true, false);
  }

  @Test
  public void testClientWithCopyOnRead() throws Exception {
    doClientTest(false, true);
  }

  @Test
  public void testClientWithCopyOnAndClone() throws Exception {
    doClientTest(true, true);
  }


  private void doPeerTest(final boolean clone, final boolean copyOnRead) throws Exception {
    var factory = (AccessorFactory) (host, cache, port1, port2) -> {
      var attr = new AttributesFactory<Integer, TestDelta>();
      attr.setCloningEnabled(clone);
      var paf =
          new PartitionAttributesFactory<Integer, TestDelta>();
      paf.setRedundantCopies(1);
      paf.setLocalMaxMemory(0);
      var prAttr = paf.create();
      attr.setPartitionAttributes(prAttr);
      attr.setDataPolicy(DataPolicy.PARTITION);
      var region = cache.createRegion("region1", attr.create());
      return region;
    };

    doTest(factory, clone, copyOnRead);
  }

  private void doClientTest(final boolean clone, final boolean copyOnRead) throws Exception {
    var factory = (AccessorFactory) (host, cache, port1, port2) -> {
      var attr = new AttributesFactory<Integer, TestDelta>();
      var pf = PoolManager.createFactory();
      pf.addServer(NetworkUtils.getServerHostName(host), port1);
      pf.addServer(NetworkUtils.getServerHostName(host), port2);
      pf.create("pool");
      attr.setCloningEnabled(clone);
      attr.setDataPolicy(DataPolicy.EMPTY);
      attr.setScope(Scope.LOCAL);
      attr.setPoolName("pool");
      var region = cache.createRegion("region1", attr.create());
      return region;
    };

    doTest(factory, clone, copyOnRead);
  }

  public void doTest(final AccessorFactory accessorFactory, final boolean clone,
      final boolean copyOnRead) throws InterruptedException {
    final var host = Host.getHost(0);
    var vm0 = host.getVM(0);
    var vm1 = host.getVM(1);
    var vm2 = host.getVM(2);

    var createDataRegion = new SerializableCallable("createRegion") {
      @Override
      public Object call() throws Exception {
        Cache cache = getCache();
        cache.setCopyOnRead(copyOnRead);
        var attr = new AttributesFactory();
        var paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        var prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        attr.setCloningEnabled(clone);
        // attr.setCacheWriter(new CacheWriterAdapter() {
        //
        // @Override
        // public void beforeCreate(EntryEvent event)
        // throws CacheWriterException {
        // assertTrue(event.getOldValue() == null);
        // assertTrue(event.getNewValue() instanceof MyClass);
        // }
        //
        // @Override
        // public void beforeUpdate(EntryEvent event)
        // throws CacheWriterException {
        // assertTrue(event.getOldValue() instanceof MyClass);
        // assertTrue(event.getNewValue() instanceof MyClass);
        // assertIndexDetailsEquals(event.getOldValue(), event.getNewValue());
        // }
        //
        // });
        cache.createRegion("region1", attr.create());
        var server = cache.addCacheServer();
        var port = AvailablePortHelper.getRandomAvailableTCPPort();
        server.setPort(port);
        server.start();
        return port;
      }
    };

    final var port1 = (Integer) vm0.invoke(createDataRegion);
    final var port2 = (Integer) vm1.invoke(createDataRegion);

    var createEmptyRegion = new SerializableRunnable("createRegion") {
      @Override
      public void run() {
        Cache cache = getCache();
        cache.setCopyOnRead(copyOnRead);
        var region =
            accessorFactory.createRegion(host, cache, port1, port2);
        // This call just creates a bucket. We do an extra serialization on entries that trigger
        // bucket creation. Thats a bug that should get fixed, but for now it's throwing off my
        // assertions. So I'll force the creation of the bucket
        region.put(113, new TestDelta(false, "bogus"));

        // Now put an entry in that we will modify
        region.put(0, new TestDelta(false, "initial"));
      }
    };

    vm2.invoke(createEmptyRegion);

    var clones = 0;
    // Get the object size in both VMS
    var size = checkObjects(vm0, 1, 1, 0, clones);
    assertEquals(size, checkObjects(vm1, 1, 1, 0, clones));

    // Now apply a delta
    vm2.invoke(new SerializableRunnable("update") {
      @Override
      public void run() {
        Cache cache = getCache();
        Region<Object, TestDelta> region = cache.getRegion("region1");
        region.put(0, new TestDelta(true, "changedAAAAAAAA"));
      }
    });

    clones = 0;
    if (copyOnRead) {
      // 1 clone to read the object when we test it (the object should be in deserialized form)
      clones += 1;
    } else if (clone) {
      // 1 clone copy the object when we modify it (the object should be in serialized form)
      clones += 1;
    }

    // Check to make sure the size hasn't changed
    assertEquals(size, checkObjects(vm0, 1, 1, 1, clones));
    assertEquals(size, checkObjects(vm1, 1, 1, 1, clones));

    // Try another
    vm2.invoke(new SerializableRunnable("update") {
      @Override
      public void run() {
        Cache cache = getCache();
        Region<Object, TestDelta> region = cache.getRegion("region1");
        region.put(0, new TestDelta(true, "changedBBBBBBB"));
      }
    });

    if (clone || copyOnRead) {
      // 1 clone to copy the object when we apply the delta.
      clones += 1;
    }
    if (copyOnRead) {
      // 1 clone to read the object when we test it
      clones += 1;
    }

    // Check to make sure the size hasn't changed
    assertEquals(size, checkObjects(vm0, 1, 1, 2, clones));
    assertEquals(size, checkObjects(vm1, 1, 1, 2, clones));
  }

  private long checkObjects(VM vm, final int serializations, final int deserializations,
      final int deltas, final int clones) {
    var getSize = new SerializableCallable("check objects") {
      @Override
      public Object call() {
        var cache = (GemFireCacheImpl) getCache();
        var region = (PartitionedRegion) cache.getRegion("region1");
        var size = region.getDataStore().getBucketSize(0);
        var value = (TestDelta) region.get(0);
        value.checkFields(serializations, deserializations, deltas, clones);
        return size;
      }
    };
    var size = vm.invoke(getSize);
    return (Long) size;
  }

  private interface AccessorFactory extends Serializable {
    Region<Integer, TestDelta> createRegion(Host host, Cache cache, int port1, int port2);
  }

}
