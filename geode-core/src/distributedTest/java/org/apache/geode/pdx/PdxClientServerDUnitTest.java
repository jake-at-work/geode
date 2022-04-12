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
package org.apache.geode.pdx;

import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.PdxSerializerObject;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.pdx.internal.AutoSerializableManager;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.SerializationTest;

@Category({SerializationTest.class})
public class PdxClientServerDUnitTest extends JUnit4CacheTestCase {

  public PdxClientServerDUnitTest() {
    super();
  }

  @Test
  public void testSimplePut() {
    var host = Host.getHost(0);
    var vm0 = host.getVM(0);
    var vm1 = host.getVM(1);
    var vm2 = host.getVM(2);
    var vm3 = host.getVM(3);

    vm0.invoke(() -> createServerRegion(SimpleClass.class));
    var port = createServerAccessor(vm3);
    createClientRegion(vm1, port);
    createClientRegion(vm2, port);

    vm1.invoke(() -> {
      Region r = getRootRegion("testSimplePdx");
      r.put(1, new SimpleClass(57, (byte) 3));
      return null;
    });
    final var checkValue = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region r = getRootRegion("testSimplePdx");
        assertEquals(new SimpleClass(57, (byte) 3), r.get(1));
        return null;
      }
    };
    vm2.invoke(checkValue);
    vm0.invoke(checkValue);
    vm1.invoke(checkValue);

  }

  /**
   * Test what happens to the client type registry if the server is restarted and PDX serialization
   * for a class has changed. This was reported in Pivotal bug #47338
   */
  @Test
  public void testNonPersistentServerRestart() {
    var host = Host.getHost(0);
    var vm0 = host.getVM(0);
    var vm1 = host.getVM(1);
    var vm2 = host.getVM(2);

    final int port = vm0.invoke(() -> createServerRegion(SimpleClass.class));
    createClientRegion(vm1, port, true);

    // Define a PDX type with 2 fields that will be cached on the client
    vm1.invoke(() -> {
      Region r = getRootRegion("testSimplePdx");
      r.put(1, new PdxType2(1, 1));
      r.get(1);
      return null;
    });

    closeCache(vm0);

    // GEODE-1037: make sure the client knows that the server
    // is gone
    vm1.invoke(() -> {
      Region r = getRootRegion("testSimplePdx");
      // make sure the client has reconnected to the server
      // by performing a get() on a key the client cache does
      // not contain
      try {
        r.get(4);
        throw new Error("expected an exception to be thrown");
      } catch (Exception ignored) {
      }
    });

    createServerRegion(vm0, port);
    createClientRegion(vm2, port, true);

    // Now defined a PDX type with only 1 field. This should
    // reuse the same type id because the server was restarted.
    vm2.invoke(() -> {
      Region r = getRootRegion("testSimplePdx");
      r.put(3, new PdxType1(3));
      r.get(3);
      return null;
    });

    // See what happens when vm1 tries to read the type.
    // If it cached the type id it will try to read a PdxType2
    // and fail with a ServerOperationException having a
    // PdxSerializationException "cause"
    vm1.invoke(() -> {
      Region r = getRootRegion("testSimplePdx");

      // now get object 3, which was put in the server by vm2
      var results = (PdxType1) r.get(3);
      assertEquals(3, results.int1);

      // Add another field to make sure the write path doesn't have
      // something cached on vm1.
      r.put(5, new PdxType2(5, 5));
      return null;
    });

    // Make sure vm2 can read what vm1 has written
    vm2.invoke(() -> {
      Region r = getRootRegion("testSimplePdx");
      var results = (PdxType2) r.get(5);
      assertEquals(5, results.int1);
      assertEquals(5, results.int2);
      return null;
    });
  }

  /**
   * Test of bug 47338 - what happens to the client type registry if the server is restarted.
   */
  @Test
  public void testNonPersistentServerRestartAutoSerializer() {
    var host = Host.getHost(0);
    var vm0 = host.getVM(0);
    var vm1 = host.getVM(1);
    var vm2 = host.getVM(2);

    System.setProperty(AutoSerializableManager.NO_HARDCODED_EXCLUDES_PARAM, "true");
    Invoke.invokeInEveryVM(new SerializableRunnable() {
      @Override
      public void run() {
        System.setProperty(AutoSerializableManager.NO_HARDCODED_EXCLUDES_PARAM, "true");
      }
    });
    try {
      var patterns =
          new String[] {"org.apache.geode.pdx.PdxClientServerDUnitTest.AutoPdxType.*"};
      var port = createServerRegion(vm0);
      createClientRegion(vm1, port, true, patterns);

      // Define a PDX type with 2 fields that will be cached on the client
      vm1.invoke(() -> {
        Region r = getRootRegion("testSimplePdx");
        r.put(1, new AutoPdxType2(1, 1));
        r.get(1);
        return null;
      });

      closeCache(vm0);
      createServerRegion(vm0, port);
      createClientRegion(vm2, port, true, patterns);

      // Now defined a PDX type with only 1 field. This should
      // reuse the same type id because the server was restarted.
      vm2.invoke(() -> {
        Region r = getRootRegion("testSimplePdx");
        r.put(3, new AutoPdxType1(3));
        r.get(3);
        return null;
      });

      // See what happens when vm1 tries to read the type.
      // If it cached the type id it will have problems.
      vm1.invoke(() -> {
        Region r = getRootRegion("testSimplePdx");
        try {
          r.get(4);
        } catch (Exception expected) {
          // The client may not have noticed the server go away and come
          // back. Let's trigger the exception so the client will retry.
        }
        var results = (AutoPdxType1) r.get(3);
        assertEquals(3, results.int1);

        // Add another field to make sure the write path doesn't have
        // something cached on vm1.
        r.put(5, new AutoPdxType2(5, 5));
        return null;
      });

      // Make sure vm2 can read what vm1 has written
      vm2.invoke(() -> {
        Region r = getRootRegion("testSimplePdx");
        var results = (AutoPdxType2) r.get(5);
        assertEquals(5, results.int1);
        assertEquals(5, results.int2);
        return null;
      });
    } finally {
      System.setProperty(AutoSerializableManager.NO_HARDCODED_EXCLUDES_PARAM, "false");
      Invoke.invokeInEveryVM(new SerializableRunnable() {
        @Override
        public void run() {
          System.setProperty(AutoSerializableManager.NO_HARDCODED_EXCLUDES_PARAM, "false");
        }
      });
    }
  }

  /**
   * Test that we through an exception if one of the servers has persistent regions but not a
   * persistent registry.
   */
  @Test
  public void testServersWithPersistence() {
    var host = Host.getHost(0);
    var vm0 = host.getVM(0);
    var vm1 = host.getVM(1);
    var vm2 = host.getVM(2);
    var vm3 = host.getVM(3);

    createServerRegionWithPersistence(vm0, false);
    var port = createServerAccessor(vm1);
    createClientRegion(vm2, port);
    createClientRegion(vm3, port);

    var createValue = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region r = getRootRegion("testSimplePdx");
        try {
          r.put(1, new SimpleClass(57, (byte) 3));
          fail("should have received an exception");
        } catch (PdxInitializationException expected) {
          // do nothing
        }
        return null;
      }
    };
  }

  private void closeCache(VM vm) {
    vm.invoke(() -> {
      closeCache();
      return null;
    });
  }

  @Test
  public void testSimplePdxInstancePut() {
    var host = Host.getHost(0);
    var vm0 = host.getVM(0);
    var vm1 = host.getVM(1);
    var vm2 = host.getVM(2);

    int port = vm0.invoke(() -> createServerRegion(SimpleClass.class));
    createClientRegion(vm1, port);
    createClientRegion(vm2, port);

    vm1.invoke(() -> {
      Region r = getRootRegion("testSimplePdx");
      r.put(1, new SimpleClass(57, (byte) 3));
      return null;
    });
    final var checkValue = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region r = getRootRegion("testSimplePdx");
        var previousPdxReadSerializedFlag = cache.getPdxReadSerializedOverride();
        cache.setPdxReadSerializedOverride(true);
        try {
          var v = r.get(1);
          if (!(v instanceof PdxInstance)) {
            fail("expected v " + v.getClass() + " to be a PdxInstance");
          }
          var piv = (PdxInstance) v;
          assertEquals(new SimpleClass(57, (byte) 3), piv.getObject());
          var v2 = r.get(1);
          if (v == v2) {
            fail("expected v and v2 to have a different identity");
          }
          assertEquals(v, v2);
        } finally {
          cache.setPdxReadSerializedOverride(previousPdxReadSerializedFlag);
        }
        return null;
      }
    };
    vm2.invoke(checkValue);

    vm0.invoke(checkValue);

  }

  /**
   * Test to make sure that types are sent to all pools, even if they are in multiple distributed
   * systems.
   */
  @Test
  public void testMultipleServerDSes() throws Exception {
    var host = Host.getHost(0);
    final var vm0 = host.getVM(0);
    var vm1 = host.getVM(1);
    var vm2 = host.getVM(2);
    var vm3 = host.getVM(3);

    final var port1 = createLonerServerRegion(vm0, "region1", "1");
    final var port2 = createLonerServerRegion(vm1, "region2", "2");

    var createRegion = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        var props = new Properties();
        props.setProperty(MCAST_PORT, "0");
        props.setProperty(LOCATORS, "");
        getSystem(props);
        Cache cache = getCache();
        var pf = PoolManager.createFactory();
        pf.addServer(NetworkUtils.getServerHostName(vm0.getHost()), port1);
        pf.create("pool1");

        pf = PoolManager.createFactory();
        pf.addServer(NetworkUtils.getServerHostName(vm0.getHost()), port2);
        pf.create("pool2");

        var af = new AttributesFactory();
        af.setPoolName("pool1");
        cache.createRegion("region1", af.create());

        af = new AttributesFactory();
        af.setPoolName("pool2");
        cache.createRegion("region2", af.create());
        return null;
      }
    };
    vm2.invoke(createRegion);
    vm3.invoke(createRegion);
    createRegion.call();

    // Serialize an object and put it in both regions, sending
    // the event to each pool
    vm2.invoke(() -> {
      var bytes = new HeapDataOutputStream(KnownVersion.CURRENT);
      Region r1 = getRootRegion("region1");
      r1.put(1, new SimpleClass(57, (byte) 3));
      Region r2 = getRootRegion("region2");
      r2.put(1, new SimpleClass(57, (byte) 3));
      return null;
    });

    // Make sure we get deserialize the value in a different client
    vm3.invoke(() -> {
      Region r = getRootRegion("region1");
      assertEquals(new SimpleClass(57, (byte) 3), r.get(1));
      return null;
    });

    // Make sure we can get the entry in the current member
    Region r = getRootRegion("region2");
    assertEquals(new SimpleClass(57, (byte) 3), r.get(1));
  }

  @Test
  public void testUserSerializesObject() {
    var host = Host.getHost(0);
    var vm0 = host.getVM(0);
    var vm1 = host.getVM(1);
    var vm2 = host.getVM(2);

    int port = vm0.invoke(() -> createServerRegion(Object.class));
    createClientRegion(vm1, port);
    createClientRegion(vm2, port);

    vm1.invoke(() -> {
      var out = new HeapDataOutputStream(KnownVersion.CURRENT);
      DataSerializer.writeObject(new SimpleClass(57, (byte) 3), out);
      var bytes = out.toByteArray();
      Region r = getRootRegion("testSimplePdx");
      r.put(1, bytes);
      return null;
    });

    var checkValue = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region r = getRootRegion("testSimplePdx");
        var bytes = (byte[]) r.get(1);
        var bis = new DataInputStream(new ByteArrayInputStream(bytes));
        var result = DataSerializer.readObject(bis);
        assertEquals(new SimpleClass(57, (byte) 3), result);
        return null;
      }
    };
    vm2.invoke(checkValue);

    vm0.invoke(checkValue);
  }

  /**
   * Test that we still use the client type registry, even if pool is created late.
   */
  @Test
  public void testLatePoolCreation() {
    var host = Host.getHost(0);
    final var vm0 = host.getVM(0);
    var vm1 = host.getVM(1);
    var vm2 = host.getVM(2);

    final int port = vm0.invoke(() -> createServerRegion(SimpleClass.class));
    var createRegion = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        var props = new Properties();
        props.setProperty(MCAST_PORT, "0");
        props.setProperty(LOCATORS, "");
        getSystem(props);
        Cache cache = getCache();
        var pf = PoolManager.createFactory();
        pf.addServer(NetworkUtils.getServerHostName(vm0.getHost()), port);
        pf.create("pool");

        var af = new AttributesFactory();
        af.setPoolName("pool");
        cache.createRegion("testSimplePdx", af.create());
        return null;
      }
    };
    vm1.invoke(createRegion);
    vm2.invoke(createRegion);

    vm1.invoke(() -> {
      Region r = getRootRegion("testSimplePdx");
      r.put(1, new SimpleClass(57, (byte) 3));
      return null;
    });
    final var checkValue = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region r = getRootRegion("testSimplePdx");
        assertEquals(new SimpleClass(57, (byte) 3), r.get(1));
        return null;
      }
    };
    vm2.invoke(checkValue);

    vm0.invoke(checkValue);
  }

  /**
   * Test that we throw an exception if someone tries to create a pool after we were forced to use a
   * peer type registry.
   */
  @Test
  public void testExceptionWithPoolAfterTypeRegistryCreation() {
    var host = Host.getHost(0);
    final var vm0 = host.getVM(0);
    var vm1 = host.getVM(1);

    final int port = vm0.invoke(() -> createServerRegion(SimpleClass.class));
    var createRegion = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        var props = new Properties();
        props.setProperty(MCAST_PORT, "0");
        props.setProperty(LOCATORS, "");
        getSystem(props);
        Cache cache = getCache();
        var out = new HeapDataOutputStream(KnownVersion.CURRENT);
        DataSerializer.writeObject(new SimpleClass(57, (byte) 3), out);

        var pf = PoolManager.createFactory();
        pf.addServer(NetworkUtils.getServerHostName(vm0.getHost()), port);
        try {
          pf.create("pool");
          fail("should have received an exception");
        } catch (PdxInitializationException expected) {
          // do nothing
        }
        return null;
      }
    };

    vm1.invoke(createRegion);
  }

  @Test
  public void testCacheRejectsJSonPdxInstanceViolatingValueConstraint() {
    var host = Host.getHost(0);
    var vm0 = host.getVM(0);
    var vm1 = host.getVM(1);

    final int port = vm0.invoke(() -> createServerRegion(SimpleClass.class));

    vm0.invoke("put value in region", () -> {
      Region r = basicGetCache().getRegion("testSimplePdx");
      var className = "objects.PersonWithoutID";
      try {
        r.put("pdxObject", getTypedJSONPdxInstance(className));
        fail("expected a ClassCastException");
      } catch (ClassCastException e) {
        assertTrue("wrong ClassCastException message: " + e.getMessage(),
            e.getMessage().contains(className));
      }
      return null;
    });
  }


  public PdxInstance getTypedJSONPdxInstance(String className) throws Exception {

    var aRESTishDoc = "{\"@type\": \" " + className
        + "\" , \" firstName\" : \" John\" , \" lastName\" : \" Smith\" , \" age\" : 25 }";
    var pdxInstance = JSONFormatter.fromJSON(aRESTishDoc);

    return pdxInstance;
  }

  private int createServerRegion(final Class constraintClass) throws IOException {
    var cf = new CacheFactory(getDistributedSystemProperties());
    Cache cache = getCache(cf);
    RegionFactory rf = cache.createRegionFactory(RegionShortcut.REPLICATE);
    rf.setValueConstraint(constraintClass);
    rf.create("testSimplePdx");
    var server = cache.addCacheServer();
    var port = AvailablePortHelper.getRandomAvailableTCPPort();
    server.setPort(port);
    server.start();
    return port;
  }

  private int createServerRegion(VM vm) {
    var createRegion = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        var af = new AttributesFactory();
        af.setScope(Scope.DISTRIBUTED_ACK);
        af.setDataPolicy(DataPolicy.REPLICATE);
        createRootRegion("testSimplePdx", af.create());
        var server = getCache().addCacheServer();
        var port = AvailablePortHelper.getRandomAvailableTCPPort();
        server.setPort(port);
        server.start();
        return port;
      }
    };

    return (Integer) vm.invoke(createRegion);
  }

  private int createServerRegion(VM vm, final int port) {
    var createRegion = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        var af = new AttributesFactory();
        af.setScope(Scope.DISTRIBUTED_ACK);
        af.setDataPolicy(DataPolicy.REPLICATE);
        createRootRegion("testSimplePdx", af.create());

        var server = getCache().addCacheServer();
        server.setPort(port);
        server.start();
        return port;
      }
    };

    return (Integer) vm.invoke(createRegion);
  }

  private int createServerRegionWithPersistence(VM vm, final boolean persistentPdxRegistry) {
    var createRegion = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        var cf = new CacheFactory();
        if (persistentPdxRegistry) {
          cf.setPdxPersistent(true).setPdxDiskStore("store");
        }
        //
        Cache cache = getCache(cf);
        cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create("store");

        var af = new AttributesFactory();
        af.setScope(Scope.DISTRIBUTED_ACK);
        af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
        af.setDiskStoreName("store");
        createRootRegion("testSimplePdx", af.create());

        var server = getCache().addCacheServer();
        var port = AvailablePortHelper.getRandomAvailableTCPPort();
        server.setPort(port);
        server.start();
        return port;
      }
    };

    return (Integer) vm.invoke(createRegion);
  }

  private int createServerAccessor(VM vm) {
    var createRegion = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        var af = new AttributesFactory();
        af.setScope(Scope.DISTRIBUTED_ACK);
        af.setDataPolicy(DataPolicy.EMPTY);
        createRootRegion("testSimplePdx", af.create());

        var server = getCache().addCacheServer();
        var port = AvailablePortHelper.getRandomAvailableTCPPort();
        server.setPort(port);
        server.start();
        return port;
      }
    };

    return (Integer) vm.invoke(createRegion);
  }

  private int createLonerServerRegion(VM vm, final String regionName, final String dsId) {
    var createRegion = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        var props = new Properties();
        props.setProperty(LOCATORS, "");
        props.setProperty(DISTRIBUTED_SYSTEM_ID, dsId);
        getSystem(props);
        var af = new AttributesFactory();
        af.setScope(Scope.DISTRIBUTED_ACK);
        af.setDataPolicy(DataPolicy.REPLICATE);
        createRootRegion(regionName, af.create());

        var server = getCache().addCacheServer();
        var port = AvailablePortHelper.getRandomAvailableTCPPort();
        server.setPort(port);
        server.start();
        return port;
      }
    };

    return (Integer) vm.invoke(createRegion);
  }

  private void createClientRegion(final VM vm, final int port) {
    createClientRegion(vm, port, false);
  }

  private void createClientRegion(final VM vm, final int port, final boolean setPdxTypeClearProp,
      final String... autoSerializerPatterns) {
    var createRegion = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        if (setPdxTypeClearProp) {
          System.setProperty(PoolImpl.ON_DISCONNECT_CLEAR_PDXTYPEIDS, "true");
        }
        var cf = new ClientCacheFactory();
        cf.addPoolServer(NetworkUtils.getServerHostName(vm.getHost()), port);
        if (autoSerializerPatterns != null && autoSerializerPatterns.length != 0) {
          cf.setPdxSerializer(new ReflectionBasedAutoSerializer(autoSerializerPatterns));
        }
        var cache = getClientCache(cf);
        cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create("testSimplePdx");
        return null;
      }
    };
    vm.invoke(createRegion);
  }

  public static class PdxType1 implements PdxSerializable {
    int int1;

    public PdxType1() {

    }

    public PdxType1(int int1) {
      this.int1 = int1;
    }

    @Override
    public void toData(PdxWriter writer) {
      writer.writeInt("int1", int1);

    }

    @Override
    public void fromData(PdxReader reader) {
      int1 = reader.readInt("int1");

    }
  }

  public static class PdxType2 implements PdxSerializable {
    int int1;
    int int2;


    public PdxType2() {

    }

    public PdxType2(int int1, int int2) {
      super();
      this.int1 = int1;
      this.int2 = int2;
    }

    @Override
    public void toData(PdxWriter writer) {
      writer.writeInt("int1", int1);
      writer.writeInt("int2", int2);
    }

    @Override
    public void fromData(PdxReader reader) {
      int1 = reader.readInt("int1");
      int2 = reader.readInt("int2");
    }
  }

  public static class AutoPdxType1 implements PdxSerializerObject {
    public int int1;

    public AutoPdxType1() {

    }

    public AutoPdxType1(int int1) {
      this.int1 = int1;
    }
  }

  public static class AutoPdxType2 implements PdxSerializerObject {
    public int int1;
    public int int2;


    public AutoPdxType2() {

    }

    public AutoPdxType2(int int1, int int2) {
      this.int1 = int1;
      this.int2 = int2;
    }
  }
}
