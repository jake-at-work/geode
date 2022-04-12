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
package org.apache.geode.internal.cache.tier.sockets;

import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.InterestPolicy;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.SubscriptionAttributes;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

@Category({ClientSubscriptionTest.class})
public class ForceInvalidateEvictionDUnitTest extends JUnit4CacheTestCase {

  private static final long serialVersionUID = -11364213547039967L;

  public ForceInvalidateEvictionDUnitTest() {
    super();
  }

  private void doPropagationTest(VM sourceVM, VM destinationVm, boolean validateCallbacks,
      boolean validateContent) {

    addListener(destinationVm);

    // Put some entries. They will be locally expired
    putEntries(sourceVM, 0, 20);

    // Make sure the local expiration happened
    checkValue(sourceVM, 0, null);

    // Make sure there was no expiration in the destination VM
    if (validateContent) {
      checkValue(destinationVm, 0, "value");
    }

    // do an invalidate
    invalidateEntry(sourceVM, 0);

    // Make sure the invalidate didn't stick around
    checkValue(sourceVM, 0, null);

    // Make sure the destination receives invalidate
    if (validateContent) {
      checkValue(destinationVm, 0, Token.INVALID);
    }

    if (validateCallbacks) {
      checkAndClearListener(destinationVm, 0, true);
    }

    removeListener(destinationVm);
  }

  @Test
  public void testPRToAccessor() {

    var host = Host.getHost(0);
    var vm0 = host.getVM(0);
    var vm1 = host.getVM(1);
    var vm2 = host.getVM(2);

    createPR(vm0);
    createPR(vm1);
    createAccessor(vm2, true);


    // Do the test twice, to make sure both
    // primary and secondary propagate the event
    doPropagationTest(vm0, vm2, true, false);
    doPropagationTest(vm1, vm2, true, false);
  }

  @Test
  public void testBridgeClientWithPR() {

    var host = Host.getHost(0);
    var vm0 = host.getVM(0);
    var vm1 = host.getVM(1);
    var vm3 = host.getVM(3);

    createPR(vm0);
    createPR(vm1);
    var port = addCacheServer(vm1);
    createClient(vm3, port);


    // Do the test twice, to make sure both
    // primary and secondary propagate the event
    doPropagationTest(vm0, vm3, true, true);
    doPropagationTest(vm1, vm3, true, true);
  }

  @Test
  public void testBridgeClientWithAccessorServer() {

    var host = Host.getHost(0);
    var vm0 = host.getVM(0);
    var vm1 = host.getVM(1);
    var vm2 = host.getVM(2);
    var vm3 = host.getVM(3);

    createPR(vm0);
    createPR(vm1);
    createAccessor(vm2, false);
    var port = addCacheServer(vm2);
    createClient(vm3, port);

    doPropagationTest(vm0, vm3, true, true);
  }

  @Test
  public void testBridgeClientWithAccessorSource() {

    var host = Host.getHost(0);
    var vm0 = host.getVM(0);
    var vm1 = host.getVM(1);
    var vm2 = host.getVM(2);
    var vm3 = host.getVM(3);

    createPR(vm0);
    createPR(vm1);
    createAccessor(vm2, false);

    // test an invalidate from the accessor through one of the data stores
    var port1 = addCacheServer(vm0);
    createClient(vm3, port1);
    doPropagationTest(vm2, vm3, true, true);
    vm3.invoke(new SerializableRunnable("close cache") {

      @Override
      public void run() {
        Cache cache = getCache();
        cache.close();
      }
    });

    // test an invalidate from the accessor through the other data store
    var port2 = addCacheServer(vm1);
    createClient(vm3, port2);
    doPropagationTest(vm2, vm3, true, true);
  }


  private void createPR(VM vm) {
    final var name = getUniqueName();
    vm.invoke(new SerializableRunnable() {

      @Override
      public void run() {
        Cache cache = getCache();
        var rf = new RegionFactory();
        rf.setOffHeap(isOffHeapEnabled());
        rf.setDataPolicy(DataPolicy.PARTITION);

        var paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        paf.setTotalNumBuckets(5);
        rf.setPartitionAttributes(paf.create());
        rf.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(1));
        rf.setConcurrencyChecksEnabled(false);
        rf.create(name);
      }
    });
  }

  private void createAccessor(VM vm, final boolean allContent) {
    final var name = getUniqueName();
    vm.invoke(new SerializableRunnable() {

      @Override
      public void run() {
        Cache cache = getCache();
        var rf = new RegionFactory();
        rf.setOffHeap(isOffHeapEnabled());
        rf.setDataPolicy(DataPolicy.PARTITION);

        var paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        paf.setTotalNumBuckets(5);
        paf.setLocalMaxMemory(0);
        rf.setPartitionAttributes(paf.create());
        rf.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(1));
        rf.setConcurrencyChecksEnabled(false);
        if (allContent) {
          // rf.initCacheListeners(new CacheListener [] { new MyListener()});
          rf.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
        }
        rf.create(name);
      }
    });
  }

  private void addListener(VM vm) {
    final var name = getUniqueName();
    vm.invoke(new SerializableRunnable() {

      @Override
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(name);
        var am = region.getAttributesMutator();
        am.initCacheListeners(new CacheListener[] {new MyListener()});
      }
    });
  }

  private void removeListener(VM vm) {
    final var name = getUniqueName();
    vm.invoke(new SerializableRunnable() {

      @Override
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(name);
        var am = region.getAttributesMutator();
        am.initCacheListeners(null);
      }
    });
  }

  private void checkAndClearListener(VM vm, final Serializable key, final boolean invalidated) {
    final var name = getUniqueName();
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(name);
        final var listener = (MyListener) region.getAttributes().getCacheListeners()[0];
        if (invalidated) {
          GeodeAwaitility.await().untilAsserted(new WaitCriterion() {

            @Override
            public String description() {
              return "Didn't receive invalidate after 30 seconds";
            }

            @Override
            public boolean done() {
              return listener.remove(key);
            }

          });
        } else {
          assertFalse(listener.remove(key));
        }
      }
    });
  }


  private void checkValue(VM vm, final Serializable key, final Object expected) {
    final var name = getUniqueName();
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        Cache cache = getCache();
        final var region = (LocalRegion) cache.getRegion(name);

        GeodeAwaitility.await().untilAsserted(new WaitCriterion() {

          @Override
          public boolean done() {
            Object value = null;
            try {
              value = region.getValueInVM(key);
              if (value instanceof CachedDeserializable) {
                value = ((CachedDeserializable) value).getDeserializedForReading();
              }
            } catch (EntryNotFoundException e) {
              // ok
            }
            return expected == null ? value == null : expected.equals(value);
          }

          @Override
          public String description() {
            return "Value did not become " + expected + " after 30s: " + region.getValueInVM(key);
          }
        });

      }
    });
  }

  private void invalidateEntry(VM vm, final Serializable key) {
    final var name = getUniqueName();
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(name);
        region.invalidate(key);
      }
    });
  }

  private void putEntries(VM vm, final int start, final int end) {
    final var name = getUniqueName();
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(name);
        for (var i = start; i < end; i++) {
          region.put(i, "value");
        }
      }
    });
  }

  private void createClient(VM vm, final int port) {
    final var name = getUniqueName();
    final var host = Host.getHost(0);
    vm.invoke(new SerializableRunnable() {

      @Override
      public void run() {
        Cache cache = getCache();

        var pf = PoolManager.createFactory();
        pf.addServer(NetworkUtils.getServerHostName(host), port);
        pf.setSubscriptionEnabled(true);
        pf.create(name);
        var rf = new RegionFactory();
        rf.setOffHeap(isOffHeapEnabled());
        rf.setScope(Scope.LOCAL);
        rf.setPoolName(name);
        var region = rf.create(name);
        region.registerInterest("ALL_KEYS");
      }
    });

  }

  private int addCacheServer(VM vm) {
    final var port = getRandomAvailableTCPPort();
    vm.invoke(new SerializableRunnable("add cache server") {
      @Override
      public void run() {
        Cache cache = getCache();
        var server = cache.addCacheServer();
        server.setNotifyBySubscription(true);
        server.setPort(port);
        try {
          server.start();
        } catch (IOException e) {
          Assert.fail("IO Exception", e);
        }
      }
    });

    return port;
  }

  private static class MyListener<K, V> extends CacheListenerAdapter<K, V> {
    // Used because a CM can't handle nulls.

    private static final Object VALUE = new Object();
    Map invalidates = new ConcurrentHashMap();

    @Override
    public void afterInvalidate(EntryEvent<K, V> event) {
      Object oldValue = event.getOldValue();
      invalidates.put(event.getKey(), VALUE);
    }

    public boolean remove(K key) {
      return invalidates.remove(key) != null;
    }
  }

  public boolean isOffHeapEnabled() {
    return false;
  }
}
