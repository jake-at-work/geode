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
package org.apache.geode.cache30;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.dunit.VM;

/**
 * This class tests the functionality of a cache {@link Region region} that has a scope of
 * {@link Scope#GLOBAL global}.
 *
 * @since GemFire 3.0
 */

public class GlobalRegionDUnitTest extends MultiVMRegionTestCase {


  /**
   * Returns region attributes for a <code>GLOBAL</code> region
   */
  @Override
  protected <K, V> RegionAttributes<K, V> getRegionAttributes() {
    var factory = new AttributesFactory<K, V>();
    factory.setScope(Scope.GLOBAL);
    factory.setConcurrencyChecksEnabled(false);
    factory.setDataPolicy(DataPolicy.PRELOADED);
    return factory.create();
  }

  ////////////////////// Test Methods //////////////////////

  /**
   * Tests the compatibility of creating certain kinds of subregions of a local region.
   *
   * @see RegionFactory#createSubregion
   */
  @Test
  public void testIncompatibleSubregions() throws CacheException {
    // Scope.DISTRIBUTED_NO_ACK is illegal if there is any other cache
    // in the distributed system that has the same region with
    // Scope.GLOBAL

    final var name = getUniqueName() + "-GLOBAL";
    vm0.invoke("Create GLOBAL Region", () -> {
      createRegion(name, "INCOMPATIBLE_ROOT", getRegionAttributes());
      assertThat(getRootRegion("INCOMPATIBLE_ROOT").getAttributes().getScope().isGlobal())
          .isTrue();
    });

    vm1.invoke("Create NO ACK Region", () -> {
      var factory =
          getCache().createRegionFactory(getRegionAttributes());
      factory.setScope(Scope.DISTRIBUTED_NO_ACK);
      try {
        assertThat(getRootRegion("INCOMPATIBLE_ROOT")).isNull();
        createRegion(name, "INCOMPATIBLE_ROOT", factory);

        fail("Should have thrown an IllegalStateException");
      } catch (IllegalStateException ignored) {
        // pass...
      }
    });

    vm1.invoke("Create ACK Region", () -> {
      var factory =
          getCache().createRegionFactory(getRegionAttributes());
      factory.setScope(Scope.DISTRIBUTED_ACK);
      try {
        var rootRegion = factory.create("INCOMPATIBLE_ROOT");
        fail("Should have thrown an IllegalStateException");
      } catch (IllegalStateException ex) {
        // pass...
        assertThat(getRootRegion()).isNull();
      }
    });
  }

  /**
   * Tests that a value in a remote cache will be fetched by <code>netSearch</code> and that no
   * loaders are invoked.
   */
  @Test
  public void testRemoteFetch() throws CacheException {
    assertThat(getRegionAttributes().getScope().isDistributed()).isTrue();

    final var name = getUniqueName();
    final Object key = "KEY";
    final Object value = "VALUE";

    SerializableRunnable create = new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        var region = createRegion(name);
        setLoader(new TestCacheLoader<Object, Object>() {
          @Override
          public Object load2(LoaderHelper<Object, Object> helper) throws CacheLoaderException {

            fail("Should not be invoked");
            return null;
          }
        });
        region.getAttributesMutator().setCacheLoader(loader());
      }
    };

    vm0.invoke("Create Region", create);
    vm0.invoke("Put", () -> {
      var region = getRootRegion().getSubregion(name);
      region.put(key, value);
      assertThat(loader().wasInvoked()).isFalse();
    });

    vm1.invoke("Create Region", create);

    vm1.invoke("Get", () -> {
      var region = getRootRegion().getSubregion(name);
      assertThat(value).isEqualTo(region.get(key));
      assertThat(loader().wasInvoked()).isFalse();
    });
  }

  /**
   * Tests that a bunch of threads in a bunch of VMs all atomically incrementing the value of an
   * entry get the right value.
   */
  @Test
  public void testSynchronousIncrements() throws InterruptedException {

    final var name = getUniqueName();
    final Object key = "KEY";

    final var vmCount = VM.getVMCount();
    final var threadsPerVM = 3;
    final var incrementsPerThread = 10;

    for (var i = 0; i < vmCount; i++) {
      var vm = VM.getVM(i);
      vm.invoke("Create Region", () -> {
        createRegion(name);
        var region = getRootRegion().getSubregion(name);
        region.put(key, 0);
      });
    }

    var invokes = new AsyncInvocation[vmCount];
    for (var i = 0; i < vmCount; i++) {
      invokes[i] = VM.getVM(i).invokeAsync("Start Threads and increment", () -> {
        final var group = new ThreadGroup("Incrementors") {
          @Override
          public void uncaughtException(Thread t, Throwable e) {
            var s = "Uncaught exception in thread " + t;
            fail(s, e);
          }
        };

        var threads = new Thread[threadsPerVM];
        for (var i1 = 0; i1 < threadsPerVM; i1++) {
          var thread = new Thread(group, () -> {
            try {
              final var rand = new Random(System.identityHashCode(this));
              Region<Object, Integer> region = getRootRegion().getSubregion(name);
              for (var j = 0; j < incrementsPerThread; j++) {
                Thread.sleep(rand.nextInt(30) + 30);

                var lock = region.getDistributedLock(key);
                assertThat(lock.tryLock(-1, TimeUnit.MILLISECONDS)).isTrue();

                var value = region.get(key);
                var oldValue = value;
                if (value == null) {
                  value = 1;

                } else {
                  var v = value;
                  value = v + 1;
                }

                assertThat(oldValue).isEqualTo(region.get(key));
                region.put(key, value);
                assertThat(value).isEqualTo(region.get(key));

                lock.unlock();
              }
            } catch (InterruptedException interruptedException) {
              fail("interrupted", interruptedException);
            }

          }, "Incrementer " + i1);
          threads[i1] = thread;
          thread.start();
        }

        for (var thread : threads) {
          ThreadUtils.join(thread, 30 * 1000);
        }
      });
    }

    for (var i = 0; i < vmCount; i++) {
      invokes[i].get();
    }

    vm0.invoke("Verify final value", () -> {
      Region region = getRootRegion().getSubregion(name);
      var value = (Integer) region.get(key);
      assertThat(value).isNotNull();
      var expected = vmCount * threadsPerVM * incrementsPerThread;
      assertThat(expected).isEqualTo(value.intValue());
    });
  }

  /**
   * Tests that {@link Region#put} and {@link Region#get} timeout when another VM holds the
   * distributed lock on the entry in question.
   */
  @Test
  public void testPutGetTimeout() {
    assertThat(Scope.GLOBAL).isEqualTo(getRegionAttributes().getScope());

    final var name = getUniqueName();
    final Object key = "KEY";
    final Object value = "VALUE";

    vm0.invoke("Create Region", () -> {
      createRegion(name);
    });

    vm1.invoke("Create Region", () -> {
      createRegion(name);
    });

    vm0.invoke("Lock entry", () -> {
      Region region = getRootRegion().getSubregion(name);
      var lock = region.getDistributedLock(key);
      lock.lock();
    });

    vm1.invoke("Attempt get/put", () -> {
      Cache cache = getCache();
      cache.setLockTimeout(1);
      cache.setSearchTimeout(1);
      var region = getRootRegion().getSubregion(name);

      try {
        region.put(key, value);
        fail("Should have thrown a TimeoutException on put");

      } catch (TimeoutException ex) {
        // pass..
      }

      // With a loader, should try to lock and time out
      region.getAttributesMutator().setCacheLoader(new TestCacheLoader<Object, Object>() {
        @Override
        public Object load2(LoaderHelper helper) {
          return null;
        }
      });
      try {
        region.get(key);
        fail("Should have thrown a TimeoutException on get");

      } catch (TimeoutException ex) {
        // pass..
      }

      // Without a loader, should succeed
      region.getAttributesMutator().setCacheLoader(null);
      region.get(key);
    });

    vm0.invoke("Unlock entry", () -> {
      Region region = getRootRegion().getSubregion(name);
      var lock = region.getDistributedLock(key);
      lock.unlock();
    });
  }

}
