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

import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;


public class DeltaFaultInDUnitTest extends JUnit4CacheTestCase {


  public DeltaFaultInDUnitTest() {
    super();
  }

  @Test
  public void bucketSizeShould_notGoNegative_onFaultInDeltaObject() throws Exception {
    final var host = Host.getHost(0);
    var vm0 = host.getVM(0);
    var vm1 = host.getVM(1);
    var vm2 = host.getVM(2);

    final var copyOnRead = false;
    final var clone = true;

    var createDataRegion = new SerializableCallable("createDataRegion") {
      @Override
      public Object call() throws Exception {
        Cache cache = getCache();
        cache.setCopyOnRead(copyOnRead);
        cache.createDiskStoreFactory().create("DeltaFaultInDUnitTestData");
        var attr = new AttributesFactory();
        attr.setDiskStoreName("DeltaFaultInDUnitTestData");
        var paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        var prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        attr.setCloningEnabled(clone);
        attr.setEvictionAttributes(
            EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK));
        var region = cache.createRegion("region1", attr.create());

        return null;
      }
    };

    vm0.invoke(createDataRegion);

    var createEmptyRegion = new SerializableRunnable("createEmptyRegion") {
      @Override
      public void run() {
        Cache cache = getCache();
        cache.setCopyOnRead(copyOnRead);
        var attr = new AttributesFactory<Integer, TestDelta>();
        attr.setCloningEnabled(clone);
        var paf =
            new PartitionAttributesFactory<Integer, TestDelta>();
        paf.setRedundantCopies(1);
        paf.setLocalMaxMemory(0);
        var prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        attr.setDataPolicy(DataPolicy.PARTITION);
        attr.setEvictionAttributes(
            EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK));
        var region = cache.createRegion("region1", attr.create());


        // Put an entry
        region.put(0, new TestDelta(false, "initial"));

        // Put a delta object that is larger
        region.put(0, new TestDelta(true, "initial_plus_some_more_data"));
      }
    };

    vm1.invoke(createEmptyRegion);

    vm0.invoke(new SerializableRunnable("doPut") {
      @Override
      public void run() {
        Cache cache = getCache();
        Region<Integer, TestDelta> region = cache.getRegion("region1");


        // Evict the other object
        region.put(113, new TestDelta(false, "bogus"));

        // Something was going weird with the LRU list. It was evicting this object.
        // I want to make sure the other object is the one evicted.
        region.get(113);

        var entriesEvicted = ((InternalRegion) region).getTotalEvictions();
        // assertIndexDetailsEquals(1, entriesEvicted);

        var result = region.get(0);
        assertEquals("initial_plus_some_more_data", result.info);
      }
    });
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
}
