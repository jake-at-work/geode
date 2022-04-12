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

/**
 * File comment
 */
package org.apache.geode.cache30;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.distributed.DistributedMember;

/**
 * @since GemFire 6.0
 */

public class PartitionedRegionMembershipListenerDUnitTest
    extends RegionMembershipListenerDUnitTest {

  private transient MyRML myPRListener;
  private transient Region prr; // root region

  public PartitionedRegionMembershipListenerDUnitTest() {
    super();
  }

  @Override
  protected RegionAttributes createSubRegionAttributes(CacheListener[] cacheListeners) {
    var af = new AttributesFactory();
    if (cacheListeners != null) {
      af.initCacheListeners(cacheListeners);
    }
    af.setPartitionAttributes(
        new PartitionAttributesFactory().setTotalNumBuckets(5).setRedundantCopies(0).create());
    return af.create();
  }

  @Override
  protected List<DistributedMember> assertInitialMembers(DistributedMember otherId) {
    var l = super.assertInitialMembers(otherId);
    assertTrue(myPRListener.lastOpWasInitialMembers());
    assertEquals(l, myPRListener.getInitialMembers());
    return l;
  }

  @Override
  protected void closeRoots() {
    super.closeRoots();
    prr.close();
  }

  @Override
  protected void createRootRegionWithListener(final String rName) throws CacheException {
    super.createRootRegionWithListener(rName);
    var to = getOpTimeout();
    myPRListener = new MyRML(to);
    var af = new AttributesFactory();
    af.initCacheListeners(new CacheListener[] {myPRListener});
    af.setPartitionAttributes(
        new PartitionAttributesFactory().setTotalNumBuckets(5).setRedundantCopies(0).create());
    prr = createRootRegion(rName + "-pr", af.create());
  }

  @Override
  protected void createRootOtherVm(final String rName) {
    // TODO Auto-generated method stub
    super.createRootOtherVm(rName);
    var vm = getOtherVm();
    vm.invoke(new CacheSerializableRunnable("create PR root") {
      @Override
      public void run2() throws CacheException {
        var af = new AttributesFactory();
        af.setPartitionAttributes(
            new PartitionAttributesFactory().setTotalNumBuckets(5).setRedundantCopies(0).create());
        createRootRegion(rName + "-pr", af.create());
      }
    });
  }

  @Override
  protected void destroyRootOtherVm(final String rName) {
    // TODO Auto-generated method stub
    super.destroyRootOtherVm(rName);
    var vm = getOtherVm();
    vm.invoke(new CacheSerializableRunnable("local destroy PR root") {
      @Override
      public void run2() throws CacheException {
        getRootRegion(rName + "-pr").localDestroyRegion();
      }
    });
  }

  @Override
  protected void closeRootOtherVm(final String rName) {
    super.closeRootOtherVm(rName);
    var vm = getOtherVm();
    vm.invoke(new CacheSerializableRunnable("close PR root") {
      @Override
      public void run2() throws CacheException {
        getRootRegion(rName + "-pr").close();
      }
    });
  }

  @Override
  protected void assertOpWasCreate() {
    super.assertOpWasCreate();
    assertTrue(myPRListener.lastOpWasCreate());
  }

  @Override
  protected void assertOpWasDeparture() {
    super.assertOpWasDeparture();
    assertTrue(myPRListener.lastOpWasDeparture());
    assertEventStuff(myPRListener.getLastEvent(), otherId, prr);
  }


}
