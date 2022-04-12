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

import static org.apache.geode.cache.ExpirationAction.LOCAL_DESTROY;
import static org.apache.geode.cache.LossAction.LIMITED_ACCESS;
import static org.apache.geode.cache.ResumptionAction.NONE;
import static org.apache.geode.distributed.ConfigurationProperties.ROLES;
import static org.apache.geode.distributed.internal.membership.InternalRole.getRole;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.CommitDistributionException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.LossAction;
import org.apache.geode.cache.MembershipAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAccessException;
import org.apache.geode.cache.RegionDistributionException;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.RegionMembershipListener;
import org.apache.geode.cache.RegionReinitializedException;
import org.apache.geode.cache.RequiredRoles;
import org.apache.geode.cache.ResumptionAction;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.util.RegionMembershipListenerAdapter;
import org.apache.geode.distributed.Role;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalRole;
import org.apache.geode.internal.cache.AbstractRegion;
import org.apache.geode.internal.cache.DistributedCacheOperation;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXState;
import org.apache.geode.internal.cache.TXStateInterface;
import org.apache.geode.internal.cache.TXStateProxyImpl;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.dunit.WaitCriterion;

/**
 * Tests region reliability defined by MembershipAttributes.
 *
 * @since GemFire 5.0
 */
public abstract class RegionReliabilityTestCase extends ReliabilityTestCase {

  @Override
  public final void preTearDownCacheTestCase() throws Exception {
    DistributedCacheOperation.setBeforePutOutgoing(null);
  }

  /** Returns scope to execute tests under. */
  protected abstract Scope getRegionScope();

  protected InternalDistributedSystem createConnection(String[] roles) {
    var rolesValue = new StringBuilder();
    if (roles != null) {
      for (var i = 0; i < roles.length; i++) {
        if (i > 0) {
          rolesValue.append(",");
        }
        rolesValue.append(roles[i]);
      }
    }
    var config = new Properties();
    config.setProperty(ROLES, rolesValue.toString());
    return getSystem(config);
  }

  protected void assertLimitedAccessThrows(Region region) throws Exception {
    try {
      region.clear();
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      region.create("KEY", "VAL");
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      region.destroy(new Object());
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      region.destroyRegion();
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    if (region.getAttributes().getScope().isGlobal()) {
      try {
        region.becomeLockGrantor();
        fail("Should have thrown an RegionAccessException");
      } catch (RegionAccessException ex) {
        // pass...
      }
      try {
        region.getDistributedLock(new Object());
        fail("Should have thrown an RegionAccessException");
      } catch (RegionAccessException ex) {
        // pass...
      }
      try {
        region.getRegionDistributedLock();
        fail("Should have thrown an RegionAccessException");
      } catch (RegionAccessException ex) {
        // pass...
      }
    }
    try {
      region.invalidate(new Object());
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      region.invalidateRegion();
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      region.loadSnapshot(new ByteArrayInputStream(new byte[] {}));
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try { // netload TODO: configure CacheLoader in region
      region.get("netload");
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try { // netsearch TODO: add 2nd VM that has the object
      region.get("netsearch");
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      region.put(new Object(), new Object());
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      Map map = new HashMap();
      map.put(new Object(), new Object());
      region.putAll(map);
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      Map map = new HashMap();
      map.put(new Object(), new Object());
      region.putAll(map, "callbackArg");
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      region.remove(new Object());
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    if (!region.getAttributes().getScope().isGlobal()) {
      var tx = region.getCache().getCacheTransactionManager();
      tx.begin();
      try {
        region.put("KEY-tx", "VAL-tx");
        fail("Should have thrown an RegionAccessException");
      } catch (RegionAccessException ex) {
        // pass...
      }
      tx.rollback();
    }
  }

  protected void assertNoAccessThrows(Region region) throws Exception {
    assertLimitedAccessThrows(region);
    try {
      region.containsKey(new Object());
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      region.containsValue(new Object());
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      region.containsValueForKey(new Object());
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      region.entrySet(false);
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      region.entrySet();
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      region.get(new Object());
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      region.getEntry(new Object());
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      region.isEmpty();
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      region.keySet();
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      region.localDestroy(new Object());
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      region.localInvalidate(new Object());
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      region.localInvalidateRegion();
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      region.saveSnapshot(new ByteArrayOutputStream());
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      region.size();
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
    try {
      region.values();
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }

    try {
      var qs = region.getCache().getQueryService();
      var query = qs.newQuery("(select distinct * from " + region.getFullPath() + ").size");
      query.execute();
      fail("Should have thrown an RegionAccessException");
    } catch (RegionAccessException ex) {
      // pass...
    }
  }

  protected void assertLimitedAccessDoesNotThrow(Region region) throws Exception {
    // insert some values for test
    var keys = new Object[] {"hip", "hop"};
    var values = new Object[] {"clip", "clop"};
    for (var i = 0; i < keys.length; i++) {
      region.put(keys[i], values[i]);
    }

    // test the ops that can throw RegionAccessException for LIMITED_ACCESS
    region.create("jack", "jill");
    region.destroy("jack");

    if (region.getAttributes().getScope().isGlobal()) {
      region.becomeLockGrantor();

      var dlock = region.getDistributedLock(keys[0]);
      dlock.lock();
      dlock.unlock();

      var rlock = region.getRegionDistributedLock();
      rlock.lock();
      rlock.unlock();
    }

    // netload (configured in vm0)
    assertEquals("netload", region.get("netload"));
    // netsearch (entry exists in vm0)
    assertEquals("netsearch", region.get("netsearch"));

    region.invalidate(keys[0]);
    region.invalidateRegion();

    var baos = new ByteArrayOutputStream();
    region.saveSnapshot(baos);
    region.loadSnapshot(new ByteArrayInputStream(baos.toByteArray()));

    // need to get a new handle to the region...
    region = getRootRegion(region.getFullPath());

    region.put(keys[0], values[0]);

    Map map = new HashMap();
    map.put("mom", "pop");
    region.putAll(map);
    region.putAll(map, "callbackArg");

    var qs = region.getCache().getQueryService();
    var query = qs.newQuery("(select distinct * from " + region.getFullPath() + ").size");
    query.execute();

    region.remove(keys[0]);

    if (!region.getAttributes().getScope().isGlobal()) {
      var tx = region.getCache().getCacheTransactionManager();
      tx.begin();
      region.put("KEY-tx", "VAL-tx");
      tx.commit();
    }

    region.clear();
    region.destroyRegion();
  }

  protected void assertNoAccessDoesNotThrow(Region region) throws Exception {
    // insert some values for test
    var keys = new Object[] {"bip", "bam"};
    var values = new Object[] {"foo", "bar"};
    for (var i = 0; i < keys.length; i++) {
      region.put(keys[i], values[i]);
    }

    // test the ops that can throw RegionAccessException for NO_ACCESS
    region.containsKey(new Object());
    region.containsValue(new Object());
    region.containsValueForKey(new Object());
    region.entrySet(false);
    region.entrySet();
    region.get(keys[0]);
    region.getEntry(keys[0]);
    region.isEmpty();
    region.keySet();
    region.localDestroy(keys[0]);
    region.localInvalidate(keys[1]);
    region.localInvalidateRegion();
    region.saveSnapshot(new ByteArrayOutputStream());
    region.size();
    region.values();

    var qs = region.getCache().getQueryService();
    var query = qs.newQuery("(select distinct * from " + region.getFullPath() + ").size");
    query.execute();

    assertLimitedAccessDoesNotThrow(region);
  }

  protected void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      fail("interrupted");
    }
  }

  // -------------------------------------------------------------------------
  // Tests to be run under every permutation of config options
  // Valid configurations include scope D-ACK, D-NOACK, GLOBAL
  // -------------------------------------------------------------------------

  /**
   * Tests affect of NO_ACCESS on region operations.
   */
  @Test
  public void testNoAccess() throws Exception {
    final var name = getUniqueName();

    final var roleA = name + "-A";

    // assign names to 4 vms...
    final var requiredRoles = new String[] {roleA};
    Set requiredRolesSet = new HashSet();
    for (final var requiredRole : requiredRoles) {
      requiredRolesSet.add(InternalRole.getRole(requiredRole));
    }
    assertEquals(requiredRoles.length, requiredRolesSet.size());

    // connect controller to system...
    var config = new Properties();
    config.setProperty(ROLES, "");
    getSystem(config);

    getCache();

    // create region in controller...
    var ra =
        new MembershipAttributes(requiredRoles, LossAction.NO_ACCESS, ResumptionAction.NONE);
    var fac = new AttributesFactory();
    fac.setMembershipAttributes(ra);
    fac.setScope(getRegionScope());
    fac.setStatisticsEnabled(true);
    var attr = fac.create();
    var region = createRootRegion(name, attr);

    // wait for memberTimeout to expire
    waitForMemberTimeout();

    // use vm0 for netsearch and netload
    Host.getHost(0).getVM(0).invoke(new CacheSerializableRunnable("Create Region") {
      @Override
      public void run2() throws CacheException {
        createConnection(null);
        var fac = new AttributesFactory();
        fac.setScope(getRegionScope());
        fac.setCacheLoader(new CacheLoader() {
          @Override
          public Object load(LoaderHelper helper) throws CacheLoaderException {
            if ("netload".equals(helper.getKey())) {
              return "netload";
            } else {
              return null;
            }
          }

          @Override
          public void close() {}
        });
        var attr = fac.create();
        var region = createRootRegion(name, attr);
        Object netsearch = "netsearch";
        region.put(netsearch, netsearch);
      }
    });

    // test ops on Region that should throw
    assertNoAccessThrows(region);

    // use vm1 to create role
    Host.getHost(0).getVM(1).invoke(new CacheSerializableRunnable("Create Region") {
      @Override
      public void run2() throws CacheException {
        createConnection(new String[] {roleA});
        var fac = new AttributesFactory();
        fac.setScope(getRegionScope());
        var attr = fac.create();
        createRootRegion(name, attr);
      }
    });

    var role = (Role) requiredRolesSet.iterator().next();
    assertTrue(RequiredRoles.isRoleInRegionMembership(region, role));

    // retest ops on Region to assert no longer throw
    assertNoAccessDoesNotThrow(region);
  }

  /**
   * Tests affect of NO_ACCESS on local entry expiration actions.
   */
  @Test
  public void testNoAccessWithLocalEntryExpiration() throws Exception {
    final var name = getUniqueName();

    final var roleA = name + "-A";

    // assign names to 4 vms...
    final var requiredRoles = new String[] {roleA};
    Set requiredRolesSet = new HashSet();
    for (final var requiredRole : requiredRoles) {
      requiredRolesSet.add(InternalRole.getRole(requiredRole));
    }
    assertEquals(requiredRoles.length, requiredRolesSet.size());

    // connect controller to system...
    var config = new Properties();
    config.setProperty(ROLES, "");
    getSystem(config);

    getCache();

    // create region in controller...
    var ra =
        new MembershipAttributes(requiredRoles, LossAction.NO_ACCESS, ResumptionAction.NONE);
    var fac = new AttributesFactory();
    fac.setMembershipAttributes(ra);
    fac.setScope(getRegionScope());
    fac.setStatisticsEnabled(true);
    var attr = fac.create();
    final var region = createExpiryRootRegion(name, attr);

    // wait for memberTimeout to expire
    waitForMemberTimeout();

    // use vm1 to create role
    Host.getHost(0).getVM(1).invoke(new CacheSerializableRunnable("Create Region") {
      @Override
      public void run2() throws CacheException {
        createConnection(new String[] {roleA});
        var fac = new AttributesFactory();
        fac.setScope(getRegionScope());
        var attr = fac.create();
        createRootRegion(name, attr);
      }
    });

    // test to make sure expiration is not suspended
    region.put("expireMe", "expireMe");
    assertTrue(region.size() == 1);
    Host.getHost(0).getVM(1).invoke(new CacheSerializableRunnable("Close Region") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion(name);
        region.close();
      }
    });
    // TODO: waitForMemberTimeout(); ?

    // set expiration and sleep
    var mutator = region.getAttributesMutator();
    mutator.setEntryTimeToLive(new ExpirationAttributes(1, ExpirationAction.LOCAL_DESTROY));
    sleep(200);

    // make sure no values were expired
    var entries = ((LocalRegion) region).basicEntries(false);
    assertTrue(entries.size() == 1);

    // create region again in vm1
    Host.getHost(0).getVM(1).invoke(new CacheSerializableRunnable("Create Region") {
      @Override
      public void run2() throws CacheException {
        var fac = new AttributesFactory();
        fac.setScope(getRegionScope());
        var attr = fac.create();
        createRootRegion(name, attr);
      }
    });

    waitForEntryDestroy(region, "expireMe");
  }

  /**
   * Tests affect of NO_ACCESS on local region expiration actions.
   */
  @Test
  public void testNoAccessWithLocalRegionExpiration() throws Exception {
    final var name = getUniqueName();

    final var roleA = name + "-A";

    // assign names to 4 vms...
    final var requiredRoles = new String[] {roleA};
    Set requiredRolesSet = new HashSet();
    for (final var requiredRole : requiredRoles) {
      requiredRolesSet.add(InternalRole.getRole(requiredRole));
    }
    assertEquals(requiredRoles.length, requiredRolesSet.size());

    // connect controller to system...
    var config = new Properties();
    config.setProperty(ROLES, "");
    getSystem(config);

    getCache();

    // create region in controller...
    var ra =
        new MembershipAttributes(requiredRoles, LossAction.NO_ACCESS, ResumptionAction.NONE);
    var fac = new AttributesFactory();
    fac.setMembershipAttributes(ra);
    fac.setScope(getRegionScope());
    fac.setStatisticsEnabled(true);
    var attr = fac.create();
    final var region = createExpiryRootRegion(name, attr);

    // wait for memberTimeout to expire
    waitForMemberTimeout();

    var mutator = region.getAttributesMutator();
    mutator.setRegionTimeToLive(new ExpirationAttributes(1, ExpirationAction.LOCAL_DESTROY));

    // sleep and make sure region does not expire
    sleep(200);
    assertFalse(region.isDestroyed());

    // create region in vm1
    Host.getHost(0).getVM(1).invoke(new CacheSerializableRunnable("Create Region") {
      @Override
      public void run2() throws CacheException {
        createConnection(new String[] {roleA});
        var fac = new AttributesFactory();
        fac.setScope(getRegionScope());
        var attr = fac.create();
        createRootRegion(name, attr);
      }
    });

    waitForRegionDestroy(region);
  }

  /**
   * Tests affect of LIMITED_ACCESS on region operations.
   */
  @Test
  public void testLimitedAccess() throws Exception {
    final var name = getUniqueName();

    final var roleA = name + "-A";

    // assign names to 4 vms...
    final var requiredRoles = new String[] {roleA};
    Set requiredRolesSet = new HashSet();
    for (final var requiredRole : requiredRoles) {
      requiredRolesSet.add(InternalRole.getRole(requiredRole));
    }
    assertEquals(requiredRoles.length, requiredRolesSet.size());

    // connect controller to system...
    var config = new Properties();
    config.setProperty(ROLES, "");
    getSystem(config);

    getCache();

    // create region in controller...
    var ra =
        new MembershipAttributes(requiredRoles, LossAction.LIMITED_ACCESS, ResumptionAction.NONE);
    var fac = new AttributesFactory();
    fac.setMembershipAttributes(ra);
    fac.setScope(getRegionScope());
    fac.setStatisticsEnabled(true);
    var attr = fac.create();
    var region = createRootRegion(name, attr);

    // wait for memberTimeout to expire
    waitForMemberTimeout();

    // use vm0 for netsearch and netload
    Host.getHost(0).getVM(0).invoke(new CacheSerializableRunnable("Create Region") {
      @Override
      public void run2() throws CacheException {
        createConnection(null);
        var fac = new AttributesFactory();
        fac.setScope(getRegionScope());
        fac.setCacheLoader(new CacheLoader() {
          @Override
          public Object load(LoaderHelper helper) throws CacheLoaderException {
            if ("netload".equals(helper.getKey())) {
              return "netload";
            } else {
              return null;
            }
          }

          @Override
          public void close() {}
        });
        var attr = fac.create();
        var region = createRootRegion(name, attr);
        Object netsearch = "netsearch";
        region.put(netsearch, netsearch);
      }
    });

    // test ops on Region that should throw
    assertLimitedAccessThrows(region);

    // this query should not throw
    var qs = region.getCache().getQueryService();
    var query = qs.newQuery("(select distinct * from " + region.getFullPath() + ").size");
    query.execute();

    // use vm1 to create role
    Host.getHost(0).getVM(1).invoke(new CacheSerializableRunnable("Create Region") {
      @Override
      public void run2() throws CacheException {
        createConnection(new String[] {roleA});
        var fac = new AttributesFactory();
        fac.setScope(getRegionScope());
        var attr = fac.create();
        createRootRegion(name, attr);
      }
    });

    // retest ops on Region to assert no longer throw
    assertLimitedAccessDoesNotThrow(region);
  }

  /**
   * Tests affect of LIMITED_ACCESS on local entry expiration actions.
   */
  @Test
  public void testLimitedAccessWithLocalEntryExpiration() throws Exception {
    final var name = getUniqueName();

    final var roleA = name + "-A";

    // assign names to 4 vms...
    final var requiredRoles = new String[] {roleA};
    Set requiredRolesSet = new HashSet();
    for (final var requiredRole : requiredRoles) {
      requiredRolesSet.add(getRole(requiredRole));
    }
    assertEquals(requiredRoles.length, requiredRolesSet.size());

    // connect controller to system...
    var config = new Properties();
    config.setProperty(ROLES, "");
    getSystem(config);

    getCache();

    // create region in controller...
    var ra =
        new MembershipAttributes(requiredRoles, LIMITED_ACCESS, NONE);
    var fac = new AttributesFactory();
    fac.setMembershipAttributes(ra);
    fac.setScope(getRegionScope());
    fac.setStatisticsEnabled(true);
    var attr = fac.create();
    final var region = createExpiryRootRegion(name, attr);

    // wait for memberTimeout to expire
    waitForMemberTimeout();

    // use vm1 to create role
    getHost(0).getVM(1).invoke(new CacheSerializableRunnable("Create Region") {
      @Override
      public void run2() throws CacheException {
        createConnection(new String[] {roleA});
        var fac = new AttributesFactory();
        fac.setScope(getRegionScope());
        var attr = fac.create();
        createRootRegion(name, attr);
      }
    });

    // test to make sure expiration is suspended
    region.put("expireMe", "expireMe");
    assertTrue(region.size() == 1);
    getHost(0).getVM(1).invoke(new CacheSerializableRunnable("Close Region") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion(name);
        region.close();
      }
    });
    // TODO: waitForMemberTimeout(); ?

    // set expiration and sleep
    var mutator = region.getAttributesMutator();
    mutator.setEntryTimeToLive(new ExpirationAttributes(1, LOCAL_DESTROY));
    var wc1 = new WaitCriterion() {
      @Override
      public boolean done() {
        return ((LocalRegion) region).basicEntries(false).size() == 0;
      }

      @Override
      public String description() {
        return "expected zero entries but have "
            + ((LocalRegion) region).basicEntries(false).size();
      }
    };
    GeodeAwaitility.await().untilAsserted(wc1);

    // create region again
    getHost(0).getVM(1).invoke(new CacheSerializableRunnable("Create Region") {
      @Override
      public void run2() throws CacheException {
        var fac = new AttributesFactory();
        fac.setScope(getRegionScope());
        var attr = fac.create();
        createRootRegion(name, attr);
      }
    });

    region.put("expireMe", "expireMe");

    waitForEntryDestroy(region, "expireMe");
    await()
        .untilAsserted(() -> assertEquals(0, region.size()));
  }

  /**
   * Tests affect of LIMITED_ACCESS on local region expiration actions.
   */
  @Test
  public void testLimitedAccessWithLocalRegionExpiration() throws Exception {
    final var name = getUniqueName();

    final var roleA = name + "-A";

    // assign names to 4 vms...
    final var requiredRoles = new String[] {roleA};
    Set requiredRolesSet = new HashSet();
    for (final var requiredRole : requiredRoles) {
      requiredRolesSet.add(InternalRole.getRole(requiredRole));
    }
    assertEquals(requiredRoles.length, requiredRolesSet.size());

    // connect controller to system...
    var config = new Properties();
    config.setProperty(ROLES, "");
    getSystem(config);

    getCache();

    // create region in controller...
    var ra =
        new MembershipAttributes(requiredRoles, LossAction.LIMITED_ACCESS, ResumptionAction.NONE);
    var fac = new AttributesFactory();
    fac.setMembershipAttributes(ra);
    fac.setScope(getRegionScope());
    fac.setStatisticsEnabled(true);
    var attr = fac.create();
    final var region = createExpiryRootRegion(name, attr);

    // wait for memberTimeout to expire
    waitForMemberTimeout();

    var mutator = region.getAttributesMutator();
    mutator.setRegionTimeToLive(new ExpirationAttributes(1, ExpirationAction.LOCAL_DESTROY));

    waitForRegionDestroy(region);
  }

  /**
   * Tests affect of FULL_ACCESS on region operations.
   */
  @Test
  public void testFullAccess() throws Exception {
    final var name = getUniqueName();

    final var roleA = name + "-A";

    // assign names to 4 vms...
    final var requiredRoles = new String[] {roleA};
    Set requiredRolesSet = new HashSet();
    for (final var requiredRole : requiredRoles) {
      requiredRolesSet.add(InternalRole.getRole(requiredRole));
    }
    assertEquals(requiredRoles.length, requiredRolesSet.size());

    // connect controller to system...
    var config = new Properties();
    config.setProperty(ROLES, "");
    getSystem(config);

    getCache();

    // create region in controller...
    var ra =
        new MembershipAttributes(requiredRoles, LossAction.FULL_ACCESS, ResumptionAction.NONE);
    var fac = new AttributesFactory();
    fac.setMembershipAttributes(ra);
    fac.setScope(getRegionScope());
    fac.setStatisticsEnabled(true);
    var attr = fac.create();
    var region = createRootRegion(name, attr);

    // wait for memberTimeout to expire
    waitForMemberTimeout();

    // use vm0 for netsearch and netload
    Host.getHost(0).getVM(0).invoke(new CacheSerializableRunnable("Create Region") {
      @Override
      public void run2() throws CacheException {
        createConnection(null);
        var fac = new AttributesFactory();
        fac.setScope(getRegionScope());
        fac.setCacheLoader(new CacheLoader() {
          @Override
          public Object load(LoaderHelper helper) throws CacheLoaderException {
            if ("netload".equals(helper.getKey())) {
              return "netload";
            } else {
              return null;
            }
          }

          @Override
          public void close() {}
        });
        var attr = fac.create();
        var region = createRootRegion(name, attr);
        Object netsearch = "netsearch";
        region.put(netsearch, netsearch);
      }
    });

    // test ops on Region that should not throw
    assertNoAccessDoesNotThrow(region);
  }

  /**
   * Tests affect of FULL_ACCESS on local entry expiration actions.
   */
  @Test
  public void testFullAccessWithLocalEntryExpiration() throws Exception {
    final var name = getUniqueName();

    final var roleA = name + "-A";

    // assign names to 4 vms...
    final var requiredRoles = new String[] {roleA};
    Set requiredRolesSet = new HashSet();
    for (final var requiredRole : requiredRoles) {
      requiredRolesSet.add(InternalRole.getRole(requiredRole));
    }
    assertEquals(requiredRoles.length, requiredRolesSet.size());

    // connect controller to system...
    var config = new Properties();
    config.setProperty(ROLES, "");
    getSystem(config);

    getCache();

    // create region in controller...
    var ra =
        new MembershipAttributes(requiredRoles, LossAction.FULL_ACCESS, ResumptionAction.NONE);
    var fac = new AttributesFactory();
    fac.setMembershipAttributes(ra);
    fac.setScope(getRegionScope());
    fac.setStatisticsEnabled(true);
    var attr = fac.create();
    final var region = createExpiryRootRegion(name, attr);

    // wait for memberTimeout to expire
    waitForMemberTimeout();

    // test to make sure expiration is not suspended
    region.put("expireMe", "expireMe");
    assertTrue(region.size() == 1);

    // set expiration and sleep
    var mutator = region.getAttributesMutator();
    mutator.setEntryTimeToLive(new ExpirationAttributes(1, ExpirationAction.LOCAL_DESTROY));

    waitForEntryDestroy(region, "expireMe");
    await()
        .untilAsserted(() -> assertEquals(0, region.size()));
  }

  public static void waitForRegionDestroy(final Region region) {
    var wc = new WaitCriterion() {
      @Override
      public boolean done() {
        return region.isDestroyed();
      }

      @Override
      public String description() {
        return "expected region " + region + " to be destroyed";
      }
    };
    GeodeAwaitility.await().untilAsserted(wc);
  }

  public static void waitForEntryDestroy(final Region region, final Object key) {
    var wc = new WaitCriterion() {
      @Override
      public boolean done() {
        return region.get(key) == null;
      }

      @Override
      public String description() {
        return "expected entry " + key + " to not exist but it has the value " + region.get(key);
      }
    };
    GeodeAwaitility.await().untilAsserted(wc);
  }

  /**
   * Tests affect of FULL_ACCESS on local region expiration actions.
   */
  @Test
  public void testFullAccessWithLocalRegionExpiration() throws Exception {
    final var name = getUniqueName();

    final var roleA = name + "-A";

    final var requiredRoles = new String[] {roleA};
    Set requiredRolesSet = new HashSet();
    for (final var requiredRole : requiredRoles) {
      requiredRolesSet.add(InternalRole.getRole(requiredRole));
    }
    assertEquals(requiredRoles.length, requiredRolesSet.size());

    // connect controller to system...
    var config = new Properties();
    config.setProperty(ROLES, "");
    getSystem(config);

    getCache();

    // create region in controller...
    var ra =
        new MembershipAttributes(requiredRoles, LossAction.FULL_ACCESS, ResumptionAction.NONE);
    var fac = new AttributesFactory();
    fac.setMembershipAttributes(ra);
    fac.setScope(getRegionScope());
    fac.setStatisticsEnabled(true);
    var attr = fac.create();
    final var region = createExpiryRootRegion(name, attr);

    // wait for memberTimeout to expire
    waitForMemberTimeout();

    var mutator = region.getAttributesMutator();
    mutator.setRegionTimeToLive(new ExpirationAttributes(1, ExpirationAction.LOCAL_DESTROY));

    waitForRegionDestroy(region);
  }

  protected static Boolean[] detectedDeparture_testCommitDistributionException = {Boolean.FALSE};

  @Test
  public void testCommitDistributionException() throws Exception {
    if (getRegionScope().isGlobal()) {
      return; // skip test under Global
    }
    if (getRegionScope().isDistributedNoAck()) {
      return; // skip test under DistributedNoAck
    }

    final var name = getUniqueName();
    final var roleA = name + "-A";
    final var requiredRoles = new String[] {roleA};
    Set requiredRolesSet = new HashSet();
    for (final var requiredRole : requiredRoles) {
      requiredRolesSet.add(InternalRole.getRole(requiredRole));
    }
    assertEquals(requiredRoles.length, requiredRolesSet.size());

    // connect controller to system...
    var config = new Properties();
    config.setProperty(ROLES, "");
    getSystem(config);

    var cache = (GemFireCacheImpl) getCache();

    RegionMembershipListener listener = new RegionMembershipListenerAdapter() {
      @Override
      public void afterRemoteRegionDeparture(RegionEvent event) {
        synchronized (detectedDeparture_testCommitDistributionException) {
          detectedDeparture_testCommitDistributionException[0] = Boolean.TRUE;
          detectedDeparture_testCommitDistributionException.notify();
        }
      }
    };

    // create region in controller...
    var ra =
        new MembershipAttributes(requiredRoles, LossAction.NO_ACCESS, ResumptionAction.NONE);
    var fac = new AttributesFactory();
    fac.setMembershipAttributes(ra);
    fac.setScope(getRegionScope());
    fac.addCacheListener(listener);
    var attr = fac.create();
    var region = createRootRegion(name, attr);

    // use vm1 to create role
    Host.getHost(0).getVM(1).invoke(new CacheSerializableRunnable("Create Region") {
      @Override
      public void run2() throws CacheException {
        createConnection(new String[] {roleA});
        var fac = new AttributesFactory();
        fac.setScope(getRegionScope());
        var attr = fac.create();
        createRootRegion(name, attr);
      }
    });

    // define the afterReleaseLocalLocks callback
    var removeRequiredRole = (SerializableRunnableIF) () -> {
      Host.getHost(0).getVM(1).invoke(new SerializableRunnable("Close Region") {
        @Override
        public void run() {
          getRootRegion(name).close();
        }
      });
      try {
        synchronized (detectedDeparture_testCommitDistributionException) {
          while (detectedDeparture_testCommitDistributionException[0] == Boolean.FALSE) {
            detectedDeparture_testCommitDistributionException.wait();
          }
        }
      } catch (InterruptedException e) {
        fail("interrupted");
      }
    };

    // define the add and remove expected exceptions
    final var expectedExceptions = "org.apache.geode.internal.cache.CommitReplyException";
    SerializableRunnable addExpectedExceptions =
        new CacheSerializableRunnable("addExpectedExceptions") {
          @Override
          public void run2() throws CacheException {
            getCache().getLogger().info(
                "<ExpectedException action=add>" + expectedExceptions + "</ExpectedException>");
          }
        };
    SerializableRunnable removeExpectedExceptions =
        new CacheSerializableRunnable("removeExpectedExceptions") {
          @Override
          public void run2() throws CacheException {
            getCache().getLogger().info(
                "<ExpectedException action=remove>" + expectedExceptions + "</ExpectedException>");
          }
        };

    // perform the actual test...

    var ctm = cache.getCacheTransactionManager();
    ctm.begin();
    TXStateInterface txStateProxy = ((TXManagerImpl) ctm).getTXState();
    ((TXStateProxyImpl) txStateProxy).forceLocalBootstrap();
    var txState = (TXState) ((TXStateProxyImpl) txStateProxy).getRealDeal(null, null);
    txState.setBeforeSend(() -> {
      try {
        removeRequiredRole.run();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    // now start a transaction and commit it
    region.put("KEY", "VAL");

    addExpectedExceptions.run();
    Host.getHost(0).getVM(1).invoke(addExpectedExceptions);

    try {
      ctm.commit();
      fail("Should have thrown CommitDistributionException");
    } catch (CommitDistributionException e) {
      // pass
    } finally {
      removeExpectedExceptions.run();
      Host.getHost(0).getVM(1).invoke(removeExpectedExceptions);
    }
  }

  protected static Boolean[] detectedDeparture_testRegionDistributionException = {Boolean.FALSE};

  @Test
  public void testRegionDistributionException() throws Exception {
    if (getRegionScope().isDistributedNoAck()) {
      return; // skip test under DistributedNoAck
    }

    final var name = getUniqueName();
    final var roleA = name + "-A";
    final var requiredRoles = new String[] {roleA};
    Set requiredRolesSet = new HashSet();
    for (final var requiredRole : requiredRoles) {
      requiredRolesSet.add(InternalRole.getRole(requiredRole));
    }
    assertEquals(requiredRoles.length, requiredRolesSet.size());

    // connect controller to system...
    var config = new Properties();
    config.setProperty(ROLES, "");
    getSystem(config);

    getCache();

    RegionMembershipListener listener = new RegionMembershipListenerAdapter() {
      @Override
      public void afterRemoteRegionDeparture(RegionEvent event) {
        synchronized (detectedDeparture_testRegionDistributionException) {
          detectedDeparture_testRegionDistributionException[0] = Boolean.TRUE;
          detectedDeparture_testRegionDistributionException.notify();
        }
      }
    };

    // create region in controller...
    var ra =
        new MembershipAttributes(requiredRoles, LossAction.NO_ACCESS, ResumptionAction.NONE);
    var fac = new AttributesFactory();
    fac.setMembershipAttributes(ra);
    fac.setScope(getRegionScope());
    fac.setDataPolicy(DataPolicy.REPLICATE);
    // fac.addCacheListener(listener);
    var attr = fac.create();
    var region = createRootRegion(name, attr);

    assertTrue(((AbstractRegion) region).requiresReliabilityCheck());

    // use vm1 to create role
    var createRegion = new CacheSerializableRunnable("Create Region") {
      @Override
      public void run2() throws CacheException {
        createConnection(new String[] {roleA});
        var fac = new AttributesFactory();
        fac.setScope(getRegionScope());
        fac.setDataPolicy(DataPolicy.REPLICATE);
        var attr = fac.create();
        createRootRegion(name, attr);
      }
    };

    Host.getHost(0).getVM(1).invoke(createRegion);
    region.put("DESTROY_ME", "VAL");
    region.put("INVALIDATE_ME", "VAL");

    // define the afterReleaseLocalLocks callback
    var removeRequiredRole = new SerializableRunnable() {
      @Override
      public void run() {
        Host.getHost(0).getVM(1).invoke(new SerializableRunnable("Close Region") {
          @Override
          public void run() {
            getRootRegion(name).close();
          }
        });
        // try {
        // synchronized (detectedDeparture_testRegionDistributionException) {
        // while (detectedDeparture_testRegionDistributionException[0] == Boolean.FALSE) {
        // detectedDeparture_testRegionDistributionException.wait();
        // }
        // }
        // }
        // catch (InterruptedException e) {}
      }
    };
    DistributedCacheOperation.setBeforePutOutgoing(() -> {
      try {
        removeRequiredRole.run();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    var reset = (Runnable) () -> {
      // synchronized (detectedDeparture_testRegionDistributionException) {
      // detectedDeparture_testRegionDistributionException[0] = Boolean.FALSE;
      // }
    };

    // PUT
    try {
      region.put("KEY", "VAL");
      fail("Should have thrown RegionDistributionException");
    } catch (RegionDistributionException e) {
      // pass
    }

    // INVALIDATE
    reset.run();
    Host.getHost(0).getVM(1).invoke(createRegion);
    try {
      region.invalidate("INVALIDATE_ME");
      fail("Should have thrown RegionDistributionException");
    } catch (RegionDistributionException e) {
      // pass
    }

    // DESTROY
    reset.run();
    Host.getHost(0).getVM(1).invoke(createRegion);
    try {
      region.destroy("DESTROY_ME");
      fail("Should have thrown RegionDistributionException");
    } catch (RegionDistributionException e) {
      // pass
    }

    // CLEAR
    reset.run();
    Host.getHost(0).getVM(1).invoke(createRegion);
    try {
      region.clear();
      fail("Should have thrown RegionDistributionException");
    } catch (RegionDistributionException e) {
      // pass
    }

    // PUTALL
    reset.run();
    Host.getHost(0).getVM(1).invoke(createRegion);
    try {
      Map putAll = new HashMap();
      putAll.put("PUTALL_ME", "VAL");
      region.putAll(putAll);
      fail("Should have thrown RegionDistributionException");
    } catch (RegionDistributionException e) {
      // pass
    }

    // INVALIDATE REGION
    reset.run();
    Host.getHost(0).getVM(1).invoke(createRegion);
    try {
      region.invalidateRegion();
      fail("Should have thrown RegionDistributionException");
    } catch (RegionDistributionException e) {
      // pass
    }

    // DESTROY REGION
    reset.run();
    Host.getHost(0).getVM(1).invoke(createRegion);
    try {
      region.destroyRegion();
      fail("Should have thrown RegionDistributionException");
    } catch (RegionDistributionException e) {
      // pass
    }
  }

  @Test
  public void testReinitialization() throws Exception {
    final var name = getUniqueName();
    final var roleA = name + "-A";
    final var requiredRoles = new String[] {roleA};
    Set requiredRolesSet = new HashSet();
    for (final var requiredRole : requiredRoles) {
      requiredRolesSet.add(InternalRole.getRole(requiredRole));
    }
    assertEquals(requiredRoles.length, requiredRolesSet.size());

    // connect controller to system...
    var config = new Properties();
    config.setProperty(ROLES, "");
    getSystem(config);

    getCache();

    // create region in controller...
    var ra = new MembershipAttributes(requiredRoles, LossAction.NO_ACCESS,
        ResumptionAction.REINITIALIZE);
    var fac = new AttributesFactory();
    fac.setMembershipAttributes(ra);
    fac.setScope(getRegionScope());
    fac.setDataPolicy(DataPolicy.REPLICATE);
    var attr = fac.create();
    var region = createRootRegion(name, attr);

    assertTrue(((AbstractRegion) region).requiresReliabilityCheck());
    assertFalse(RequiredRoles.checkForRequiredRoles(region).isEmpty());

    final var key = "KEY-testReinitialization";
    final var val = "VALUE-testReinitialization";

    Host.getHost(0).getVM(0).invoke(new CacheSerializableRunnable("Create Data") {
      @Override
      public void run2() throws CacheException {
        createConnection(new String[] {});
        var fac = new AttributesFactory();
        fac.setScope(getRegionScope());
        fac.setDataPolicy(DataPolicy.REPLICATE);
        var attr = fac.create();
        var region = createRootRegion(name, attr);
        region.put(key, val);
      }
    });

    final var finalRegion = region;
    var thread = new Thread(() -> {
      try {
        RequiredRoles.waitForRequiredRoles(finalRegion, -1);
      } catch (InterruptedException e) {
        fail("interrupted");
      } catch (RegionReinitializedException ignored) {
      }
    });
    thread.start();

    // create role and verify reinitialization took place
    Host.getHost(0).getVM(1).invokeAsync(new CacheSerializableRunnable("Create Role") {
      @Override
      public void run2() throws CacheException {
        createConnection(new String[] {roleA});
        var fac = new AttributesFactory();
        fac.setScope(getRegionScope());
        var attr = fac.create();
        createRootRegion(name, attr);
      }
    });

    ThreadUtils.join(thread, 30 * 1000);
    assertTrue(region.isDestroyed());
    try {
      region.put("fee", "fi");
      fail("Should have thrown RegionReinitializedException");
    } catch (RegionReinitializedException e) {
      // pass
    }
    try {
      RequiredRoles.checkForRequiredRoles(region);
      fail("Should have thrown RegionReinitializedException");
    } catch (RegionReinitializedException e) {
      // pass
    }
    try {
      var role = (Role) requiredRolesSet.iterator().next();
      RequiredRoles.isRoleInRegionMembership(region, role);
      fail("Should have thrown RegionReinitializedException");
    } catch (RegionReinitializedException e) {
      // pass
    }

    region = getRootRegion(name);
    assertNotNull(region);
    assertTrue(((AbstractRegion) region).requiresReliabilityCheck());
    assertTrue(RequiredRoles.checkForRequiredRoles(region).isEmpty());
    assertNotNull(region.getEntry(key));
    assertEquals(val, region.getEntry(key).getValue());
  }

}
