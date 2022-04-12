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
/*
 * Created on Feb 3, 2006
 *
 */
package org.apache.geode.internal.cache.tier.sockets;

import static org.apache.geode.cache.client.PoolManager.createFactory;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.time.Duration;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.client.internal.PutOp;
import org.apache.geode.cache.client.internal.QueueStateImpl.SequenceIdAndExpirationObject;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.ha.ThreadIdentifier;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

/**
 *
 * Tests the functionality of operations of AbstractConnectionProxy & its derived classes.
 */
@Category({ClientSubscriptionTest.class})
public class ConnectionProxyJUnitTest {
  private static final String expectedRedundantErrorMsg =
      "Could not find any server to host redundant client queue.";
  private static final String expectedPrimaryErrorMsg =
      "Could not find any server to host primary client queue.";

  DistributedSystem system;

  Cache cache;

  PoolImpl proxy = null;

  SequenceIdAndExpirationObject seo = null;

  final Duration timeoutToVerifyExpiry = Duration.ofSeconds(30);
  final Duration timeoutToVerifyAckSend = Duration.ofSeconds(30);

  @Before
  public void setUp() throws Exception {

    var p = new Properties();
    p.setProperty(MCAST_PORT, "0");
    p.setProperty(LOCATORS, "");
    system = DistributedSystem.connect(p);
    cache = CacheFactory.create(system);
    final var addExpectedPEM =
        "<ExpectedException action=add>" + expectedPrimaryErrorMsg + "</ExpectedException>";
    final var addExpectedREM =
        "<ExpectedException action=add>" + expectedRedundantErrorMsg + "</ExpectedException>";
    system.getLogWriter().info(addExpectedPEM);
    system.getLogWriter().info(addExpectedREM);
  }

  @After
  public void tearDown() throws Exception {
    cache.close();

    final var removeExpectedPEM =
        "<ExpectedException action=remove>" + expectedPrimaryErrorMsg + "</ExpectedException>";
    final var removeExpectedREM =
        "<ExpectedException action=remove>" + expectedRedundantErrorMsg + "</ExpectedException>";

    system.getLogWriter().info(removeExpectedPEM);
    system.getLogWriter().info(removeExpectedREM);

    system.disconnect();
    if (proxy != null) {
      proxy.destroy();
    }
  }

  /**
   * This test verifies the behaviour of client request when the listener on the server sits
   * forever. This is done in following steps:<br>
   * 1)create server<br>
   * 2)initialize proxy object and create region for client having a CacheListener and make
   * afterCreate in the listener to wait infinitely<br>
   * 3)perform a PUT on client by acquiring Connection through proxy<br>
   * 4)Verify that exception occurs due to infinite wait in the listener<br>
   * 5)Verify that above exception occurs sometime after the readTimeout configured for the client
   * <br>
   *
   */
  @Ignore
  @Test
  public void testListenerOnServerSitForever() throws Exception {
    var port3 = getRandomAvailableTCPPort();
    Region testRegion = null;

    var server = cache.addCacheServer();
    server.setMaximumTimeBetweenPings(10000);
    server.setPort(port3);
    server.start();

    try {
      var pf = PoolManager.createFactory();
      pf.addServer("localhost", port3);
      pf.setSubscriptionEnabled(false);
      pf.setSubscriptionRedundancy(-1);
      pf.setReadTimeout(2000);
      pf.setSocketBufferSize(32768);
      pf.setRetryAttempts(1);
      pf.setPingInterval(10000);

      proxy = (PoolImpl) pf.create("clientPool");

      var factory = new AttributesFactory();
      factory.setScope(Scope.DISTRIBUTED_ACK);
      factory.setCacheListener(new CacheListenerAdapter() {
        @Override
        public void afterCreate(EntryEvent event) {
          synchronized (ConnectionProxyJUnitTest.this) {
            try {
              ConnectionProxyJUnitTest.this.wait();
            } catch (InterruptedException e) {
              fail("interrupted");
            }
          }
        }
      });
      var attrs = factory.create();
      testRegion = cache.createRegion("testregion", attrs);

    } catch (Exception ex) {
      ex.printStackTrace();
      fail("Failed to initialize client");
    }
    var conn = (proxy).acquireConnection();
    long t1 = 0;
    try {
      t1 = System.currentTimeMillis();
      var event = new EntryEventImpl((Object) null, false);
      try {
        event.setEventId(new EventID(new byte[] {1}, 1, 1));
        PutOp.execute(conn, proxy, testRegion.getFullPath(), "key1", "val1", event, null, false);
      } finally {
        event.release();
      }
      fail("Test failed as exception was expected");
    } catch (Exception e) {
      var t2 = System.currentTimeMillis();
      var net = (t2 - t1);
      assertTrue(net / 1000 < 5);
    }
    synchronized (this) {
      notify();
    }
  }

  /**
   * Tests the DeadServerMonitor when identifying an Endpoint as alive , does not create a
   * persistent Ping connection ( i.e sends a CLOSE protocol , if the number of connections is zero.
   */
  @Test
  public void testDeadServerMonitorPingNature1() {
    var port3 = getRandomAvailableTCPPort();

    // final int maxWaitTime = 10000;
    try {
      var pf = PoolManager.createFactory();
      pf.addServer("localhost", port3);
      pf.setSubscriptionEnabled(false);
      pf.setReadTimeout(2000);
      pf.setMinConnections(1);
      pf.setSocketBufferSize(32768);
      pf.setRetryAttempts(1);
      pf.setPingInterval(500);

      proxy = (PoolImpl) pf.create("clientPool");
    } catch (Exception ex) {
      ex.printStackTrace();
      fail("Failed to initialize client");
    }
    try {
      (proxy).acquireConnection();
    } catch (Exception ok) {
      ok.printStackTrace();
    }

    try {
      (proxy).acquireConnection();
    } catch (Exception ok) {
      ok.printStackTrace();
    }

    // long start = System.currentTimeMillis();
    assertEquals(0, proxy.getConnectedServerCount());
    // start the server
    CacheServer server = null;
    try {
      try {
        server = cache.addCacheServer();
        server.setMaximumTimeBetweenPings(15000);
        server.setPort(port3);
        server.start();
      } catch (Exception e) {
        e.printStackTrace();
        fail("Failed to create server");
      }
      GeodeAwaitility.await().untilAsserted(() -> {
        assertEquals(1, proxy.getConnectedServerCount());
      });
    } finally {
      if (server != null) {
        server.stop();
      }
    }
  }

  /**
   * Tests the DeadServerMonitor when identifying an Endpoint as alive , does creates a persistent
   * Ping connection ( i.e sends a PING protocol , if the number of connections is more than zero.
   */
  @Test
  public void testDeadServerMonitorPingNature2() {
    var port3 = getRandomAvailableTCPPort();

    // final int maxWaitTime = 10000;
    try {
      var pf = PoolManager.createFactory();
      pf.addServer("localhost", port3);
      pf.setSubscriptionEnabled(false);
      pf.setReadTimeout(2000);
      pf.setMinConnections(1);
      pf.setSocketBufferSize(32768);
      pf.setRetryAttempts(1);
      pf.setPingInterval(500);
      proxy = (PoolImpl) pf.create("clientPool");
    } catch (Exception ex) {
      ex.printStackTrace();
      fail("Failed to initialize client");
    }
    // let LiveServerMonitor detect it as alive as the numConnection is more than zero

    // long start = System.currentTimeMillis();
    assertEquals(0, proxy.getConnectedServerCount());
    // start the server
    CacheServer server = null;
    try {
      try {
        server = cache.addCacheServer();
        server.setMaximumTimeBetweenPings(15000);
        server.setPort(port3);
        server.start();
      } catch (Exception e) {
        e.printStackTrace();
        fail("Failed to create server");
      }
      GeodeAwaitility.await().untilAsserted(() -> {
        assertEquals(1, proxy.getConnectedServerCount());
      });
    } finally {
      if (server != null) {
        server.stop();
      }
    }
  }

  @Test
  public void testThreadIdToSequenceIdMapCreation() {
    var port3 = getRandomAvailableTCPPort();
    CacheServer server = null;
    try {
      try {
        server = cache.addCacheServer();
        server.setMaximumTimeBetweenPings(10000);
        server.setPort(port3);
        server.start();
      } catch (Exception e) {
        e.printStackTrace();
        fail("Failed to create server");
      }
      try {
        var pf = PoolManager.createFactory();
        pf.addServer("localhost", port3);
        pf.setSubscriptionEnabled(true);
        pf.setSubscriptionRedundancy(-1);
        proxy = (PoolImpl) pf.create("clientPool");
        if (proxy.getThreadIdToSequenceIdMap() == null) {
          fail(" ThreadIdToSequenceIdMap is null. ");
        }
      } catch (Exception ex) {
        ex.printStackTrace();
        fail("Failed to initialize client");
      }
    } finally {
      if (server != null) {
        try {
          Thread.sleep(500);
        } catch (InterruptedException ie) {
          fail("interrupted");
        }
        server.stop();
      }
    }
  }

  @Test
  public void testThreadIdToSequenceIdMapExpiryPositive() {
    var port3 = getRandomAvailableTCPPort();
    CacheServer server = null;
    try {
      try {
        server = cache.addCacheServer();
        server.setMaximumTimeBetweenPings(10000);
        server.setPort(port3);
        server.start();
      } catch (Exception e) {
        e.printStackTrace();
        fail("Failed to create server");
      }
      try {
        var pf = PoolManager.createFactory();
        pf.addServer("localhost", port3);
        pf.setSubscriptionEnabled(true);
        pf.setSubscriptionRedundancy(-1);
        pf.setSubscriptionMessageTrackingTimeout(4000);
        pf.setSubscriptionAckInterval(2000);
        proxy = (PoolImpl) pf.create("clientPool");

        var eid = new EventID(new byte[0], 1, 1);
        if (proxy.verifyIfDuplicate(eid)) {
          fail(" eid should not be duplicate as it is a new entry");
        }

        verifyExpiry();

        if (proxy.verifyIfDuplicate(eid)) {
          fail(" eid should not be duplicate as the previous entry should have expired ");
        }

      } catch (Exception ex) {
        ex.printStackTrace();
        fail("Failed to initialize client");
      }
    } finally {
      if (server != null) {
        server.stop();
      }
    }
  }


  @Test
  public void testThreadIdToSequenceIdMapExpiryNegative() {
    var port3 = getRandomAvailableTCPPort();
    CacheServer server = null;
    try {
      try {
        server = cache.addCacheServer();
        server.setMaximumTimeBetweenPings(10000);
        server.setPort(port3);
        server.start();
      } catch (Exception e) {
        e.printStackTrace();
        fail("Failed to create server");
      }
      try {
        var pf = createFactory();
        pf.addServer("localhost", port3);
        pf.setSubscriptionEnabled(true);
        pf.setSubscriptionRedundancy(-1);
        pf.setSubscriptionMessageTrackingTimeout(10000);

        proxy = (PoolImpl) pf.create("clientPool");

        final var eid = new EventID(new byte[0], 1, 1);
        if (proxy.verifyIfDuplicate(eid)) {
          fail(" eid should not be duplicate as it is a new entry");
        }

        GeodeAwaitility.await().untilAsserted(() -> assertTrue(proxy.verifyIfDuplicate(eid)));
      } catch (Exception ex) {
        ex.printStackTrace();
        fail("Failed to initialize client");
      }
    } finally {
      if (server != null) {
        server.stop();
      }
    }
  }

  @Test
  public void testThreadIdToSequenceIdMapConcurrency() {
    var port3 = getRandomAvailableTCPPort();
    CacheServer server = null;
    try {
      try {
        server = cache.addCacheServer();
        server.setMaximumTimeBetweenPings(10000);
        server.setPort(port3);
        server.start();
      } catch (Exception e) {
        e.printStackTrace();
        fail("Failed to create server");
      }
      try {
        var pf = PoolManager.createFactory();
        pf.addServer("localhost", port3);
        pf.setSubscriptionEnabled(true);
        pf.setSubscriptionRedundancy(-1);
        pf.setSubscriptionMessageTrackingTimeout(5000);
        pf.setSubscriptionAckInterval(2000);
        proxy = (PoolImpl) pf.create("clientPool");

        final var EVENT_ID_COUNT = 10000; // why 10,000?
        var eid = new EventID[EVENT_ID_COUNT];
        for (var i = 0; i < EVENT_ID_COUNT; i++) {
          eid[i] = new EventID(new byte[0], i, i);
          if (proxy.verifyIfDuplicate(eid[i])) {
            fail(" eid can never be duplicate, it is being created for the first time! ");
          }
        }
        verifyExpiry();

        for (var i = 0; i < EVENT_ID_COUNT; i++) {
          if (proxy.verifyIfDuplicate(eid[i])) {
            fail(
                " eid can not be found to be  duplicate since the entry should have expired! " + i);
          }
        }
      } catch (Exception ex) {
        ex.printStackTrace();
        fail("Failed to initialize client");
      }
    } finally {
      if (server != null) {
        server.stop();
      }
    }
  }



  @Test
  public void testDuplicateSeqIdLesserThanCurrentSeqIdBeingIgnored() {
    var port3 = getRandomAvailableTCPPort();
    CacheServer server = null;
    try {
      try {
        server = cache.addCacheServer();
        server.setMaximumTimeBetweenPings(10000);
        server.setPort(port3);
        server.start();
      } catch (Exception e) {
        e.printStackTrace();
        fail("Failed to create server");
      }
      try {
        var pf = PoolManager.createFactory();
        pf.addServer("localhost", port3);
        pf.setSubscriptionEnabled(true);
        pf.setSubscriptionRedundancy(-1);
        pf.setSubscriptionMessageTrackingTimeout(100000);
        proxy = (PoolImpl) pf.create("clientPool");

        var eid1 = new EventID(new byte[0], 1, 5);
        if (proxy.verifyIfDuplicate(eid1)) {
          fail(" eid1 can never be duplicate, it is being created for the first time! ");
        }

        var eid2 = new EventID(new byte[0], 1, 2);

        if (!proxy.verifyIfDuplicate(eid2)) {
          fail(" eid2 should be duplicate, seqId is less than highest (5)");
        }

        var eid3 = new EventID(new byte[0], 1, 3);

        if (!proxy.verifyIfDuplicate(eid3)) {
          fail(" eid3 should be duplicate, seqId is less than highest (5)");
        }

        assertTrue(!proxy.getThreadIdToSequenceIdMap().isEmpty());
        proxy.destroy();
      } catch (Exception ex) {
        ex.printStackTrace();
        fail("Failed to initialize client");
      }
    } finally {
      if (server != null) {
        server.stop();
      }
    }
  }



  @Test
  public void testCleanCloseOfThreadIdToSeqId() {
    var port3 = getRandomAvailableTCPPort();
    CacheServer server = null;
    try {
      try {
        server = cache.addCacheServer();
        server.setMaximumTimeBetweenPings(10000);
        server.setPort(port3);
        server.start();
      } catch (Exception e) {
        e.printStackTrace();
        fail("Failed to create server");
      }
      try {
        var pf = PoolManager.createFactory();
        pf.addServer("localhost", port3);
        pf.setSubscriptionEnabled(true);
        pf.setSubscriptionRedundancy(-1);
        pf.setSubscriptionMessageTrackingTimeout(100000);
        proxy = (PoolImpl) pf.create("clientPool");

        var eid1 = new EventID(new byte[0], 1, 2);
        if (proxy.verifyIfDuplicate(eid1)) {
          fail(" eid can never be duplicate, it is being created for the first time! ");
        }
        var eid2 = new EventID(new byte[0], 1, 3);
        if (proxy.verifyIfDuplicate(eid2)) {
          fail(" eid can never be duplicate, since sequenceId is greater ");
        }

        if (!proxy.verifyIfDuplicate(eid2)) {
          fail(" eid had to be a duplicate, since sequenceId is equal ");
        }
        var eid3 = new EventID(new byte[0], 1, 1);
        if (!proxy.verifyIfDuplicate(eid3)) {
          fail(" eid had to be a duplicate, since sequenceId is lesser ");
        }
      } catch (Exception ex) {
        ex.printStackTrace();
        fail("Failed to initialize client");
      }
    } finally {
      if (server != null) {
        try {
          Thread.sleep(500);
        } catch (InterruptedException ie) {
          fail("interrupted");
        }
        server.stop();
      }
    }
  }

  @Test
  public void testTwoClientsHavingDifferentThreadIdMaps() {
    var port3 = getRandomAvailableTCPPort();
    CacheServer server = null;
    try {
      try {
        server = cache.addCacheServer();
        server.setMaximumTimeBetweenPings(10000);
        server.setPort(port3);
        server.start();
      } catch (Exception e) {
        e.printStackTrace();
        fail("Failed to create server");
      }
      try {
        var pf = PoolManager.createFactory();
        pf.addServer("localhost", port3);
        pf.setSubscriptionEnabled(true);
        pf.setSubscriptionRedundancy(-1);
        pf.setSubscriptionMessageTrackingTimeout(100000);

        var proxy1 = (PoolImpl) pf.create("clientPool1");
        try {
          var proxy2 = (PoolImpl) pf.create("clientPool2");
          try {

            var map1 = proxy1.getThreadIdToSequenceIdMap();
            var map2 = proxy2.getThreadIdToSequenceIdMap();

            assertTrue(!(map1 == map2));

          } finally {
            proxy2.destroy();
          }
        } finally {
          proxy1.destroy();
        }
      } catch (Exception ex) {
        ex.printStackTrace();
        fail("Failed to initialize client");
      }
    } finally {
      if (server != null) {
        server.stop();
      }
    }
  }

  @Test
  public void testPeriodicAckSendByClient() {
    var port = getRandomAvailableTCPPort();
    CacheServer server = null;
    try {
      try {
        server = cache.addCacheServer();
        server.setPort(port);
        server.start();
      } catch (Exception e) {
        e.printStackTrace();
        fail("Failed to create server");
      }
      try {
        var pf = PoolManager.createFactory();
        pf.addServer("localhost", port);
        pf.setSubscriptionEnabled(true);
        pf.setSubscriptionRedundancy(1);
        pf.setReadTimeout(20000);
        pf.setSubscriptionMessageTrackingTimeout(15000);
        pf.setSubscriptionAckInterval(5000);

        proxy = (PoolImpl) pf.create("clientPool");

        var eid = new EventID(new byte[0], 1, 1);

        if (proxy.verifyIfDuplicate(eid)) {
          fail(" eid should not be duplicate as it is a new entry");
        }

        seo = (SequenceIdAndExpirationObject) proxy.getThreadIdToSequenceIdMap()
            .get(new ThreadIdentifier(new byte[0], 1));
        assertFalse(seo.getAckSend());

        // should send the ack to server
        seo = (SequenceIdAndExpirationObject) proxy.getThreadIdToSequenceIdMap()
            .get(new ThreadIdentifier(new byte[0], 1));
        verifyAckSend(true);

        // New update on same threadId
        eid = new EventID(new byte[0], 1, 2);
        if (proxy.verifyIfDuplicate(eid)) {
          fail(" eid should not be duplicate as it is a new entry");
        }
        seo = (SequenceIdAndExpirationObject) proxy.getThreadIdToSequenceIdMap()
            .get(new ThreadIdentifier(new byte[0], 1));
        assertFalse(seo.getAckSend());

        // should send another ack to server
        seo = (SequenceIdAndExpirationObject) proxy.getThreadIdToSequenceIdMap()
            .get(new ThreadIdentifier(new byte[0], 1));
        verifyAckSend(true);

        // should expire with the this mentioned.
        verifyExpiry();
      } catch (Exception ex) {
        ex.printStackTrace();
        fail("Test testPeriodicAckSendByClient Failed");
      }
    } finally {
      if (server != null) {
        server.stop();
      }
    }
  }

  // No ack will be send if Redundancy level = 0
  @Test
  public void testNoAckSendByClient() {
    var port = getRandomAvailableTCPPort();
    CacheServer server = null;
    try {
      try {
        server = cache.addCacheServer();
        server.setPort(port);
        server.start();
      } catch (Exception e) {
        e.printStackTrace();
        fail("Failed to create server");
      }
      try {
        var pf = PoolManager.createFactory();
        pf.addServer("localhost", port);
        pf.setSubscriptionEnabled(true);
        pf.setSubscriptionRedundancy(1);
        pf.setReadTimeout(20000);
        pf.setSubscriptionMessageTrackingTimeout(8000);
        pf.setSubscriptionAckInterval(2000);

        proxy = (PoolImpl) pf.create("clientPool");

        var eid = new EventID(new byte[0], 1, 1);

        if (proxy.verifyIfDuplicate(eid)) {
          fail(" eid should not be duplicate as it is a new entry");
        }

        seo = (SequenceIdAndExpirationObject) proxy.getThreadIdToSequenceIdMap()
            .get(new ThreadIdentifier(new byte[0], 1));
        assertFalse(seo.getAckSend());

        // should not send an ack as redundancy level = 0;
        seo = (SequenceIdAndExpirationObject) proxy.getThreadIdToSequenceIdMap()
            .get(new ThreadIdentifier(new byte[0], 1));
        verifyAckSend(false);

        // should expire without sending an ack as redundancy level = 0.
        verifyExpiry();
      }

      catch (Exception ex) {
        ex.printStackTrace();
        fail("Test testPeriodicAckSendByClient Failed");
      }
    } finally {
      if (server != null) {
        server.stop();
      }
    }
  }

  private void verifyAckSend(final boolean expectedAckSend) {
    GeodeAwaitility.await().timeout(timeoutToVerifyAckSend).untilAsserted(() -> {
      assertEquals(expectedAckSend, seo.getAckSend());
    });
  }

  private void verifyExpiry() {
    GeodeAwaitility.await().timeout(timeoutToVerifyExpiry).untilAsserted(() -> {
      assertEquals(0, proxy.getThreadIdToSequenceIdMap().size());
    });
  }

}
