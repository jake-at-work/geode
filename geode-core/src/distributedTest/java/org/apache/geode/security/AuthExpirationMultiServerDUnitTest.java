/*
 *
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
 *
 */
package org.apache.geode.security;

import static org.apache.geode.cache.query.dunit.SecurityTestUtils.collectSecurityManagers;
import static org.apache.geode.cache.query.dunit.SecurityTestUtils.createAndExecuteCQ;
import static org.apache.geode.cache.query.dunit.SecurityTestUtils.getSecurityManager;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTH_INIT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.test.concurrent.FileBasedCountDownLatch;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ClientCacheRule;

@Category({SecurityTest.class})
public class AuthExpirationMultiServerDUnitTest {
  public static final String REPLICATE_REGION = "replicateRegion";
  public static final String PARTITION_REGION = "partitionRegion";
  private MemberVM locator;
  private MemberVM server1;
  private MemberVM server2;

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Rule
  public ClientCacheRule clientCacheRule = new ClientCacheRule();

  @Before
  public void setup() {
    locator = cluster.startLocatorVM(0, l -> l.withSecurityManager(ExpirableSecurityManager.class));
    var locatorPort = locator.getPort();
    server1 = cluster.startServerVM(1, s -> s.withSecurityManager(ExpirableSecurityManager.class)
        .withCredential("test", "test")
        .withRegion(RegionShortcut.REPLICATE, REPLICATE_REGION)
        .withRegion(RegionShortcut.PARTITION, PARTITION_REGION)
        .withConnectionToLocator(locatorPort));
    server2 = cluster.startServerVM(2, s -> s.withSecurityManager(ExpirableSecurityManager.class)
        .withCredential("test", "test")
        .withRegion(RegionShortcut.REPLICATE, REPLICATE_REGION)
        .withRegion(RegionShortcut.PARTITION, PARTITION_REGION)
        .withConnectionToLocator(locatorPort));
  }

  @After
  public void after() {
    UpdatableUserAuthInitialize.reset();
    closeSecurityManager();
  }

  @Test
  public void clientConnectToServerShouldReauthenticate() throws Exception {
    UpdatableUserAuthInitialize.setUser("user1");
    clientCacheRule
        .withProperty(SECURITY_CLIENT_AUTH_INIT, UpdatableUserAuthInitialize.class.getName())
        .withPoolSubscription(true)
        .withServerConnection(server1.getPort());
    clientCacheRule.createCache();
    var region1 = clientCacheRule.createProxyRegion(REPLICATE_REGION);
    var region2 = clientCacheRule.createProxyRegion(PARTITION_REGION);
    region1.put("0", "value0");
    region2.put("0", "value0");

    expireUserOnAllVms("user1");

    UpdatableUserAuthInitialize.setUser("user2");
    region1.put("1", "value1");
    region2.put("1", "value1");

    // locator only validates peer
    locator.invoke(() -> {
      var securityManager = getSecurityManager();
      var authorizedOps = securityManager.getAuthorizedOps();
      assertThat(authorizedOps.keySet()).containsExactly("test");
      var unAuthorizedOps = securityManager.getUnAuthorizedOps();
      assertThat(unAuthorizedOps.keySet()).isEmpty();
    });

    // client is connected to server1, server1 gets all the initial contact,
    // authorization checks happens here
    server1.invoke(() -> {
      var securityManager = getSecurityManager();
      var authorizedOps = securityManager.getAuthorizedOps();
      assertThat(authorizedOps.get("user1")).containsExactlyInAnyOrder(
          "DATA:WRITE:replicateRegion:0", "DATA:WRITE:partitionRegion:0");
      assertThat(authorizedOps.get("user2")).containsExactlyInAnyOrder(
          "DATA:WRITE:replicateRegion:1", "DATA:WRITE:partitionRegion:1");
      var unAuthorizedOps = securityManager.getUnAuthorizedOps();
      assertThat(unAuthorizedOps.get("user1"))
          .containsExactly("DATA:WRITE:replicateRegion:1");
    });

    // server2 performs no authorization checks
    server2.invoke(() -> {
      var securityManager = getSecurityManager();
      var authorizedOps = securityManager.getAuthorizedOps();
      var unAuthorizedOps = securityManager.getUnAuthorizedOps();
      assertThat(authorizedOps.size()).isEqualTo(0);
      assertThat(unAuthorizedOps.size()).isEqualTo(0);
    });

    MemberVM.invokeInEveryMember(() -> {
      var cache = ClusterStartupRule.getCache();
      var serverRegion1 = cache.getRegion(REPLICATE_REGION);
      assertThat(serverRegion1.size()).isEqualTo(2);
      var serverRegion2 = cache.getRegion(PARTITION_REGION);
      assertThat(serverRegion2.size()).isEqualTo(2);
    }, server1, server2);
  }

  @Test
  public void clientConnectToLocatorShouldReAuthenticate() throws Exception {
    UpdatableUserAuthInitialize.setUser("user1");
    clientCacheRule
        .withProperty(SECURITY_CLIENT_AUTH_INIT, UpdatableUserAuthInitialize.class.getName())
        .withPoolSubscription(true)
        .withLocatorConnection(locator.getPort());
    clientCacheRule.createCache();
    var region = clientCacheRule.createProxyRegion(PARTITION_REGION);
    expireUserOnAllVms("user1");
    UpdatableUserAuthInitialize.setUser("user2");
    IntStream.range(0, 100).forEach(i -> region.put(i, "value" + i));

    var consolidated = collectSecurityManagers(server1, server2);
    var authorized = consolidated.getAuthorizedOps();
    var unAuthorized = consolidated.getUnAuthorizedOps();

    assertThat(authorized.keySet()).containsExactly("user2");
    assertThat(authorized.get("user2")).hasSize(100);

    assertThat(unAuthorized.keySet()).containsExactly("user1");
    assertThat(unAuthorized.get("user1")).hasSize(1);
  }

  @Test
  public void clientConnectToLocatorShouldNotAllowOperationIfUserIsNotRefreshed() throws Exception {
    UpdatableUserAuthInitialize.setUser("user1");
    clientCacheRule
        .withProperty(SECURITY_CLIENT_AUTH_INIT, UpdatableUserAuthInitialize.class.getName())
        .withPoolSubscription(true)
        .withLocatorConnection(locator.getPort());
    clientCacheRule.createCache();
    var region = clientCacheRule.createProxyRegion(PARTITION_REGION);
    expireUserOnAllVms("user1");
    for (var i = 1; i < 100; i++) {
      try {
        region.put(1, "value1");
        fail("Exception expected");
      } catch (Exception e) {
        assertThat(e).isInstanceOf(ServerOperationException.class);
        assertThat(e.getCause()).isInstanceOfAny(AuthenticationFailedException.class,
            AuthenticationRequiredException.class, AuthenticationExpiredException.class);
      }
    }
    var consolidated = collectSecurityManagers(server1, server2);
    assertThat(consolidated.getAuthorizedOps().keySet()).isEmpty();
  }

  @Test
  public void cqWithMultiServer() throws Exception {
    var locatorPort = locator.getPort();
    UpdatableUserAuthInitialize.setUser("user1");
    clientCacheRule
        .withProperty(SECURITY_CLIENT_AUTH_INIT, UpdatableUserAuthInitialize.class.getName())
        .withPoolSubscription(true)
        .withLocatorConnection(locatorPort);
    var cache = clientCacheRule.createCache();
    var listener =
        createAndExecuteCQ(cache.getQueryService(), "cq1", "select * from /" + PARTITION_REGION);

    UpdatableUserAuthInitialize.setUser("user2");
    expireUserOnAllVms("user1");
    doPutsUsingAnotherClient(locatorPort, "user3", 100);

    // make sure listener still gets all the events
    await().untilAsserted(() -> assertThat(listener.getKeys()).hasSize(100));
    var securityManager = collectSecurityManagers(server1, server2);
    assertThat(securityManager.getAuthorizedOps().get("user1"))
        .containsExactly("DATA:READ:partitionRegion");
    assertThat(securityManager.getUnAuthorizedOps().get("user1"))
        .containsExactly("DATA:READ:partitionRegion:key0");
  }

  private void doPutsUsingAnotherClient(int locatorPort, String user, int size) throws Exception {
    // create another client to do puts
    var client = cluster.startClientVM(3,
        c -> c.withProperty(SECURITY_CLIENT_AUTH_INIT, UpdatableUserAuthInitialize.class.getName())
            .withPoolSubscription(true)
            .withLocatorConnection(locatorPort));
    client.invoke(() -> {
      UpdatableUserAuthInitialize.setUser(user);
      var proxyRegion =
          ClusterStartupRule.clientCacheRule.createProxyRegion(PARTITION_REGION);
      IntStream.range(0, size).forEach(i -> proxyRegion.put("key" + i, "value" + i));
    });
  }

  @Test
  public void registerInterestsWithMultiServers() throws Exception {
    var locatorPort = locator.getPort();
    UpdatableUserAuthInitialize.setUser("user1");
    var cache = clientCacheRule
        .withProperty(SECURITY_CLIENT_AUTH_INIT, UpdatableUserAuthInitialize.class.getName())
        .withPoolSubscription(true)
        .withLocatorConnection(locatorPort).createCache();
    var clientRegion =
        cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
            .create(PARTITION_REGION);
    clientRegion.registerInterestForAllKeys();
    UpdatableUserAuthInitialize.setUser("User2");

    expireUserOnAllVms("user1");
    doPutsUsingAnotherClient(locatorPort, "user3", 100);

    // make sure clientRegion gets all the events
    await().untilAsserted(() -> assertThat(clientRegion).hasSize(100));
    var securityManager = collectSecurityManagers(server1, server2);
    assertThat(securityManager.getAuthorizedOps().get("user1"))
        .containsExactly("DATA:READ:partitionRegion");
    assertThat(securityManager.getUnAuthorizedOps().get("user1"))
        .containsExactly("DATA:READ:partitionRegion:key0");
  }

  @Test
  public void consecutivePut() throws Exception {
    var latch = new FileBasedCountDownLatch(1);
    var locatorPort = locator.getPort();
    // do consecutive puts using a client
    var client = cluster.startClientVM(3,
        c -> c.withProperty(SECURITY_CLIENT_AUTH_INIT, UpdatableUserAuthInitialize.class.getName())
            .withCacheSetup(ccf -> ccf.setPoolMaxConnections(2))
            .withLocatorConnection(locatorPort));
    AsyncInvocation<Void> invokePut = client.invokeAsync(() -> {
      UpdatableUserAuthInitialize.setUser("user1");
      var proxyRegion =
          ClusterStartupRule.getClientCache()
              .createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
              .create(PARTITION_REGION);
      for (var i = 0; i < 1000; i++) {
        // make sure at least some data is put by user2
        if (i == 900) {
          latch.await();
        }
        proxyRegion.put("key" + i, "value" + i);
      }
    });

    client.invoke(() -> {
      // wait till at least one of the data is in the region to expire the user
      await().untilAsserted(() -> assertThat(getProxyRegion()).isNotNull());
      await().untilAsserted(() -> assertThat(getProxyRegion().size()).isGreaterThan(1));
      UpdatableUserAuthInitialize.setUser("user2");
    });
    expireUserOnAllVms("user1");
    latch.countDown();
    invokePut.await();

    var securityManager = collectSecurityManagers(server1, server2);
    var authorizedOps = securityManager.getAuthorizedOps();

    assertThat(authorizedOps).hasSize(2);
    assertThat(authorizedOps.get("user1").size() + authorizedOps.get("user2").size())
        .as(String.format("Combined sizes of user1 %s and user2 %s",
            authorizedOps.get("user1").size(),
            authorizedOps.get("user2").size()))
        .isEqualTo(1000);
    var unAuthorizedOps = securityManager.getUnAuthorizedOps();
    assertThat(unAuthorizedOps).hasSize(1);
    // user1 may not be unauthorized for just 1 operations, puts maybe done by different
    // connections
    assertThat(unAuthorizedOps.get("user1")).isNotEmpty();
  }

  @Test
  public void peerStillCommunicateWhenCredentialExpired() throws Exception {
    // expire the peer to peer users
    expireUserOnAllVms("test");
    // do puts using a client
    UpdatableUserAuthInitialize.setUser("user1");
    clientCacheRule
        .withProperty(SECURITY_CLIENT_AUTH_INIT, UpdatableUserAuthInitialize.class.getName())
        .withPoolSubscription(true)
        .withLocatorConnection(locator.getPort());
    clientCacheRule.createCache();
    var region = clientCacheRule.createProxyRegion(REPLICATE_REGION);
    IntStream.range(0, 10).forEach(i -> region.put("key" + i, "value" + i));

    // assert that data still get into all servers
    MemberVM.invokeInEveryMember(() -> {
      var serverRegion =
          ClusterStartupRule.getCache().getRegion(REPLICATE_REGION);
      assertThat(serverRegion).hasSize(10);
    }, server1, server2);
  }

  @Test
  public void putAll() throws Exception {
    var locatorPort = locator.getPort();
    // do putAll using a client
    var client = cluster.startClientVM(3,
        c -> c.withProperty(SECURITY_CLIENT_AUTH_INIT, UpdatableUserAuthInitialize.class.getName())
            .withPoolSubscription(true)
            .withLocatorConnection(locatorPort));
    var invokePut = client.invokeAsync(() -> {
      UpdatableUserAuthInitialize.setUser("user1");
      var proxyRegion =
          ClusterStartupRule.getClientCache()
              .createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
              .create(PARTITION_REGION);
      Map<String, String> values = new HashMap<>();
      IntStream.range(0, 1000).forEach(i -> values.put("key" + i, "value" + i));
      proxyRegion.putAll(values);
    });

    client.invoke(() -> {
      await().until(() -> getProxyRegion() != null);
      await().until(() -> !getProxyRegion().isEmpty());
      UpdatableUserAuthInitialize.setUser("user2");
    });

    expireUserOnAllVms("user1");
    invokePut.await();

    var securityManager = collectSecurityManagers(server1, server2);
    assertThat(securityManager.getAuthorizedOps()).hasSize(1);
    assertThat(securityManager.getAuthorizedOps().get("user1"))
        .containsExactly("DATA:WRITE:partitionRegion");
    assertThat(securityManager.getUnAuthorizedOps()).isEmpty();
  }

  private static Region<Object, Object> getProxyRegion() {
    return ClusterStartupRule.getClientCache().getRegion(PARTITION_REGION);
  }

  private void expireUserOnAllVms(String user) {
    MemberVM.invokeInEveryMember(() -> {
      getSecurityManager().addExpiredUser(user);
    }, locator, server1, server2);
  }

  private void closeSecurityManager() {
    MemberVM.invokeInEveryMember(() -> {
      getSecurityManager().close();
    }, locator, server1, server2);
  }
}
