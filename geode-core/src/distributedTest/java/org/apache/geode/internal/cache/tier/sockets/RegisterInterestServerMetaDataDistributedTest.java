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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Disconnect.disconnectAllFromDS;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.apache.geode.test.dunit.NetworkUtils.getServerHostName;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.api.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.InternalClientCache;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheServer;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

@Category(ClientSubscriptionTest.class)
@SuppressWarnings("serial")
public class RegisterInterestServerMetaDataDistributedTest implements Serializable {

  private static final String PROXY_REGION_NAME = "PROXY_REGION_NAME";
  private static final String CACHING_PROXY_REGION_NAME = "CACHING_PROXY_REGION_NAME";

  private static InternalCache cache;
  private static InternalClientCache clientCache;

  private String hostName;
  private int serverPort1;

  private VM server;
  private VM client;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Before
  public void setUp() throws Exception {
    server = getVM(0);
    client = getVM(1);

    hostName = getServerHostName();

    serverPort1 = server.invoke(this::createServerCache);
    client.invoke(() -> createClientCacheWithTwoRegions(hostName, serverPort1));
  }

  @After
  public void tearDown() throws Exception {
    disconnectAllFromDS();

    cache = null;
    invokeInEveryVM(() -> cache = null);

    clientCache = null;
    invokeInEveryVM(() -> clientCache = null);
  }

  @Test
  public void registerInterestSingleKeyCanBeInvokedMultipleTimes() {
    client.invoke(() -> registerKey(PROXY_REGION_NAME, 1));
    client.invoke(() -> registerKey(CACHING_PROXY_REGION_NAME, 1));

    server.invoke(this::awaitServerMetaDataToContainClient);
    server.invoke(this::validateServerMetaDataKnowsThatClientRegisteredInterest);
    server.invoke(this::validateServerMetaDataKnowsWhichClientRegionIsEmpty);

    // invoke registerKey multiple times

    client.invoke(() -> registerKey(PROXY_REGION_NAME, 2));
    client.invoke(() -> registerKey(PROXY_REGION_NAME, 3));
    client.invoke(() -> registerKey(CACHING_PROXY_REGION_NAME, 4));

    server.invoke(this::validateServerMetaDataKnowsThatClientRegisteredInterest);
    server.invoke(this::validateServerMetaDataKnowsWhichClientRegionIsEmpty);
  }

  @Test
  public void registerInterestRegexCanBeInvokedMultipleTimes() {
    client.invoke(() -> registerRegex(PROXY_REGION_NAME, ".*"));
    client.invoke(() -> registerRegex(CACHING_PROXY_REGION_NAME, ".*"));

    server.invoke(this::awaitServerMetaDataToContainClient);
    server.invoke(this::validateServerMetaDataKnowsThatClientRegisteredInterest);
    server.invoke(this::validateServerMetaDataKnowsWhichClientRegionIsEmpty);

    // invoke registerRegex multiple times

    client.invoke(() -> registerRegex(PROXY_REGION_NAME, "8"));
    client.invoke(() -> registerRegex(PROXY_REGION_NAME, "7"));
    client.invoke(() -> registerRegex(CACHING_PROXY_REGION_NAME, "9"));

    server.invoke(this::validateServerMetaDataKnowsThatClientRegisteredInterest);
    server.invoke(this::validateServerMetaDataKnowsWhichClientRegionIsEmpty);
  }

  @Test
  public void registerInterestKeyListCanBeInvokedMultipleTimes() {
    client.invoke(() -> registerKeys(PROXY_REGION_NAME, 1, 2));
    client.invoke(() -> registerKeys(CACHING_PROXY_REGION_NAME, 1, 2));

    server.invoke(this::awaitServerMetaDataToContainClient);
    server.invoke(this::validateServerMetaDataKnowsThatClientRegisteredInterest);
    server.invoke(this::validateServerMetaDataKnowsWhichClientRegionIsEmpty);

    // invoke registerKeys multiple times

    client.invoke(() -> registerKeys(PROXY_REGION_NAME, 3, 4));
    client.invoke(() -> registerKeys(PROXY_REGION_NAME, 5));
    client.invoke(() -> registerKeys(CACHING_PROXY_REGION_NAME, 3));

    server.invoke(this::validateServerMetaDataKnowsThatClientRegisteredInterest);
    server.invoke(this::validateServerMetaDataKnowsWhichClientRegionIsEmpty);
  }

  private int createServerCache() throws IOException {
    cache = (InternalCache) new CacheFactory().create();

    RegionFactory<Integer, Object> regionFactory = cache.createRegionFactory();
    regionFactory.setDataPolicy(DataPolicy.REPLICATE);
    regionFactory.setScope(Scope.DISTRIBUTED_ACK);

    regionFactory.create(PROXY_REGION_NAME);
    regionFactory.create(CACHING_PROXY_REGION_NAME);

    CacheServer cacheServer = cache.addCacheServer();
    cacheServer.setPort(0);
    cacheServer.start();
    return cacheServer.getPort();
  }

  private void createClientCacheWithTwoRegions(final String host, final int port) {
    createClientCache();

    PoolFactory poolFactory = createPoolFactory();
    poolFactory.addServer(host, port);

    Pool pool = poolFactory.create(getClass().getSimpleName() + "-Pool");

    createRegionOnClient(PROXY_REGION_NAME, ClientRegionShortcut.PROXY, pool);
    Region<Integer, Object> region2 =
        createRegionOnClient(CACHING_PROXY_REGION_NAME, ClientRegionShortcut.CACHING_PROXY, pool);

    region2.getAttributesMutator().setCloningEnabled(true);
    assertThat(region2.getAttributes().getCloningEnabled()).isTrue();
  }

  private void createClientCache() {
    clientCache = (InternalClientCache) new ClientCacheFactory().create();
    assertThat(clientCache.isClient()).isTrue();
  }

  private PoolFactory createPoolFactory() {
    return PoolManager.createFactory().setMinConnections(3)
        .setSubscriptionEnabled(true).setSubscriptionRedundancy(0).setReadTimeout(10000)
        .setSocketBufferSize(32768);
  }

  private Region<Integer, Object> createRegionOnClient(final String regionName,
      final ClientRegionShortcut shortcut, final Pool pool) {
    ClientRegionFactory<Integer, Object> regionFactory =
        clientCache.createClientRegionFactory(shortcut);
    regionFactory.setPoolName(pool.getName());
    Region<Integer, Object> region = regionFactory.create(regionName);
    assertThat(region.getAttributes().getCloningEnabled()).isFalse();
    return region;
  }

  private CacheClientProxy getClientProxy() {
    CacheClientNotifier notifier = getCacheServer().getAcceptor().getCacheClientNotifier();
    return notifier.getClientProxies().stream().findFirst().orElse(null);
  }

  private InternalCacheServer getCacheServer() {
    return (InternalCacheServer) cache.getCacheServers().iterator().next();
  }

  /**
   * Register single key on given region
   */
  private void registerKey(final String regionName, final int key) {
    Region<Integer, Object> region = clientCache.getRegion(regionName);
    region.registerInterest(key);
  }

  /**
   * Register list of keys on given region
   */
  private void registerKeys(final String regionName, final int... keys) {
    Region<Object, ?> region = clientCache.getRegion(regionName);

    List<Integer> list = new ArrayList<>();
    for (int key : keys) {
      list.add(key);
    }

    region.registerInterest(list);
  }

  private void registerRegex(final String regionName, final String regex) {
    Region<Integer, Object> region = clientCache.getRegion(regionName);
    region.registerInterestRegex(regex);
  }

  private void awaitServerMetaDataToContainClient() {
    await()
        .untilAsserted(() -> assertThat(
            getCacheServer().getAcceptor().getCacheClientNotifier().getClientProxies().size())
                .isEqualTo(1));

    CacheClientProxy proxy = getClientProxy();
    assertThat(proxy).isNotNull();

    await().until(() -> getClientProxy().isAlive() && getClientProxy()
        .getRegionsWithEmptyDataPolicy().containsKey(SEPARATOR + PROXY_REGION_NAME));
  }

  private void validateServerMetaDataKnowsThatClientRegisteredInterest() {
    CacheClientProxy proxy = getClientProxy();
    assertThat(proxy.hasRegisteredInterested()).isTrue();
  }

  private void validateServerMetaDataKnowsWhichClientRegionIsEmpty() {
    CacheClientProxy proxy = getClientProxy();
    assertThat(proxy.getRegionsWithEmptyDataPolicy()).containsKey(SEPARATOR + PROXY_REGION_NAME);
    assertThat(proxy.getRegionsWithEmptyDataPolicy())
        .doesNotContainKey(SEPARATOR + CACHING_PROXY_REGION_NAME);
    assertThat(proxy.getRegionsWithEmptyDataPolicy()).hasSize(1);
    assertThat(proxy.getRegionsWithEmptyDataPolicy()).containsEntry(SEPARATOR + PROXY_REGION_NAME,
        0);
  }
}
