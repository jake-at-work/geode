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

import static org.apache.geode.test.dunit.VM.getHostName;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.TransactionDataNotColocatedException;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

public class ClientServerNotColocatedTransactionDistributedTest implements Serializable {
  private String hostName;
  private String uniqueName;
  private String regionName;
  private String replicateRegionName;
  private VM server1;
  private VM server2;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public ClientCacheRule clientCacheRule = new ClientCacheRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUp() {
    server1 = getVM(0);
    server2 = getVM(1);

    hostName = getHostName();
    uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();
    regionName = uniqueName + "_region";
    replicateRegionName = uniqueName + "_replicateRegion";
  }

  @Test
  public void getOnRemoteNodeInATransactionThrowsTransactionDataNotColocatedException() {
    var initialPuts = 4;
    setupClientAndServer(initialPuts);
    var txManager =
        (TXManagerImpl) clientCacheRule.getClientCache().getCacheTransactionManager();
    Region<Integer, Integer> region = clientCacheRule.getClientCache().getRegion(regionName);
    txManager.begin();
    try {
      var caughtException = catchThrowable(() -> doTransactionalGets(initialPuts, region));
      assertThat(caughtException).isInstanceOf(TransactionDataNotColocatedException.class);
    } finally {
      txManager.commit();
    }
  }

  private void setupClientAndServer(int initialPuts) {
    int port1 = server1.invoke(() -> createServerRegion(2, false, 0));
    int port2 = server2.invoke(() -> createServerRegion(2, false, 0));
    server1.invoke(() -> doPuts(initialPuts));

    createClientRegion(port1, port2);
  }

  private int createServerRegion(int totalNumBuckets, boolean isAccessor, int redundancy)
      throws Exception {
    var factory = new PartitionAttributesFactory();
    factory.setTotalNumBuckets(totalNumBuckets).setRedundantCopies(redundancy);
    if (isAccessor) {
      factory.setLocalMaxMemory(0);
    }
    var partitionAttributes = factory.create();
    cacheRule.getOrCreateCache().createRegionFactory(RegionShortcut.PARTITION)
        .setPartitionAttributes(partitionAttributes).create(regionName);

    var server = cacheRule.getCache().addCacheServer();
    server.setPort(0);
    server.start();
    return server.getPort();
  }

  private void createClientRegion(int... ports) {
    clientCacheRule.createClientCache();
    var pool = getPool(ports);
    ClientRegionFactory crf =
        clientCacheRule.getClientCache().createClientRegionFactory(ClientRegionShortcut.LOCAL);
    crf.setPoolName(pool.getName());
    crf.create(regionName);
  }

  private void doPuts(int numOfEntries) {
    for (var i = 0; i <= numOfEntries; i++) {
      cacheRule.getCache().getRegion(regionName).put(i, i);
    }
  }

  private PoolImpl getPool(int... ports) {
    var factory = PoolManager.createFactory();
    for (var port : ports) {
      factory.addServer(hostName, port);
    }
    factory.setReadTimeout(12000).setSocketBufferSize(1000);

    return (PoolImpl) factory.create(uniqueName);
  }

  private void doTransactionalGets(int initialPuts, Region<Integer, Integer> region) {
    for (var i = 1; i < initialPuts; i++) {
      region.get(i);
    }
  }

  @Test
  public void getAllOnMultipleNodesInATransactionThrowsTransactionDataNotColocatedException() {
    var initialPuts = 4;
    setupClientAndServer(initialPuts);
    var txManager =
        (TXManagerImpl) clientCacheRule.getClientCache().getCacheTransactionManager();
    Region<Integer, Integer> region = clientCacheRule.getClientCache().getRegion(regionName);
    txManager.begin();
    try {
      var caughtException = catchThrowable(() -> doTransactionalGetAll(initialPuts, region));
      assertThat(caughtException).isInstanceOf(TransactionDataNotColocatedException.class);
    } finally {
      txManager.commit();
    }
  }

  private void doTransactionalGetAll(int initialPuts, Region<Integer, Integer> region) {
    List<Integer> list = new ArrayList<>();
    for (var i = 1; i < initialPuts; i++) {
      list.add(i);
    }
    region.getAll(list);
  }

  @Test
  public void putOnRemoteNodeInATransactionThrowsTransactionDataNotColocatedException() {
    var initialPuts = 4;
    setupClientAndServer(initialPuts);
    var txManager =
        (TXManagerImpl) clientCacheRule.getClientCache().getCacheTransactionManager();
    Region<Integer, Integer> region = clientCacheRule.getClientCache().getRegion(regionName);
    txManager.begin();
    try {
      var caughtException = catchThrowable(() -> doTransactionalPuts(initialPuts, region));
      assertThat(caughtException).isInstanceOf(TransactionDataNotColocatedException.class);
    } finally {
      txManager.commit();
    }
  }

  private void doTransactionalPuts(int initialPuts, Region<Integer, Integer> region) {
    for (var i = 1; i < initialPuts; i++) {
      region.put(i, i + 100);
    }
  }

  @Test
  public void putAllOnMultipleNodesInATransactionThrowsTransactionDataNotColocatedException() {
    var initialPuts = 4;
    setupClientAndServer(initialPuts);
    var txManager =
        (TXManagerImpl) clientCacheRule.getClientCache().getCacheTransactionManager();
    Region<Integer, Integer> region = clientCacheRule.getClientCache().getRegion(regionName);
    txManager.begin();
    try {
      var caughtException = catchThrowable(() -> doTransactionalPutAll(initialPuts, region));
      assertThat(caughtException).isInstanceOf(TransactionDataNotColocatedException.class);
    } finally {
      txManager.commit();
    }
  }

  private void doTransactionalPutAll(int initialPuts, Region<Integer, Integer> region) {
    Map<Integer, Integer> map = new HashMap<>();
    for (var i = 1; i < initialPuts; i++) {
      map.put(i, i + 100);
    }
    region.putAll(map);
  }

  @Test
  public void invalidateOnRemoteNodeInATransactionThrowsTransactionDataNotColocatedException() {
    var initialPuts = 4;
    setupClientAndServer(initialPuts);
    var txManager =
        (TXManagerImpl) clientCacheRule.getClientCache().getCacheTransactionManager();
    Region<Integer, Integer> region = clientCacheRule.getClientCache().getRegion(regionName);
    txManager.begin();
    try {
      var caughtException =
          catchThrowable(() -> doTransactionalInvalidates(initialPuts, region));
      assertThat(caughtException).isInstanceOf(TransactionDataNotColocatedException.class);
    } finally {
      txManager.commit();
    }
  }

  private void doTransactionalInvalidates(int initialPuts, Region<Integer, Integer> region) {
    for (var i = 1; i < initialPuts; i++) {
      region.invalidate(i);
    }
  }

  @Test
  public void destroyOnRemoteNodeInATransactionThrowsTransactionDataNotColocatedException() {
    var initialPuts = 4;
    setupClientAndServer(initialPuts);
    var txManager =
        (TXManagerImpl) clientCacheRule.getClientCache().getCacheTransactionManager();
    Region<Integer, Integer> region = clientCacheRule.getClientCache().getRegion(regionName);
    txManager.begin();
    try {
      var caughtException =
          catchThrowable(() -> doTransactionalDestroys(initialPuts, region));
      assertThat(caughtException).isInstanceOf(TransactionDataNotColocatedException.class);
    } finally {
      txManager.commit();
    }
  }

  private void doTransactionalDestroys(int initialPuts, Region<Integer, Integer> region) {
    for (var i = 1; i < initialPuts; i++) {
      region.destroy(i);
    }
  }

  @Test
  public void getEntryOnRemoteNodeInATransactionThrowsTransactionDataNotColocatedException() {
    var initialPuts = 4;
    setupClientAndServer(initialPuts);
    var txManager =
        (TXManagerImpl) clientCacheRule.getClientCache().getCacheTransactionManager();
    Region<Integer, Integer> region = clientCacheRule.getClientCache().getRegion(regionName);
    txManager.begin();
    try {
      var caughtException =
          catchThrowable(() -> doTransactionalGetEntries(initialPuts, region));
      assertThat(caughtException).isInstanceOf(TransactionDataNotColocatedException.class);
    } finally {
      txManager.commit();
    }
  }

  private void doTransactionalGetEntries(int initialPuts, Region<Integer, Integer> region) {
    for (var i = 1; i < initialPuts; i++) {
      region.getEntry(i);
    }
  }

  @Test
  public void containKeyOnRemoteNodeInATransactionThrowsTransactionDataNotColocatedException() {
    var initialPuts = 4;
    setupClientAndServer(initialPuts);
    var txManager =
        (TXManagerImpl) clientCacheRule.getClientCache().getCacheTransactionManager();
    Region<Integer, Integer> region = clientCacheRule.getClientCache().getRegion(regionName);
    txManager.begin();
    try {
      var caughtException =
          catchThrowable(() -> doTransactionalContainsKey(initialPuts, region));
      assertThat(caughtException).isInstanceOf(TransactionDataNotColocatedException.class);
    } finally {
      txManager.commit();
    }
  }

  private void doTransactionalContainsKey(int initialPuts, Region<Integer, Integer> region) {
    for (var i = 1; i < initialPuts; i++) {
      region.containsKey(i);
    }
  }

  @Test
  public void containsValueForKeyOnRemoteNodeInATransactionThrowsTransactionDataNotColocatedException() {
    var initialPuts = 4;
    setupClientAndServer(initialPuts);
    var txManager =
        (TXManagerImpl) clientCacheRule.getClientCache().getCacheTransactionManager();
    Region<Integer, Integer> region = clientCacheRule.getClientCache().getRegion(regionName);
    txManager.begin();
    try {
      var caughtException =
          catchThrowable(() -> doTransactionalContainsValueForKey(initialPuts, region));
      assertThat(caughtException).isInstanceOf(TransactionDataNotColocatedException.class);
    } finally {
      txManager.commit();
    }
  }

  private void doTransactionalContainsValueForKey(int initialPuts,
      Region<Integer, Integer> region) {
    for (var i = 1; i < initialPuts; i++) {
      region.containsValueForKey(i);
    }
  }

  @Test
  public void getOnReplicateRegionThenPartitionedRegionInATransactionThrowsTransactionDataNotColocatedException() {
    var initialPuts = 4;
    setupClientAndServerWithTwoRegions(initialPuts);
    var txManager =
        (TXManagerImpl) clientCacheRule.getClientCache().getCacheTransactionManager();
    txManager.begin();
    try {
      // do first operation on replicate region
      Region<Integer, Integer> replicateRegion =
          clientCacheRule.getClientCache().getRegion(replicateRegionName);
      replicateRegion.get(1);
      Region<Integer, Integer> partitionedRegion =
          clientCacheRule.getClientCache().getRegion(regionName);
      try {
        partitionedRegion.get(1);
      } catch (TransactionException exception) {
        assertThat(exception).isInstanceOf(TransactionDataNotColocatedException.class);
      }
    } finally {
      txManager.commit();
    }
  }

  private void doPutsInRegions(int numOfEntries) {
    for (var i = 0; i <= numOfEntries; i++) {
      cacheRule.getCache().getRegion(regionName).put(i, i);
      cacheRule.getCache().getRegion(replicateRegionName).put(i, i);
    }
  }

  private void setupClientAndServerWithTwoRegions(int initialPuts) {
    int port1 = server1.invoke(() -> createServerRegion(2, false, 0));
    server1.invoke(this::createServerReplicateRegion);
    int port2 = server2.invoke(() -> createServerRegion(2, false, 0));
    server2.invoke(this::createServerReplicateRegion);
    server1.invoke(() -> doPutsInRegions(initialPuts));

    createClientRegion(port1, port2);
    createClientRegion(replicateRegionName);
  }

  private void createServerReplicateRegion() {
    cacheRule.getOrCreateCache().createRegionFactory(RegionShortcut.REPLICATE)
        .create(replicateRegionName);
  }

  private void createClientRegion(String regionName) {
    var pool = clientCacheRule.getClientCache().getDefaultPool();
    clientCacheRule.getClientCache().createClientRegionFactory(ClientRegionShortcut.LOCAL)
        .setPoolName(pool.getName())
        .create(regionName);
  }

  @Test
  public void putOnReplicateRegionThenPartitionedRegionInATransactionThrowsTransactionDataNotColocatedException() {
    var initialPuts = 4;
    setupClientAndServerWithTwoRegions(initialPuts);
    var txManager =
        (TXManagerImpl) clientCacheRule.getClientCache().getCacheTransactionManager();
    txManager.begin();
    try {
      // do first operation on replicate region
      Region<Integer, Integer> replicateRegion =
          clientCacheRule.getClientCache().getRegion(replicateRegionName);
      replicateRegion.put(1, 5);
      Region<Integer, Integer> partitionedRegion =
          clientCacheRule.getClientCache().getRegion(regionName);
      try {
        partitionedRegion.put(1, 6);
      } catch (TransactionException exception) {
        assertThat(exception).isInstanceOf(TransactionDataNotColocatedException.class);
      }
    } finally {
      txManager.commit();
    }
  }

  @Test
  public void putAllOnReplicateRegionThenPartitionedRegionInATransactionThrowsTransactionDataNotColocatedException() {
    var initialPuts = 4;
    setupClientAndServerWithTwoRegions(initialPuts);
    var txManager =
        (TXManagerImpl) clientCacheRule.getClientCache().getCacheTransactionManager();
    txManager.begin();
    try {
      // do first operation on replicate region
      Region<Integer, Integer> replicateRegion =
          clientCacheRule.getClientCache().getRegion(replicateRegionName);
      HashMap<Integer, Integer> map = new HashMap();
      map.put(1, 5);
      replicateRegion.putAll(map);
      Region<Integer, Integer> partitionedRegion =
          clientCacheRule.getClientCache().getRegion(regionName);

      try {
        partitionedRegion.putAll(map);
      } catch (TransactionException exception) {
        assertThat(exception).isInstanceOf(TransactionDataNotColocatedException.class);
      }
    } finally {
      txManager.commit();
    }
  }

  @Test
  public void invalidateOnReplicateRegionThenPartitionedRegionInATransactionThrowsTransactionDataNotColocatedException() {
    var initialPuts = 4;
    setupClientAndServerWithTwoRegions(initialPuts);
    var txManager =
        (TXManagerImpl) clientCacheRule.getClientCache().getCacheTransactionManager();
    txManager.begin();
    try {
      // do first operation on replicate region
      Region<Integer, Integer> replicateRegion =
          clientCacheRule.getClientCache().getRegion(replicateRegionName);
      replicateRegion.invalidate(1);
      Region<Integer, Integer> partitionedRegion =
          clientCacheRule.getClientCache().getRegion(regionName);
      try {
        partitionedRegion.invalidate(1);
      } catch (TransactionException exception) {
        assertThat(exception).isInstanceOf(TransactionDataNotColocatedException.class);
      }
    } finally {
      txManager.commit();
    }
  }

  @Test
  public void destroyOnReplicateRegionThenPartitionedRegionInATransactionThrowsTransactionDataNotColocatedException() {
    var initialPuts = 4;
    setupClientAndServerWithTwoRegions(initialPuts);
    var txManager =
        (TXManagerImpl) clientCacheRule.getClientCache().getCacheTransactionManager();
    txManager.begin();
    try {
      // do first operation on replicate region
      Region<Integer, Integer> replicateRegion =
          clientCacheRule.getClientCache().getRegion(replicateRegionName);
      replicateRegion.destroy(1);
      Region<Integer, Integer> partitionedRegion =
          clientCacheRule.getClientCache().getRegion(regionName);
      try {
        partitionedRegion.destroy(1);
      } catch (TransactionException exception) {
        assertThat(exception).isInstanceOf(TransactionDataNotColocatedException.class);
      }
    } finally {
      txManager.commit();
    }
  }
}
