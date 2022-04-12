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
package org.apache.geode.cache.client.internal;

import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.CancelCriterion;
import org.apache.geode.Statistics;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.client.internal.pooling.ConnectionManagerImpl;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PoolFactoryImpl;
import org.apache.geode.internal.cache.PoolManagerImpl;
import org.apache.geode.internal.monitoring.ThreadsMonitoring;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class PoolImplTest {

  @Test
  public void calculateRetryAttemptsDoesNotDecrementRetryCountForFailureWithUnexpectedSocketClose() {
    var servers = mock(List.class);
    when(servers.size()).thenReturn(1);
    var connectionSource = mock(ConnectionSource.class);
    when(connectionSource.getAllServers()).thenReturn(servers);
    var serverConnectivityException =
        mock(ServerConnectivityException.class);
    when(serverConnectivityException.getMessage())
        .thenReturn(ConnectionManagerImpl.UNEXPECTED_SOCKET_CLOSED_MSG);

    var poolImpl = spy(getPool(PoolFactory.DEFAULT_RETRY_ATTEMPTS));
    when(poolImpl.getConnectionSource()).thenReturn(connectionSource);

    assertThat(poolImpl.calculateRetryAttempts(serverConnectivityException)).isEqualTo(1);
  }

  @Test
  public void calculateRetryAttemptsDoesNotDecrementRetryCountForFailureDuringBorrowConnection() {
    var servers = mock(List.class);
    when(servers.size()).thenReturn(1);
    var connectionSource = mock(ConnectionSource.class);
    when(connectionSource.getAllServers()).thenReturn(servers);
    var serverConnectivityException =
        mock(ServerConnectivityException.class);
    when(serverConnectivityException.getMessage())
        .thenReturn(ConnectionManagerImpl.BORROW_CONN_ERROR_MSG);

    var poolImpl = spy(getPool(PoolFactory.DEFAULT_RETRY_ATTEMPTS));
    when(poolImpl.getConnectionSource()).thenReturn(connectionSource);

    assertThat(poolImpl.calculateRetryAttempts(serverConnectivityException)).isEqualTo(1);
  }

  @Test
  public void calculateRetryAttemptsDecrementsRetryCountForFailureAfterSendingTheRequest() {
    var servers = mock(List.class);
    when(servers.size()).thenReturn(1);
    var connectionSource = mock(ConnectionSource.class);
    when(connectionSource.getAllServers()).thenReturn(servers);
    var serverConnectivityException =
        mock(ServerConnectivityException.class);
    when(serverConnectivityException.getMessage())
        .thenReturn(ConnectionManagerImpl.SOCKET_TIME_OUT_MSG);

    var poolImpl = spy(getPool(PoolFactory.DEFAULT_RETRY_ATTEMPTS));
    when(poolImpl.getConnectionSource()).thenReturn(connectionSource);

    assertThat(poolImpl.calculateRetryAttempts(serverConnectivityException)).isEqualTo(0);
  }

  @Test
  public void calculateRetryAttemptsReturnsTheRetyCountConfiguredWithPool() {
    var retryCount = 1;
    var servers = mock(List.class);
    when(servers.size()).thenReturn(1);
    var connectionSource = mock(ConnectionSource.class);
    when(connectionSource.getAllServers()).thenReturn(servers);
    var serverConnectivityException =
        mock(ServerConnectivityException.class);
    when(serverConnectivityException.getMessage()).thenReturn("Timeout Exception");

    var poolImpl = spy(getPool(retryCount));
    when(poolImpl.getConnectionSource()).thenReturn(connectionSource);

    assertThat(poolImpl.calculateRetryAttempts(serverConnectivityException)).isEqualTo(retryCount);
  }

  private PoolImpl getPool(int retryAttemptsAttribute) {
    final var distributionConfig = mock(DistributionConfig.class);
    doReturn(new SecurableCommunicationChannel[] {}).when(distributionConfig)
        .getSecurableCommunicationChannels();

    final var properties = new Properties();
    properties.put(DURABLE_CLIENT_ID, "1");

    final var statistics = mock(Statistics.class);

    final var poolAttributes =
        mock(PoolFactoryImpl.PoolAttributes.class);

    /*
     * These are the minimum pool attributes required
     * so that basic validation and setup completes successfully. The values of
     * these attributes have no importance to the assertions of the test itself.
     */
    doReturn(1).when(poolAttributes).getMaxConnections();
    doReturn((long) 10e8).when(poolAttributes).getPingInterval();
    doReturn(retryAttemptsAttribute).when(poolAttributes).getRetryAttempts();

    final var cancelCriterion = mock(CancelCriterion.class);

    final var internalCache = mock(InternalCache.class);
    doReturn(cancelCriterion).when(internalCache).getCancelCriterion();

    final var internalDistributedSystem =
        mock(InternalDistributedSystem.class);
    doReturn(distributionConfig).when(internalDistributedSystem).getConfig();
    doReturn(properties).when(internalDistributedSystem).getProperties();
    doReturn(statistics).when(internalDistributedSystem).createAtomicStatistics(any(), anyString());

    final var poolManager = mock(PoolManagerImpl.class);
    doReturn(true).when(poolManager).isNormal();

    final var tMonitoring = mock(ThreadsMonitoring.class);

    return PoolImpl.create(poolManager, "pool", poolAttributes, new LinkedList<>(),
        internalDistributedSystem, internalCache, tMonitoring);
  }

}
