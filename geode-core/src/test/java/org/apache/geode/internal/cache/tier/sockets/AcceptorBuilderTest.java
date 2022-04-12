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

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.STRICT_STUBS;

import java.util.function.Supplier;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheServer;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier.CacheClientNotifierProvider;
import org.apache.geode.internal.cache.tier.sockets.ClientHealthMonitor.ClientHealthMonitorProvider;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category(ClientServerTest.class)
public class AcceptorBuilderTest {

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(STRICT_STUBS);

  @Test
  public void forServerSetsPortFromServer() {
    var server = mock(InternalCacheServer.class);
    var port = 42;
    when(server.getPort()).thenReturn(port);
    var builder = new AcceptorBuilder();

    builder.forServer(server);

    assertThat(builder.getPort()).isEqualTo(port);
  }

  @Test
  public void forServerSetsBindAddressFromServer() {
    var server = mock(InternalCacheServer.class);
    var bindAddress = "bind-address";
    when(server.getBindAddress()).thenReturn(bindAddress);
    var builder = new AcceptorBuilder();

    builder.forServer(server);

    assertThat(builder.getBindAddress()).isEqualTo(bindAddress);
  }

  @Test
  public void forServerSetsNotifyBySubscriptionFromServer() {
    var server = mock(InternalCacheServer.class);
    var notifyBySubscription = true;
    when(server.getNotifyBySubscription()).thenReturn(notifyBySubscription);
    var builder = new AcceptorBuilder();

    builder.forServer(server);

    assertThat(builder.isNotifyBySubscription()).isEqualTo(notifyBySubscription);
  }

  @Test
  public void forServerSetsSocketBufferSizeFromServer() {
    var server = mock(InternalCacheServer.class);
    var socketBufferSize = 84;
    when(server.getSocketBufferSize()).thenReturn(socketBufferSize);
    var builder = new AcceptorBuilder();

    builder.forServer(server);

    assertThat(builder.getSocketBufferSize()).isEqualTo(socketBufferSize);
  }

  @Test
  public void forServerSetsMaximumTimeBetweenPingsFromServer() {
    var server = mock(InternalCacheServer.class);
    var maximumTimeBetweenPings = 84;
    when(server.getMaximumTimeBetweenPings()).thenReturn(maximumTimeBetweenPings);
    var builder = new AcceptorBuilder();

    builder.forServer(server);

    assertThat(builder.getMaximumTimeBetweenPings()).isEqualTo(maximumTimeBetweenPings);
  }

  @Test
  public void forServerSetsCacheFromServer() {
    var server = mock(InternalCacheServer.class);
    var cache = mock(InternalCache.class);
    when(server.getCache()).thenReturn(cache);
    var builder = new AcceptorBuilder();

    builder.forServer(server);

    assertThat(builder.getCache()).isEqualTo(cache);
  }

  @Test
  public void forServerSetsMaxConnectionsFromServer() {
    var server = mock(InternalCacheServer.class);
    var maxConnections = 99;
    when(server.getMaxConnections()).thenReturn(maxConnections);
    var builder = new AcceptorBuilder();

    builder.forServer(server);

    assertThat(builder.getMaxConnections()).isEqualTo(maxConnections);
  }

  @Test
  public void forServerSetsMaxThreadsFromServer() {
    var server = mock(InternalCacheServer.class);
    var maxThreads = 50;
    when(server.getMaxThreads()).thenReturn(maxThreads);
    var builder = new AcceptorBuilder();

    builder.forServer(server);

    assertThat(builder.getMaxThreads()).isEqualTo(maxThreads);
  }

  @Test
  public void forServerSetsMaximumMessageCountFromServer() {
    var server = mock(InternalCacheServer.class);
    var maximumMessageCount = 500;
    when(server.getMaximumMessageCount()).thenReturn(maximumMessageCount);
    var builder = new AcceptorBuilder();

    builder.forServer(server);

    assertThat(builder.getMaximumMessageCount()).isEqualTo(maximumMessageCount);
  }

  @Test
  public void forServerSetsMessageTimeToLiveFromServer() {
    var server = mock(InternalCacheServer.class);
    var messageTimeToLive = 400;
    when(server.getMessageTimeToLive()).thenReturn(messageTimeToLive);
    var builder = new AcceptorBuilder();

    builder.forServer(server);

    assertThat(builder.getMessageTimeToLive()).isEqualTo(messageTimeToLive);
  }

  @Test
  public void forServerSetsConnectionListenerFromServer() {
    var server = mock(InternalCacheServer.class);
    var connectionListener = mock(ConnectionListener.class);
    when(server.getConnectionListener()).thenReturn(connectionListener);
    var builder = new AcceptorBuilder();

    builder.forServer(server);

    assertThat(builder.getConnectionListener()).isEqualTo(connectionListener);
  }

  @Test
  public void forServerSetsTcpNoDelayFromServer() {
    var server = mock(InternalCacheServer.class);
    var tcpNoDelay = true;
    when(server.getTcpNoDelay()).thenReturn(tcpNoDelay);
    var builder = new AcceptorBuilder();

    builder.forServer(server);

    assertThat(builder.isTcpNoDelay()).isEqualTo(tcpNoDelay);
  }

  @Test
  public void forServerSetsTimeLimitMillisFromServer() {
    var server = mock(InternalCacheServer.class);
    var timeLimitMillis = Long.MAX_VALUE - 1;
    when(server.getTimeLimitMillis()).thenReturn(timeLimitMillis);
    var builder = new AcceptorBuilder();

    builder.forServer(server);

    assertThat(builder.getTimeLimitMillis()).isEqualTo(timeLimitMillis);
  }

  @Test
  public void forServerSetsSecurityServiceFromServer() {
    var server = mock(InternalCacheServer.class);
    var securityService = mock(SecurityService.class);
    when(server.getSecurityService()).thenReturn(securityService);
    var builder = new AcceptorBuilder();

    builder.forServer(server);

    assertThat(builder.getSecurityService()).isEqualTo(securityService);
  }

  @Test
  public void forServerSetsSocketCreatorSupplierFromServer() {
    var server = mock(InternalCacheServer.class);
    var socketCreatorSupplier = (Supplier<SocketCreator>) () -> mock(SocketCreator.class);
    when(server.getSocketCreatorSupplier()).thenReturn(socketCreatorSupplier);
    var builder = new AcceptorBuilder();

    builder.forServer(server);

    assertThat(builder.getSocketCreatorSupplier()).isEqualTo(socketCreatorSupplier);
  }

  @Test
  public void forServerSetsCacheClientNotifierProviderFromServer() {
    var server = mock(InternalCacheServer.class);
    var cacheClientNotifierProvider =
        mock(CacheClientNotifierProvider.class);
    when(server.getCacheClientNotifierProvider()).thenReturn(cacheClientNotifierProvider);
    var builder = new AcceptorBuilder();

    builder.forServer(server);

    assertThat(builder.getCacheClientNotifierProvider()).isEqualTo(cacheClientNotifierProvider);
  }

  @Test
  public void forServerSetsClientHealthMonitorProviderFromServer() {
    var server = mock(InternalCacheServer.class);
    var clientHealthMonitorProvider =
        mock(ClientHealthMonitorProvider.class);
    when(server.getClientHealthMonitorProvider()).thenReturn(clientHealthMonitorProvider);
    var builder = new AcceptorBuilder();

    builder.forServer(server);

    assertThat(builder.getClientHealthMonitorProvider()).isEqualTo(clientHealthMonitorProvider);
  }

  @Test
  public void forServerDoesNotUnsetIsGatewayReceiver() {
    var server = mock(InternalCacheServer.class);
    var builder = new AcceptorBuilder();
    var isGatewayReceiver = true;
    builder.setIsGatewayReceiver(isGatewayReceiver);

    builder.forServer(server);

    assertThat(builder.isGatewayReceiver()).isEqualTo(isGatewayReceiver);
  }

  @Test
  public void forServerDoesNotUnsetGatewayTransportFilters() {
    var server = mock(InternalCacheServer.class);
    var builder = new AcceptorBuilder();
    var gatewayTransportFilters =
        singletonList(mock(GatewayTransportFilter.class));
    builder.setGatewayTransportFilters(gatewayTransportFilters);

    builder.forServer(server);

    assertThat(builder.getGatewayTransportFilters()).isEqualTo(gatewayTransportFilters);
  }

  @Test
  public void forServerSetsStatisticsClockFromServer() {
    var server = mock(InternalCacheServer.class);
    var statisticsClock = mock(StatisticsClock.class);
    when(server.getStatisticsClock()).thenReturn(statisticsClock);
    var builder = new AcceptorBuilder();

    builder.forServer(server);

    assertThat(builder.getStatisticsClock()).isEqualTo(statisticsClock);
  }

  @Test
  public void setCacheReplacesCacheFromServer() {
    var server = mock(InternalCacheServer.class);
    var cacheFromServer = mock(InternalCache.class, "fromServer");
    when(server.getCache()).thenReturn(cacheFromServer);
    var builder = new AcceptorBuilder();
    builder.forServer(server);
    var cacheFromSetter = mock(InternalCache.class, "fromSetter");

    builder.setCache(cacheFromSetter);

    assertThat(builder.getCache()).isEqualTo(cacheFromSetter);
  }

  @Test
  public void setConnectionListenerReplacesConnectionListenerFromServer() {
    var server = mock(InternalCacheServer.class);
    var connectionListenerFromServer = mock(ConnectionListener.class, "fromServer");
    when(server.getConnectionListener()).thenReturn(connectionListenerFromServer);
    var builder = new AcceptorBuilder();
    builder.forServer(server);
    var connectionListenerFromSetter = mock(ConnectionListener.class, "fromSetter");

    builder.setConnectionListener(connectionListenerFromSetter);

    assertThat(builder.getConnectionListener()).isEqualTo(connectionListenerFromSetter);
  }

  @Test
  public void setSecurityServiceReplacesSecurityServiceFromServer() {
    var server = mock(InternalCacheServer.class);
    var securityServiceFromServer = mock(SecurityService.class, "fromServer");
    when(server.getSecurityService()).thenReturn(securityServiceFromServer);
    var builder = new AcceptorBuilder();
    builder.forServer(server);
    var securityServiceFromSetter = mock(SecurityService.class, "fromSetter");

    builder.setSecurityService(securityServiceFromSetter);

    assertThat(builder.getSecurityService()).isEqualTo(securityServiceFromSetter);
  }

  @Test
  public void setSocketCreatorSupplierReplacesSocketCreatorSupplierFromServer() {
    var server = mock(InternalCacheServer.class);
    var socketCreatorSupplierFromServer = (Supplier<SocketCreator>) () -> mock(SocketCreator.class);
    when(server.getSocketCreatorSupplier()).thenReturn(socketCreatorSupplierFromServer);
    var builder = new AcceptorBuilder();
    builder.forServer(server);
    var socketCreatorSupplierFromSetter = (Supplier<SocketCreator>) () -> mock(SocketCreator.class);

    builder.setSocketCreatorSupplier(socketCreatorSupplierFromSetter);

    assertThat(builder.getSocketCreatorSupplier()).isEqualTo(socketCreatorSupplierFromSetter);
  }

  @Test
  public void setCacheClientNotifierProviderReplacesCacheClientNotifierProviderFromServer() {
    var server = mock(InternalCacheServer.class);
    var cacheClientNotifierProviderFromServer =
        mock(CacheClientNotifierProvider.class, "fromServer");
    when(server.getCacheClientNotifierProvider()).thenReturn(cacheClientNotifierProviderFromServer);
    var builder = new AcceptorBuilder();
    builder.forServer(server);
    var cacheClientNotifierProviderFromSetter =
        mock(CacheClientNotifierProvider.class, "fromSetter");

    builder.setCacheClientNotifierProvider(cacheClientNotifierProviderFromSetter);

    assertThat(builder.getCacheClientNotifierProvider())
        .isEqualTo(cacheClientNotifierProviderFromSetter);
  }

  @Test
  public void setClientHealthMonitorProviderReplacesClientHealthMonitorProviderFromServer() {
    var server = mock(InternalCacheServer.class);
    var clientHealthMonitorProviderFromServer =
        mock(ClientHealthMonitorProvider.class, "fromServer");
    when(server.getClientHealthMonitorProvider()).thenReturn(clientHealthMonitorProviderFromServer);
    var builder = new AcceptorBuilder();
    builder.forServer(server);
    var clientHealthMonitorProviderFromSetter =
        mock(ClientHealthMonitorProvider.class, "fromSetter");

    builder.setClientHealthMonitorProvider(clientHealthMonitorProviderFromSetter);

    assertThat(builder.getClientHealthMonitorProvider())
        .isEqualTo(clientHealthMonitorProviderFromSetter);
  }

  @Test
  public void setStatisticsClockReplacesStatisticsClockFromServer() {
    var server = mock(InternalCacheServer.class);
    var statisticsClockFromServer = mock(StatisticsClock.class, "fromServer");
    when(server.getStatisticsClock()).thenReturn(statisticsClockFromServer);
    var builder = new AcceptorBuilder();
    builder.forServer(server);
    var statisticsClockFromSetter = mock(StatisticsClock.class, "fromSetter");

    builder.setStatisticsClock(statisticsClockFromSetter);

    assertThat(builder.getStatisticsClock()).isEqualTo(statisticsClockFromSetter);
  }
}
