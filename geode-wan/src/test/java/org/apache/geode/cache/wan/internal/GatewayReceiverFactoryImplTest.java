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
package org.apache.geode.cache.wan.internal;

import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheServer;
import org.apache.geode.internal.cache.xmlcache.CacheCreation;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class GatewayReceiverFactoryImplTest {

  @Parameter
  public InternalCache cache;

  private GatewayReceiverFactoryImpl gatewayReceiverFactory;

  @Parameters(name = "{0}")
  public static Collection<InternalCache> cacheTypes() {
    InternalCache gemFireCacheImpl = mock(GemFireCacheImpl.class, "GemFireCacheImpl");
    InternalCache cacheCreation = mock(CacheCreation.class, "CacheCreation");
    var system = mock(InternalDistributedSystem.class);

    when(gemFireCacheImpl.addGatewayReceiverServer(any()))
        .thenReturn(mock(InternalCacheServer.class));
    when(gemFireCacheImpl.getDistributedSystem()).thenReturn(system);
    when(gemFireCacheImpl.getInternalDistributedSystem()).thenReturn(system);

    return Arrays.asList(gemFireCacheImpl, cacheCreation);
  }

  @Before
  public void setUp() {
    when(cache.getGatewayReceivers()).thenReturn(Collections.emptySet());
    when(cache.getMeterRegistry()).thenReturn(new SimpleMeterRegistry());

    gatewayReceiverFactory = new GatewayReceiverFactoryImpl(cache);
  }

  @Test
  public void createDoesNotUseManualStartByDefault() {
    var receiver = gatewayReceiverFactory.create();

    assertThat(receiver.isManualStart()).isFalse();
  }

  @Test
  public void createUsesSpecifiedManualStart() {
    gatewayReceiverFactory.setManualStart(true);

    var receiver = gatewayReceiverFactory.create();

    assertThat(receiver.isManualStart()).isTrue();
  }

  @Test
  public void createDoesNotUseGatewayTransportFiltersByDefault() {
    var receiver = gatewayReceiverFactory.create();

    assertThat(receiver.getGatewayTransportFilters()).isEmpty();
  }

  @Test
  public void createUsesSpecifiedGatewayTransportFilter() {
    var gatewayTransportFilter = mock(GatewayTransportFilter.class);
    gatewayReceiverFactory.addGatewayTransportFilter(gatewayTransportFilter);

    var receiver = gatewayReceiverFactory.create();

    assertThat(receiver.getGatewayTransportFilters()).containsOnly(gatewayTransportFilter);
  }

  @Test
  public void createUsesMultipleSpecifiedGatewayTransportFilters() {
    var gatewayTransportFilter1 = mock(GatewayTransportFilter.class);
    var gatewayTransportFilter2 = mock(GatewayTransportFilter.class);
    var gatewayTransportFilter3 = mock(GatewayTransportFilter.class);
    gatewayReceiverFactory.addGatewayTransportFilter(gatewayTransportFilter1);
    gatewayReceiverFactory.addGatewayTransportFilter(gatewayTransportFilter2);
    gatewayReceiverFactory.addGatewayTransportFilter(gatewayTransportFilter3);

    var receiver = gatewayReceiverFactory.create();

    assertThat(receiver.getGatewayTransportFilters())
        .containsExactlyInAnyOrder(gatewayTransportFilter1, gatewayTransportFilter2,
            gatewayTransportFilter3);
  }

  @Test
  public void createUsesEndPortDefault() {
    var endPortDefault = 5500;

    var receiver = gatewayReceiverFactory.create();

    assertThat(receiver.getEndPort()).isEqualTo(endPortDefault);
  }

  @Test
  public void createUsesSpecifiedEndPort() {
    var endPort = 6000;
    gatewayReceiverFactory.setEndPort(endPort);

    var receiver = gatewayReceiverFactory.create();

    assertThat(receiver.getEndPort()).isEqualTo(endPort);
  }

  @Test
  public void createThrowsIllegalStateExceptionIfEndPortIsLessThanStartPortDefault() {
    var endPort = 2500;
    gatewayReceiverFactory.setEndPort(endPort);

    var thrown = catchThrowable(() -> gatewayReceiverFactory.create());

    assertThat(thrown).isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void createUsesStartPortDefault() {
    var startPortDefault = 5000;

    var receiver = gatewayReceiverFactory.create();

    assertThat(receiver.getStartPort()).isEqualTo(startPortDefault);
  }

  @Test
  public void createUsesSpecifiedStartPort() {
    var startPort = 2500;
    gatewayReceiverFactory.setStartPort(startPort);

    var receiver = gatewayReceiverFactory.create();

    assertThat(receiver.getStartPort()).isEqualTo(startPort);
  }

  @Test
  public void createThrowsIllegalStateExceptionIfSpecifiedStartPortIsGreaterThanEndPortDefault() {
    var startPort = 6000;
    gatewayReceiverFactory.setStartPort(startPort);

    var thrown = catchThrowable(() -> gatewayReceiverFactory.create());

    assertThat(thrown).isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void createUsesSpecifiedStartPortAndEndPort() {
    var startPort = 4000;
    var endPort = 6000;
    gatewayReceiverFactory.setStartPort(startPort);
    gatewayReceiverFactory.setEndPort(endPort);

    var receiver = gatewayReceiverFactory.create();

    assertThat(receiver.getStartPort()).isEqualTo(startPort);
    assertThat(receiver.getEndPort()).isEqualTo(endPort);
  }

  @Test
  public void createThrowsIllegalStateExceptionIfSpecifiedEndPortIsLessThanSpecifiedStartPort() {
    var startPort = 6000;
    var endPort = 4000;
    gatewayReceiverFactory.setStartPort(startPort);
    gatewayReceiverFactory.setEndPort(endPort);

    var thrown = catchThrowable(() -> gatewayReceiverFactory.create());

    assertThat(thrown).isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void createUsesBindAddressDefault() {
    var receiver = gatewayReceiverFactory.create();

    assertThat(receiver.getBindAddress()).isEqualTo("");
  }

  @Test
  public void createUsesSpecifiedBindAddress() {
    var bindAddress = "kaos";
    gatewayReceiverFactory.setBindAddress(bindAddress);

    var receiver = gatewayReceiverFactory.create();

    assertThat(receiver.getBindAddress()).isEqualTo(bindAddress);
  }

  @Test
  public void createUsesSocketBufferSizeDefault() {
    var socketBufferSizeDefault = "524288";

    var receiver = gatewayReceiverFactory.create();

    assertThat(receiver.getSocketBufferSize()).isEqualTo(Integer.valueOf(socketBufferSizeDefault));
  }

  @Test
  public void createUsesSpecifiedSocketBufferSize() {
    var socketBufferSize = 128;
    gatewayReceiverFactory.setSocketBufferSize(socketBufferSize);

    var receiver = gatewayReceiverFactory.create();

    assertThat(receiver.getSocketBufferSize()).isEqualTo(socketBufferSize);
  }

  @Test
  public void createUsesHostnameForSendersDefault() {
    var receiver = gatewayReceiverFactory.create();

    assertThat(receiver.getHostnameForSenders()).isEqualTo("");
  }

  @Test
  public void createUsesSpecifiedHostnameForSenders() {
    var hostnameForSenders = "kaos.com";
    gatewayReceiverFactory.setHostnameForSenders(hostnameForSenders);

    var receiver = gatewayReceiverFactory.create();

    assertThat(receiver.getHostnameForSenders()).isEqualTo(hostnameForSenders);
  }

  @Test
  public void createUsesMaximumTimeBetweenPingsDefault() {
    var maximumTimeBetweenPingsDefault = 60000;

    var receiver = gatewayReceiverFactory.create();

    assertThat(receiver.getMaximumTimeBetweenPings()).isEqualTo(maximumTimeBetweenPingsDefault);
  }

  @Test
  public void createUsesSpecifiedMaximumTimeBetweenPings() {
    var timeoutBetweenPings = 1;
    gatewayReceiverFactory.setMaximumTimeBetweenPings(timeoutBetweenPings);

    var receiver = gatewayReceiverFactory.create();

    assertThat(receiver.getMaximumTimeBetweenPings()).isEqualTo(timeoutBetweenPings);
  }

  @Test
  public void createAddsGatewayReceiverToCache() {
    var receiver = gatewayReceiverFactory.create();

    verify(cache).addGatewayReceiver(receiver);
  }

  @Test
  public void createThrowsIllegalStateExceptionIfGatewayReceiverAlreadyExists() {
    when(cache.getGatewayReceivers()).thenReturn(singleton(mock(GatewayReceiver.class)));

    var thrown = catchThrowable(() -> gatewayReceiverFactory.create());

    assertThat(thrown).isInstanceOf(IllegalStateException.class);
  }
}
