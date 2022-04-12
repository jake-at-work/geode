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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.SocketException;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.client.SocketFactory;
import org.apache.geode.cache.client.internal.Endpoint;
import org.apache.geode.cache.client.internal.EndpointManager;
import org.apache.geode.cache.client.internal.QueueManager;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.tcpserver.ClientSocketCreator;
import org.apache.geode.distributed.internal.tcpserver.HostAndPort;
import org.apache.geode.internal.cache.tier.ClientSideHandshake;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

@Category({ClientSubscriptionTest.class})
public class CacheClientUpdaterJUnitTest {

  @Test
  public void failureToConnectClosesStatistics() throws Exception {
    // CacheClientUpdater's constructor takes a lot of parameters that we need to mock
    var location = new ServerLocation("localhost", 1234);
    var handshake = mock(ClientSideHandshake.class);
    when(handshake.isDurable()).thenReturn(Boolean.FALSE);
    QueueManager queueManager = null;
    mock(QueueManager.class);
    var endpointManager = mock(EndpointManager.class);
    var endpoint = mock(Endpoint.class);

    // shutdown checks
    var distributedSystem = mock(DistributedSystem.class);
    var cancelCriterion = mock(CancelCriterion.class);
    when(distributedSystem.getCancelCriterion()).thenReturn(cancelCriterion);
    when(cancelCriterion.isCancelInProgress()).thenReturn(Boolean.FALSE);

    // engineer a failure to connect via SocketCreator
    var socketCreator = mock(SocketCreator.class);
    var csc = mock(ClientSocketCreator.class);
    when(socketCreator.forClient()).thenReturn(csc);
    when(csc.connect(any(HostAndPort.class),
        any(Integer.class), any(Integer.class), any())).thenThrow(new SocketException("ouch"));

    // mock some stats that we can then use to ensure that they're closed when the problem occurs
    var statisticsProvider = mock(
        CacheClientUpdater.StatisticsProvider.class);
    var ccuStats = mock(CacheClientUpdater.CCUStats.class);
    when(statisticsProvider
        .createStatistics(distributedSystem, location))
            .thenReturn(ccuStats);

    // CCU's constructor fails to connect
    var clientUpdater =
        new CacheClientUpdater("testUpdater", location, false, distributedSystem, handshake,
            queueManager, endpointManager, endpoint, 10000, socketCreator, statisticsProvider,
            SocketFactory.DEFAULT);

    // now introspect to make sure the right actions were taken
    // The updater should not be in a connected state
    assertThat(clientUpdater.isConnected()).isFalse();
    // The statistics should be closed
    verify(ccuStats).close();
    // The endpoint should be reported as having crashed
    verify(endpointManager).serverCrashed(endpoint);
  }
}
