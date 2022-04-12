/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.distributed.internal.membership.gms.locator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import org.apache.geode.distributed.internal.tcpserver.TcpHandler;

public class PrimaryHandlerTest {

  @Test
  public void processRequest() throws IOException {
    final var sleepCount = new AtomicInteger();
    final TcpHandler fallbackHandler = null;
    final var locatorWaitTime = 5;
    final var clockTimes = Arrays.asList(0L, 1000L, 2000L, 3000L, 4000L, 5000L);
    var primaryHandler = new PrimaryHandler(fallbackHandler, locatorWaitTime,
        () -> clockTimes.get(sleepCount.get()),
        x -> sleepCount.incrementAndGet());
    final var result = primaryHandler.processRequest(new Object());
    assertThat(sleepCount.get()).isEqualTo(locatorWaitTime);
    assertThat(result).isNull();
  }

  @Test
  public void fallbackHandler() throws IOException {
    final var handlerInvoked = new AtomicInteger();
    final var fallbackHandler = mock(TcpHandler.class);
    when(fallbackHandler.processRequest(isA(Object.class))).thenAnswer(context -> {
      handlerInvoked.incrementAndGet();
      return context.getArgument(0);
    });
    final var locatorWaitTime = 5;
    var primaryHandler = new PrimaryHandler(fallbackHandler, locatorWaitTime,
        null, null);
    // process a request that has no handler - this should invoke fallbackHandler
    final var request = new Object();
    final var result = primaryHandler.processRequest(request);
    assertThat(result).isEqualTo(request);
    assertThat(handlerInvoked.get()).isEqualTo(1);
  }


  @Test
  public void registeredHandler() throws IOException {
    final var handlerInvoked = new AtomicInteger();
    final var registeredHandler = mock(TcpHandler.class);
    when(registeredHandler.processRequest(isA(Object.class))).thenAnswer(context -> {
      handlerInvoked.incrementAndGet();
      return context.getArgument(0);
    });
    final var locatorWaitTime = 5;
    var primaryHandler = new PrimaryHandler(registeredHandler, locatorWaitTime,
        null, null);
    primaryHandler.addHandler(FindCoordinatorRequest.class, registeredHandler);
    // process a request that has a registered handler - this should invoke registeredHandler
    final Object request = new FindCoordinatorRequest<>();
    final var result = primaryHandler.processRequest(request);
    assertThat(result).isEqualTo(request);
    assertThat(handlerInvoked.get()).isEqualTo(1);
    assertThat(primaryHandler.isHandled(FindCoordinatorRequest.class)).isTrue();
  }
}
