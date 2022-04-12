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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Set;

import org.junit.Test;

import org.apache.geode.cache.wan.GatewaySender;

public class AbstractRegionTest {

  @Test
  public void shouldBeMockable() throws Exception {
    var mockAbstractRegion = mock(AbstractRegion.class);
    var millis = System.currentTimeMillis();

    when(mockAbstractRegion.isAllEvents()).thenReturn(true);
    when(mockAbstractRegion.cacheTimeMillis()).thenReturn(millis);

    assertThat(mockAbstractRegion.isAllEvents()).isTrue();
    assertThat(mockAbstractRegion.cacheTimeMillis()).isEqualTo(millis);
  }

  @Test
  public void hasRunningGatewaySender_returnsFalse_ifSendersIsEmpty() {
    var sender = mock(GatewaySender.class);

    var value = AbstractRegion.hasRunningGatewaySender(Collections.emptySet(), sender);

    assertThat(value).isFalse();
  }

  @Test
  public void hasRunningGatewaySender_returnsFalse_ifSenderIsStopped() {
    var mockSender = mock(GatewaySender.class);
    var senders = (Set<GatewaySender>) mock(Set.class);

    when(senders.contains(mockSender)).thenReturn(true);
    when(mockSender.isRunning()).thenReturn(false);

    var value = AbstractRegion.hasRunningGatewaySender(senders, mockSender);

    assertThat(value).isFalse();
  }

  @Test
  public void hasRunningGatewaySender_returnsTrue_ifSenderIsRunning() {
    var mockSender = mock(GatewaySender.class);
    var senders = (Set<GatewaySender>) mock(Set.class);

    when(senders.contains(mockSender)).thenReturn(true);
    when(mockSender.isRunning()).thenReturn(true);

    var value = AbstractRegion.hasRunningGatewaySender(senders, mockSender);

    assertThat(value).isTrue();
  }
}
