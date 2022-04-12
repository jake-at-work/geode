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
package org.apache.geode.alerting.internal;

import static org.apache.geode.internal.serialization.DataSerializableFixedID.ALERT_LISTENER_MESSAGE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.mock;

import java.time.Instant;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import org.apache.geode.alerting.internal.spi.AlertLevel;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.test.junit.categories.AlertingTest;

/**
 * Unit tests for {@link AlertListenerMessageFactory}.
 */
@Category(AlertingTest.class)
public class AlertListenerMessageFactoryTest {

  private DistributedMember member;
  private AlertListenerMessageFactory alertListenerMessageFactory;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Before
  public void setUp() {
    member = mock(InternalDistributedMember.class);
    alertListenerMessageFactory = new AlertListenerMessageFactory();
  }

  @Test
  public void createAlertListenerMessage() {
    var message = alertListenerMessageFactory.createAlertListenerMessage(member,
        AlertLevel.WARNING, Instant.now(), "connectionName", "threadName",
        Thread.currentThread().getId(), "formattedMessage", null);

    assertThat(message).isNotNull();
    assertThat(message.getDSFID()).isEqualTo(ALERT_LISTENER_MESSAGE);
    assertThat(message.sendViaUDP()).isTrue();
  }

  @Test
  public void createAlertListenerMessage_requiresInternalDistributedMember() {
    member = mock(DistributedMember.class);

    var thrown = catchThrowable(
        () -> alertListenerMessageFactory.createAlertListenerMessage(member, AlertLevel.WARNING,
            Instant.now(), "connectionName", "threadName", Thread.currentThread().getId(),
            "formattedMessage", null));

    assertThat(thrown).isNotNull().isInstanceOf(IllegalArgumentException.class);
  }
}
