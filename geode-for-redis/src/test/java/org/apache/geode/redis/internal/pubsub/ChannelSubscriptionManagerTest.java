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
package org.apache.geode.redis.internal.pubsub;

import static org.apache.geode.redis.internal.netty.Coder.stringToBytes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;

import org.apache.geode.redis.internal.netty.Client;

public class ChannelSubscriptionManagerTest extends SubscriptionManagerTestBase {

  @Override
  protected ChannelSubscriptionManager createManager() {
    return new ChannelSubscriptionManager(redisStats);
  }

  @Test
  public void emptyManagerReturnsEmptyChannelSubscriptions() {
    var manager = createManager();
    var channel = stringToBytes("channel");

    var subscriptions = manager.getChannelSubscriptions(channel);

    assertThat(subscriptions).isEmpty();
  }

  @Test
  public void managerWithOneSubscriptionReturnsIt() {
    var channel = stringToBytes("channel");
    var otherChannel = stringToBytes("otherChannel");
    var client = mock(Client.class);
    when(client.addChannelSubscription(eq(channel))).thenReturn(true);
    var manager = createManager();
    var addedSubscription = manager.add(channel, client);

    var subscriptions = manager.getChannelSubscriptions(channel);

    assertThat(subscriptions).containsExactly(addedSubscription);
    assertThat(manager.getChannelSubscriptions(otherChannel)).isEmpty();
    verify(redisStats, times(1)).changeSubscribers(1L);
    verify(redisStats, times(1)).changeUniqueChannelSubscriptions(1L);
  }

  @Test
  public void clientsSubscribedToSameChannel() {
    var channel = stringToBytes("channel");
    var otherChannel = stringToBytes("otherChannel");
    var client = mock(Client.class);
    when(client.addChannelSubscription(eq(channel))).thenReturn(true);
    var client2 = mock(Client.class);
    when(client2.addChannelSubscription(eq(channel))).thenReturn(true);
    var manager = createManager();
    var addedSubscription = manager.add(channel, client);
    var addedSubscription2 = manager.add(channel, client2);

    var subscriptions = manager.getChannelSubscriptions(channel);

    assertThat(subscriptions).containsExactlyInAnyOrder(addedSubscription, addedSubscription2);
    assertThat(manager.getChannelSubscriptions(otherChannel)).isEmpty();
    verify(redisStats, times(2)).changeSubscribers(1L);
    verify(redisStats, times(1)).changeUniqueChannelSubscriptions(1L);
  }

  @Test
  public void clientSubscribedToTwoChannels() {
    var channel = stringToBytes("channel");
    var channel2 = stringToBytes("channel2");
    var client = mock(Client.class);
    when(client.addChannelSubscription(eq(channel))).thenReturn(true);
    when(client.addChannelSubscription(eq(channel2))).thenReturn(true);
    var manager = createManager();
    var addedSubscription = manager.add(channel, client);
    var addedSubscription2 = manager.add(channel2, client);

    var subscriptions = manager.getChannelSubscriptions(channel);
    var subscriptions2 = manager.getChannelSubscriptions(channel2);

    assertThat(subscriptions).containsExactly(addedSubscription);
    assertThat(subscriptions2).containsExactly(addedSubscription2);
    verify(redisStats, times(2)).changeSubscribers(1L);
    verify(redisStats, times(2)).changeUniqueChannelSubscriptions(1L);
  }
}
