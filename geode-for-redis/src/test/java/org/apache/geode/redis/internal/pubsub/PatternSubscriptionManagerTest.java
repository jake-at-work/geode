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

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.geode.redis.internal.netty.Coder.stringToBytes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;

import org.apache.geode.redis.internal.netty.Client;
import org.apache.geode.redis.internal.pubsub.Subscriptions.PatternSubscriptions;

public class PatternSubscriptionManagerTest extends SubscriptionManagerTestBase {

  @Override
  protected PatternSubscriptionManager createManager() {
    return new PatternSubscriptionManager(redisStats);
  }

  @Test
  public void twoPatternsThatMatchSameChannel() {
    var client1 = createClient();
    var client2 = createClient();
    AbstractSubscriptionManager manager = createManager();
    var channel = stringToBytes("channel");
    var pattern1 = stringToBytes("ch*");
    var pattern2 = stringToBytes("chan*");

    Object result1 = manager.add(pattern1, client1);
    Object result2 = manager.add(pattern2, client2);

    assertThat(manager.getSubscriptionCount()).isEqualTo(2);
    assertThat(result1).isNotNull();
    assertThat(result2).isNotNull();
    assertThat(manager.getSubscriptionCount(channel)).isEqualTo(2);
    assertThat(manager.getIds()).containsExactlyInAnyOrder(pattern1, pattern2);
    assertThat(manager.getIds(stringToBytes("cha*"))).containsExactlyInAnyOrder(pattern2);
    verify(redisStats, times(2)).changeSubscribers(1L);
    verify(redisStats, times(2)).changeUniquePatternSubscriptions(1L);
  }

  @Test
  public void emptyManagerReturnsEmptyPatternSubscriptions() {
    var manager = createManager();
    var channel = stringToBytes("channel");

    var subscriptions = manager.getPatternSubscriptions(channel);
    var cachedSubscriptions = manager.getPatternSubscriptions(channel);

    assertThat(subscriptions).isEmpty();
    assertThat(cachedSubscriptions).isSameAs(subscriptions);
  }

  @Test
  public void emptyManagerDoesNotCachePatternSubscriptions() {
    var manager = createManager();
    var channel = stringToBytes("channel");

    var subscriptions = manager.getPatternSubscriptions(channel);

    assertThat(subscriptions).isEmpty();
    assertThat(manager.cacheSize()).isZero();
  }

  @Test
  public void managerWithOneSubscriptionReturnsIt() {
    var manager = createManager();
    var pattern = stringToBytes("ch*");
    var channel = stringToBytes("channel");
    var otherChannel = stringToBytes("otherChannel");
    var client = mock(Client.class);
    when(client.addPatternSubscription(eq(pattern))).thenReturn(true);
    var addedSubscription = manager.add(pattern, client);
    var expected = new PatternSubscriptions(pattern,
        singletonList(addedSubscription));

    var subscriptions = manager.getPatternSubscriptions(channel);
    var cachedSubscriptions = manager.getPatternSubscriptions(channel);

    assertThat(cachedSubscriptions).isSameAs(subscriptions);
    assertThat(subscriptions).containsExactly(expected);
    assertThat(manager.getPatternSubscriptions(otherChannel)).isEmpty();
    assertThat(manager.cacheSize()).isOne();
    verify(redisStats, times(1)).changeSubscribers(1L);
    verify(redisStats, times(1)).changeUniquePatternSubscriptions(1L);
  }

  @Test
  public void managerWithOneSubscriptionThatDoesNotMatchChannelDoesNotCache() {
    var manager = createManager();
    var pattern = stringToBytes("ch*");
    var otherChannel = stringToBytes("otherChannel");
    var client = mock(Client.class);
    when(client.addPatternSubscription(eq(pattern))).thenReturn(true);
    manager.add(pattern, client);

    assertThat(manager.getPatternSubscriptions(otherChannel)).isEmpty();
    assertThat(manager.cacheSize()).isZero();
  }

  @Test
  public void clientsSubscribedToSamePattern() {
    var manager = createManager();
    var pattern = stringToBytes("ch*");
    var channel = stringToBytes("channel");
    var client = mock(Client.class);
    when(client.addPatternSubscription(eq(pattern))).thenReturn(true);
    var client2 = mock(Client.class);
    when(client2.addPatternSubscription(eq(pattern))).thenReturn(true);
    var addedSubscription = manager.add(pattern, client);
    var addedSubscription2 = manager.add(pattern, client2);
    var expected = new PatternSubscriptions(pattern,
        asList(addedSubscription, addedSubscription2));

    var subscriptions = manager.getPatternSubscriptions(channel);
    var cachedSubscriptions = manager.getPatternSubscriptions(channel);

    assertThat(cachedSubscriptions).isSameAs(subscriptions);
    assertThat(subscriptions).containsExactly(expected);
    verify(redisStats, times(2)).changeSubscribers(1L);
    verify(redisStats, times(1)).changeUniquePatternSubscriptions(1L);
  }

  @Test
  public void clientSubscribedToTwoPatterns() {
    var manager = createManager();
    var pattern = stringToBytes("ch*");
    var pattern2 = stringToBytes("*l");
    var channel = stringToBytes("channel");
    var client = mock(Client.class);
    when(client.addPatternSubscription(eq(pattern))).thenReturn(true);
    when(client.addPatternSubscription(eq(pattern2))).thenReturn(true);
    var addedSubscription = manager.add(pattern, client);
    var addedSubscription2 = manager.add(pattern2, client);
    var expected = new PatternSubscriptions(pattern,
        singletonList(addedSubscription));
    var expected2 = new PatternSubscriptions(pattern2,
        singletonList(addedSubscription2));

    var subscriptions = manager.getPatternSubscriptions(channel);
    var cachedSubscriptions = manager.getPatternSubscriptions(channel);

    assertThat(cachedSubscriptions).isSameAs(subscriptions);
    assertThat(subscriptions).containsExactlyInAnyOrder(expected, expected2);
    verify(redisStats, times(2)).changeSubscribers(1L);
    verify(redisStats, times(2)).changeUniquePatternSubscriptions(1L);
  }
}
