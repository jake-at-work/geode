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
 *
 */

package org.apache.geode.redis.internal.pubsub;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.geode.redis.internal.netty.Coder.stringToBytes;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.PUNSUBSCRIBE;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.UNSUBSCRIBE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.junit.Test;

import org.apache.geode.redis.internal.netty.Client;
import org.apache.geode.redis.internal.statistics.RedisStats;

public class SubscriptionsJUnitTest {

  private final Subscriptions subscriptions = new Subscriptions(mock(RedisStats.class));

  @Test
  public void correctlyIdentifiesChannelSubscriber() {
    var client = createClient();

    addChannelSubscription(client, "subscriptions");

    assertThat(subscriptions.getChannelSubscriptionCount(stringToBytes("unknown"))).isZero();
    assertThat(subscriptions.getChannelSubscriptionCount(stringToBytes("subscriptions"))).isOne();
  }

  @Test
  public void correctlyCountsSubscriptions() {
    var client1 = createClient();
    var client2 = createClient();
    var client3 = createClient();

    addChannelSubscription(client1, "subscription1");
    addChannelSubscription(client3, "subscription1");
    addChannelSubscription(client2, "subscription2");
    addPatternSubscription(client1, "sub*");
    addPatternSubscription(client3, "sub*");
    addPatternSubscription(client2, "subscription?");

    assertThat(subscriptions.getAllSubscriptionCount(stringToBytes("subscription1"))).isEqualTo(5);
    assertThat(subscriptions.getAllSubscriptionCount(stringToBytes("subscription2"))).isEqualTo(4);
    assertThat(subscriptions.getAllSubscriptionCount(stringToBytes("subscription3"))).isEqualTo(3);
    assertThat(subscriptions.getAllSubscriptionCount(stringToBytes("sub1"))).isEqualTo(2);
    assertThat(subscriptions.getAllSubscriptionCount(stringToBytes("none"))).isEqualTo(0);
  }

  @Test
  public void subscribeReturnsExpectedResult() {
    var client = createClient();
    final var channel = stringToBytes("channel");

    var result = subscriptions.subscribe(channel, client);

    assertThat(result.getChannelCount()).isOne();
    assertThat(result.getChannel()).isEqualTo(channel);
    assertThat(client.getSubscriptionCount()).isOne();
    assertThat(client.getChannelSubscriptions()).containsExactlyInAnyOrder(channel);
  }

  @Test
  public void psubscribeReturnsExpectedResult() {
    var client = createClient();
    final var pattern = stringToBytes("pattern");

    var result = subscriptions.psubscribe(pattern, client);

    assertThat(result.getChannelCount()).isOne();
    assertThat(result.getChannel()).isEqualTo(pattern);
    assertThat(client.getSubscriptionCount()).isOne();
    assertThat(client.getPatternSubscriptions()).containsExactlyInAnyOrder(pattern);
  }

  @Test
  public void subscribeDoesNothingIfAlreadySubscribed() {
    var client = createClient();
    final var channel = stringToBytes("channel");

    subscriptions.subscribe(channel, client);
    var result = subscriptions.subscribe(channel, client);

    assertThat(result.getChannelCount()).isOne();
    assertThat(result.getChannel()).isEqualTo(channel);
    assertThat(result.getSubscription()).isNull();
    assertThat(client.getSubscriptionCount()).isOne();
    assertThat(client.getChannelSubscriptions()).containsExactlyInAnyOrder(channel);
  }

  @Test
  public void psubscribeDoesNothingIfAlreadySubscribed() {
    var client = createClient();
    final var pattern = stringToBytes("pattern");

    subscriptions.psubscribe(pattern, client);
    var result = subscriptions.psubscribe(pattern, client);

    assertThat(result.getChannelCount()).isOne();
    assertThat(result.getChannel()).isEqualTo(pattern);
    assertThat(result.getSubscription()).isNull();
    assertThat(client.getSubscriptionCount()).isOne();
    assertThat(client.getPatternSubscriptions()).containsExactlyInAnyOrder(pattern);
  }

  @Test
  public void correctlyIdentifiesPatternSubscriber() {
    var client = createClient();

    addPatternSubscription(client, "sub*s");

    assertThat(subscriptions.getChannelSubscriptionCount()).isZero();
    assertThat(subscriptions.getPatternSubscriptionCount()).isOne();
    assertThat(subscriptions.getChannelSubscriptionCount(stringToBytes("subscriptions"))).isZero();
    assertThat(subscriptions.getPatternSubscriptionCount(stringToBytes("subscriptions"))).isOne();
  }

  @Test
  public void doesNotMisidentifyChannelAsPattern() {
    var client = createClient();

    addChannelSubscription(client, "subscriptions");

    assertThat(subscriptions.getChannelSubscriptionCount()).isOne();
    assertThat(subscriptions.getPatternSubscriptionCount()).isZero();
    assertThat(subscriptions.getPatternSubscriptionCount(stringToBytes("subscriptions"))).isZero();
  }

  @Test
  public void doesNotMisidentifyWhenBothTypesArePresent() {
    var client = createClient();

    addChannelSubscription(client, "subscriptions");
    addPatternSubscription(client, "sub*s");

    assertThat(subscriptions.size()).isEqualTo(2);
    assertThat(subscriptions.getPatternSubscriptionCount()).isOne();
    assertThat(subscriptions.getChannelSubscriptionCount()).isOne();
    assertThat(subscriptions.getPatternSubscriptionCount(stringToBytes("sub1s"))).isOne();
    assertThat(subscriptions.getPatternSubscriptionCount(stringToBytes("sub1sF"))).isZero();
    assertThat(subscriptions.findChannelNames())
        .containsExactlyInAnyOrder(
            stringToBytes("subscriptions"));
  }

  @Test
  public void findSubscribers() {
    var client1 = createClient();
    var client2 = createClient();

    addChannelSubscription(client1, "subscriptions");
    addChannelSubscription(client2, "monkeys");

    assertThat(subscriptions.findChannelNames())
        .containsExactlyInAnyOrder(
            stringToBytes("subscriptions"),
            stringToBytes("monkeys"));
  }

  @Test
  public void removeByClient() {
    var clientOne = createClient();
    var clientTwo = createClient();
    addChannelSubscription(clientOne, "subscriptions");
    addChannelSubscription(clientTwo, "monkeys");

    subscriptions.remove(clientOne);

    assertThat(subscriptions.findChannelNames())
        .containsExactlyInAnyOrder(
            stringToBytes("monkeys"));
  }

  @Test
  public void removeByClientAndPattern() {
    var pattern = stringToBytes("monkeys");
    var client = createClient();
    addChannelSubscription(client, "subscriptions");
    addPatternSubscription(client, "monkeys");
    addChannelSubscription(client, "monkeys");

    var result = subscriptions.punsubscribe(singletonList(pattern), client);

    assertThat(result).containsExactly(asList(PUNSUBSCRIBE, pattern, 2L));
    assertThat(subscriptions.getPatternSubscriptionCount()).isZero();
  }

  @Test
  public void unsubscribeOnePattern() {
    var pattern = stringToBytes("monkeys");
    var client = createClient();
    addChannelSubscription(client, "subscriptions");
    addPatternSubscription(client, "monkeys");
    addChannelSubscription(client, "monkeys");

    var result = subscriptions.punsubscribe(singletonList(pattern), client);

    assertThat(result).containsExactly(asList(PUNSUBSCRIBE, pattern, 2L));
    assertThat(subscriptions.getPatternSubscriptionCount()).isZero();
  }

  @Test
  public void unsubscribeTwoPatterns() {
    var pattern1 = stringToBytes("monkeys");
    var pattern2 = stringToBytes("subscriptions");
    var client = createClient();
    addPatternSubscription(client, "subscriptions");
    addPatternSubscription(client, "monkeys");
    addChannelSubscription(client, "monkeys");

    var result =
        subscriptions.punsubscribe(asList(pattern1, pattern2), client);

    assertThat(result).containsExactly(asList(PUNSUBSCRIBE, pattern1, 2L),
        asList(PUNSUBSCRIBE, pattern2, 1L));
    assertThat(subscriptions.getPatternSubscriptionCount()).isZero();
  }

  @Test
  public void unsubscribeAllPatterns() {
    var pattern = stringToBytes("monkeys");
    var client = createClient();
    addPatternSubscription(client, "monkeys");
    addChannelSubscription(client, "monkeys");

    var result = subscriptions.punsubscribe(emptyList(), client);

    assertThat(result).hasSize(1);
    @SuppressWarnings("unchecked")
    var firstItem = (Collection<Object>) result.iterator().next();
    assertThat(firstItem).containsExactly(PUNSUBSCRIBE, pattern, 1L);
    assertThat(subscriptions.getPatternSubscriptionCount()).isZero();
  }

  @Test
  public void unsubscribeAllChannelsWhenNoSubscriptions() {
    var client = createClient();

    var result = subscriptions.unsubscribe(emptyList(), client);

    assertThat(result).hasSize(1);
    @SuppressWarnings("unchecked")
    var firstItem = (Collection<Object>) result.iterator().next();
    assertThat(firstItem).containsExactly(UNSUBSCRIBE, null, 0L);
  }

  @Test
  public void unsubscribeAllPatternsWhenNoSubscriptions() {
    var client = createClient();

    var result = subscriptions.punsubscribe(emptyList(), client);

    assertThat(result).hasSize(1);
    @SuppressWarnings("unchecked")
    var firstItem = (Collection<Object>) result.iterator().next();
    assertThat(firstItem).containsExactly(PUNSUBSCRIBE, null, 0L);
  }

  @Test
  public void unsubscribeOneChannel() {
    var channel = stringToBytes("monkeys");
    var client = createClient();
    addChannelSubscription(client, "subscriptions");
    addPatternSubscription(client, "monkeys");
    addChannelSubscription(client, "monkeys");

    var result = subscriptions.unsubscribe(singletonList(channel), client);

    assertThat(result).containsExactly(asList(UNSUBSCRIBE, channel, 2L));
    assertThat(subscriptions.findChannelNames())
        .containsExactlyInAnyOrder(
            stringToBytes("subscriptions"));
  }

  @Test
  public void unsubscribeTwoChannels() {
    var channel1 = stringToBytes("monkeys");
    var channel2 = stringToBytes("subscriptions");
    var client = createClient();
    addChannelSubscription(client, "subscriptions");
    addPatternSubscription(client, "monkeys");
    addChannelSubscription(client, "monkeys");

    var result =
        subscriptions.unsubscribe(asList(channel1, channel2), client);

    assertThat(result).containsExactly(asList(UNSUBSCRIBE, channel1, 2L),
        asList(UNSUBSCRIBE, channel2, 1L));
    assertThat(subscriptions.findChannelNames()).isEmpty();
  }

  @Test
  public void unsubscribeAllChannels() {
    var channel = stringToBytes("monkeys");
    var client = createClient();
    addPatternSubscription(client, "monkeys");
    addChannelSubscription(client, "monkeys");

    var result = subscriptions.unsubscribe(emptyList(), client);

    assertThat(result).hasSize(1);
    @SuppressWarnings("unchecked")
    var firstItem = (Collection<Object>) result.iterator().next();
    assertThat(firstItem).containsExactly(UNSUBSCRIBE, channel, 1L);
    assertThat(subscriptions.findChannelNames()).isEmpty();
  }

  @Test
  public void findChannelNames_shouldReturnAllChannelNames_whenCalledWithoutParameter() {
    var client = createClient();
    addChannelSubscription(client, "foo");
    addChannelSubscription(client, "bar");

    var result = subscriptions.findChannelNames();

    assertThat(result).containsExactlyInAnyOrder(stringToBytes("foo"), stringToBytes("bar"));
  }

  @Test
  public void findChannelNames_shouldReturnOnlyMatchingChannelNames_whenCalledWithPattern() {
    var pattern = stringToBytes("b*");
    var client = createClient();
    addChannelSubscription(client, "foo");
    addChannelSubscription(client, "bar");
    addChannelSubscription(client, "barbarella");

    var result = subscriptions.findChannelNames(pattern);

    assertThat(result).containsExactlyInAnyOrder(stringToBytes("bar"),
        stringToBytes("barbarella"));
  }

  @Test
  public void findChannelNames_shouldNotReturnPatternSubscriptions() {
    var client = createClient();
    addChannelSubscription(client, "foo");
    addPatternSubscription(client, "bar");

    var result = subscriptions.findChannelNames();

    assertThat(result).containsExactlyInAnyOrder(stringToBytes("foo"));
  }

  @Test
  public void findChannelNames_shouldNotReturnDuplicates_givenMultipleSubscriptionsToSameChannel_whenCalledWithoutPattern() {
    var client1 = createClient();
    var client2 = createClient();
    addChannelSubscription(client1, "foo");
    addChannelSubscription(client2, "foo");

    var result = subscriptions.findChannelNames();

    assertThat(result).containsExactlyInAnyOrder(stringToBytes("foo"));
  }

  @Test
  public void findChannelNames_shouldNotReturnDuplicates_givenMultipleSubscriptionsToSameChannel_whenCalledWithPattern() {
    var client1 = createClient();
    var client2 = createClient();
    addChannelSubscription(client1, "foo");
    addChannelSubscription(client2, "foo");

    var result = subscriptions.findChannelNames(stringToBytes("f*"));

    assertThat(result).containsExactlyInAnyOrder(stringToBytes("foo"));
  }


  private void addPatternSubscription(Client client, String pattern) {
    var patternBytes = stringToBytes(pattern);
    subscriptions.addPattern(patternBytes, client);
  }

  private void addChannelSubscription(Client client, String channel) {
    var channelBytes = stringToBytes(channel);
    subscriptions.addChannel(channelBytes, client);
  }

  private Channel createChannel() {
    var channel = mock(Channel.class);
    when(channel.closeFuture()).thenReturn(mock(ChannelFuture.class));
    return channel;
  }

  private Client createClient() {
    return new Client(createChannel(), mock(PubSub.class));
  }

}
