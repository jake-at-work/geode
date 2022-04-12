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

package org.apache.geode.redis.internal.commands.executor.key;

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertExactNumberOfArgs;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NO_SUCH_KEY;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_WRONG_SLOT;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.junit.Assert.fail;
import static redis.clients.jedis.Protocol.Command.RENAME;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.services.locking.LockingStripedCoordinator;
import org.apache.geode.redis.internal.services.locking.StripedCoordinator;

public abstract class AbstractRenameIntegrationTest implements RedisIntegrationTest {
  private JedisCluster jedis;
  private static Random rand;

  @Before
  public void setUp() {
    rand = new Random();
    jedis = new JedisCluster(new HostAndPort("localhost", getPort()), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    flushAll();
    jedis.close();
  }

  @Test
  public void errors_GivenWrongNumberOfArguments() {
    assertExactNumberOfArgs(jedis, RENAME, 2);
  }

  @Test
  public void shouldReturnCrossSlotError_givenKeysInDifferentSlots() {
    var key1 = "{tag1}key1";
    var key2 = "{tag2}key2";
    jedis.set(key1, "value1");
    jedis.set(key2, "value1");
    assertThatThrownBy(() -> jedis.sendCommand(key1, RENAME, key1, key2))
        .hasMessage(ERROR_WRONG_SLOT);
  }

  @Test
  public void shouldRename_givenNewKey() {
    jedis.set("{tag1}foo", "bar");
    assertThat(jedis.rename("{tag1}foo", "{tag1}newfoo")).isEqualTo("OK");
    assertThat(jedis.get("{tag1}newfoo")).isEqualTo("bar");
  }

  @Test
  public void shouldDeleteOldKey_whenRenamed() {
    jedis.set("{tag1}foo", "bar");
    jedis.rename("{tag1}foo", "{tag1}newfoo");
    assertThat(jedis.get("{tag1}foo")).isNull();
  }

  @Test
  public void shouldReturnError_givenNonexistantKey() {
    assertThatThrownBy(() -> jedis.rename("{tag1}foo", "{tag1}newfoo"))
        .hasMessage(ERROR_NO_SUCH_KEY);
  }

  @Test
  public void shouldRename_withHash() {
    jedis.hset("{tag1}foo", "field", "va");
    jedis.rename("{tag1}foo", "{tag1}newfoo");
    assertThat(jedis.hget("{tag1}newfoo", "field")).isEqualTo("va");
  }

  @Test
  public void shouldRename_withSet() {
    jedis.sadd("{tag1}foo", "data");
    jedis.rename("{tag1}foo", "{tag1}newfoo");
    assertThat(jedis.smembers("{tag1}newfoo")).containsExactly("data");
  }

  @Test
  public void shouldReturnOkay_withSameSourceAndTargetKey() {
    jedis.set("{tag1}blue", "moon");
    assertThat(jedis.rename("{tag1}blue", "{tag1}blue")).isEqualTo("OK");
    assertThat(jedis.get("{tag1}blue")).isEqualTo("moon");
  }

  @Test
  public void shouldRename_withExistingTargetKey() {
    jedis.set("{tag1}foo1", "bar1");
    jedis.set("{tag1}foo12", "bar2");
    var result = jedis.rename("{tag1}foo1", "{tag1}foo12");
    assertThat(result).isEqualTo("OK");
    assertThat(jedis.get("{tag1}foo12")).isEqualTo("bar1");
    assertThat(jedis.get("{tag1}foo1")).isNull();
  }

  @Test
  public void repeatedRename_shouldReturnNoSuchKeyError() {
    var oldKey = "{1}key";
    var newKey = "{1}newKey";
    jedis.set(oldKey, "value");
    jedis.rename(oldKey, newKey);
    assertThatThrownBy(() -> jedis.rename(oldKey, newKey)).hasMessage(ERROR_NO_SUCH_KEY);
  }

  @Test
  public void error_whenNeitherKeyExists() {
    var oldKey = "{1}key";
    var newKey = "{1}newKey";
    assertThatThrownBy(() -> jedis.renamenx(oldKey, newKey)).hasMessage(ERROR_NO_SUCH_KEY);
  }

  @Test
  public void shouldRenameAtomically() {
    var numIterations = 100;
    var numStringsFirstKey = 10000;
    var numStringsSecondKey = 1000;

    var k1 = "{tag1}k1";
    var k2 = "{tag1}k2";

    var initAction = (Runnable) () -> {
      flushAll();
      jedis.sadd(k1, "initialEntry");
      assertThat(jedis.exists(k1)).isTrue();
      assertThat(jedis.exists(k2)).isFalse();
      assertThat(jedis.scard(k1)).isEqualTo(1);
    };

    initAction.run();
    new ConcurrentLoopingThreads(numIterations,
        i -> addStringsToKeys(k1, numStringsFirstKey, jedis),
        i -> addStringsToKeys(k2, numStringsSecondKey, jedis),
        i -> {
          // introduce more variability as to when the rename happens
          try {
            Thread.sleep(rand.nextInt(100));
          } catch (InterruptedException ignored) {
          }
          jedis.rename(k1, k2);
        })
            .runWithAction(() -> {
              // four possible results depending on when the rename is executed
              // relative to the other actions
              assertThat(jedis.scard(k2)).satisfiesAnyOf(
                  sizeK2 -> {
                    // rename ran before adds to key1 and before adds to key2
                    assertThat(jedis.exists(k1)).isTrue();
                    assertThat(jedis.scard(k1)).isEqualTo(numStringsFirstKey);
                    assertThat(sizeK2).isEqualTo(numStringsSecondKey + 1);
                    assertThat(jedis.sismember(k2, "initialEntry")).isTrue();
                  },
                  sizeK2 -> {
                    // rename ran before adds to key1 and after adds to key2
                    assertThat(jedis.exists(k1)).isTrue();
                    assertThat(jedis.scard(k1)).isEqualTo(numStringsFirstKey);
                    assertThat(sizeK2).isEqualTo(1);
                    assertThat(jedis.sismember(k2, "initialEntry")).isTrue();
                  },
                  sizeK2 -> {
                    // rename ran after adds to key1 and before adds to key2
                    assertThat(jedis.exists(k1)).isFalse();
                    assertThat(sizeK2).isEqualTo(numStringsSecondKey);
                    assertThat(jedis.sismember(k2, "initialEntry")).isFalse();
                  },
                  sizeK2 -> {
                    // rename ran after adds to key1 and after adds to key2
                    assertThat(jedis.exists(k1)).isFalse();
                    assertThat(sizeK2).isEqualTo(numStringsFirstKey + 1);
                    assertThat(jedis.sismember(k2, "initialEntry")).isTrue();
                  });
              initAction.run();
            });
  }

  @Test
  public void should_succeed_givenTwoKeysOnDifferentStripes() {
    var listOfKeys = getKeysOnDifferentStripes();
    var oldKey = listOfKeys.get(0);
    var newKey = listOfKeys.get(1);

    jedis.sadd(oldKey, "value1");
    jedis.sadd(newKey, "value2");

    assertThat(jedis.rename(oldKey, newKey)).isEqualTo("OK");
    assertThat(jedis.smembers(newKey)).containsExactly("value1");
    assertThat(jedis.exists(oldKey)).isFalse();
  }

  @Test
  public void should_succeed_givenTwoKeysOnSameStripe() {
    List<String> listOfKeys = new ArrayList<>(getKeysOnSameRandomStripe(2));
    var oldKey = listOfKeys.get(0);
    var newKey = listOfKeys.get(1);

    jedis.sadd(oldKey, "value1");
    jedis.sadd(newKey, "value2");

    assertThat(jedis.rename(oldKey, newKey)).isEqualTo("OK");
    assertThat(jedis.smembers(newKey)).containsExactly("value1");
    assertThat(jedis.exists(oldKey)).isFalse();
  }

  @Test
  public void shouldNotDeadlock_concurrentRenames_givenStripeContention()
      throws ExecutionException, InterruptedException {
    List<String> keysOnStripe1 = new ArrayList<>(getKeysOnSameRandomStripe(2));
    var keysOnStripe2 = getKeysOnSameRandomStripe(2, keysOnStripe1.get(0));

    for (var i = 0; i < 5; i++) {
      doConcurrentRenamesDifferentKeys(
          Arrays.asList(keysOnStripe1.get(0), keysOnStripe2.get(0)),
          Arrays.asList(keysOnStripe2.get(1), keysOnStripe1.get(1)));
    }
  }

  @Test
  public void shouldError_givenKeyDeletedDuringRename() {
    var iterations = 2000;

    jedis.set("{tag1}oldKey", "foo");

    try {
      new ConcurrentLoopingThreads(iterations,
          i -> jedis.rename("{tag1}oldKey", "{tag1}newKey"),
          i -> jedis.del("{tag1}oldKey"))
              .runWithAction(() -> {
                assertThat(jedis.get("{tag1}newKey")).isEqualTo("foo");
                assertThat(jedis.exists("{tag1}oldKey")).isFalse();
                flushAll();
                jedis.set("{tag1}oldKey", "foo");
              });
    } catch (RuntimeException e) {
      assertThat(e).getRootCause().hasMessage(ERROR_NO_SUCH_KEY);
      return;
    }

    fail("Did not hit exception after " + iterations + " iterations");
  }

  @Test
  public void shouldNotDeadlock_concurrentRenames_givenTwoKeysOnDifferentStripe()
      throws ExecutionException, InterruptedException {
    doConcurrentRenamesSameKeys(getKeysOnDifferentStripes());
  }

  @Test
  public void shouldNotDeadlock_concurrentRenames_givenTwoKeysOnSameStripe()
      throws ExecutionException, InterruptedException {
    doConcurrentRenamesSameKeys(new ArrayList<>(getKeysOnSameRandomStripe(2)));
  }

  private List<String> getKeysOnDifferentStripes() {
    var key1 = "{tag1}keyz" + new Random().nextInt();

    var key1RedisKey = new RedisKey(key1.getBytes());
    StripedCoordinator stripedCoordinator = new LockingStripedCoordinator();
    var iterator = 0;
    String key2;
    do {
      key2 = "{tag1}key" + iterator;
      iterator++;
    } while (stripedCoordinator.compareStripes(key1RedisKey,
        new RedisKey(key2.getBytes())) == 0);

    return Arrays.asList(key1, key2);
  }

  private Set<String> getKeysOnSameRandomStripe(int numKeysNeeded) {
    var random = new Random();
    var key1 = "{tag1}keyz" + random.nextInt();
    var key1RedisKey = new RedisKey(key1.getBytes());
    StripedCoordinator stripedCoordinator = new LockingStripedCoordinator();
    Set<String> keys = new HashSet<>();
    keys.add(key1);

    do {
      var key2 = "{tag1}key" + random.nextInt();
      if (stripedCoordinator.compareStripes(key1RedisKey,
          new RedisKey(key2.getBytes())) == 0) {
        keys.add(key2);
      }
    } while (keys.size() < numKeysNeeded);

    return keys;
  }

  public void doConcurrentRenamesDifferentKeys(List<String> listOfKeys1, List<String> listOfKeys2)
      throws ExecutionException, InterruptedException {
    var startCyclicBarrier = new CyclicBarrier(2);

    var oldKey1 = listOfKeys1.get(0);
    var newKey1 = listOfKeys1.get(1);
    var oldKey2 = listOfKeys2.get(0);
    var newKey2 = listOfKeys2.get(1);

    jedis.sadd(oldKey1, "foo", "bar");
    jedis.sadd(oldKey2, "bar3", "back3");

    var pool = Executors.newFixedThreadPool(2);

    var renameOldKey1ToNewKey1 = (Runnable) () -> {
      cyclicBarrierAwait(startCyclicBarrier);

      jedis.rename(oldKey1, newKey1);
    };

    var renameOldKey2ToNewKey2 = (Runnable) () -> {
      cyclicBarrierAwait(startCyclicBarrier);
      jedis.rename(oldKey2, newKey2);
    };

    var future1 = pool.submit(renameOldKey1ToNewKey1);
    var future2 = pool.submit(renameOldKey2ToNewKey2);

    future1.get();
    future2.get();
  }

  private void cyclicBarrierAwait(CyclicBarrier startCyclicBarrier) {
    try {
      startCyclicBarrier.await();
    } catch (InterruptedException | BrokenBarrierException e) {
      throw new RuntimeException(e);
    }
  }

  private List<String> getKeysOnSameRandomStripe(int numKeysNeeded, Object toAvoid) {

    StripedCoordinator stripedCoordinator = new LockingStripedCoordinator();

    List<String> keys = new ArrayList<>();

    String key1;
    RedisKey key1RedisKey;
    do {
      key1 = "{tag1}keyz" + new Random().nextInt();
      key1RedisKey = new RedisKey(key1.getBytes());
    } while (stripedCoordinator.compareStripes(key1RedisKey, toAvoid) == 0 && keys.add(key1));

    do {
      var key2 = "{tag1}key" + new Random().nextInt();

      if (stripedCoordinator.compareStripes(key1RedisKey,
          new RedisKey(key2.getBytes())) == 0) {
        keys.add(key2);
      }
    } while (keys.size() < numKeysNeeded);

    return keys;
  }

  public void doConcurrentRenamesSameKeys(List<String> listOfKeys)
      throws ExecutionException, InterruptedException {
    var key1 = listOfKeys.get(0);
    var key2 = listOfKeys.get(1);

    var startCyclicBarrier = new CyclicBarrier(2);

    jedis.sadd(key1, "foo", "bar");
    jedis.sadd(key2, "bar", "back");

    var pool = Executors.newFixedThreadPool(2);

    var renameKey1ToKey2 = (Runnable) () -> {
      cyclicBarrierAwait(startCyclicBarrier);
      jedis.rename(key1, key2);
    };

    var renameKey2ToKey1 = (Runnable) () -> {
      cyclicBarrierAwait(startCyclicBarrier);
      jedis.rename(key2, key1);
    };

    var future1 = pool.submit(renameKey1ToKey2);
    var future2 = pool.submit(renameKey2ToKey1);

    future1.get();
    future2.get();
  }

  private Long addStringsToKeys(String key, int numOfStrings,
      JedisCluster client) {
    Set<String> strings = new HashSet<>();
    generateStrings(numOfStrings, strings);
    var stringArray = strings.toArray(new String[strings.size()]);
    return client.sadd(key, stringArray);
  }

  private Set<String> generateStrings(int elements, Set<String> strings) {
    for (var i = 0; i < elements; i++) {
      var elem = String.valueOf(i);
      strings.add(elem);
    }
    return strings;
  }

}
