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

package org.apache.geode.redis.internal.commands.executor.list;

import static org.apache.geode.distributed.ConfigurationProperties.GEODE_FOR_REDIS_PORT;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import com.google.common.collect.Streams;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.internal.services.RegionProvider;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class RPushDUnitTest {
  @ClassRule
  public static RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule();

  @ClassRule
  public static ExecutorServiceRule executor = new ExecutorServiceRule();

  private static JedisCluster jedis;
  private static int locatorPort;
  private static int redisServerPort;
  private static MemberVM server1;

  @BeforeClass
  public static void testSetup() {
    var locator = clusterStartUp.startLocatorVM(0);
    locatorPort = locator.getPort();
    server1 = clusterStartUp.startRedisVM(1, locatorPort);
    clusterStartUp.startRedisVM(2, locatorPort);

    redisServerPort = AvailablePortHelper.getRandomAvailableTCPPort();
    clusterStartUp.startRedisVM(3, Integer.toString(redisServerPort), locatorPort);

    jedis = new JedisCluster(new HostAndPort(BIND_ADDRESS, redisServerPort), 10_000, 20);
  }

  @After
  public void cleanup() {
    clusterStartUp.flushAll();
  }

  @AfterClass
  public static void teardown() {
    jedis.close();
  }

  @Test
  public void givenBucketsMovedDuringRPush_elementsAreAddedAtomically() throws Exception {
    var running = new AtomicBoolean(true);
    var KEY = "key";

    Future<?> movingFuture = executor.submit(() -> {
      try {
        for (var i = 0; i < 20; i++) {
          clusterStartUp.moveBucketForKey(KEY);
          Thread.sleep(500);
        }
      } finally {
        running.set(false);
      }
    });

    var i = 0;
    while (running.get()) {
      jedis.rpush(KEY, value(i++), value(i++), value(i++), value(i++), value(i++));
    }
    movingFuture.get();

    compareBuckets();

    String popped;
    var j = 0;
    while ((popped = jedis.lpop(KEY)) != null) {
      assertThat(popped).isEqualTo(value(j));
      j++;
    }
  }

  @Test
  public void concurrentRPush_behavesCorrectly() {
    var KEY = "muggle";
    var rpushElements1 = IntStream.range(0, 10)
        .mapToObj(Integer::toString)
        .toArray(String[]::new);
    var rpushElements2 = IntStream.range(10, 20)
        .mapToObj(Integer::toString)
        .toArray(String[]::new);

    var expectedContents1 =
        Streams.concat(Arrays.stream(rpushElements1), Arrays.stream(rpushElements2))
            .toArray(String[]::new);

    var expectedContents2 =
        Streams.concat(Arrays.stream(rpushElements2), Arrays.stream(rpushElements1))
            .toArray(String[]::new);

    new ConcurrentLoopingThreads(1000,
        i -> jedis.rpush(KEY, rpushElements1),
        i -> jedis.rpush(KEY, rpushElements2))
            .runWithAction(() -> {
              var actualContents = new String[expectedContents1.length];
              for (var i = 0; i < actualContents.length; ++i) {
                actualContents[i] = jedis.lindex(KEY, i);
              }
              assertThat(actualContents).satisfiesAnyOf(
                  actual -> assertThat(actual).containsExactly(expectedContents1),
                  actual -> assertThat(actual).containsExactly(expectedContents2));
              jedis.del(KEY);
            });
  }

  @Test
  public void shouldNotLoseData_givenPrimaryServerCrashesDuringOperations() throws Exception {
    var running = new AtomicBoolean(true);
    var KEY = "key";
    var finalLocatorPort = locatorPort;
    var finalRedisPort = redisServerPort;

    Future<?> crasherFuture = executor.submit(() -> {
      try {
        for (var i = 0; i < 10 && running.get(); i++) {
          clusterStartUp.moveBucketForKey(KEY, "server-3");
          // Sleep for a bit so that rename can execute
          Thread.sleep(1000);
          clusterStartUp.crashVM(3);
          clusterStartUp.startRedisVM(3, x -> x
              .withProperty(GEODE_FOR_REDIS_PORT, Integer.toString(finalRedisPort))
              .withConnectionToLocator(finalLocatorPort));
          clusterStartUp.rebalanceAllRegions();
        }
      } finally {
        running.set(false);
      }
    });

    var i = 0;
    while (running.get()) {
      jedis.rpush(KEY, value(i++));
    }
    crasherFuture.get();

    compareBuckets();

    String popped;
    var j = 0;
    while ((popped = jedis.lpop(KEY)) != null) {
      try {
        assertThat(popped).isEqualTo(value(j));
        j++;
      } catch (AssertionError e) {
        // It's OK if there is a duplicate since any retries are not idempotent.
        assertThat(popped).as("duplicate check failed")
            .isEqualTo(value(j - 1));
      }
    }
  }

  private String value(int i) {
    return "value-" + i;
  }

  private void compareBuckets() {
    server1.invoke(() -> {
      var cache = ClusterStartupRule.getCache();
      var region =
          (PartitionedRegion) cache.getRegion(RegionProvider.DEFAULT_REDIS_REGION_NAME);
      for (var j = 0; j < region.getTotalNumberOfBuckets(); j++) {
        var buckets = region.getAllBucketEntries(j);
        assertThat(buckets.size()).isEqualTo(2);
        var bucket1 = buckets.get(0).getValues();
        var bucket2 = buckets.get(1).getValues();
        assertThat(bucket1).containsExactlyEntriesOf(bucket2);
      }
    });
  }
}
