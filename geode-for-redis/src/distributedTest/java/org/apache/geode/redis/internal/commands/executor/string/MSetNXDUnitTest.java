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

package org.apache.geode.redis.internal.commands.executor.string;

import static org.apache.geode.distributed.ConfigurationProperties.GEODE_FOR_REDIS_PORT;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class MSetNXDUnitTest {

  @ClassRule
  public static RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule(4);

  @ClassRule
  public static ExecutorServiceRule executor = new ExecutorServiceRule();

  private static final String HASHTAG = "{tag}";
  private static JedisCluster jedis;
  private static MemberVM server1;
  private static int locatorPort;
  private static int server3Port;

  @BeforeClass
  public static void classSetup() {
    final var locator = clusterStartUp.startLocatorVM(0);
    locatorPort = locator.getPort();
    server1 = clusterStartUp.startRedisVM(1, locatorPort);
    clusterStartUp.startRedisVM(2, locatorPort);

    server3Port = AvailablePortHelper.getRandomAvailableTCPPort();
    final var finalRedisPort = Integer.toString(server3Port);
    final var finalLocatorPort = locatorPort;
    clusterStartUp.startRedisVM(3, x -> x
        .withProperty(GEODE_FOR_REDIS_PORT, finalRedisPort)
        .withConnectionToLocator(finalLocatorPort));

    final var redisServerPort1 = clusterStartUp.getRedisPort(1);
    jedis = new JedisCluster(new HostAndPort(BIND_ADDRESS, redisServerPort1), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void after() {
    clusterStartUp.flushAll();
  }

  @AfterClass
  public static void tearDown() {
    jedis.close();
  }

  @Test
  public void testMSetnx_concurrentInstancesHandleBucketMovement() {
    var KEY_COUNT = 5000;
    var keys = new String[KEY_COUNT];

    for (var i = 0; i < keys.length; i++) {
      keys[i] = HASHTAG + "key" + i;
    }
    var keysAndValues1 = makeKeysAndValues(keys, "valueOne");
    var keysAndValues2 = makeKeysAndValues(keys, "valueTwo");

    new ConcurrentLoopingThreads(100,
        i -> jedis.msetnx(keysAndValues1),
        i -> jedis.msetnx(keysAndValues2),
        i -> clusterStartUp.moveBucketForKey(keys[0]))
            .runWithAction(() -> {
              assertThat(jedis.mget(keys)).satisfiesAnyOf(
                  values -> assertThat(values)
                      .allSatisfy(value -> assertThat(value).startsWith("valueOne")),
                  values -> assertThat(values)
                      .allSatisfy(value -> assertThat(value).startsWith("valueTwo")));
              jedis.del(keys);
            });
  }

  @Test
  public void testMSetnx_crashDoesNotLeaveInconsistencies() throws Exception {
    var KEY_COUNT = 1000;
    var keys = new String[KEY_COUNT];

    for (var i = 0; i < keys.length; i++) {
      keys[i] = HASHTAG + "key" + i;
    }
    var keysAndValues1 = makeKeysAndValues(keys, "valueOne");
    var keysAndValues2 = makeKeysAndValues(keys, "valueTwo");
    var running = new AtomicBoolean(true);

    final var finalRedisPort = Integer.toString(server3Port);
    final var finalLocatorPort = locatorPort;
    Future<?> future = executor.submit(() -> {
      for (var i = 0; i < 20 && running.get(); i++) {
        clusterStartUp.moveBucketForKey(keys[0], "server-3");
        // Sleep for a bit so that MSETs can execute
        Thread.sleep(2000);
        clusterStartUp.crashVM(3);
        clusterStartUp.startRedisVM(3, x -> x
            .withProperty(GEODE_FOR_REDIS_PORT, finalRedisPort)
            .withConnectionToLocator(finalLocatorPort));
        rebalanceAllRegions(server1);
      }
      running.set(false);
    });

    try {
      new ConcurrentLoopingThreads(running,
          i -> jedis.msetnx(keysAndValues1),
          i -> jedis.msetnx(keysAndValues2))
              .runWithAction(() -> {
                var count = 0;
                var values = jedis.mget(keys);
                for (var v : values) {
                  if (v == null) {
                    continue;
                  }
                  count += v.startsWith("valueOne") ? 1 : -1;
                }
                assertThat(Math.abs(count)).isEqualTo(KEY_COUNT);
                jedis.del(keys);
              });
    } finally {
      running.set(false);
      future.get();
    }
  }

  private String[] makeKeysAndValues(String[] keys, String valueBase) {
    var keysValues = new String[keys.length * 2];
    for (var i = 0; i < keys.length * 2; i += 2) {
      keysValues[i] = keys[i / 2];
      keysValues[i + 1] = valueBase + i;
    }

    return keysValues;
  }

  private static void rebalanceAllRegions(MemberVM vm) {
    vm.invoke("Running rebalance", () -> {
      var manager = ClusterStartupRule.getCache().getResourceManager();
      var factory = manager.createRebalanceFactory();
      try {
        factory.start().getResults();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
  }

}
