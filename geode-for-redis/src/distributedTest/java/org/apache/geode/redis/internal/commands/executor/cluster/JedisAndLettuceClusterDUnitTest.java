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

package org.apache.geode.redis.internal.commands.executor.cluster;

import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;

public class JedisAndLettuceClusterDUnitTest {

  @ClassRule
  public static RedisClusterStartupRule cluster = new RedisClusterStartupRule();

  private static final int KEYS = 1000;
  private static int redisServerPort1;

  @BeforeClass
  public static void classSetup() {
    var locator = cluster.startLocatorVM(0);
    cluster.startRedisVM(1, locator.getPort());
    cluster.startRedisVM(2, locator.getPort());

    redisServerPort1 = cluster.getRedisPort(1);
  }

  @Test
  public void testJedisClusterCompatibility() {
    try (var jedis =
        new JedisCluster(new HostAndPort(BIND_ADDRESS, redisServerPort1),
            REDIS_CLIENT_TIMEOUT)) {

      for (var i = 0; i < KEYS; i++) {
        var key = "jedis-" + i;
        var value = "value-" + i;
        jedis.set(key, value);
        assertThat(jedis.get(key)).isEqualTo(value);
      }
    }
  }

  @Test
  public void testLettuceClusterCompatibility() {
    var clusterClient = RedisClusterClient.create(
        new RedisURI("localhost", redisServerPort1, Duration.ofSeconds(60)));
    var commands =
        clusterClient.connect().sync();

    for (var i = 0; i < KEYS; i++) {
      var key = "lettuce-" + i;
      var value = "value-" + i;
      commands.set(key, value);
      assertThat(commands.get(key)).isEqualTo(value);
    }

    clusterClient.shutdown();
  }

}
