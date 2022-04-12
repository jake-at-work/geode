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

import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;

public class PersistDUnitTest {

  @ClassRule
  public static RedisClusterStartupRule redisClusterStartupRule = new RedisClusterStartupRule(5);
  private static JedisCluster jedis;

  private static VM client1;
  private static VM client2;

  @BeforeClass
  public static void setup() {
    var locator = redisClusterStartupRule.startLocatorVM(0);
    redisClusterStartupRule.startRedisVM(1, locator.getPort());
    redisClusterStartupRule.startRedisVM(2, locator.getPort());
    var serverPort = redisClusterStartupRule.getRedisPort(1);

    client1 = redisClusterStartupRule.getVM(3);
    client2 = redisClusterStartupRule.getVM(4);

    jedis = new JedisCluster(new HostAndPort(BIND_ADDRESS, serverPort), REDIS_CLIENT_TIMEOUT);
  }

  private static class ConcurrentPersistOperation extends SerializableCallable<AtomicLong> {

    private final int port;
    private final String keyBaseName;
    private final AtomicLong persistedCount;
    private final long iterationCount;

    protected ConcurrentPersistOperation(int port, String keyBaseName, long iterationCount) {
      this.port = port;
      this.keyBaseName = keyBaseName;
      persistedCount = new AtomicLong(0);
      this.iterationCount = iterationCount;
    }

    @Override
    public AtomicLong call() {
      var internalJedisCluster =
          new JedisCluster(new HostAndPort(BIND_ADDRESS, port), REDIS_CLIENT_TIMEOUT);

      for (var i = 0; i < iterationCount; i++) {
        var key = keyBaseName + i;
        persistedCount.addAndGet(internalJedisCluster.persist(key));
      }
      return persistedCount;
    }
  }

  @Test
  public void testConcurrentPersistOperations_shouldReportCorrectNumberOfTotalKeysPersisted()
      throws InterruptedException {
    Long iterationCount = 5000L;
    var serverPort = redisClusterStartupRule.getRedisPort(1);

    setKeysWithExpiration(jedis, iterationCount, "key");

    var remotePersistInvocationClient1 =
        client1.invokeAsync("remotePersistInvocation1",
            new ConcurrentPersistOperation(serverPort, "key", iterationCount));

    var remotePersistInvocationClient2 =
        client2.invoke("remotePersistInvocation2",
            new ConcurrentPersistOperation(serverPort, "key", iterationCount));

    remotePersistInvocationClient1.await();

    // Sum of persisted counts returned from both clients should equal total number of keys set
    // (iteration count)
    assertThat(remotePersistInvocationClient2.get() + remotePersistInvocationClient1.get().get())
        .isEqualTo(iterationCount);
  }

  private void setKeysWithExpiration(JedisCluster jedis, Long iterationCount, String key) {
    for (var i = 0; i < iterationCount; i++) {
      jedis.sadd(key + i, "value" + 9);
      jedis.expire(key + i, 600L);
    }
  }
}
