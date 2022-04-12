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

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertExactNumberOfArgs;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NAN_OR_INFINITY;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_A_VALID_FLOAT;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_WRONG_TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public abstract class AbstractIncrByFloatIntegrationTest implements RedisIntegrationTest {

  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  private JedisCluster jedis;

  @Before
  public void setUp() {
    jedis = new JedisCluster(new HostAndPort("localhost", getPort()), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    flushAll();
    jedis.close();
  }

  @Test
  public void errors_givenWrongNumberOfParameters() {
    assertExactNumberOfArgs(jedis, Protocol.Command.INCRBYFLOAT, 2);
  }

  @Test
  public void testIncrByFloat() {
    var key1 = "key1";
    var key2 = "key2";
    var incr1 = 23.5;
    var incr2 = -14.78;
    double num1 = 100;
    double num2 = -100;
    jedis.set(key1, "" + num1);
    jedis.set(key2, "" + num2);

    jedis.incrByFloat(key1, incr1);
    jedis.incrByFloat(key2, incr2);

    assertThat(Double.valueOf(jedis.get(key1))).isEqualTo(num1 + incr1);
    assertThat(Double.valueOf(jedis.get(key2))).isEqualTo(num2 + incr2);
  }

  @Test
  public void testIncrByFloat_whenUsingExponents() {
    var key1 = "key1";
    var num1 = 5e2;
    jedis.set(key1, "5e2");

    var incr1 = 2.0e4;
    jedis.sendCommand(key1, Protocol.Command.INCRBYFLOAT, key1, "2.0e4");
    assertThat(Double.valueOf(jedis.get(key1))).isEqualTo(num1 + incr1);
  }

  @Test
  public void testCorrectErrorIsReturned_whenKeyIsNotANumber() {
    jedis.set("nan", "abc");

    assertThatThrownBy(() -> jedis.incrByFloat("nan", 1)).hasMessage(ERROR_NOT_A_VALID_FLOAT);
  }

  @Test
  public void testCorrectErrorIsReturned_whenKeyIsAnIncorrectType() {
    jedis.sadd("set", "abc");

    assertThatThrownBy(() -> jedis.incrByFloat("set", 1)).hasMessage(ERROR_WRONG_TYPE);
  }

  @Test
  public void testCorrectErrorIsReturned_whenIncrByIsInvalid() {
    var key = "number";
    var number1 = 1.4;
    jedis.set(key, "" + number1);

    assertThatThrownBy(() -> jedis.sendCommand(key, Protocol.Command.INCRBYFLOAT, key, " a b c"))
        .hasMessage(ERROR_NOT_A_VALID_FLOAT);
  }

  @Test
  public void testIncrByFloat_withInfinityAndVariants() {
    var key = "number";
    jedis.set(key, "1.4");

    assertThatThrownBy(() -> jedis.sendCommand(key, Protocol.Command.INCRBYFLOAT, key, "+inf"))
        .hasMessage(ERROR_NAN_OR_INFINITY);

    assertThatThrownBy(() -> jedis.sendCommand(key, Protocol.Command.INCRBYFLOAT, key, "-inf"))
        .hasMessage(ERROR_NAN_OR_INFINITY);

    assertThatThrownBy(() -> jedis.sendCommand(key, Protocol.Command.INCRBYFLOAT, key, "inf"))
        .hasMessage(ERROR_NAN_OR_INFINITY);

    assertThatThrownBy(() -> jedis.sendCommand(key, Protocol.Command.INCRBYFLOAT, key, "+infinity"))
        .hasMessage(ERROR_NAN_OR_INFINITY);

    assertThatThrownBy(() -> jedis.sendCommand(key, Protocol.Command.INCRBYFLOAT, key, "-infinity"))
        .hasMessage(ERROR_NAN_OR_INFINITY);

    assertThatThrownBy(() -> jedis.sendCommand(key, Protocol.Command.INCRBYFLOAT, key, "infinity"))
        .hasMessage(ERROR_NAN_OR_INFINITY);

    assertThatThrownBy(() -> jedis.sendCommand(key, Protocol.Command.INCRBYFLOAT, key, "nan"))
        .hasMessage(ERROR_NOT_A_VALID_FLOAT);

    assertThatThrownBy(() -> jedis.sendCommand(key, Protocol.Command.INCRBYFLOAT, key, "infant"))
        .hasMessage(ERROR_NOT_A_VALID_FLOAT);
  }

  @Test
  public void testIncrByFloat_withReallyBigNumbers() {
    var key = "number";
    // max unsigned long long - 1
    var biggy = new BigDecimal("18446744073709551614");
    jedis.set(key, biggy.toPlainString());

    // Beyond this, native redis produces inconsistent results.
    var rawResult = jedis.sendCommand(key, Protocol.Command.INCRBYFLOAT, key, "1");
    var result = new BigDecimal(new String((byte[]) rawResult));

    assertThat(result.toPlainString()).isEqualTo(biggy.add(BigDecimal.ONE).toPlainString());
  }

  @Test
  public void testConcurrentIncrByFloat_performsAllIncrByFloats() {
    var key = "key";
    var random = new Random();

    var expectedValue = new AtomicReference<BigDecimal>();
    expectedValue.set(new BigDecimal(0));

    jedis.set(key, "0");

    new ConcurrentLoopingThreads(1000,
        (i) -> {
          var increment = BigDecimal.valueOf(random.nextInt(37));
          expectedValue.getAndUpdate(x -> x.add(increment));
          jedis.sendCommand(key, Protocol.Command.INCRBYFLOAT, key, increment.toPlainString());
        },
        (i) -> {
          var increment = BigDecimal.valueOf(random.nextInt(37));
          expectedValue.getAndUpdate(x -> x.add(increment));
          jedis.sendCommand(key, Protocol.Command.INCRBYFLOAT, key, increment.toPlainString());
        }).run();

    assertThat(new BigDecimal(jedis.get(key))).isEqualTo(expectedValue.get());
  }
}
