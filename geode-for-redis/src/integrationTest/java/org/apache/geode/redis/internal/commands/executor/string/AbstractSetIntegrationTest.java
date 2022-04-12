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

import static org.apache.geode.redis.internal.RedisConstants.ERROR_INVALID_EXPIRE_TIME;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;
import static org.apache.geode.redis.internal.RedisConstants.WRONG_NUMBER_OF_ARGUMENTS_FOR_COMMAND;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static redis.clients.jedis.Protocol.Command.SET;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.SoftAssertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.params.SetParams;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.RedisIntegrationTest;

public abstract class AbstractSetIntegrationTest implements RedisIntegrationTest {

  private JedisCluster jedis;
  private final String key = "key";
  private final String value = "value";
  private static final int ITERATION_COUNT = 4000;

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
  public void givenKeyNotProvided_returnsWrongNumberOfArgumentsError() {
    assertThatThrownBy(() -> jedis.sendCommand(key, Protocol.Command.SET))
        .hasMessage(String.format(WRONG_NUMBER_OF_ARGUMENTS_FOR_COMMAND, "set"));
  }

  @Test
  public void givenValueNotProvided_returnsWrongNumberOfArgumentsError() {
    assertThatThrownBy(() -> jedis.sendCommand(key, Protocol.Command.SET, key))
        .hasMessage(String.format(WRONG_NUMBER_OF_ARGUMENTS_FOR_COMMAND, "set"));
  }

  @Test
  public void givenEXKeyword_withoutParameter_returnsSyntaxError() {
    assertThatThrownBy(() -> jedis.sendCommand(key, Protocol.Command.SET, key, value, "EX"))
        .hasMessage(ERROR_SYNTAX);
  }

  @Test
  public void givenEXKeyword_whenParameterIsNotAnInteger_returnsNotIntegerError() {
    assertThatThrownBy(
        () -> jedis.sendCommand(key, Protocol.Command.SET, key, value, "EX", "NaN"))
            .hasMessage(ERROR_NOT_INTEGER);
  }

  @Test
  public void givenEXKeyword_whenParameterIsZero_returnsInvalidExpireTimeError() {
    assertThatThrownBy(
        () -> jedis.sendCommand(key, Protocol.Command.SET, key, value, "PX", "0"))
            .hasMessage(String.format(ERROR_INVALID_EXPIRE_TIME, "set"));
  }

  @Test
  public void givenPXKeyword_withoutParameter_returnsSyntaxError() {
    assertThatThrownBy(() -> jedis.sendCommand(key, Protocol.Command.SET, key, value, "PX"))
        .hasMessage(ERROR_SYNTAX);
  }

  @Test
  public void givenPXKeyword_whenParameterIsNotAnInteger_returnsNotIntegerError() {
    assertThatThrownBy(
        () -> jedis.sendCommand(key, Protocol.Command.SET, key, value, "PX", "NaN"))
            .hasMessage(ERROR_NOT_INTEGER);
  }

  @Test
  public void givenPXKeyword_whenParameterIsZero_returnsInvalidExpireTimeError() {
    assertThatThrownBy(
        () -> jedis.sendCommand(key, Protocol.Command.SET, key, value, "PX", "0"))
            .hasMessage(String.format(ERROR_INVALID_EXPIRE_TIME, "set"));
  }

  @Test
  public void givenPXAndEXInSameCommand_returnsSyntaxError() {
    assertThatThrownBy(
        () -> jedis.sendCommand(key, Protocol.Command.SET, key, value, "PX", "3000", "EX",
            "3")).hasMessage(ERROR_SYNTAX);
  }

  @Test
  public void givenNXAndXXInSameCommand_returnsSyntaxError() {
    assertThatThrownBy(
        () -> jedis.sendCommand(key, Protocol.Command.SET, key, value, "NX", "XX"))
            .hasMessage(ERROR_SYNTAX);
  }

  @Test
  public void givenInvalidKeyword_returnsSyntaxError() {
    assertThatThrownBy(
        () -> jedis.sendCommand(key, Protocol.Command.SET, key, value, "invalidKeyword"))
            .hasMessage(ERROR_SYNTAX);
  }

  @Test
  public void testSET_shouldSetStringValueToKey_givenEmptyKey() {
    var result = jedis.get(key);
    assertThat(result).isNull();

    jedis.set(key, value);
    result = jedis.get(key);
    assertThat(result).isEqualTo(value);
  }

  @Test
  public void testSET_shouldSetStringValueToKey_givenKeyIsOfDataTypeSet() {
    jedis.sadd(key, "member1", "member2");

    jedis.set(key, value);
    var result = jedis.get(key);

    assertThat(result).isEqualTo(value);
  }

  @Test
  public void testSET_shouldSetStringValueToKey_givenKeyIsOfDataTypeHash() {
    jedis.hset(key, "field", "something else");

    var result = jedis.set(key, value);
    assertThat(result).isEqualTo("OK");

    assertThat(value).isEqualTo(jedis.get(key));
  }

  @Test
  public void testSET_shouldSetNX_evenIfKeyContainsOtherDataType() {
    jedis.sadd(key, "member1", "member2");
    var setParams = new SetParams();
    setParams.nx();

    var result = jedis.set(key, value, setParams);
    assertThat(result).isNull();
  }

  @Test
  public void testSET_shouldSetXX_evenIfKeyContainsOtherDataType() {
    jedis.sadd(key, "member1", "member2");
    var setParams = new SetParams();
    setParams.xx();

    jedis.set(key, value, setParams);
    var result = jedis.get(key);

    assertThat(result).isEqualTo(value);
  }

  @Test
  public void testSET_withNXAndExArguments() {
    var setParams = new SetParams();
    setParams.nx();
    setParams.ex(20L);

    jedis.set(key, value, setParams);
    assertThat(jedis.ttl(key)).isGreaterThan(15);
    assertThat(jedis.get(key)).isEqualTo(value);
  }

  @Test
  public void testSET_withXXAndExArguments() {
    jedis.set(key, "differentValue");

    var setParams = new SetParams();
    setParams.xx();
    setParams.ex(20L);

    jedis.set(key, value, setParams);
    assertThat(jedis.ttl(key)).isGreaterThan(15);
    assertThat(jedis.get(key)).isEqualTo(value);
  }

  @Test
  public void testSET_withNXAndPxArguments() {
    var setParams = new SetParams();
    setParams.nx();
    setParams.px(2000);

    jedis.set(key, value, setParams);
    assertThat(jedis.pttl(key)).isGreaterThan(1500);
    assertThat(jedis.get(key)).isEqualTo(value);
  }

  @Test
  public void testSET_withXXAndPxArguments() {
    jedis.set(key, "differentValue");

    var setParams = new SetParams();
    setParams.xx();
    setParams.px(2000);

    jedis.set(key, value, setParams);
    assertThat(jedis.pttl(key)).isGreaterThan(1500);
    assertThat(jedis.get(key)).isEqualTo(value);
  }

  @Test
  public void setNX_shouldNotConflictWithRegularSet() {
    List<String> keys = new ArrayList<>();
    List<String> values = new ArrayList<>();
    for (var i = 0; i < ITERATION_COUNT; i++) {
      keys.add("key-" + i);
      values.add("value-" + i);
    }

    var counter = new AtomicInteger(0);
    var setParams = new SetParams();
    setParams.nx();

    new ConcurrentLoopingThreads(ITERATION_COUNT,
        (i) -> {
          var ok = jedis.set(keys.get(i), values.get(i));
          if ("OK".equals(ok)) {
            counter.addAndGet(1);
          }
        },
        (i) -> jedis.set(keys.get(i), values.get(i), setParams))
            .run();

    assertThat(counter.get()).isEqualTo(ITERATION_COUNT);
  }

  @Test
  public void testSET_withEXArgument_shouldSetExpireTime() {
    long secondsUntilExpiration = 20;

    var setParams = new SetParams();
    setParams.ex(secondsUntilExpiration);

    jedis.set(key, value, setParams);

    Long result = jedis.ttl(key);

    assertThat(result).isGreaterThan(15L);
  }

  @Test
  public void testSET_withNegativeEXTime_shouldReturnError() {
    long millisecondsUntilExpiration = -1;

    var setParams = new SetParams();
    setParams.ex(millisecondsUntilExpiration);

    assertThatThrownBy(() -> jedis.set(key, value, setParams))
        .hasMessage(String.format(ERROR_INVALID_EXPIRE_TIME, "set"));
  }

  @Test
  public void set_withEX_withNonIntegerExpiration_returnsError() {
    assertThatThrownBy(() -> jedis.sendCommand("key", SET, "key", "value", "EX", "notAnInteger"))
        .hasMessage(ERROR_NOT_INTEGER);
  }

  @Test
  public void testSET_withPXArgument_shouldSetExpireTime() {
    var millisecondsUntilExpiration = 20000;

    var setParams = new SetParams();
    setParams.px(millisecondsUntilExpiration);

    jedis.set(key, value, setParams);

    Long result = jedis.ttl(key);

    assertThat(result).isGreaterThan(15L);
  }

  @Test
  public void testSET_withNegativePXTime_shouldReturnError() {
    var millisecondsUntilExpiration = -1;

    var setParams = new SetParams();
    setParams.px(millisecondsUntilExpiration);

    assertThatThrownBy(() -> jedis.set(key, value, setParams))
        .hasMessage(String.format(ERROR_INVALID_EXPIRE_TIME, "set"));
  }

  @Test
  public void set_withPX_withNonIntegerExpiration_returnsError() {
    assertThatThrownBy(() -> jedis.sendCommand("key", SET, "key", "value", "EX", "notAnInteger"))
        .hasMessage(ERROR_NOT_INTEGER);
  }

  @Test
  public void testSET_shouldClearPreviousTTL() {
    long secondsUntilExpiration = 20;

    var setParams = new SetParams();
    setParams.ex(secondsUntilExpiration);

    jedis.set(key, value, setParams);

    jedis.set(key, "other value");

    Long result = jedis.ttl(key);

    assertThat(result).isEqualTo(-1L);
  }

  @Test
  public void testSET_withXXArgument_shouldClearPreviousTTL() {
    var value = "did exist";
    long secondsUntilExpiration = 20;
    var setParamsXX = new SetParams();
    setParamsXX.xx();
    var setParamsEX = new SetParams();
    setParamsEX.ex(secondsUntilExpiration);
    var result_EX = jedis.set(key, value, setParamsEX);
    assertThat(result_EX).isEqualTo("OK");
    assertThat(jedis.ttl(key)).isGreaterThan(15L);

    var result_XX = jedis.set(key, value, setParamsXX);

    assertThat(result_XX).isEqualTo("OK");
    Long result = jedis.ttl(key);
    assertThat(result).isEqualTo(-1L);
  }

  @Test
  public void testSET_shouldNotClearPreviousTTL_onFailure() {
    var key_NX = "nx_key";
    var value_NX = "set only if key did not exist";
    long secondsUntilExpiration = 20;

    var setParamsEX = new SetParams();
    setParamsEX.ex(secondsUntilExpiration);

    var setParamsNX = new SetParams();
    setParamsNX.nx();

    jedis.set(key_NX, value_NX, setParamsEX);
    var result_NX = jedis.set(key_NX, value_NX, setParamsNX);
    assertThat(result_NX).isNull();

    Long result = jedis.ttl(key_NX);
    assertThat(result).isGreaterThan(15L);
  }

  @Test
  @Ignore("KEEPTTL is part of redis 6")
  public void testSET_withKEEPTTL_shouldRetainPreviousTTL_onSuccess() {
    long secondsToExpire = 30;

    var setParamsEx = new SetParams();
    setParamsEx.ex(secondsToExpire);

    jedis.set(key, value, setParamsEx);

    var setParamsKeepTTL = new SetParams();
    // setParamsKeepTTL.keepTtl();
    // Jedis Doesn't support KEEPTTL yet.

    jedis.set(key, "newValue", setParamsKeepTTL);

    Long result = jedis.ttl(key);
    assertThat(result).isGreaterThan(15L);
  }

  @Test
  public void testSET_withNXArgument_shouldOnlySetKeyIfKeyDoesNotExist() {
    var key1 = "key_1";
    var key2 = "key_2";
    var value1 = "value_1";
    var value2 = "value_2";

    jedis.set(key1, value1);

    var setParams = new SetParams();
    setParams.nx();

    jedis.set(key1, value2, setParams);
    var result1 = jedis.get(key1);

    assertThat(result1).isEqualTo(value1);

    jedis.set(key2, value2, setParams);
    var result2 = jedis.get(key2);

    assertThat(result2).isEqualTo(value2);
  }

  @Test
  public void testSET_withXXArgument_shouldOnlySetKeyIfKeyExists() {
    var key1 = "key_1";
    var key2 = "key_2";
    var value1 = "value_1";
    var value2 = "value_2";

    jedis.set(key1, value1);

    var setParams = new SetParams();
    setParams.xx();

    jedis.set(key1, value2, setParams);
    var result1 = jedis.get(key1);

    assertThat(result1).isEqualTo(value2);

    jedis.set(key2, value2, setParams);
    var result2 = jedis.get(key2);

    assertThat(result2).isNull();
  }

  @Test
  public void testSET_XX_NX_arguments_shouldReturnOK_if_Successful() {
    var key_NX = "nx_key";
    var key_XX = "xx_key";
    var value_NX = "did not exist";
    var value_XX = "did exist";

    var setParamsXX = new SetParams();
    setParamsXX.xx();

    var setParamsNX = new SetParams();
    setParamsNX.nx();

    var result_NX = jedis.set(key_NX, value_NX, setParamsNX);
    assertThat(result_NX).isEqualTo("OK");

    jedis.set(key_XX, value_XX);
    var result_XX = jedis.set(key_NX, value_NX, setParamsXX);
    assertThat(result_XX).isEqualTo("OK");
  }

  @Test
  public void testSET_XX_NX_arguments_should_return_NULL_if_Not_Successful() {
    var key_NX = "nx_key";
    var key_XX = "xx_key";
    var value_NX = "set only if key did not exist";
    var value_XX = "set only if key did exist";

    var setParamsXX = new SetParams();
    setParamsXX.xx();

    var setParamsNX = new SetParams();
    setParamsNX.nx();

    jedis.set(key_NX, value_NX);
    var result_NX = jedis.set(key_NX, value_NX, setParamsNX);
    assertThat(result_NX).isNull();

    var result_XX = jedis.set(key_XX, value_XX, setParamsXX);
    assertThat(result_XX).isNull();
  }

  @Test
  public void testSET_withInvalidOptions() {
    var soft = new SoftAssertions();

    soft.assertThatThrownBy(() -> jedis.sendCommand(key, SET))
        .as("no key")
        .hasMessage(String.format(WRONG_NUMBER_OF_ARGUMENTS_FOR_COMMAND, "set"));

    soft.assertThatThrownBy(() -> jedis.sendCommand(key, SET, key, "EX", "0"))
        .as("no value")
        .hasMessage(ERROR_SYNTAX);

    soft.assertThatThrownBy(() -> jedis.sendCommand(key, SET, key, value, "EX", "a"))
        .as("non-integer expiration value")
        .hasMessage(ERROR_NOT_INTEGER);

    soft.assertThatThrownBy(() -> jedis.sendCommand(key, SET, key, value, "PX", "1", "EX", "0"))
        .as("both PX and EX provided")
        .hasMessage(ERROR_SYNTAX);

    soft.assertThatThrownBy(() -> jedis.sendCommand(key, SET, key, value, "PX", "1", "XX", "0"))
        .as("extra integer option as last option")
        .hasMessage(ERROR_SYNTAX);

    soft.assertThatThrownBy(() -> jedis.sendCommand(key, SET, key, value, "PX", "XX", "0"))
        .as("expiration option used with no integer value")
        .hasMessage(ERROR_SYNTAX);

    soft.assertThatThrownBy(() -> jedis.sendCommand(key, SET, key, value, "1", "PX", "1"))
        .as("extra integer option as first option")
        .hasMessage(ERROR_SYNTAX);

    soft.assertThatThrownBy(() -> jedis.sendCommand(key, SET, key, value, "NX", "XX"))
        .as("both NX and XX provided")
        .hasMessage(ERROR_SYNTAX);

    soft.assertThatThrownBy(() -> jedis.sendCommand(key, SET, key, value, "NX", "a"))
        .as("invalid option after valid option")
        .hasMessage(ERROR_SYNTAX);

    soft.assertThatThrownBy(() -> jedis.sendCommand(key, SET, key, value, "blah"))
        .as("invalid option")
        .hasMessage(ERROR_SYNTAX);

    soft.assertAll();
  }

  @Test
  public void testSET_withBinaryKeyAndValue() {
    var blob = new byte[256];
    for (var i = 0; i < 256; i++) {
      blob[i] = (byte) i;
    }

    jedis.set(blob, blob);
    var result = jedis.get(blob);

    assertThat(result).isEqualTo(blob);
  }

}
