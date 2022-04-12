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
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.RedisIntegrationTest;

public abstract class AbstractExpireIntegrationTest implements RedisIntegrationTest {

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
  public void errors_givenWrongNumberOfArguments() {
    assertExactNumberOfArgs(jedis, Protocol.Command.EXPIRE, 2);
  }

  @Test
  public void givenInvalidTimestamp_returnsNotIntegerError() {
    assertThatThrownBy(() -> jedis.sendCommand("key", Protocol.Command.EXPIRE, "key", "notInteger"))
        .hasMessage(ERROR_NOT_INTEGER);
  }

  @Test
  public void Should_SetExpiration_givenKeyTo_StringValue() {

    var key = "key";
    var value = "value";
    jedis.set(key, value);

    Long timeToLive = jedis.ttl(key);

    assertThat(timeToLive).isEqualTo(-1);

    jedis.expire(key, 20L);

    timeToLive = jedis.ttl(key);
    assertThat(timeToLive).isGreaterThanOrEqualTo(15);
  }

  @Test
  public void should_setExpiration_givenKeyTo_SetValue() {

    var key = "key";
    var value = "value";

    jedis.sadd(key, value);
    Long timeToLive = jedis.ttl(key);

    assertThat(timeToLive).isEqualTo(-1);

    jedis.expire(key, 20L);
    timeToLive = jedis.ttl(key);

    assertThat(timeToLive).isGreaterThanOrEqualTo(15);
  }

  @Test
  public void should_setExpiration_givenKeyTo_HashValue() {

    var key = "key";
    var field = "field";
    var value = "value";

    jedis.hset(key, field, value);
    Long timeToLive = jedis.ttl(key);

    assertThat(timeToLive).isEqualTo(-1);

    jedis.expire(key, 20L);
    timeToLive = jedis.ttl(key);

    assertThat(timeToLive).isGreaterThanOrEqualTo(15);
  }

  @Test
  public void should_setExpiration_givenKeyTo_BitMapValue() {

    var key = "key";
    var offset = 1L;

    jedis.setbit(key, offset, false);

    Long timeToLive = jedis.ttl(key);
    assertThat(timeToLive).isEqualTo(-1);

    jedis.expire(key, 20L);
    timeToLive = jedis.ttl(key);

    assertThat(timeToLive).isGreaterThanOrEqualTo(15);
  }

  @Test
  public void settingAnExistingKeyToANewValue_ShouldClearExpirationTime() {

    var key = "key";
    var value = "value";
    var anotherValue = "anotherValue";
    jedis.set(key, value);

    jedis.expire(key, 20L);

    jedis.set(key, anotherValue);

    Long timeToLive = jedis.ttl(key);

    assertThat(timeToLive).isEqualTo(-1);
  }

  @Test
  public void callingGETSETonExistingKey_ShouldClearExpirationTime() {
    var key = "key";
    var value = "value";
    var anotherValue = "anotherValue";

    jedis.set(key, value);
    jedis.expire(key, 20L);

    jedis.getSet(key, anotherValue);
    Long timeToLive = jedis.ttl(key);

    assertThat(timeToLive).isEqualTo(-1);
  }

  @Test
  public void deletingAnExistingKeyAndRecreatingTheSameKey_ShouldClearExistingExpirationTime() {

    var key = "key";
    var value = "value";

    jedis.set(key, value);
    jedis.expire(key, 20L);

    jedis.del(key);
    jedis.set(key, value);
    Long timeToLive = jedis.ttl(key);

    assertThat(timeToLive).isEqualTo(-1);
  }

  @Test
  public void callingSDIFFSTOREonExistingKey_ShouldClearExpirationTime() {

    var key1 = "{tag1}key1";
    var key2 = "{tag1}key2";
    var key3 = "{tag1}key3";
    var value1 = "value1";
    var value2 = "value2";
    var value3 = "value3";

    jedis.sadd(key1, value1);
    jedis.sadd(key1, value2);

    jedis.sadd(key2, value2);

    jedis.sadd(key3, value3);
    jedis.expire(key3, 20L);

    jedis.sdiffstore(key3, key1, key2);

    Long timeToLive = jedis.ttl(key3);
    assertThat(timeToLive).isEqualTo(-1);
  }

  @Test
  public void callingSINTERSTOREonExistingKey_ShouldClearExpirationTime() {
    var key1 = "{tag1}key1";
    var key2 = "{tag1}key2";
    var key3 = "{tag1}key3";
    var value1 = "value1";
    var value2 = "value2";
    var value3 = "value3";

    jedis.sadd(key1, value1);
    jedis.sadd(key1, value2);

    jedis.sadd(key2, value2);

    jedis.sadd(key3, value3);
    jedis.expire(key3, 20L);

    jedis.sinterstore(key3, key1, key2);

    Long timeToLive = jedis.ttl(key3);
    assertThat(timeToLive).isEqualTo(-1);
  }

  @Test
  public void callingSUNIONSTOREonExistingKey_ShouldClearExpirationTime() {
    var key1 = "{tag1}key1";
    var key2 = "{tag1}key2";
    var key3 = "{tag1}key3";
    var value1 = "value1";
    var value2 = "value2";
    var value3 = "value3";

    jedis.sadd(key1, value1);
    jedis.sadd(key1, value2);

    jedis.sadd(key2, value2);

    jedis.sadd(key3, value3);
    jedis.expire(key3, 20L);

    jedis.sinterstore(key3, key1, key2);

    Long timeToLive = jedis.ttl(key3);
    assertThat(timeToLive).isEqualTo(-1);
  }

  @Test
  public void callingINCRonExistingKey_should_NOT_ClearExpirationTime() {
    var key = "key";
    var value = "0";

    jedis.set(key, value);
    jedis.expire(key, 20L);

    jedis.incr(key);

    Long timeToLive = jedis.ttl(key);
    assertThat(timeToLive).isGreaterThanOrEqualTo(15);
  }

  @Test
  public void usingHSETCommandToAlterAFieldValue_should_NOT_ClearExpirationTimeOnKey() {
    var key = "key";
    var field = "field";
    var value = "value";
    var value2 = "value2";

    jedis.hset(key, field, value);

    jedis.expire(key, 20L);

    jedis.hset(key, field, value2);

    Long timeToLive = jedis.ttl(key);

    assertThat(timeToLive).isGreaterThanOrEqualTo(15);
  }

  @Test
  public void callingRENAMEonExistingKey_shouldTransferExpirationTimeToNewKeyName_GivenNewName_Not_InUse() {
    var key = "{tag1}key";
    var newKeyName = "{tag1}new key name";
    var value = "value";
    jedis.set(key, value);
    jedis.expire(key, 20L);

    jedis.rename(key, newKeyName);

    Long timeToLive = jedis.ttl(newKeyName);
    assertThat(timeToLive).isGreaterThanOrEqualTo(15);
  }

  @Test
  public void callingRENAMEonExistingKey_shouldTransferExpirationTimeToNewKeyName_GivenNewName_is_InUse_ButNo_ExpirationSet() {
    var key = "{tag1}key";
    var key2 = "{tag1}key2";
    var value = "value";

    jedis.set(key, value);
    jedis.expire(key, 20L);

    jedis.set(key2, value);

    jedis.rename(key, key2);

    Long timeToLive = jedis.ttl(key2);
    assertThat(timeToLive).isGreaterThanOrEqualTo(15);
  }

  @Test
  public void callingRENAMEonExistingKey_shouldTransferExpirationTimeToNewKeyName_GivenNewName_is_InUse_AndHas_ExpirationSet() {
    var key = "{tag1}key";
    var key2 = "{tag1}key2";
    var value = "value";

    jedis.set(key, value);
    jedis.expire(key, 20L);

    jedis.set(key2, value);
    jedis.expire(key2, 14L);

    jedis.rename(key, key2);

    Long timeToLive = jedis.ttl(key2);
    assertThat(timeToLive).isGreaterThanOrEqualTo(15);
  }

  @Test
  public void SettingExpirationToNegativeValue_ShouldDeleteKey() {

    var key = "key";
    var value = "value";
    jedis.set(key, value);

    Long expirationWasSet = jedis.expire(key, -5L);
    assertThat(expirationWasSet).isEqualTo(1);

    Boolean keyExists = jedis.exists(key);
    assertThat(keyExists).isFalse();
  }


  @Test
  public void CallingExpireOnAKeyThatAlreadyHasAnExpirationTime_ShouldUpdateTheExpirationTime() {
    var key = "key";
    var value = "value";
    jedis.set(key, value);

    jedis.expire(key, 20L);
    jedis.expire(key, 20000L);

    Long timeToLive = jedis.ttl(key);
    assertThat(timeToLive).isGreaterThan(21);
  }
}
