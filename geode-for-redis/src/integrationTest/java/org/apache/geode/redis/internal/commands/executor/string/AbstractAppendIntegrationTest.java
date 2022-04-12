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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public abstract class AbstractAppendIntegrationTest implements RedisIntegrationTest {

  private JedisCluster jedis;
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

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
  public void testAppend_shouldAppendValueWithInputStringAndReturnResultingLength() {
    var key = "key";
    var value = randString();
    var originalValueLength = value.length();

    var result = jedis.exists(key);
    assertThat(result).isFalse();

    Long output = jedis.append(key, value);
    assertThat(output).isEqualTo(originalValueLength);

    var randomString = randString();

    output = jedis.append(key, randomString);
    assertThat(output).isEqualTo(originalValueLength + randomString.length());

    var finalValue = jedis.get(key);
    assertThat(finalValue).isEqualTo(value.concat(randomString));
  }


  @Test
  public void testAppend_concurrent() {
    var listSize = 1000;
    var key = "key";

    var values1 = makeStringList(listSize, "values1-");
    var values2 = makeStringList(listSize, "values2-");

    new ConcurrentLoopingThreads(listSize,
        (i) -> jedis.append(key, values1.get(i)),
        (i) -> jedis.append(key, values2.get(i))).run();

    for (var i = 0; i < listSize; i++) {
      assertThat(jedis.get(key)).contains(values1.get(i));
      assertThat(jedis.get(key)).contains(values2.get(i));
    }
  }

  @Test
  public void testAppend_withBinaryKeyAndValue() {
    var blob = new byte[256];
    var doubleBlob = new byte[512];
    for (var i = 0; i < 256; i++) {
      blob[i] = (byte) i;
      doubleBlob[i] = (byte) i;
      doubleBlob[i + 256] = (byte) i;
    }

    jedis.set(blob, blob);
    jedis.append(blob, blob);
    var result = jedis.get(blob);

    assertThat(result).isEqualTo(doubleBlob);
  }

  @Test
  public void testAppend_withUTF16KeyAndValue() throws IOException {
    var test_utf16_string = "最𐐷𤭢";
    var testBytes = test_utf16_string.getBytes(StandardCharsets.UTF_16);

    var output = new ByteArrayOutputStream();
    output.write(testBytes);
    output.write(testBytes);
    var appendedBytes = output.toByteArray();

    jedis.set(testBytes, testBytes);
    jedis.append(testBytes, testBytes);
    var result = jedis.get(testBytes);
    assertThat(result).isEqualTo(appendedBytes);
  }

  private String randString() {
    return Long.toHexString(Double.doubleToLongBits(Math.random()));
  }

  private List<String> makeStringList(int setSize, String baseString) {
    List<String> strings = new ArrayList<>();
    for (var i = 0; i < setSize; i++) {
      strings.add(baseString + i);
    }
    return strings;
  }
}
