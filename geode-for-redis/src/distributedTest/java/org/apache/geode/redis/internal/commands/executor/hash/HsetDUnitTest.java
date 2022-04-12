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

package org.apache.geode.redis.internal.commands.executor.hash;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;

public class HsetDUnitTest {

  @ClassRule
  public static RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule(4);

  private static final String LOCAL_HOST = "127.0.0.1";
  private static final int HASH_SIZE = 1000;
  private static final int JEDIS_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  private static JedisCluster jedis;

  private static MemberVM locator;
  private static MemberVM server1;
  private static MemberVM server2;
  private static MemberVM server3;

  @BeforeClass
  public static void classSetup() {
    locator = clusterStartUp.startLocatorVM(0);
    server1 = clusterStartUp.startRedisVM(1, locator.getPort());
    server2 = clusterStartUp.startRedisVM(2, locator.getPort());
    server3 = clusterStartUp.startRedisVM(3, locator.getPort());

    var redisServerPort = clusterStartUp.getRedisPort(1);
    jedis = new JedisCluster(new HostAndPort(LOCAL_HOST, redisServerPort), JEDIS_TIMEOUT);
  }

  @Before
  public void testSetup() {
    clusterStartUp.flushAll();
  }

  @AfterClass
  public static void tearDown() {
    jedis.close();
  }

  @Test
  public void shouldDistributeDataAmongCluster_givenMultipleClients() {

    var key = "key";

    var testMap = makeHashMap(HASH_SIZE, "field-", "value-");

    jedis.hset(key, testMap);

    var result = jedis.hgetAll(key);

    assertThat(result.keySet().toArray()).containsExactlyInAnyOrder(testMap.keySet().toArray());
    assertThat(result.values().toArray()).containsExactlyInAnyOrder(testMap.values().toArray());

  }


  @Test
  public void shouldDistributeDataAmongCluster_givenMultipleThreadsAddingDifferentDataToSameHashConcurrently() {

    var key = "key";

    var testMap1 = makeHashMap(HASH_SIZE, "field1-", "value1-");
    var testMap2 = makeHashMap(HASH_SIZE, "field2-", "value2-");

    Map<String, String> wholeMap = new HashMap<>();
    wholeMap.putAll(testMap1);
    wholeMap.putAll(testMap2);

    var testMap1Fields = testMap1.keySet().toArray(new String[] {});
    var testMap2Fields = testMap2.keySet().toArray(new String[] {});

    var hsetJedis1Consumer = makeHSetConsumer(testMap1, testMap1Fields, key, jedis);
    var hsetJedis2Consumer = makeHSetConsumer(testMap2, testMap2Fields, key, jedis);

    new ConcurrentLoopingThreads(HASH_SIZE, hsetJedis1Consumer, hsetJedis2Consumer).run();

    var results = jedis.hgetAll(key);

    assertThat(results.keySet().toArray()).containsExactlyInAnyOrder(wholeMap.keySet().toArray());
    assertThat(results.values().toArray()).containsExactlyInAnyOrder(wholeMap.values().toArray());
  }

  @Test
  public void shouldDistributeDataAmongCluster_givenMultipleThreadsAddingSameDataToSameHashConcurrently() {

    var key = "key";

    var testMap = makeHashMap(HASH_SIZE, "field-", "value-");

    var testMapFields = testMap.keySet().toArray(new String[] {});

    var hsetJedis1Consumer = makeHSetConsumer(testMap, testMapFields, key, jedis);
    var hsetJedis2Consumer = makeHSetConsumer(testMap, testMapFields, key, jedis);

    new ConcurrentLoopingThreads(HASH_SIZE, hsetJedis1Consumer, hsetJedis2Consumer).run();

    var results = jedis.hgetAll(key);

    assertThat(results.keySet().toArray()).containsExactlyInAnyOrder(testMap.keySet().toArray());
    assertThat(results.values().toArray()).containsExactlyInAnyOrder(testMap.values().toArray());
  }

  @Test
  public void shouldDistributeDataAmongCluster_givenMultipleThreadsAddingToDifferentHashesConcurrently() {

    var key1 = "key1";
    var key2 = "key2";

    var testMap1 = makeHashMap(HASH_SIZE, "field1-", "value1-");
    var testMap2 = makeHashMap(HASH_SIZE, "field2-", "value2-");

    var testMap1Fields = testMap1.keySet().toArray(new String[] {});
    var testMap2Fields = testMap2.keySet().toArray(new String[] {});

    var hsetJedis1Consumer = makeHSetConsumer(testMap1, testMap1Fields, key1, jedis);
    var hsetJedis2Consumer = makeHSetConsumer(testMap2, testMap2Fields, key2, jedis);

    new ConcurrentLoopingThreads(HASH_SIZE, hsetJedis1Consumer, hsetJedis2Consumer).run();

    var results1 = jedis.hgetAll(key1);
    var results2 = jedis.hgetAll(key2);

    assertThat(results1.keySet().toArray()).containsExactlyInAnyOrder(testMap1.keySet().toArray());
    assertThat(results1.values().toArray()).containsExactlyInAnyOrder(testMap1.values().toArray());
    assertThat(results2.values().toArray()).containsExactlyInAnyOrder(testMap2.values().toArray());
    assertThat(results2.values().toArray()).containsExactlyInAnyOrder(testMap2.values().toArray());
  }

  @Test
  public void shouldDistributeDataAmongCluster_givenMultipleThreadsAddingSameDataToSameSetConcurrently() {

    var key = "key";

    var testMap = makeHashMap(HASH_SIZE, "field1-", "value1-");

    var testMapFields = testMap.keySet().toArray(new String[] {});

    var hsetJedis1Consumer = makeHSetConsumer(testMap, testMapFields, key, jedis);
    var hsetJedis1BConsumer = makeHSetConsumer(testMap, testMapFields, key, jedis);
    var hsetJedis2Consumer = makeHSetConsumer(testMap, testMapFields, key, jedis);
    var hsetJedis2BConsumer = makeHSetConsumer(testMap, testMapFields, key, jedis);

    new ConcurrentLoopingThreads(HASH_SIZE, hsetJedis1Consumer, hsetJedis1BConsumer,
        hsetJedis2Consumer, hsetJedis2BConsumer).run();

    var results = jedis.hgetAll(key);

    assertThat(results.keySet().toArray()).containsExactlyInAnyOrder(testMap.keySet().toArray());
    assertThat(results.values().toArray()).containsExactlyInAnyOrder(testMap.values().toArray());
  }

  @Test
  public void shouldDistributeDataAmongCluster_givenMultipleThreadsAddingDifferentDataToSameSetConcurrently() {

    var key = "key1";

    var testMap1 = makeHashMap(HASH_SIZE, "field1-", "value1-");
    var testMap2 = makeHashMap(HASH_SIZE, "field2-", "value2-");

    Map<String, String> wholeMap = new HashMap<>();
    wholeMap.putAll(testMap1);
    wholeMap.putAll(testMap2);

    var testMap1Fields = testMap1.keySet().toArray(new String[] {});
    var testMap2Fields = testMap2.keySet().toArray(new String[] {});

    var consumer1 = makeHSetConsumer(testMap1, testMap1Fields, key, jedis);
    var consumer1B = makeHSetConsumer(testMap1, testMap1Fields, key, jedis);
    var consumer2 = makeHSetConsumer(testMap2, testMap2Fields, key, jedis);
    var consumer2B = makeHSetConsumer(testMap2, testMap2Fields, key, jedis);

    new ConcurrentLoopingThreads(HASH_SIZE, consumer1, consumer1B, consumer2, consumer2B).run();

    var results = jedis.hgetAll(key);

    assertThat(results.keySet().toArray()).containsExactlyInAnyOrder(wholeMap.keySet().toArray());
    assertThat(results.values().toArray()).containsExactlyInAnyOrder(wholeMap.values().toArray());
  }

  private Consumer<Integer> makeHSetConsumer(Map<String, String> testMap, String[] fields,
      String hashKey, JedisCluster jedis) {
    var consumer = (Consumer<Integer>) (i) -> {
      var field = fields[i];
      jedis.hset(hashKey, field, testMap.get(field));
    };

    return consumer;
  }

  private Map<String, String> makeHashMap(int hashSize, String baseFieldName,
      String baseValueName) {
    Map<String, String> map = new HashMap<>();
    for (var i = 0; i < hashSize; i++) {
      map.put(baseFieldName + i, baseValueName + i);
    }
    return map;
  }
}
