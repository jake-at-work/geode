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

package org.apache.geode.redis.internal.data;

import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.redis.internal.services.RegionProvider;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;

public class DeltaDUnitTest {

  @ClassRule
  public static RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule(4);

  private static final int ITERATION_COUNT = 1000;
  public static MemberVM server1;
  private static Jedis jedis1;
  private static JedisCluster jedisCluster;
  private static String KEY;

  @BeforeClass
  public static void classSetup() {
    var locator = clusterStartUp.startLocatorVM(0);
    server1 = clusterStartUp.startRedisVM(1, locator.getPort());
    clusterStartUp.startRedisVM(2, locator.getPort());

    var redisServerPort1 = clusterStartUp.getRedisPort(1);
    jedis1 = new Jedis(BIND_ADDRESS, redisServerPort1, REDIS_CLIENT_TIMEOUT);
    jedisCluster =
        new JedisCluster(new HostAndPort(BIND_ADDRESS, redisServerPort1), REDIS_CLIENT_TIMEOUT);

    KEY = findKeyHostedOnServer("key", "server-1");
  }

  @Before
  public void testSetup() {
    clusterStartUp.flushAll();
  }

  @AfterClass
  public static void tearDown() {
    jedis1.disconnect();
  }

  @Test
  public void shouldCorrectlyPropagateDeltaToSecondaryServer_whenAppending() {
    var baseValue = "value-";
    jedis1.set(KEY, baseValue);
    for (var i = 0; i < ITERATION_COUNT; i++) {
      jedis1.append(KEY, String.valueOf(i));
    }
    compareBuckets();
  }

  @Test
  public void shouldCorrectlyPropagateDeltaToSecondaryServer_whenAddingToSet() {
    var members = makeMemberList(ITERATION_COUNT, "member-");

    for (var member : members) {
      jedis1.sadd(KEY, member);
    }

    compareBuckets();
  }

  @Test
  public void shouldCorrectlyPropagateDeltaToSecondaryServer_whenRemovingFromSet() {
    var members = makeMemberList(ITERATION_COUNT, "member-");
    jedis1.sadd(KEY, members.toArray(new String[] {}));

    for (var member : members) {
      jedis1.srem(KEY, member);
    }
    compareBuckets();
  }

  @Test
  public void shouldCorrectlyPropagateDeltaToSecondaryServer_whenAddingToHash() {
    var testMap = makeHashMap(ITERATION_COUNT, "field-", "value-");

    for (var field : testMap.keySet()) {
      jedis1.hset(KEY, field, testMap.get(field));
    }
    compareBuckets();
  }

  @Test
  public void shouldCorrectlyPropagateDeltaToSecondaryServer_whenUpdatingHashValues() {
    var random = new Random();

    var testMap = makeHashMap(ITERATION_COUNT, "field-", "value-");
    jedis1.hset(KEY, testMap);

    for (var i = 0; i < 100; i++) {
      var retrievedMap = jedis1.hgetAll(KEY);
      var rand = random.nextInt(retrievedMap.size());
      var fieldToUpdate = "field-" + rand;
      var valueToUpdate = retrievedMap.get(fieldToUpdate);
      retrievedMap.put(fieldToUpdate, valueToUpdate + " updated");
      jedis1.hset(KEY, retrievedMap);
    }
    compareBuckets();
  }

  @Test
  public void shouldCorrectlyPropagateDeltaToSecondaryServer_whenRemovingFromHash() {
    var testMap = makeHashMap(ITERATION_COUNT, "field-", "value-");
    jedis1.hset(KEY, testMap);

    for (var field : testMap.keySet()) {
      jedis1.hdel(KEY, field, testMap.get(field));
    }
    compareBuckets();
  }

  @Test
  public void shouldCorrectlyPropagateDeltaToSecondaryServer_whenExpiring() {
    var baseKey = "key-";

    for (var i = 0; i < ITERATION_COUNT; i++) {
      var key = baseKey + i;
      jedisCluster.set(key, "value");
      jedisCluster.expire(key, 80L);
    }
    compareBuckets();
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

  private Map<String, String> makeHashMap(int hashSize, String baseFieldName,
      String baseValueName) {
    Map<String, String> map = new HashMap<>();
    for (var i = 0; i < hashSize; i++) {
      map.put(baseFieldName + i, baseValueName + i);
    }
    return map;
  }

  private List<String> makeMemberList(int setSize, String baseString) {
    List<String> members = new ArrayList<>();
    for (var i = 0; i < setSize; i++) {
      members.add(baseString + i);
    }
    return members;
  }

  private static String findKeyHostedOnServer(String prefix, String memberLike) {
    var x = 0;
    while (true) {
      var key = prefix + "-" + x++;
      var info = clusterStartUp.getMemberInfo(key);
      if (info.getMember().getUniqueId().contains(memberLike)) {
        return key;
      }
    }
  }
}
