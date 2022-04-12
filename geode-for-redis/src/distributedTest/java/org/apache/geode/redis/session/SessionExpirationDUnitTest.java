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

package org.apache.geode.redis.session;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.web.client.RestTemplate;

import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.redis.internal.data.RedisHash;
import org.apache.geode.redis.internal.services.RegionProvider;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;

public class SessionExpirationDUnitTest extends SessionDUnitTest {

  protected static final int SHORT_SESSION_TIMEOUT = 5;

  @BeforeClass
  public static void setup() {
    SessionDUnitTest.setup();
    setupSpringApps(SHORT_SESSION_TIMEOUT);
  }

  @Test
  public void sessionShouldTimeout_whenRequestedFromSameServer() throws Exception {
    var sessionCookie = createNewSessionWithNote(APP1, "note1");
    var sessionId = getSessionId(sessionCookie);

    waitForTheSessionToExpire(sessionId);

    assertThat(getSessionNotes(APP1, sessionCookie)).isNull();
  }

  @Test
  public void sessionShouldTimeout_OnSecondaryServer() throws Exception {
    var sessionCookie = createNewSessionWithNote(APP1, "note1");
    var sessionId = getSessionId(sessionCookie);

    waitForTheSessionToExpire(sessionId);

    assertThat(getSessionNotes(APP2, sessionCookie)).isNull();
  }

  @Test
  public void sessionShouldNotTimeoutOnFirstServer_whenAccessedOnSecondaryServer()
      throws Exception {
    var sessionCookie = createNewSessionWithNote(APP1, "note1");
    var sessionId = getSessionId(sessionCookie);

    refreshSession(sessionCookie, APP2);

    assertThat(commands.ttl("spring:session:sessions:expires:" + sessionId))
        .isGreaterThan(0);

    assertThat(getSessionNotes(APP1, sessionCookie)).isNotNull();
  }

  @Test
  public void sessionShouldTimeout_whenAppFailsOverToAnotherRedisServer() throws Exception {
    var sessionCookie = createNewSessionWithNote(APP2, "note1");
    var sessionId = getSessionId(sessionCookie);

    cluster.crashVM(SERVER2);

    try {
      waitForTheSessionToExpire(sessionId);

      assertThat(getSessionNotes(APP1, sessionCookie)).isNull();
      assertThat(getSessionNotes(APP2, sessionCookie)).isNull();

    } finally {
      startRedisServer(SERVER2);
    }
  }

  @Test
  public void sessionShouldNotTimeout_whenPersisted() throws Exception {
    var sessionCookie = createNewSessionWithNote(APP2, "note1");
    setMaxInactiveInterval(APP2, sessionCookie, -1);

    compareMaxInactiveIntervals();
  }

  private void waitForTheSessionToExpire(String sessionId) {
    GeodeAwaitility.await().ignoreExceptions().atMost((SHORT_SESSION_TIMEOUT + 5), TimeUnit.SECONDS)
        .until(() -> commands.ttl("spring:session:sessions:expires:" + sessionId) == -2);
  }

  private void refreshSession(String sessionCookie, int sessionApp) {
    GeodeAwaitility.await()
        .during(SHORT_SESSION_TIMEOUT + 2, TimeUnit.SECONDS)
        .until(() -> getSessionNotes(sessionApp, sessionCookie) != null);
  }

  void setMaxInactiveInterval(int sessionApp, String sessionCookie, int maxInactiveInterval) {
    var requestHeaders = new HttpHeaders();
    requestHeaders.add("Cookie", sessionCookie);
    var request = new HttpEntity<Integer>(maxInactiveInterval, requestHeaders);
    new RestTemplate()
        .postForEntity(
            "http://localhost:" + ports.get(sessionApp) + "/setMaxInactiveInterval",
            request,
            Integer.class)
        .getHeaders();
  }

  private void compareMaxInactiveIntervals() {
    cluster.getVM(1).invoke(() -> {
      var cache = ClusterStartupRule.getCache();
      var region =
          (PartitionedRegion) cache.getRegion(RegionProvider.DEFAULT_REDIS_REGION_NAME);
      for (var j = 0; j < region.getTotalNumberOfBuckets(); j++) {
        var buckets = region.getAllBucketEntries(j);
        if (buckets.isEmpty()) {
          continue;
        }
        assertThat(buckets.size()).isEqualTo(2);
        var bucket1 = buckets.get(0).getValues();
        var bucket2 = buckets.get(1).getValues();

        bucket1.keySet().forEach(key -> {
          if (bucket1.get(key) instanceof RedisHash) {

            var value1 = (RedisHash) bucket1.get(key);
            var value2 = (RedisHash) bucket2.get(key);

            assertThat(getIntFromBytes(value1)).isEqualTo(getIntFromBytes(value2));
          }
        });
      }
    });
  }

  private static int getIntFromBytes(RedisHash redisHash) {
    if (redisHash == null) {
      return 0;
    }
    ObjectInputStream inputStream;
    var bytes = redisHash.hget("maxInactiveInterval".getBytes());
    var byteStream = new ByteArrayInputStream(bytes);
    try {
      inputStream = new ObjectInputStream(byteStream);
      return (int) inputStream.readObject();
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}
