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

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class RedisSessionDUnitTest extends SessionDUnitTest {

  @Rule
  public ExecutorServiceRule executor = new ExecutorServiceRule();

  @BeforeClass
  public static void setup() {
    SessionDUnitTest.setup();
    setupSpringApps(DEFAULT_SESSION_TIMEOUT);
  }

  @Test
  public void should_beAbleToCreateASession_storedInRedis() throws Exception {
    var sessionCookie = createNewSessionWithNote(APP1, "note1");
    var sessionId = getSessionId(sessionCookie);

    var sessionInfo =
        commands.hgetall("spring:session:sessions:" + sessionId);

    assertThat(sessionInfo.get("sessionAttr:NOTES")).contains("note1");
  }

  @Test
  public void createSessionsConcurrently() throws Exception {
    var running = new AtomicBoolean(true);
    var barrier = new CyclicBarrier(3);

    var future1 = executor.submit(() -> sessionCreator(1, 100, running, barrier));
    var future2 = executor.submit(() -> sessionCreator(2, 100, running, barrier));
    var future3 = executor.submit(() -> sessionCreator(3, 100, running, barrier));

    future1.get();
    future2.get();
    future3.get();
  }

  private void sessionCreator(int index, int iterations, AtomicBoolean running,
      CyclicBarrier barrier) throws BrokenBarrierException, InterruptedException {
    var iterationCount = 0;
    barrier.await();
    while (iterationCount < iterations && running.get()) {
      var noteName = String.format("note-%d-%d", index, iterationCount);
      try {
        var sessionCookie = createNewSessionWithNote(APP1, noteName);
        var sessionNotes = getSessionNotes(APP2, sessionCookie);
        assertThat(sessionNotes).containsExactly(noteName);
      } catch (Exception e) {
        running.set(false);
        throw new RuntimeException("BANG " + noteName, e);
      }

      iterationCount++;
    }
  }

  @Test
  public void should_storeSession() throws Exception {
    var sessionCookie = createNewSessionWithNote(APP1, "note1");

    var sessionNotes = getSessionNotes(APP2, sessionCookie);

    assertThat(sessionNotes).containsExactly("note1");
  }

  @Test
  public void should_propagateSession_toOtherServers() throws Exception {
    var sessionCookie = createNewSessionWithNote(APP1, "noteFromClient1");

    var sessionNotes = getSessionNotes(APP2, sessionCookie);

    assertThat(sessionNotes).containsExactly("noteFromClient1");
  }

  @Test
  public void should_getSessionFromServer1_whenServer2GoesDown() throws Exception {
    var sessionCookie = createNewSessionWithNote(APP2, "noteFromClient2");
    cluster.crashVM(SERVER2);
    try {
      var sessionNotes = getSessionNotes(APP1, sessionCookie);

      assertThat(sessionNotes).containsExactly("noteFromClient2");
    } finally {
      startRedisServer(SERVER2);
    }
  }

  @Test
  public void should_getSessionFromServer_whenServerGoesDownAndIsRestarted() throws Exception {
    var sessionCookie = createNewSessionWithNote(APP2, "noteFromClient2");
    cluster.crashVM(SERVER2);
    addNoteToSession(APP1, sessionCookie, "noteFromClient1");
    startRedisServer(SERVER2);

    var sessionNotes = getSessionNotes(APP2, sessionCookie);

    assertThat(sessionNotes).containsExactlyInAnyOrder("noteFromClient2", "noteFromClient1");
  }

  @Test
  public void should_getSession_whenServer2GoesDown_andAppFailsOverToServer1() throws Exception {
    var sessionCookie = createNewSessionWithNote(APP2, "noteFromClient2");
    cluster.crashVM(SERVER2);

    try {
      var sessionNotes = getSessionNotes(APP2, sessionCookie);

      assertThat(sessionNotes).containsExactly("noteFromClient2");
    } finally {
      startRedisServer(SERVER2);
    }
  }

  @Test
  public void should_getSessionCreatedByApp2_whenApp2GoesDown_andClientConnectsToApp1()
      throws Exception {
    var sessionCookie = createNewSessionWithNote(APP2, "noteFromClient2");
    stopSpringApp(APP2);

    try {
      var sessionNotes = getSessionNotes(APP1, sessionCookie);

      assertThat(sessionNotes).containsExactly("noteFromClient2");
    } finally {
      startSpringApp(APP2, DEFAULT_SESSION_TIMEOUT, ports.get(SERVER2));
    }
  }
}
