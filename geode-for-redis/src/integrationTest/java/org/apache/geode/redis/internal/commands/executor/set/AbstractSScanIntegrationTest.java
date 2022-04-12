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
package org.apache.geode.redis.internal.commands.executor.set;

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertAtLeastNArgs;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_CURSOR;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_WRONG_TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.redis.internal.data.KeyHashUtil;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;

public abstract class AbstractSScanIntegrationTest implements RedisIntegrationTest {
  protected JedisCluster jedis;
  public static final String KEY = "key";
  public static final byte[] KEY_BYTES = KEY.getBytes();
  public static final int SLOT_FOR_KEY = KeyHashUtil.slotForKey(KEY_BYTES);
  public static final String ZERO_CURSOR = "0";
  public static final byte[] ZERO_CURSOR_BYTES = ZERO_CURSOR.getBytes();
  public static final BigInteger SIGNED_LONG_MAX = new BigInteger(Long.toString(Long.MAX_VALUE));
  public static final BigInteger SIGNED_LONG_MIN = new BigInteger(Long.toString(Long.MIN_VALUE));
  public static final String MEMBER_ONE = "1";
  public static final String MEMBER_TWELVE = "12";
  public static final String MEMBER_THREE = "3";
  public static final String BASE_MEMBER_NAME = "baseMember_";

  @Before
  public void setUp() {
    jedis = new JedisCluster(new HostAndPort(RedisClusterStartupRule.BIND_ADDRESS, getPort()),
        RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    flushAll();
    jedis.close();
  }

  @Test
  public void givenLessThanTwoArguments_returnsWrongNumberOfArgumentsError() {
    assertAtLeastNArgs(jedis, Protocol.Command.SSCAN, 2);
  }

  @Test
  public void givenNonexistentKey_returnsEmptyArray() {
    var result = jedis.sscan("nonexistentKey", ZERO_CURSOR);
    assertThat(result.isCompleteIteration()).isTrue();
    assertThat(result.getResult()).isEmpty();
  }

  @Test
  public void givenNonexistentKeyAndIncorrectOptionalArguments_returnsEmptyArray() {
    var result =
        sendCustomSscanCommand("nonexistentKey", "nonexistentKey", ZERO_CURSOR, "ANY");
    assertThat(result.getResult()).isEmpty();
  }

  @Test
  public void givenIncorrectOptionalArgumentsAndKeyExists_returnsSyntaxError() {
    jedis.sadd(KEY, MEMBER_ONE);
    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.SSCAN, KEY, ZERO_CURSOR, "a*"))
            .hasMessage(ERROR_SYNTAX);
  }

  @Test
  public void givenMatchArgumentWithoutPatternOnExistingKey_returnsSyntaxError() {
    jedis.sadd(KEY, MEMBER_ONE);
    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.SSCAN, KEY, ZERO_CURSOR, "MATCH"))
            .hasMessage(ERROR_SYNTAX);
  }

  @Test
  public void givenCountArgumentWithoutNumberOnExistingKey_returnsSyntaxError() {
    jedis.sadd(KEY, MEMBER_ONE);
    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.SSCAN, KEY, ZERO_CURSOR, "COUNT"))
            .hasMessage(ERROR_SYNTAX);
  }

  @Test
  public void givenAdditionalArgumentNotEqualToMatchOrCount_returnsSyntaxError() {
    jedis.sadd(KEY, MEMBER_ONE);
    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.SSCAN, KEY, ZERO_CURSOR, "a*", "1"))
            .hasMessage(ERROR_SYNTAX);
  }

  @Test
  public void givenCount_whenCountParameterIsNotAnInteger_returnsNotIntegerError() {
    jedis.sadd(KEY, MEMBER_ONE);
    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.SSCAN, KEY, ZERO_CURSOR, "COUNT",
            "notAnInteger")).hasMessage(ERROR_NOT_INTEGER);
  }

  @Test
  public void givenMultipleCounts_whenAnyCountParameterIsNotAnInteger_returnsNotIntegerError() {
    jedis.sadd(KEY, MEMBER_ONE);
    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.SSCAN, KEY, ZERO_CURSOR, "COUNT", "12",
            "COUNT", "notAnInteger", "COUNT", "1")).hasMessage(ERROR_NOT_INTEGER);
  }

  @Test
  public void givenMultipleCounts_whenAnyCountParameterIsLessThanOne_returnsSyntaxError() {
    jedis.sadd(KEY, MEMBER_ONE);
    assertThatThrownBy(
        () -> jedis.sendCommand(KEY, Protocol.Command.SSCAN, KEY, ZERO_CURSOR, "COUNT", "12",
            "COUNT", "0", "COUNT", "1")).hasMessage(ERROR_SYNTAX);
  }

  @Test
  public void givenCount_whenCountParameterIsZero_returnsSyntaxError() {
    jedis.sadd(KEY, MEMBER_ONE);
    assertThatThrownBy(
        () -> jedis.sscan(KEY_BYTES, ZERO_CURSOR_BYTES, new ScanParams().count(0)))
            .hasMessage(ERROR_SYNTAX);
  }

  @Test
  public void givenCount_whenCountParameterIsNegative_returnsSyntaxError() {
    jedis.sadd(KEY, MEMBER_ONE);
    assertThatThrownBy(
        () -> jedis.sscan(KEY_BYTES, ZERO_CURSOR_BYTES, new ScanParams().count(-37)))
            .hasMessage(ERROR_SYNTAX);
  }

  @Test
  public void givenKeyIsNotASet_returnsWrongTypeError() {
    jedis.hset(KEY, "b", MEMBER_ONE);
    assertThatThrownBy(
        () -> jedis.sscan(KEY, ZERO_CURSOR)).hasMessage(ERROR_WRONG_TYPE);
  }

  @Test
  public void givenKeyIsNotASetAndCountIsNegative_returnsWrongTypeError() {
    jedis.hset(KEY, "b", MEMBER_ONE);
    assertThatThrownBy(
        () -> jedis.sscan(KEY_BYTES, ZERO_CURSOR_BYTES, new ScanParams().count(-37)))
            .hasMessage(ERROR_WRONG_TYPE);
  }

  @Test
  public void givenKeyIsNotASet_andCursorIsNotAnInteger_returnsInvalidCursorError() {
    jedis.hset(KEY, "b", MEMBER_ONE);
    assertThatThrownBy(
        () -> jedis.sscan(KEY, "notAnInteger")).hasMessage(ERROR_CURSOR);
  }

  @Test
  public void givenNonexistentKey_andCursorIsNotInteger_returnsInvalidCursorError() {
    assertThatThrownBy(
        () -> jedis.sscan("nonexistentKey", "notAnInteger")).hasMessage(ERROR_CURSOR);
  }

  @Test
  public void givenExistentSetKey_andCursorIsNotAnInteger_returnsInvalidCursorError() {
    jedis.set(KEY, "b");
    assertThatThrownBy(
        () -> jedis.sscan(KEY, "notAnInteger")).hasMessage(ERROR_CURSOR);
  }

  @Test
  public void givenNegativeCursor_doesNotError() {
    initializeThousandMemberSet();
    assertThatNoException().isThrownBy(() -> jedis.sscan(KEY, "-1"));
  }

  @Test
  public void givenSetWithOneMember_returnsMember() {
    jedis.sadd(KEY, MEMBER_ONE);

    var result = jedis.sscan(KEY, ZERO_CURSOR);

    assertThat(result.isCompleteIteration()).isTrue();
    assertThat(result.getResult()).containsOnly(MEMBER_ONE);
  }

  @Test
  public void givenSetWithMultipleMembers_returnsSubsetOfMembers() {
    var initialMemberData = initializeThousandMemberSet();

    var result = jedis.sscan(KEY, ZERO_CURSOR);

    assertThat(result.getResult()).isSubsetOf(initialMemberData);
  }

  @Test
  public void givenCount_returnsAllMembersWithoutDuplicates() {
    var initialTotalSet = initializeThousandMemberByteSet();
    var count = 99;

    var result =
        jedis.sscan(KEY_BYTES, ZERO_CURSOR_BYTES, new ScanParams().count(count));

    assertThat(result.getResult().size()).isGreaterThanOrEqualTo(count);
    assertThat(result.getResult()).isSubsetOf(initialTotalSet);
  }

  @Test
  public void givenMultipleCounts_usesLastCountSpecified() {
    var initialMemberData = initializeThousandMemberByteSet();
    // Choose two COUNT arguments with a large difference, so that it's extremely unlikely that if
    // the first COUNT is used, a number of members greater than or equal to the second COUNT will
    // be returned.
    var firstCount = 1;
    var secondCount = 500;
    var result = sendCustomSscanCommand(KEY, KEY, ZERO_CURSOR,
        "COUNT", String.valueOf(firstCount),
        "COUNT", String.valueOf(secondCount));

    var returnedMembers = result.getResult();
    assertThat(returnedMembers.size()).isGreaterThanOrEqualTo(secondCount);
    assertThat(returnedMembers).isSubsetOf(initialMemberData);
  }

  @Test
  public void givenSetWithThreeEntriesAndMatch_returnsOnlyMatchingElements() {
    jedis.sadd(KEY, MEMBER_ONE, MEMBER_TWELVE, MEMBER_THREE);
    var scanParams = new ScanParams().match("1*");

    var result = jedis.sscan(KEY_BYTES, ZERO_CURSOR_BYTES, scanParams);

    assertThat(result.isCompleteIteration()).isTrue();
    assertThat(result.getResult()).containsOnly(MEMBER_ONE.getBytes(),
        MEMBER_TWELVE.getBytes());
  }

  @Test
  public void givenSetWithThreeEntriesAndMultipleMatchArguments_returnsOnlyElementsMatchingLastMatchArgument() {
    jedis.sadd(KEY, MEMBER_ONE, MEMBER_TWELVE, MEMBER_THREE);

    var result =
        sendCustomSscanCommand(KEY, KEY, ZERO_CURSOR, "MATCH", "3*", "MATCH", "1*");

    assertThat(result.getCursor()).isEqualTo(ZERO_CURSOR);
    assertThat(result.getResult()).containsOnly(MEMBER_ONE.getBytes(),
        MEMBER_TWELVE.getBytes());
  }

  @Test
  public void givenLargeCountAndMatch_returnsOnlyMatchingMembers() {
    var initialMemberData = initializeThousandMemberByteSet();
    var scanParams = new ScanParams();
    // There are 111 matching members in the set 0..999
    scanParams.match("9*");
    // Choose a large COUNT to ensure that some matching members are returned
    scanParams.count(950);

    var result = jedis.sscan(KEY_BYTES, ZERO_CURSOR_BYTES, scanParams);

    var returnedMembers = result.getResult();
    // We know that we must have found at least 61 matching members, given the size of COUNT and the
    // number of matching members in the set
    assertThat(returnedMembers.size()).isGreaterThanOrEqualTo(61);
    assertThat(returnedMembers).isSubsetOf(initialMemberData);
    assertThat(returnedMembers).allSatisfy(bytes -> assertThat(new String(bytes)).startsWith("9"));
  }

  @Test
  public void givenMultipleCountAndMatch_usesLastSpecified() {
    var initialMemberData = initializeThousandMemberByteSet();
    // Choose a large COUNT to ensure that some matching members are returned
    // There are 111 matching members in the set 0..999

    var result = sendCustomSscanCommand(KEY, KEY, ZERO_CURSOR,
        "COUNT", "20",
        "MATCH", "1*",
        "COUNT", "950",
        "MATCH", "9*");

    var returnedMembers = result.getResult();
    // We know that we must have found at least 61 matching members, given the size of COUNT and the
    // number of matching members in the set
    assertThat(returnedMembers.size()).isGreaterThanOrEqualTo(61);
    assertThat(returnedMembers).isSubsetOf(initialMemberData);
    assertThat(returnedMembers).allSatisfy(bytes -> assertThat(new String(bytes)).startsWith("9"));
  }

  @Test
  public void givenNonMatchingPattern_returnsEmptyResult() {
    jedis.sadd(KEY, "cat dog elk");
    var scanParams = new ScanParams().match("*fish*");

    var result = jedis.sscan(KEY_BYTES, ZERO_CURSOR_BYTES, scanParams);

    assertThat(result.getResult()).isEmpty();
  }

  @Test
  public void should_notReturnValue_givenValueWasRemovedBeforeSscanIsCalled() {
    initializeThreeMemberSet();
    jedis.srem(KEY, MEMBER_THREE);
    GeodeAwaitility.await().untilAsserted(
        () -> assertThat(jedis.sismember(KEY, MEMBER_THREE)).isFalse());

    var result = jedis.sscan(KEY, ZERO_CURSOR);

    assertThat(result.getResult()).doesNotContain(MEMBER_THREE);
  }

  @Test
  public void should_notErrorGivenNonzeroCursorOnFirstCall() {
    initializeThreeMemberSet();
    assertThatNoException().isThrownBy(() -> jedis.sscan(KEY, "5"));
  }

  @Test
  public void should_notErrorGivenCountEqualToIntegerMaxValue() {
    var set = initializeThreeMemberByteSet();
    var scanParams = new ScanParams().count(Integer.MAX_VALUE);

    var result = jedis.sscan(KEY_BYTES, ZERO_CURSOR_BYTES, scanParams);

    assertThat(result.getResult())
        .containsExactlyInAnyOrderElementsOf(set);
  }

  @Test
  public void should_notErrorGivenCountGreaterThanIntegerMaxValue() {
    initializeThreeMemberByteSet();
    var greaterThanInt = String.valueOf(2L * Integer.MAX_VALUE);

    var result =
        sendCustomSscanCommand(KEY, KEY, ZERO_CURSOR, "COUNT", greaterThanInt);

    assertThat(result.getCursor()).isEqualTo(ZERO_CURSOR);
    assertThat(result.getResult()).containsExactlyInAnyOrder(
        MEMBER_ONE.getBytes(),
        MEMBER_TWELVE.getBytes(),
        MEMBER_THREE.getBytes());
  }

  /**** Concurrency ***/

  @Test
  public void should_returnAllConsistentlyPresentMembers_givenConcurrentThreadsAddingAndRemovingMembers() {
    final var initialMemberData = initializeThousandMemberSet();
    final var iterationCount = 500;
    final var jedis1 = new Jedis(jedis.getConnectionFromSlot(SLOT_FOR_KEY));
    final var jedis2 = new Jedis(jedis.getConnectionFromSlot(SLOT_FOR_KEY));

    new ConcurrentLoopingThreads(iterationCount,
        (i) -> multipleSScanAndAssertOnContentOfResultSet(i, jedis1, initialMemberData),
        (i) -> multipleSScanAndAssertOnContentOfResultSet(i, jedis2, initialMemberData),
        (i) -> {
          var member = "new_" + BASE_MEMBER_NAME + i;
          jedis.sadd(KEY, member);
          jedis.srem(KEY, member);
        }).run();

    jedis1.close();
    jedis2.close();
  }

  @Test
  public void should_notAlterUnderlyingData_givenMultipleConcurrentSscans() {
    final var initialMemberData = initializeThousandMemberSet();
    jedis.sadd(KEY, initialMemberData.toArray(new String[0]));
    final var iterationCount = 500;
    var jedis1 = new Jedis(jedis.getConnectionFromSlot(SLOT_FOR_KEY));
    var jedis2 = new Jedis(jedis.getConnectionFromSlot(SLOT_FOR_KEY));

    new ConcurrentLoopingThreads(iterationCount,
        (i) -> multipleSScanAndAssertOnContentOfResultSet(i, jedis1, initialMemberData),
        (i) -> multipleSScanAndAssertOnContentOfResultSet(i, jedis2, initialMemberData))
            .run();
    assertThat(jedis.smembers(KEY)).containsExactlyInAnyOrderElementsOf(initialMemberData);

    jedis1.close();
    jedis2.close();
  }

  private void multipleSScanAndAssertOnContentOfResultSet(int iteration, Jedis jedis,
      final Set<String> initialMemberData) {
    List<String> allEntries = new ArrayList<>();
    var cursor = ZERO_CURSOR;
    ScanResult<String> result;

    do {
      result = jedis.sscan(KEY, cursor);
      cursor = result.getCursor();
      var resultEntries = result.getResult();
      allEntries.addAll(resultEntries);
    } while (!result.isCompleteIteration());

    assertThat(allEntries).as("failed on iteration " + iteration)
        .containsAll(initialMemberData);
  }

  @SuppressWarnings("unchecked")
  private ScanResult<byte[]> sendCustomSscanCommand(String key, String... args) {
    var result = (List<Object>) (jedis.sendCommand(key, Protocol.Command.SSCAN, args));
    return new ScanResult<>((byte[]) result.get(0), (List<byte[]>) result.get(1));
  }

  private void initializeThreeMemberSet() {
    jedis.sadd(KEY, MEMBER_ONE, MEMBER_TWELVE, MEMBER_THREE);
  }

  private Set<byte[]> initializeThreeMemberByteSet() {
    Set<byte[]> set = new HashSet<>();
    set.add(MEMBER_ONE.getBytes());
    set.add(MEMBER_TWELVE.getBytes());
    set.add(MEMBER_THREE.getBytes());
    jedis.sadd(KEY_BYTES, MEMBER_ONE.getBytes(), MEMBER_TWELVE.getBytes(),
        MEMBER_THREE.getBytes());
    return set;
  }

  private Set<String> initializeThousandMemberSet() {
    Set<String> set = new HashSet<>();
    var sizeOfSet = 1000;
    for (var i = 0; i < sizeOfSet; i++) {
      set.add((BASE_MEMBER_NAME + i));
    }
    jedis.sadd(KEY, set.toArray(new String[0]));
    return set;
  }

  private Set<byte[]> initializeThousandMemberByteSet() {
    Set<byte[]> set = new HashSet<>();
    var sizeOfSet = 1000;
    for (var i = 0; i < sizeOfSet; i++) {
      var memberToAdd = Integer.toString(i).getBytes();
      set.add(memberToAdd);
      jedis.sadd(KEY_BYTES, memberToAdd);
    }
    return set;
  }

}
