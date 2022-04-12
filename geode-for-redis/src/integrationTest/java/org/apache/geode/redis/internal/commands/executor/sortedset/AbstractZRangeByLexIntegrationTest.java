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
package org.apache.geode.redis.internal.commands.executor.sortedset;

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertAtLeastNArgs;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_MIN_MAX_NOT_A_VALID_STRING;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import junitparams.Parameters;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
public abstract class AbstractZRangeByLexIntegrationTest implements RedisIntegrationTest {
  public static final String KEY = "key";
  public static final int SCORE = 1;
  public static final String BASE_MEMBER_NAME = "v";

  JedisCluster jedis;

  @Before
  public void setUp() {
    jedis = new JedisCluster(new HostAndPort(BIND_ADDRESS, getPort()), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    flushAll();
    jedis.close();
  }

  @Test
  public void shouldError_givenWrongNumberOfArguments() {
    assertAtLeastNArgs(jedis, Protocol.Command.ZRANGEBYLEX, 3);
  }

  @Test
  @Parameters({"a", "--", "++"})
  public void shouldError_givenInvalidMinOrMax(String invalidArgument) {
    assertThatThrownBy(() -> jedis.zrangeByLex("fakeKey", invalidArgument, "+"))
        .hasMessage(ERROR_MIN_MAX_NOT_A_VALID_STRING);
    assertThatThrownBy(() -> jedis.zrangeByLex("fakeKey", "-", invalidArgument))
        .hasMessage(ERROR_MIN_MAX_NOT_A_VALID_STRING);
    assertThatThrownBy(() -> jedis.zrangeByLex("fakeKey", invalidArgument, invalidArgument))
        .hasMessage(ERROR_MIN_MAX_NOT_A_VALID_STRING);
  }

  @Test
  public void shouldError_givenInvalidLimitFormat() {
    assertThatThrownBy(() -> jedis.sendCommand(KEY, Protocol.Command.ZRANGEBYLEX, KEY, "-", "+",
        "LIMIT")).hasMessage(ERROR_SYNTAX);

    assertThatThrownBy(() -> jedis.sendCommand(KEY, Protocol.Command.ZRANGEBYLEX, KEY, "-", "+",
        "LIMIT", "0")).hasMessage(ERROR_SYNTAX);

    assertThatThrownBy(() -> jedis.sendCommand(KEY, Protocol.Command.ZRANGEBYLEX, KEY, "-", "+",
        "LAMAT", "0", "10")).hasMessage(ERROR_SYNTAX);
  }

  @Test
  public void shouldError_givenNonIntegerLimitArguments() {
    assertThatThrownBy(() -> jedis.sendCommand(KEY, Protocol.Command.ZRANGEBYLEX, KEY, "-", "+",
        "LIMIT", "0", "invalid")).hasMessage(ERROR_NOT_INTEGER);

    assertThatThrownBy(() -> jedis.sendCommand(KEY, Protocol.Command.ZRANGEBYLEX, KEY, "-", "+",
        "LIMIT", "invalid", "10")).hasMessage(ERROR_NOT_INTEGER);
  }

  @Test
  public void shouldError_givenNegativeZeroLimitOffset() {
    assertThatThrownBy(() -> jedis.sendCommand(KEY, Protocol.Command.ZRANGEBYLEX, KEY, "-", "+",
        "LIMIT", "-0", "10")).hasMessage(ERROR_NOT_INTEGER);
  }

  @Test
  public void shouldError_givenMultipleLimits_withFirstLimitIncorrectlySpecified() {
    assertThatThrownBy(() -> jedis.sendCommand(KEY, Protocol.Command.ZRANGEBYLEX, KEY, "-", "+",
        "LIMIT", "0", "invalid",
        "LIMIT", "0", "10")).hasMessage(ERROR_NOT_INTEGER);

    assertThatThrownBy(() -> jedis.sendCommand(KEY, Protocol.Command.ZRANGEBYLEX, KEY, "-", "+",
        "LIMIT", "0",
        "LIMIT", "0", "10")).hasMessage(ERROR_NOT_INTEGER);
  }

  @Test
  public void shouldError_givenMultipleLimits_withLastLimitIncorrectlySpecified() {
    assertThatThrownBy(() -> jedis.sendCommand(KEY, Protocol.Command.ZRANGEBYLEX, KEY, "-", "+",
        "LIMIT", "0", "10",
        "LIMIT", "0", "invalid")).hasMessage(ERROR_NOT_INTEGER);

    assertThatThrownBy(() -> jedis.sendCommand(KEY, Protocol.Command.ZRANGEBYLEX, KEY, "-", "+",
        "LIMIT", "0", "10",
        "LIMIT", "0")).hasMessage(ERROR_SYNTAX);
  }

  @Test
  public void shouldReturnEmptyCollection_givenNonExistentKey() {
    assertThat(jedis.zrangeByLex("fakeKey", "-", "+")).isEmpty();
  }

  @Test
  public void shouldReturnEmptyCollection_givenMinGreaterThanMax() {
    jedis.zadd(KEY, SCORE, "member");

    // Range + <= member name <= -
    assertThat(jedis.zrangeByLex(KEY, "+", "-")).isEmpty();
    // Range z <= member name <= a
    assertThat(jedis.zrangeByLex(KEY, "[z", "[a")).isEmpty();
  }

  @Test
  public void shouldReturnMember_givenMemberNameInRange() {
    var memberName = "member";
    jedis.zadd(KEY, SCORE, memberName);

    // Range m <= member name <= n
    assertThat(jedis.zrangeByLex(KEY, "[m", "[n")).containsExactly(memberName);
    // Range -infinity <= member name <= n
    assertThat(jedis.zrangeByLex(KEY, "-", "[n")).containsExactly(memberName);
    // Range m <= member name <= +infinity
    assertThat(jedis.zrangeByLex(KEY, "[m", "+")).containsExactly(memberName);
  }

  @Test
  public void shouldReturnMember_givenMinEqualToMemberNameAndMinInclusive() {
    var memberName = "member";
    jedis.zadd(KEY, SCORE, memberName);

    // Range member <= member name <= n
    assertThat(jedis.zrangeByLex(KEY, "[" + memberName, "[n")).containsExactly(memberName);
  }

  @Test
  public void shouldReturnMember_givenMaxEqualToMemberNameAndMaxInclusive() {
    var memberName = "member";
    jedis.zadd(KEY, SCORE, memberName);

    // Range a <= member name <= member
    assertThat(jedis.zrangeByLex(KEY, "[a", "[" + memberName)).containsExactly(memberName);
  }

  @Test
  public void shouldReturnMember_givenMinAndMaxEqualToMemberNameAndMinAndMaxInclusive() {
    var memberName = "member";
    jedis.zadd(KEY, SCORE, memberName);

    assertThat(jedis.zrangeByLex(KEY, "[" + memberName, "[" + memberName))
        .containsExactly(memberName);
  }

  @Test
  @Parameters({"[", "(", "", "-", "+"})
  public void shouldReturnMember_givenMemberNameIsSpecialCharacter(String memberName) {
    jedis.zadd(KEY, SCORE, memberName);

    assertThat(jedis.zrangeByLex(KEY, "[" + memberName, "[" + memberName))
        .containsExactly(memberName);
  }

  @Test
  public void shouldReturnEmptyCollection_givenMinEqualToMemberNameAndMinExclusive() {
    var memberName = "member";
    jedis.zadd(KEY, SCORE, memberName);

    // Range member < member name <= n
    assertThat(jedis.zrangeByLex(KEY, "(" + memberName, "[n")).isEmpty();
  }

  @Test
  public void shouldReturnEmptyCollection_givenMaxEqualToMemberNameAndMaxExclusive() {
    var memberName = "member";
    jedis.zadd(KEY, SCORE, memberName);

    // Range a <= member name < member
    assertThat(jedis.zrangeByLex(KEY, "[a", "(" + memberName)).isEmpty();
  }

  @Test
  public void shouldReturnEmptyCollection_givenRangeExcludingMember() {
    var memberName = "member";
    jedis.zadd(KEY, SCORE, memberName);

    // Range n <= member name <= o
    assertThat(jedis.zrangeByLex(KEY, "[n", "[o")).isEmpty();
  }

  @Test
  public void shouldReturnRange_givenMultipleMembersInRange_withInclusiveMinAndMax() {
    var members = populateSortedSet();

    var minLength = 3;
    var maxLength = 6;
    var min = StringUtils.repeat(BASE_MEMBER_NAME, minLength);
    var max = StringUtils.repeat(BASE_MEMBER_NAME, maxLength);

    var expected = members.subList(minLength - 1, maxLength);

    // Range (v * 3) <= member name <= (v * 6)
    assertThat(jedis.zrangeByLex(KEY, "[" + min, "[" + max))
        .containsExactlyElementsOf(expected);
  }

  @Test
  public void shouldReturnRange_givenMultipleMembersInRange_withExclusiveMinAndMax() {
    var members = populateSortedSet();

    var minLength = 1;
    var maxLength = 7;
    var min = StringUtils.repeat(BASE_MEMBER_NAME, minLength);
    var max = StringUtils.repeat(BASE_MEMBER_NAME, maxLength);

    var expected = members.subList(minLength, maxLength - 1);

    // Range (v * 1) < member name < (v * 7)
    assertThat(jedis.zrangeByLex(KEY, "(" + min, "(" + max))
        .containsExactlyElementsOf(expected);
  }

  @Test
  public void shouldReturnRange_givenMultipleMembersInRange_withInclusiveMinAndExclusiveMax() {
    var members = populateSortedSet();

    var minLength = 5;
    var maxLength = 8;
    var min = StringUtils.repeat(BASE_MEMBER_NAME, minLength);
    var max = StringUtils.repeat(BASE_MEMBER_NAME, maxLength);

    var expected = members.subList(minLength - 1, maxLength - 1);

    // Range (v * 5) <= member name < (v * 8)
    assertThat(jedis.zrangeByLex(KEY, "[" + min, "(" + max))
        .containsExactlyElementsOf(expected);
  }

  @Test
  public void shouldReturnRange_givenMultipleMembersInRange_withExclusiveMinAndInclusiveMax() {
    var members = populateSortedSet();

    var minLength = 2;
    var maxLength = 5;
    var min = StringUtils.repeat(BASE_MEMBER_NAME, minLength);
    var max = StringUtils.repeat(BASE_MEMBER_NAME, maxLength);

    var expected = members.subList(minLength, maxLength);

    // Range (v * 2) < member name <= (v * 5)
    assertThat(jedis.zrangeByLex(KEY, "(" + min, "[" + max))
        .containsExactlyElementsOf(expected);
  }

  @Test
  public void shouldReturnRange_givenMultipleMembersInRangeUsingMinusAndPlusArguments() {
    var members = populateSortedSet();

    var minLength = 4;
    var maxLength = 8;
    var min = StringUtils.repeat(BASE_MEMBER_NAME, minLength);
    var max = StringUtils.repeat(BASE_MEMBER_NAME, maxLength);

    var expected = members.subList(0, maxLength);

    // Range -infinity <= member name <= (v * 8)
    assertThat(jedis.zrangeByLex(KEY, "-", "[" + max))
        .containsExactlyElementsOf(expected);

    expected = members.subList(minLength - 1, members.size());

    // Range (v * 4) <= member name < +infinity
    assertThat(jedis.zrangeByLex(KEY, "[" + min, "+"))
        .containsExactlyElementsOf(expected);

    // Range -infinity <= member name < +infinity
    assertThat(jedis.zrangeByLex(KEY, "-", "+"))
        .containsExactlyElementsOf(members);
  }

  @Test
  public void shouldReturnRange_givenValidLimit() {
    var members = populateSortedSet();

    var minLength = 1;
    var maxLength = 7;

    var min = StringUtils.repeat(BASE_MEMBER_NAME, minLength);
    var max = StringUtils.repeat(BASE_MEMBER_NAME, maxLength);

    var offset = 2;
    var count = 3;

    var sublistMin = minLength + offset - 1;
    var sublistMax = sublistMin + count;

    var expected = members.subList(sublistMin, sublistMax);

    // Range (v * 1) <= member name <= (v * 7), offset = 2, count = 3
    assertThat(jedis.zrangeByLex(KEY, "[" + min, "[" + max, offset, count))
        .containsExactlyElementsOf(expected);
  }

  @Test
  public void shouldReturnAllElementsInRange_givenNegativeCount() {
    var members = populateSortedSet();

    var minLength = 1;

    var min = StringUtils.repeat(BASE_MEMBER_NAME, minLength);

    var offset = 2;

    var sublistMin = minLength + offset - 1;
    var expected = members.subList(sublistMin, members.size());

    // Range (v * 1) <= member name <= +infinity, offset = 2, count = -1
    assertThat(jedis.zrangeByLex(KEY, "[" + min, "+", offset, -1))
        .containsExactlyElementsOf(expected);
  }

  @Test
  public void shouldReturnRange_givenCountLargerThanRange() {
    var members = populateSortedSet();

    var minLength = 4;
    var maxLength = 6;

    var min = StringUtils.repeat(BASE_MEMBER_NAME, minLength);
    var max = StringUtils.repeat(BASE_MEMBER_NAME, maxLength);

    var expected = members.subList(minLength - 1, maxLength);

    // Range (v * 4) <= member name <= (v * 6), offset = 0, count = 10
    assertThat(jedis.zrangeByLex(KEY, "[" + min, "[" + max, 0, 10))
        .containsExactlyElementsOf(expected);
  }

  @Test
  public void shouldReturnEmptyCollection_givenNonZeroNegativeLimitOffset() {
    populateSortedSet();

    assertThat(jedis.zrangeByLex(KEY, "-", "+", -7, 10)).isEmpty();
  }

  @Test
  public void shouldReturnEmptyCollection_givenOffsetLargerThanRange() {
    populateSortedSet();

    var minLength = 1;
    var maxLength = 3;

    var min = StringUtils.repeat(BASE_MEMBER_NAME, minLength);
    var max = StringUtils.repeat(BASE_MEMBER_NAME, maxLength);

    var offset = 7;

    // Range (v * 1) <= member name <= (v * 3), offset = 7, count = 10
    assertThat(jedis.zrangeByLex(KEY, "[" + min, "[" + max, offset, 10)).isEmpty();
  }

  @Test
  public void shouldUseLastLimit_givenMultipleValidLimitsProvided() {
    var members = populateSortedSet();

    var minLength = 2;
    var maxLength = 8;

    var min = StringUtils.repeat(BASE_MEMBER_NAME, minLength);
    var max = StringUtils.repeat(BASE_MEMBER_NAME, maxLength);

    var offset = 2;
    var count = 3;

    var sublistMin = minLength + offset - 1;
    var sublistMax = sublistMin + count;

    // Add 1 to sublistMax, as subList uses exclusive maximum
    var expected = members.subList(sublistMin, sublistMax);

    // Range (v * 2) <= member name <= (v * 8), offset = 2, count = 3
    List<byte[]> result = uncheckedCast(
        jedis.sendCommand(KEY, Protocol.Command.ZRANGEBYLEX, KEY, "[" + min, "[" + max,
            "LIMIT", "0", "10",
            "LIMIT", String.valueOf(offset), String.valueOf(count)));

    var actual = result.stream().map(Coder::bytesToString).collect(Collectors.toList());
    assertThat(actual).containsExactlyElementsOf(expected);
  }

  // Add 10 members with the same score and member names consisting of 'v' repeated an increasing
  // number of times
  private List<String> populateSortedSet() {
    List<String> members = new ArrayList<>();
    var memberName = BASE_MEMBER_NAME;
    for (var i = 0; i < 10; ++i) {
      jedis.zadd(KEY, SCORE, memberName);
      members.add(memberName);
      memberName += BASE_MEMBER_NAME;
    }
    return members;
  }
}
