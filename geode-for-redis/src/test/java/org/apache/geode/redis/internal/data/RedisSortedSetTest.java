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
 *
 */

package org.apache.geode.redis.internal.data;

import static java.util.Collections.singletonList;
import static org.apache.geode.redis.internal.data.AbstractRedisData.NO_EXPIRATION;
import static org.apache.geode.redis.internal.data.RedisDataType.REDIS_SORTED_SET;
import static org.apache.geode.redis.internal.data.RedisSortedSet.OrderedSetEntry.ORDERED_SET_ENTRY_OVERHEAD;
import static org.apache.geode.redis.internal.data.RedisSortedSet.REDIS_SORTED_SET_OVERHEAD;
import static org.apache.geode.redis.internal.data.RedisSortedSet.sortedSetOpStoreResult;
import static org.apache.geode.redis.internal.netty.Coder.stringToBytes;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.GREATEST_MEMBER_NAME;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.LEAST_MEMBER_NAME;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.serialization.ByteArrayDataInput;
import org.apache.geode.internal.size.ReflectionObjectSizer;
import org.apache.geode.redis.internal.commands.executor.GlobPattern;
import org.apache.geode.redis.internal.commands.executor.sortedset.SortedSetLexRangeOptions;
import org.apache.geode.redis.internal.commands.executor.sortedset.SortedSetRankRangeOptions;
import org.apache.geode.redis.internal.commands.executor.sortedset.ZAddOptions;
import org.apache.geode.redis.internal.data.delta.RemoveByteArrays;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.services.RegionProvider;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
public class RedisSortedSetTest {
  private final ReflectionObjectSizer sizer = ReflectionObjectSizer.getInstance();

  private final String member1 = "member1";
  private final String member2 = "member2";
  private final String score1 = "5.55555";
  private final String score2 = "209030.31";
  private final RedisSortedSet rangeSortedSet =
      createRedisSortedSet(
          "1.0", member1, "1.1", member2, "1.2", "member3", "1.3", "member4",
          "1.4", "member5", "1.5", "member6", "1.6", "member7", "1.7", "member8",
          "1.8", "member9", "1.9", "member10", "2.0", "member11", "2.1", "member12");

  @Test
  public void confirmToDataIsSynchronized() throws NoSuchMethodException {
    assertThat(Modifier.isSynchronized(
        RedisSortedSet.class.getMethod("toData", DataOutput.class).getModifiers()))
            .isTrue();
  }

  @Test
  public void confirmSerializationIsStable() throws IOException, ClassNotFoundException {
    var sortedSet1 = createRedisSortedSet("3.14159", "v1", "2.71828", "v2");
    sortedSet1.setExpirationTimestampNoDelta(1000);

    var out = new HeapDataOutputStream(100);
    DataSerializer.writeObject(sortedSet1, out);
    var in = new ByteArrayDataInput(out.toByteArray());

    RedisSortedSet sortedSet2 = DataSerializer.readObject(in);
    assertThat(sortedSet2.equals(sortedSet1)).isTrue();
  }

  @Test
  public void equals_returnsFalse_givenDifferentExpirationTimes() {
    var sortedSet1 = createRedisSortedSet("3.14159", "v1", "2.71828", "v2");
    sortedSet1.setExpirationTimestampNoDelta(1000);

    var sortedSet2 = createRedisSortedSet("3.14159", "v1", "2.71828", "v2");
    sortedSet2.setExpirationTimestampNoDelta(999);
    assertThat(sortedSet1).isNotEqualTo(sortedSet2);
  }

  @Test
  public void equals_returnsFalse_givenDifferentValueBytes() {
    var sortedSet1 = createRedisSortedSet("3.14159", "v1", "2.71828", "v2");
    sortedSet1.setExpirationTimestampNoDelta(1000);
    var sortedSet2 = createRedisSortedSet("3.14159", "v1", "2.71828", "v3");
    sortedSet2.setExpirationTimestampNoDelta(1000);
    assertThat(sortedSet1).isNotEqualTo(sortedSet2);
  }

  @Test
  public void equals_returnsTrue_givenEqualValueBytesAndExpiration() {
    var sortedSet1 = createRedisSortedSet("3.14159", "v1", "2.71828", "v2");
    sortedSet1.setExpirationTimestampNoDelta(1000);
    var sortedSet2 = createRedisSortedSet("3.14159", "v1", "2.71828", "v2");
    sortedSet2.setExpirationTimestampNoDelta(1000);
    assertThat(sortedSet1).isEqualTo(sortedSet2);
  }

  @Test
  public void equals_returnsTrue_givenDifferentEmptySortedSets() {
    var sortedSet1 =
        new RedisSortedSet(Collections.emptyList(), new double[0]);
    RedisSortedSet sortedSet2 = NullRedisDataStructures.NULL_REDIS_SORTED_SET;
    assertThat(sortedSet1).isEqualTo(sortedSet2);
    assertThat(sortedSet2).isEqualTo(sortedSet1);
  }

  @Test
  public void zadd_stores_delta_that_is_stable() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(PartitionedRegion.class));
    when(region.put(any(), any())).thenAnswer(this::validateDeltaSerialization);
    var sortedSet1 = createRedisSortedSet("3.14159", "v1", "2.71828", "v2");
    var members = singletonList(stringToBytes("v3"));
    var scores = new double[] {1.61803D};

    sortedSet1.zadd(region, null, members, scores,
        new ZAddOptions(ZAddOptions.Exists.NONE, false, false));

    verify(region).put(any(), any());
    assertThat(sortedSet1.hasDelta()).isFalse();
  }

  @Test
  public void sortedSetOpStoreResult_stores_delta_that_is_stable() {
    RegionProvider regionProvider = uncheckedCast(mock(RegionProvider.class));
    Region<RedisKey, RedisData> dataRegion = uncheckedCast(mock(PartitionedRegion.class));

    var sortedSet1 = createRedisSortedSet("3.14159", "v1", "2.71828", "v2");
    when(regionProvider.getTypedRedisDataElseRemove(REDIS_SORTED_SET, null, false))
        .thenReturn(sortedSet1);
    when(regionProvider.getDataRegion()).thenReturn(dataRegion);
    when(dataRegion.put(any(), any())).thenAnswer(this::validateDeltaSerialization);

    // Setting up for store operation
    var members = new RedisSortedSet.MemberMap(1);
    var scores = new RedisSortedSet.ScoreSet();
    var member = new byte[] {4};
    var entry = new RedisSortedSet.OrderedSetEntry(member, 5);
    members.put(member, entry);
    scores.add(entry);

    sortedSetOpStoreResult(regionProvider, null, members, scores);

    verify(dataRegion).put(any(), any());
    assertThat(sortedSet1.hasDelta()).isFalse();
  }

  @Test
  public void sortedSetOpStoreResult_sets_expiration_time_to_zero() {
    RegionProvider regionProvider = uncheckedCast(mock(RegionProvider.class));
    Region<RedisKey, RedisData> dataRegion = uncheckedCast(mock(PartitionedRegion.class));

    var setDest = createRedisSortedSet("3.14159", "v1", "2.71828", "v2");
    setDest.setExpirationTimestamp(dataRegion, null, 100);

    when(regionProvider.getTypedRedisDataElseRemove(REDIS_SORTED_SET, null, false))
        .thenReturn(setDest);
    when(regionProvider.getDataRegion()).thenReturn(dataRegion);
    when(dataRegion.put(any(), any())).thenAnswer(this::validateDeltaSerialization);

    // Setting up for store operation
    var members = new RedisSortedSet.MemberMap(1);
    var scores = new RedisSortedSet.ScoreSet();
    var member = new byte[] {4};
    var entry = new RedisSortedSet.OrderedSetEntry(member, 5);
    members.put(member, entry);
    scores.add(entry);

    sortedSetOpStoreResult(regionProvider, null, members, scores);

    assertThat(setDest.getExpirationTimestamp()).isEqualTo(NO_EXPIRATION);
    assertThat(setDest.hasDelta()).isFalse();
  }

  private Object validateDeltaSerialization(InvocationOnMock invocation) throws IOException {
    var value = invocation.getArgument(1, RedisSortedSet.class);
    assertThat(value.hasDelta()).isTrue();
    var out = new HeapDataOutputStream(100);
    value.toDelta(out);
    var in = new ByteArrayDataInput(out.toByteArray());
    var sortedSet2 = createRedisSortedSet("3.14159", "v1", "2.71828", "v2");
    assertThat(sortedSet2).isNotEqualTo(value);
    sortedSet2.fromDelta(in);
    assertThat(sortedSet2).isEqualTo(value);
    return null;
  }

  @Test
  public void setExpirationTimestamp_stores_delta_that_is_stable() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(PartitionedRegion.class));
    when(region.put(any(), any())).thenAnswer(this::validateDeltaSerialization);
    var sortedSet1 = createRedisSortedSet("3.14159", "v1", "2.71828", "v2");

    sortedSet1.setExpirationTimestamp(region, null, 999);

    verify(region).put(any(), any());
    assertThat(sortedSet1.hasDelta()).isFalse();
  }

  @Test
  public void zremCanRemoveMembersToBeRemoved() {
    var member3 = "member3";
    var score3 = "998955255.66361191";
    var sortedSet =
        spy(createRedisSortedSet(score1, member1, score2, member2, score3, member3));
    Region<RedisKey, RedisData> region = uncheckedCast(mock(PartitionedRegion.class));
    var key = new RedisKey();
    var membersToRemove = new ArrayList<byte[]>();
    membersToRemove.add(stringToBytes("nonExisting"));
    membersToRemove.add(stringToBytes(member1));
    membersToRemove.add(stringToBytes(member3));

    var removed = sortedSet.zrem(region, key, membersToRemove);

    assertThat(removed).isEqualTo(2);
    verify(sortedSet).storeChanges(eq(region), eq(key), any(RemoveByteArrays.class));
  }

  @Test
  public void memberRemoveCanRemoveMemberInSortedSet() {
    var sortedSet = createRedisSortedSet(score1, member1, score2, member2);
    var sortedSet2 = createRedisSortedSet(score2, member2);

    var returnValue = sortedSet.memberRemove(stringToBytes(member1));

    assertThat(sortedSet).isEqualTo(sortedSet2);
    assertThat(returnValue).isTrue();
  }

  @Test
  @Parameters({"5,0", "13,15", "17,-2", "12,12"})
  public void zrange_ShouldReturnEmptyList_GivenInvalidRanges(int start, int end) {
    var rangeOptions = mock(SortedSetRankRangeOptions.class);
    when(rangeOptions.getCount()).thenReturn(Integer.MAX_VALUE);
    when(rangeOptions.getRangeIndex(any(), eq(true))).thenReturn(start);
    when(rangeOptions.getRangeIndex(any(), eq(false))).thenReturn(end);
    Collection<byte[]> rangeList = rangeSortedSet.zrange(rangeOptions);
    assertThat(rangeList).isEmpty();
  }

  @Test
  public void zrange_ShouldReturnSimpleRanges() {
    var rangeOptions = mock(SortedSetRankRangeOptions.class);
    when(rangeOptions.getCount()).thenReturn(Integer.MAX_VALUE);
    when(rangeOptions.getRangeIndex(any(), eq(true))).thenReturn(0);
    when(rangeOptions.getRangeIndex(any(), eq(false))).thenReturn(6);
    Collection<byte[]> rangeList = rangeSortedSet.zrange(rangeOptions);
    assertThat(rangeList).hasSize(6);
    assertThat(rangeList)
        .containsExactly("member1".getBytes(), "member2".getBytes(), "member3".getBytes(),
            "member4".getBytes(), "member5".getBytes(), "member6".getBytes());

    when(rangeOptions.getRangeIndex(any(), eq(true))).thenReturn(5);
    when(rangeOptions.getRangeIndex(any(), eq(false))).thenReturn(11);
    rangeList = rangeSortedSet.zrange(rangeOptions);
    assertThat(rangeList).hasSize(6);
    assertThat(rangeList)
        .containsExactly("member6".getBytes(), "member7".getBytes(), "member8".getBytes(),
            "member9".getBytes(), "member10".getBytes(), "member11".getBytes());

    when(rangeOptions.getRangeIndex(any(), eq(true))).thenReturn(10);
    when(rangeOptions.getRangeIndex(any(), eq(false))).thenReturn(13);
    rangeList = rangeSortedSet.zrange(rangeOptions);
    assertThat(rangeList).hasSize(2);
    assertThat(rangeList).containsExactly("member11".getBytes(), "member12".getBytes());
  }

  @Test
  public void zrange_shouldAlsoReturnScores_whenWithScoresSpecified() {
    var rangeOptions = mock(SortedSetRankRangeOptions.class);
    when(rangeOptions.getCount()).thenReturn(Integer.MAX_VALUE);
    when(rangeOptions.getRangeIndex(any(), eq(true))).thenReturn(0);
    when(rangeOptions.getRangeIndex(any(), eq(false))).thenReturn(6);
    when(rangeOptions.isWithScores()).thenReturn(true);
    Collection<byte[]> rangeList = rangeSortedSet.zrange(rangeOptions);
    assertThat(rangeList).hasSize(12);
    assertThat(rangeList).containsExactly("member1".getBytes(), "1".getBytes(),
        "member2".getBytes(), "1.1".getBytes(), "member3".getBytes(), "1.2".getBytes(),
        "member4".getBytes(), "1.3".getBytes(), "member5".getBytes(), "1.4".getBytes(),
        "member6".getBytes(), "1.5".getBytes());
  }

  @Test
  public void zlexcount_shouldBeInclusiveWhenSpecified() {
    var sortedSet = createRedisSortedSet(
        score1, "member1",
        score1, "member2",
        score1, "member3",
        score1, "member4",
        score1, "member5");
    var lexOptions = new SortedSetLexRangeOptions(
        Arrays.asList("command".getBytes(), "key".getBytes(), "[member1".getBytes(),
            "[member3".getBytes()),
        false);
    assertThat(sortedSet.zlexcount(lexOptions)).isEqualTo(3);
  }

  @Test
  public void zlexcount_shouldBeExclusiveWhenSpecified() {
    var sortedSet = createRedisSortedSet(
        score1, "member1",
        score1, "member2",
        score1, "member3",
        score1, "member4",
        score1, "member5");
    var lexOptions = new SortedSetLexRangeOptions(
        Arrays.asList("command".getBytes(), "key".getBytes(), "(member1".getBytes(),
            "(member3".getBytes()),
        false);
    assertThat(sortedSet.zlexcount(lexOptions)).isEqualTo(1);
  }

  @Test
  public void zlexcount_shouldBeZero_whenMinIsTooGreat() {
    var sortedSet = createRedisSortedSet(
        score1, "member1",
        score1, "member2",
        score1, "member3",
        score1, "member4",
        score1, "member5");
    var lexOptions = new SortedSetLexRangeOptions(
        Arrays.asList("command".getBytes(), "key".getBytes(), "[member6".getBytes(),
            "(member8".getBytes()),
        false);
    assertThat(sortedSet.zlexcount(lexOptions)).isEqualTo(0);
  }

  @Test
  public void zlexcount_shouldBeZero_whenMaxIsTooSmall() {
    var sortedSet = createRedisSortedSet(
        score1, "member1",
        score1, "member2",
        score1, "member3",
        score1, "member4",
        score1, "member5");
    var lexOptions = new SortedSetLexRangeOptions(
        Arrays.asList("command".getBytes(), "key".getBytes(), "[membeq0".getBytes(),
            "[member0".getBytes()),
        false);
    assertThat(sortedSet.zlexcount(lexOptions)).isEqualTo(0);
  }

  @Test
  public void zlexcount_shouldBeZero_whenMinAndMaxAreReversed() {
    var sortedSet = createRedisSortedSet(
        score1, "member1",
        score1, "member2",
        score1, "member3",
        score1, "member4",
        score1, "member5");
    var lexOptions = new SortedSetLexRangeOptions(
        Arrays.asList("command".getBytes(), "key".getBytes(), "[member5".getBytes(),
            "[member0".getBytes()),
        false);
    assertThat(sortedSet.zlexcount(lexOptions)).isEqualTo(0);
  }

  @Test
  public void zlexcount_shouldBeAbleToCountAllEntries() {
    var sortedSet = createRedisSortedSet(
        score1, "member1",
        score1, "member2",
        score1, "member3",
        score1, "member4",
        score1, "member5");
    var lexOptions =
        new SortedSetLexRangeOptions(
            Arrays.asList("command".getBytes(), "key".getBytes(), "-".getBytes(), "+".getBytes()),
            false);
    assertThat(sortedSet.zlexcount(lexOptions)).isEqualTo(5);
  }

  @Test
  public void zscanOnlyReturnsElementsMatchingPattern() {
    var result =
        rangeSortedSet.zscan(new GlobPattern(stringToBytes("member1*")),
            (int) rangeSortedSet.zcard(), 0);

    var fieldsAndValues =
        result.right.stream().map(Coder::bytesToString).collect(Collectors.toList());

    assertThat(fieldsAndValues).containsExactlyInAnyOrder("member1", "1", "member10", "1.9",
        "member11", "2", "member12", "2.1");
  }

  @Test
  public void zscanThrowsWhenReturnedArrayListLengthWouldExceedVMLimit() {
    var sortedSet = spy(new RedisSortedSet(Collections.emptyList(), new double[] {}));
    doReturn(Integer.MAX_VALUE - 10L).when(sortedSet).zcard();

    assertThatThrownBy(() -> sortedSet.zscan(null, Integer.MAX_VALUE - 10, 0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void scoreSet_shouldNotRetainOldEntries_whenEntriesUpdated() {
    var rangeOptions =
        new SortedSetRankRangeOptions(
            Arrays.asList("command".getBytes(), "key".getBytes(), "0".getBytes(), "100".getBytes()),
            false);
    Collection<byte[]> rangeList = rangeSortedSet.zrange(rangeOptions);
    assertThat(rangeList).hasSize(12);
    assertThat(rangeList).containsExactly("member1".getBytes(), "member2".getBytes(),
        "member3".getBytes(), "member4".getBytes(), "member5".getBytes(),
        "member6".getBytes(), "member7".getBytes(), "member8".getBytes(),
        "member9".getBytes(), "member10".getBytes(), "member11".getBytes(), "member12".getBytes());
  }

  @Test
  public void orderedSetEntryCompareTo_returnsCorrectly_givenDifferentScores() {
    var memberName = stringToBytes("member");

    RedisSortedSet.AbstractOrderedSetEntry negativeInf =
        new RedisSortedSet.OrderedSetEntry(memberName, Double.NEGATIVE_INFINITY);
    RedisSortedSet.AbstractOrderedSetEntry negativeOne =
        new RedisSortedSet.OrderedSetEntry(memberName, -1.0);
    RedisSortedSet.AbstractOrderedSetEntry zero =
        new RedisSortedSet.OrderedSetEntry(memberName, 0.0);
    RedisSortedSet.AbstractOrderedSetEntry one =
        new RedisSortedSet.OrderedSetEntry(memberName, 1.0);
    RedisSortedSet.AbstractOrderedSetEntry positiveInf =
        new RedisSortedSet.OrderedSetEntry(memberName, Double.POSITIVE_INFINITY);

    assertThat(negativeInf.compareTo(negativeOne)).isEqualTo(-1);
    assertThat(negativeOne.compareTo(zero)).isEqualTo(-1);
    assertThat(zero.compareTo(one)).isEqualTo(-1);
    assertThat(one.compareTo(positiveInf)).isEqualTo(-1);

    assertThat(positiveInf.compareTo(one)).isEqualTo(1);
    assertThat(one.compareTo(zero)).isEqualTo(1);
    assertThat(zero.compareTo(negativeOne)).isEqualTo(1);
    assertThat(negativeOne.compareTo(negativeInf)).isEqualTo(1);
  }

  @Test
  public void scoreDummyOrderedSetEntryCompareTo_throws_givenBothArraysAreGreatestOrLeastMemberNameAndScoresAreEqual() {
    var score = 1.0;

    RedisSortedSet.AbstractOrderedSetEntry greatest1 =
        new RedisSortedSet.ScoreDummyOrderedSetEntry(score, true, true);
    RedisSortedSet.AbstractOrderedSetEntry greatest2 =
        new RedisSortedSet.ScoreDummyOrderedSetEntry(score, false, false);

    // noinspection ResultOfMethodCallIgnored
    assertThatThrownBy(() -> greatest1.compareTo(greatest2))
        .isInstanceOf(IllegalStateException.class);

    RedisSortedSet.AbstractOrderedSetEntry least1 =
        new RedisSortedSet.ScoreDummyOrderedSetEntry(score, false, true);
    RedisSortedSet.AbstractOrderedSetEntry least2 =
        new RedisSortedSet.ScoreDummyOrderedSetEntry(score, true, false);

    // noinspection ResultOfMethodCallIgnored
    assertThatThrownBy(() -> least1.compareTo(least2)).isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void scoreDummyOrderedSetEntryCompareTo_handlesDummyMemberNames_givenScoresAreEqual() {
    var score = 1.0;

    RedisSortedSet.AbstractOrderedSetEntry greatest =
        new RedisSortedSet.ScoreDummyOrderedSetEntry(score, true, true);
    RedisSortedSet.AbstractOrderedSetEntry least =
        new RedisSortedSet.ScoreDummyOrderedSetEntry(score, true, false);
    RedisSortedSet.AbstractOrderedSetEntry middle =
        new RedisSortedSet.OrderedSetEntry(stringToBytes("middle"), score);

    // greatest > least
    assertThat(greatest.compareTo(least)).isEqualTo(1);
    // least < greatest
    assertThat(least.compareTo(greatest)).isEqualTo(-1);
    // greatest > middle
    assertThat(greatest.compareTo(middle)).isEqualTo(1);
    // least < middle
    assertThat(least.compareTo(middle)).isEqualTo(-1);
  }

  @Test
  public void scoreDummyOrderedSetEntryCompareTo_handlesDummyMemberNameEquivalents_givenScoresAreEqual() {
    var score = 1.0;

    RedisSortedSet.AbstractOrderedSetEntry greatest =
        new RedisSortedSet.ScoreDummyOrderedSetEntry(score, true, true);
    RedisSortedSet.AbstractOrderedSetEntry greatestEquivalent =
        new RedisSortedSet.OrderedSetEntry(GREATEST_MEMBER_NAME.clone(), score);

    RedisSortedSet.AbstractOrderedSetEntry least =
        new RedisSortedSet.ScoreDummyOrderedSetEntry(score, true, false);
    RedisSortedSet.AbstractOrderedSetEntry leastEquivalent =
        new RedisSortedSet.OrderedSetEntry(LEAST_MEMBER_NAME.clone(), score);

    // GREATEST_MEMBER_NAME > an array with contents equal to GREATEST_MEMBER_NAME
    assertThat(greatest.compareTo(greatestEquivalent)).isEqualTo(1);

    // LEAST_MEMBER_NAME < an array with contents equal to LEAST_MEMBER_NAME
    assertThat(least.compareTo(leastEquivalent)).isEqualTo(-1);
  }

  @Test
  public void scoreDummyOrderedSetEntryConstructor_setsAppropriateMemberName() {
    RedisSortedSet.AbstractOrderedSetEntry entry =
        new RedisSortedSet.ScoreDummyOrderedSetEntry(1, false, false);
    assertThat(entry.getMember()).isSameAs(GREATEST_MEMBER_NAME);

    entry = new RedisSortedSet.ScoreDummyOrderedSetEntry(1, true, false);
    assertThat(entry.getMember()).isSameAs(LEAST_MEMBER_NAME);

    entry = new RedisSortedSet.ScoreDummyOrderedSetEntry(1, false, true);
    assertThat(entry.getMember()).isSameAs(LEAST_MEMBER_NAME);

    entry = new RedisSortedSet.ScoreDummyOrderedSetEntry(1, true, true);
    assertThat(entry.getMember()).isSameAs(GREATEST_MEMBER_NAME);
  }

  @Test
  public void memberDummyOrderedSetEntryCompareTo_handlesDummyMemberNames() {
    RedisSortedSet.AbstractOrderedSetEntry greatest =
        new RedisSortedSet.MemberDummyOrderedSetEntry(GREATEST_MEMBER_NAME, false, false);
    RedisSortedSet.AbstractOrderedSetEntry least =
        new RedisSortedSet.MemberDummyOrderedSetEntry(LEAST_MEMBER_NAME, false, false);
    RedisSortedSet.AbstractOrderedSetEntry middle =
        new RedisSortedSet.MemberDummyOrderedSetEntry(stringToBytes("middle"), false, false);

    // greatest > least
    assertThat(greatest.compareTo(least)).isEqualTo(1);
    // greatest > middle
    assertThat(greatest.compareTo(middle)).isEqualTo(1);
    // middle < greatest
    assertThat(middle.compareTo(greatest)).isEqualTo(-1);
    // middle > least
    assertThat(middle.compareTo(least)).isEqualTo(1);
    // least < greatest
    assertThat(least.compareTo(greatest)).isEqualTo(-1);
    // least < middle
    assertThat(least.compareTo(middle)).isEqualTo(-1);
  }

  @Test
  public void memberDummyOrderedSetEntryCompareTo_withEqualMemberNamesAndExclusiveMinimum() {
    var memberName = stringToBytes("member");
    var score = 1.0;
    RedisSortedSet.AbstractOrderedSetEntry realEntry =
        new RedisSortedSet.OrderedSetEntry(memberName, score);

    RedisSortedSet.AbstractOrderedSetEntry exclusiveMin =
        new RedisSortedSet.MemberDummyOrderedSetEntry(memberName, true, true);

    // exclusiveMin > realEntry
    assertThat(exclusiveMin.compareTo(realEntry)).isEqualTo(1);
  }

  @Test
  public void memberDummyOrderedSetEntryCompareTo_withEqualMemberNamesAndInclusiveMinimum() {
    var memberName = stringToBytes("member");
    var score = 1.0;
    RedisSortedSet.AbstractOrderedSetEntry realEntry =
        new RedisSortedSet.OrderedSetEntry(memberName, score);

    RedisSortedSet.AbstractOrderedSetEntry inclusiveMin =
        new RedisSortedSet.MemberDummyOrderedSetEntry(memberName, false, true);

    // inclusiveMin < realEntry
    assertThat(inclusiveMin.compareTo(realEntry)).isEqualTo(-1);
  }

  @Test
  public void memberDummyOrderedSetEntryCompareTo_withEqualMemberNamesAndExclusiveMaximum() {
    var memberName = stringToBytes("member");
    var score = 1.0;
    RedisSortedSet.AbstractOrderedSetEntry realEntry =
        new RedisSortedSet.OrderedSetEntry(memberName, score);

    RedisSortedSet.AbstractOrderedSetEntry exclusiveMax =
        new RedisSortedSet.MemberDummyOrderedSetEntry(memberName, true, false);

    // exclusiveMax < realEntry
    assertThat(exclusiveMax.compareTo(realEntry)).isEqualTo(-1);
  }

  @Test
  public void memberDummyOrderedSetEntryCompareTo_withEqualMemberNamesAndInclusiveMaximum() {
    var memberName = stringToBytes("member");
    var score = 1.0;
    RedisSortedSet.AbstractOrderedSetEntry realEntry =
        new RedisSortedSet.OrderedSetEntry(memberName, score);

    RedisSortedSet.AbstractOrderedSetEntry inclusiveMax =
        new RedisSortedSet.MemberDummyOrderedSetEntry(memberName, false, false);

    // inclusiveMax > realEntry
    assertThat(inclusiveMax.compareTo(realEntry)).isEqualTo(1);
  }

  @Test
  public void zpopminRemovesMemberWithLowestScore() {
    var originalSize = rangeSortedSet.getSortedSetSize();
    var sortedSet = spy(rangeSortedSet);
    Region<RedisKey, RedisData> region = uncheckedCast(mock(PartitionedRegion.class));
    var key = new RedisKey();
    var count = 1;

    var result = sortedSet.zpopmin(region, key, count);
    assertThat(result).containsExactly("member1".getBytes(), "1".getBytes());

    var argumentCaptor = ArgumentCaptor.forClass(
        RemoveByteArrays.class);
    verify(sortedSet).storeChanges(eq(region), eq(key), argumentCaptor.capture());
    assertThat(argumentCaptor.getValue().getRemoves()).containsExactly("member1".getBytes());
    assertThat(rangeSortedSet.getSortedSetSize()).isEqualTo(originalSize - count);
  }

  @Test
  public void zpopminRemovesMembersWithLowestScores_whenCountIsGreaterThanOne() {
    var originalSize = rangeSortedSet.getSortedSetSize();
    var sortedSet = spy(rangeSortedSet);
    Region<RedisKey, RedisData> region = uncheckedCast(mock(PartitionedRegion.class));
    var key = new RedisKey();
    var count = 3;

    var result = sortedSet.zpopmin(region, key, count);
    assertThat(result).containsExactlyInAnyOrder("member1".getBytes(), "1".getBytes(),
        "member2".getBytes(), "1.1".getBytes(), "member3".getBytes(), "1.2".getBytes());

    var argumentCaptor = ArgumentCaptor.forClass(
        RemoveByteArrays.class);
    verify(sortedSet).storeChanges(eq(region), eq(key), argumentCaptor.capture());
    assertThat(argumentCaptor.getValue().getRemoves()).containsExactlyInAnyOrder(
        "member1".getBytes(), "member2".getBytes(), "member3".getBytes());
    assertThat(rangeSortedSet.getSortedSetSize()).isEqualTo(originalSize - count);
  }

  @Test
  public void zpopminRemovesRegionEntryWhenSetBecomesEmpty() {
    var sortedSet = spy(createRedisSortedSet(score1, member1));
    Region<RedisKey, RedisData> region = uncheckedCast(mock(PartitionedRegion.class));
    var key = new RedisKey();

    var result = sortedSet.zpopmin(region, key, 1);
    assertThat(result).containsExactly(member1.getBytes(), score1.getBytes());

    verify(sortedSet).storeChanges(eq(region), eq(key), any(RemoveByteArrays.class));
    verify(region).remove(key);
  }

  @Test
  public void zpopminRemovesLowestLexWhenScoresAreEqual() {
    var sortedSet = spy(createRedisSortedSet(
        "1.1", "member5",
        "1.1", "member4",
        "1.1", "member3",
        "1.1", "member2",
        "1.1", "member1"));
    Region<RedisKey, RedisData> region = uncheckedCast(mock(PartitionedRegion.class));
    var key = new RedisKey();

    var result = sortedSet.zpopmin(region, key, 1);
    assertThat(result).containsExactly("member1".getBytes(), "1.1".getBytes());
  }

  @Test
  public void zpopmaxRemovesMemberWithHighestScore() {
    var originalSize = rangeSortedSet.getSortedSetSize();
    var sortedSet = spy(rangeSortedSet);
    Region<RedisKey, RedisData> region = uncheckedCast(mock(PartitionedRegion.class));
    var key = new RedisKey();
    var count = 1;

    var result = sortedSet.zpopmax(region, key, count);
    assertThat(result).containsExactly("member12".getBytes(), "2.1".getBytes());

    var argumentCaptor = ArgumentCaptor.forClass(
        RemoveByteArrays.class);
    verify(sortedSet).storeChanges(eq(region), eq(key), argumentCaptor.capture());
    assertThat(argumentCaptor.getValue().getRemoves()).containsExactly("member12".getBytes());
    assertThat(rangeSortedSet.getSortedSetSize()).isEqualTo(originalSize - count);
  }

  @Test
  public void zpopmaxRemovesMembersWithHighestScores_whenCountIsGreaterThanOne() {
    var originalSize = rangeSortedSet.getSortedSetSize();
    var sortedSet = spy(rangeSortedSet);
    Region<RedisKey, RedisData> region = uncheckedCast(mock(PartitionedRegion.class));
    var key = new RedisKey();
    var count = 3;

    var result = sortedSet.zpopmax(region, key, count);
    assertThat(result).containsExactlyInAnyOrder("member10".getBytes(), "1.9".getBytes(),
        "member11".getBytes(), "2".getBytes(), "member12".getBytes(), "2.1".getBytes());

    var argumentCaptor = ArgumentCaptor.forClass(
        RemoveByteArrays.class);
    verify(sortedSet).storeChanges(eq(region), eq(key), argumentCaptor.capture());
    assertThat(argumentCaptor.getValue().getRemoves()).containsExactlyInAnyOrder(
        "member10".getBytes(), "member11".getBytes(), "member12".getBytes());
    assertThat(rangeSortedSet.getSortedSetSize()).isEqualTo(originalSize - count);
  }

  @Test
  public void zpopmaxRemovesRegionEntryWhenSetBecomesEmpty() {
    var sortedSet = spy(createRedisSortedSet(score1, member1));
    Region<RedisKey, RedisData> region = uncheckedCast(mock(PartitionedRegion.class));
    var key = new RedisKey();

    var result = sortedSet.zpopmax(region, key, 1);
    assertThat(result).containsExactly(member1.getBytes(), score1.getBytes());

    verify(sortedSet).storeChanges(eq(region), eq(key), any(RemoveByteArrays.class));
    verify(region).remove(key);
  }

  @Test
  public void zpopmaxRemovesHighestLexWhenScoresAreEqual() {
    var sortedSet = spy(createRedisSortedSet(
        "1.1", "member5",
        "1.1", "member4",
        "1.1", "member3",
        "1.1", "member2",
        "1.1", "member1"));
    Region<RedisKey, RedisData> region = uncheckedCast(mock(PartitionedRegion.class));
    var key = new RedisKey();

    var result = sortedSet.zpopmax(region, key, 1);
    assertThat(result).containsExactly("member5".getBytes(), "1.1".getBytes());
  }

  /******** constants *******/
  // These tests contain the math that is used to derive the constants in RedisSortedSet and
  // OrderedSetEntry. If these tests start failing, it is because the overheads of RedisSortedSet or
  // OrderedSetEntry have changed. If they have decreased, good job! You can change the constant in
  // RedisSortedSet or OrderedSetEntry to reflect that. If they have increased, carefully consider
  // that increase before changing the constant.
  @Test
  public void baseRedisSortedSetOverheadConstant_shouldMatchReflectedSize() {
    var set = new RedisSortedSet(Collections.emptyList(), new double[0]);
    var backingMap = new RedisSortedSet.MemberMap(0);
    var backingTree = new RedisSortedSet.ScoreSet();
    var baseRedisSetOverhead =
        sizer.sizeof(set) - sizer.sizeof(backingMap) - sizer.sizeof(backingTree);

    assertThat(REDIS_SORTED_SET_OVERHEAD).isEqualTo(baseRedisSetOverhead);
  }

  @Test
  public void baseOrderedSetEntrySize_shouldMatchReflectedSize() {
    var score = 1.0;
    var memberBytes = stringToBytes("member");
    var entry =
        new RedisSortedSet.OrderedSetEntry(memberBytes, score);
    var expectedSize = sizer.sizeof(entry) - sizer.sizeof(memberBytes);

    assertThat(ORDERED_SET_ENTRY_OVERHEAD).isEqualTo(expectedSize);
  }

  /****************** Size ******************/

  @Test
  public void redisSortedSetGetSizeInBytes_isAccurateForAdds() {
    Region<RedisKey, RedisData> mockRegion = uncheckedCast(mock(PartitionedRegion.class));
    var mockKey = mock(RedisKey.class);
    var options = new ZAddOptions(ZAddOptions.Exists.NONE, false, false);
    var sortedSet = new RedisSortedSet(Collections.emptyList(), new double[0]);

    var expectedSize = sizer.sizeof(sortedSet);
    var actualSize = sortedSet.getSizeInBytes();
    assertThat(actualSize).isEqualTo(expectedSize);

    // Add members and scores and confirm that the actual size is accurate after each operation
    var numberOfEntries = 100;
    for (var i = 0; i < numberOfEntries; ++i) {
      var scores = new double[] {(double) i};
      var members = singletonList(new byte[i]);
      sortedSet.zadd(mockRegion, mockKey, members, scores, options);
      expectedSize = sizer.sizeof(sortedSet);
      actualSize = sortedSet.getSizeInBytes();
      assertThat(actualSize).isEqualTo(expectedSize);
    }
  }

  @Test
  public void redisSortedSetGetSizeInBytes_isAccurateForUpdates() {
    Region<RedisKey, RedisData> mockRegion = uncheckedCast(mock(PartitionedRegion.class));
    var mockKey = mock(RedisKey.class);
    var options = new ZAddOptions(ZAddOptions.Exists.NONE, false, false);
    var sortedSet = new RedisSortedSet(Collections.emptyList(), new double[0]);

    var numberOfEntries = 100;
    for (var i = 0; i < numberOfEntries; ++i) {
      var members = singletonList(new byte[i]);
      var scores = new double[] {(double) i};
      sortedSet.zadd(mockRegion, mockKey, members, scores, options);
    }

    // Update half the scores and confirm that the actual size is accurate after each operation
    for (var i = 0; i < numberOfEntries / 2; ++i) {
      var members = singletonList(new byte[i]);
      var scores = new double[] {i * 2d};
      sortedSet.zadd(mockRegion, mockKey, members, scores, options);
      var expectedSize = sizer.sizeof(sortedSet);
      var actualSize = sortedSet.getSizeInBytes();
      assertThat(actualSize).isEqualTo(expectedSize);
    }
  }

  @Test
  public void redisSortedSetGetSizeInBytes_isAccurateForRemoves() {
    Region<RedisKey, RedisData> mockRegion = uncheckedCast(mock(PartitionedRegion.class));
    var mockKey = mock(RedisKey.class);
    var options = new ZAddOptions(ZAddOptions.Exists.NONE, false, false);
    var sortedSet = new RedisSortedSet(Collections.emptyList(), new double[0]);

    var numberOfEntries = 100;
    for (var i = 0; i < numberOfEntries; ++i) {
      var members = singletonList(new byte[i]);
      var scores = new double[] {(double) i};
      sortedSet.zadd(mockRegion, mockKey, members, scores, options);
    }

    // Remove all members and confirm that the actual size is accurate after each operation
    for (var i = 0; i < numberOfEntries; ++i) {
      var memberToRemove = singletonList(new byte[i]);
      sortedSet.zrem(mockRegion, mockKey, memberToRemove);
      var expectedSize = sizer.sizeof(sortedSet);
      var actualSize = sortedSet.getSizeInBytes();
      assertThat(actualSize).isEqualTo(expectedSize);
    }
  }

  @Test
  public void redisSortedSetGetSizeInBytes_isAccurateForZpopmax() {
    Region<RedisKey, RedisData> mockRegion = uncheckedCast(mock(PartitionedRegion.class));
    var mockKey = mock(RedisKey.class);
    var options = new ZAddOptions(ZAddOptions.Exists.NONE, false, false);
    var sortedSet = new RedisSortedSet(Collections.emptyList(), new double[0]);

    var numberOfEntries = 100;
    for (var i = 0; i < numberOfEntries; ++i) {
      var members = singletonList(new byte[i]);
      var scores = new double[] {(double) i};
      sortedSet.zadd(mockRegion, mockKey, members, scores, options);
    }

    // Remove all members by zpopmax and ensure size is correct
    for (var i = 0; i < numberOfEntries; ++i) {
      sortedSet.zpopmax(mockRegion, mockKey, 1);
      var expectedSize = sizer.sizeof(sortedSet);
      var actualSize = sortedSet.getSizeInBytes();
      assertThat(actualSize).isEqualTo(expectedSize);
    }
  }

  @Test
  public void redisSortedSetGetSizeInBytes_isAccurateForZpopmin() {
    Region<RedisKey, RedisData> mockRegion = uncheckedCast(mock(PartitionedRegion.class));
    var mockKey = mock(RedisKey.class);
    var options = new ZAddOptions(ZAddOptions.Exists.NONE, false, false);
    var sortedSet = new RedisSortedSet(Collections.emptyList(), new double[0]);

    var numberOfEntries = 100;
    for (var i = 0; i < numberOfEntries; ++i) {
      var members = singletonList(new byte[i]);
      var scores = new double[] {(double) i};
      sortedSet.zadd(mockRegion, mockKey, members, scores, options);
    }

    // Remove all members by zpopmin and ensure size is correct
    for (var i = 0; i < numberOfEntries; ++i) {
      sortedSet.zpopmin(mockRegion, mockKey, 1);
      var expectedSize = sizer.sizeof(sortedSet);
      var actualSize = sortedSet.getSizeInBytes();
      assertThat(actualSize).isEqualTo(expectedSize);
    }
  }

  @Test
  public void orderedSetEntryGetSizeInBytes_isAccurate() {
    var random = new Random();
    byte[] member;
    for (var i = 0; i < 100; ++i) {
      member = new byte[random.nextInt(50_000)];
      var entry =
          new RedisSortedSet.OrderedSetEntry(member, random.nextDouble());
      assertThat(entry.getSizeInBytes()).isEqualTo(sizer.sizeof(entry) - sizer.sizeof(member));
    }
  }

  private RedisSortedSet createRedisSortedSet(String... membersAndScores) {
    List<byte[]> members = new ArrayList<>();
    var scores = new double[100];

    var iterator = Arrays.stream(membersAndScores).iterator();
    var i = 0;
    while (iterator.hasNext()) {
      scores[i] = Coder.bytesToDouble(iterator.next().getBytes(StandardCharsets.UTF_8));
      members.add(iterator.next().getBytes(StandardCharsets.UTF_8));
      i++;
    }

    return new RedisSortedSet(members, scores);
  }
}
