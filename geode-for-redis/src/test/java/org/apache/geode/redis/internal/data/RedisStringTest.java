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

import static org.apache.geode.redis.internal.netty.Coder.longToBytes;
import static org.apache.geode.redis.internal.netty.Coder.stringToBytes;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.serialization.ByteArrayDataInput;
import org.apache.geode.internal.size.ReflectionObjectSizer;

public class RedisStringTest {
  private final ReflectionObjectSizer reflectionObjectSizer = ReflectionObjectSizer.getInstance();

  @Test
  public void constructorSetsValue() {
    byte[] bytes = {0, 1, 2};
    var string = new RedisString(bytes);
    var returnedBytes = string.get();
    assertThat(returnedBytes).isNotNull();
    assertThat(returnedBytes).isEqualTo(bytes);
  }

  @Test
  public void setSetsValue() {
    var string = new RedisString();
    byte[] bytes = {0, 1, 2};
    string.set(bytes);
    var returnedBytes = string.get();
    assertThat(returnedBytes).isNotNull();
    assertThat(returnedBytes).isEqualTo(bytes);
  }

  @Test
  public void getReturnsSetValue() {
    byte[] bytes = {0, 1};
    var string = new RedisString(bytes);
    var returnedBytes = string.get();
    assertThat(returnedBytes).isNotNull();
    assertThat(returnedBytes).isEqualTo(bytes);
  }

  @Test
  public void getsetSetsValueAndReturnsOldValue() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(PartitionedRegion.class));
    byte[] oldBytes = {0, 1};
    byte[] newBytes = {0, 1, 2};
    var string = new RedisString(oldBytes);
    var returnedBytes = string.getset(region, null, newBytes);
    assertThat(returnedBytes).isNotNull();
    assertThat(returnedBytes).isEqualTo(oldBytes);
    assertThat(string.get()).isNotNull();
    assertThat(string.get()).isEqualTo(newBytes);
  }

  @Test
  public void appendResizesByteArray() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(PartitionedRegion.class));
    var redisString = new RedisString(new byte[] {0, 1});
    var redisStringSize = redisString.strlen();
    byte[] bytesToAppend = {2, 3, 4, 5};
    var appendedSize = bytesToAppend.length;
    var appendedStringSize = redisString.append(region, null, bytesToAppend);
    assertThat(appendedStringSize).isEqualTo(redisStringSize + appendedSize);
  }

  @Test
  public void appendStoresStableDelta() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(PartitionedRegion.class));
    final byte[] baseBytes = {'0', '1'};
    final byte[] bytesToAppend = {'2', '3'};

    when(region.put(any(), any()))
        .thenAnswer(invocation -> validateDeltaSerialization(baseBytes, invocation));
    var stringOne = new RedisString(baseBytes);

    stringOne.append(region, null, bytesToAppend);

    verify(region).put(any(), any());
    assertThat(stringOne.hasDelta()).isFalse();
  }

  @Test
  public void setStoresStableDelta() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(PartitionedRegion.class));
    final byte[] baseBytes = {'0', '1'};
    final byte[] bytesToSet = {'2', '3'};

    when(region.put(any(), any()))
        .thenAnswer(invocation -> validateDeltaSerialization(baseBytes, invocation));
    var stringOne = new RedisString(baseBytes);

    stringOne.set(region, null, bytesToSet, null);

    verify(region).put(any(), any());
    assertThat(stringOne.hasDelta()).isFalse();
  }

  @Test
  public void incrStoresStableDelta() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(PartitionedRegion.class));
    final byte[] baseBytes = {'1'};

    when(region.put(any(), any()))
        .thenAnswer(invocation -> validateDeltaSerialization(baseBytes, invocation));
    var stringOne = new RedisString(baseBytes);

    stringOne.incr(region, null);

    verify(region).put(any(), any());
    assertThat(stringOne.hasDelta()).isFalse();
  }

  @Test
  public void incrbyStoresStableDelta() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(PartitionedRegion.class));
    final byte[] baseBytes = {'1'};

    when(region.put(any(), any()))
        .thenAnswer(invocation -> validateDeltaSerialization(baseBytes, invocation));
    var stringOne = new RedisString(baseBytes);

    stringOne.incrby(region, null, 3);

    verify(region).put(any(), any());
    assertThat(stringOne.hasDelta()).isFalse();
  }

  @Test
  public void incrbyfloatStoresStableDelta() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(PartitionedRegion.class));
    final byte[] baseBytes = {'1'};

    when(region.put(any(), any()))
        .thenAnswer(invocation -> validateDeltaSerialization(baseBytes, invocation));
    var stringOne = new RedisString(baseBytes);

    stringOne.incrbyfloat(region, null, new BigDecimal("3.0"));

    verify(region).put(any(), any());
    assertThat(stringOne.hasDelta()).isFalse();
  }

  @Test
  public void setbitStoresStableDelta() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(PartitionedRegion.class));
    final byte[] baseBytes = {'0', '1'};

    when(region.put(any(), any()))
        .thenAnswer(invocation -> validateDeltaSerialization(new byte[] {'0', '1'}, invocation));
    var stringOne = new RedisString(baseBytes);

    stringOne.setbit(region, null, 1, 0, (byte) 6);

    verify(region).put(any(), any());
    assertThat(stringOne.hasDelta()).isFalse();
  }

  @Test
  public void confirmSerializationIsStable() throws IOException, ClassNotFoundException {
    var stringOne = new RedisString(new byte[] {0, 1, 2, 3});
    var expirationTimestamp = 1000;
    stringOne.setExpirationTimestampNoDelta(expirationTimestamp);
    var outputStream = new HeapDataOutputStream(100);
    DataSerializer.writeObject(stringOne, outputStream);
    var dataInput = new ByteArrayDataInput(outputStream.toByteArray());
    RedisString stringTwo = DataSerializer.readObject(dataInput);
    assertThat(stringTwo).isEqualTo(stringOne);
    assertThat(stringTwo.getExpirationTimestamp())
        .isEqualTo(stringOne.getExpirationTimestamp())
        .isEqualTo(expirationTimestamp);
  }

  @Test
  public void decrStoresStableDelta() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(PartitionedRegion.class));
    final byte[] baseBytes = {'1'};

    when(region.put(any(), any()))
        .thenAnswer(invocation -> validateDeltaSerialization(baseBytes, invocation));
    var stringOne = new RedisString(baseBytes);

    stringOne.decr(region, null);

    verify(region).put(any(), any());
    assertThat(stringOne.hasDelta()).isFalse();
  }

  @Test
  public void decrbyStoresStableDelta() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(PartitionedRegion.class));
    final byte[] baseBytes = {'1'};

    when(region.put(any(), any()))
        .thenAnswer(invocation -> validateDeltaSerialization(baseBytes, invocation));
    var stringOne = new RedisString(baseBytes);

    stringOne.decrby(region, null, 3);

    verify(region).put(any(), any());
    assertThat(stringOne.hasDelta()).isFalse();
  }

  @Test
  public void decrbyfloatStoresStableDelta() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(PartitionedRegion.class));
    final byte[] baseBytes = {'1'};

    when(region.put(any(), any()))
        .thenAnswer(invocation -> validateDeltaSerialization(baseBytes, invocation));
    var stringOne = new RedisString(baseBytes);

    stringOne.incrbyfloat(region, null, new BigDecimal("3.0"));

    verify(region).put(any(), any());
    assertThat(stringOne.hasDelta()).isFalse();
  }

  @Test
  public void confirmToDataIsSynchronized() throws NoSuchMethodException {
    assertThat(Modifier.isSynchronized(
        RedisString.class.getMethod("toData", DataOutput.class).getModifiers()))
            .isTrue();
  }

  @Test
  public void incrThrowsArithmeticErrorWhenNotALong() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(PartitionedRegion.class));
    var bytes = stringToBytes("10 1");
    var string = new RedisString(bytes);
    assertThatThrownBy(() -> string.incr(region, null)).isInstanceOf(NumberFormatException.class);
  }

  @Test
  public void incrErrorsWhenValueOverflows() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(PartitionedRegion.class));
    var bytes = longToBytes(Long.MAX_VALUE);
    var string = new RedisString(bytes);
    assertThatThrownBy(() -> string.incr(region, null)).isInstanceOf(ArithmeticException.class);
  }

  @Test
  public void incrIncrementsValueAtGivenKey() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(PartitionedRegion.class));
    var bytes = stringToBytes("10");
    var string = new RedisString(bytes);
    string.incr(region, null);
    assertThat(string.get()).isEqualTo(stringToBytes("11"));
  }

  @Test
  public void incrbyThrowsNumberFormatExceptionWhenNotALong() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(PartitionedRegion.class));
    var bytes = stringToBytes("10 1");
    var string = new RedisString(bytes);
    assertThatThrownBy(() -> string.incrby(region, null, 2L))
        .isInstanceOf(NumberFormatException.class);
  }

  @Test
  public void incrbyErrorsWhenValueOverflows() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(PartitionedRegion.class));
    var bytes = longToBytes(Long.MAX_VALUE);
    var string = new RedisString(bytes);
    assertThatThrownBy(() -> string.incrby(region, null, 2L))
        .isInstanceOf(ArithmeticException.class);
  }

  @Test
  public void incrbyIncrementsValueByGivenLong() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(PartitionedRegion.class));
    var bytes = stringToBytes("10");
    var string = new RedisString(bytes);
    string.incrby(region, null, 2L);
    assertThat(string.get()).isEqualTo(stringToBytes("12"));
  }

  @Test
  public void incrbyfloatThrowsArithmeticErrorWhenNotADouble() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(PartitionedRegion.class));
    var bytes = stringToBytes("10 1");
    var string = new RedisString(bytes);
    assertThatThrownBy(() -> string.incrbyfloat(region, null, new BigDecimal("1.1")))
        .isInstanceOf(NumberFormatException.class);
  }

  @Test
  public void incrbyfloatIncrementsValueByGivenFloat() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(PartitionedRegion.class));
    var bytes = stringToBytes("10");
    var string = new RedisString(bytes);
    string.incrbyfloat(region, null, new BigDecimal("2.20"));
    assertThat(string.get()).isEqualTo(stringToBytes("12.20"));
  }

  @Test
  public void decrThrowsNumberFormatExceptionWhenNotALong() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(PartitionedRegion.class));
    byte[] bytes = {0};
    var string = new RedisString(bytes);
    assertThatThrownBy(() -> string.decr(region, null)).isInstanceOf(NumberFormatException.class);
  }

  @Test
  public void decrThrowsArithmeticExceptionWhenDecrementingMin() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(PartitionedRegion.class));
    var bytes = longToBytes(Long.MIN_VALUE);
    var string = new RedisString(bytes);
    assertThatThrownBy(() -> string.decr(region, null)).isInstanceOf(ArithmeticException.class);
  }

  @Test
  public void decrDecrementsValue() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(PartitionedRegion.class));
    var bytes = stringToBytes("10");
    var string = new RedisString(bytes);
    string.decr(region, null);
    assertThat(string.get()).isEqualTo(stringToBytes("9"));
  }

  @Test
  public void decrbyThrowsNumberFormatExceptionWhenNotALong() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(PartitionedRegion.class));
    byte[] bytes = {1};
    var string = new RedisString(bytes);
    assertThatThrownBy(() -> string.decrby(region, null, 2))
        .isInstanceOf(NumberFormatException.class);
  }

  @Test
  public void decrbyThrowsArithmeticExceptionWhenDecrementingMin() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(PartitionedRegion.class));
    var bytes = longToBytes(Long.MIN_VALUE);
    var string = new RedisString(bytes);
    assertThatThrownBy(() -> string.decrby(region, null, 2))
        .isInstanceOf(ArithmeticException.class);
  }

  @Test
  public void decrbyDecrementsValue() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(PartitionedRegion.class));
    var bytes = stringToBytes("10");
    var string = new RedisString(bytes);
    string.decrby(region, null, 2);
    assertThat(string.get()).isEqualTo(stringToBytes("8"));
  }

  @Test
  public void strlenReturnsStringLength() {
    byte[] bytes = {1, 2, 3, 4};
    var string = new RedisString(bytes);
    assertThat(string.strlen()).isEqualTo(bytes.length);
  }

  @Test
  public void strlenReturnsLengthOfEmptyString() {
    var string = new RedisString(new byte[] {});
    assertThat(string.strlen()).isEqualTo(0);
  }

  @Test
  public void equals_returnsFalse_givenDifferentExpirationTimes() {
    byte[] bytes = {0, 1, 2, 3};
    var stringOne = new RedisString(bytes);
    stringOne.setExpirationTimestampNoDelta(1000);
    var stringTwo = new RedisString(bytes);
    stringTwo.setExpirationTimestampNoDelta(999);
    assertThat(stringOne).isNotEqualTo(stringTwo);
  }

  @Test
  public void equals_returnsFalse_givenDifferentValueBytes() {
    var expirationTimestamp = 1000;
    var stringOne = new RedisString(new byte[] {0, 1, 2, 3});
    stringOne.setExpirationTimestampNoDelta(expirationTimestamp);
    var stringTwo = new RedisString(new byte[] {0, 1, 2, 2});
    stringTwo.setExpirationTimestampNoDelta(expirationTimestamp);
    assertThat(stringOne).isNotEqualTo(stringTwo);
  }

  @Test
  public void equals_returnsTrue_givenEqualValueBytesAndExpiration() {
    byte[] bytes = {0, 1, 2, 3};
    var expirationTimestamp = 1000;
    var stringOne = new RedisString(bytes);
    stringOne.setExpirationTimestampNoDelta(expirationTimestamp);
    var stringTwo = new RedisString(bytes);
    stringTwo.setExpirationTimestampNoDelta(expirationTimestamp);
    assertThat(stringOne).isEqualTo(stringTwo);
  }

  @Test
  public void setExpirationTimestamp_stores_delta_that_is_stable() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(PartitionedRegion.class));
    final byte[] bytes = {0, 1};
    when(region.put(any(), any()))
        .thenAnswer(invocation -> validateDeltaSerialization(bytes, invocation));
    var stringOne = new RedisString(bytes);

    stringOne.setExpirationTimestamp(region, null, 999);

    verify(region).put(any(), any());
    assertThat(stringOne.hasDelta()).isFalse();
  }

  private Object validateDeltaSerialization(byte[] bytes, InvocationOnMock invocation)
      throws IOException {
    var value = invocation.getArgument(1, RedisString.class);
    assertThat(value.hasDelta()).isTrue();
    var out = new HeapDataOutputStream(100);
    value.toDelta(out);
    var in = new ByteArrayDataInput(out.toByteArray());
    var stringTwo = new RedisString(bytes);
    assertThat(stringTwo).isNotEqualTo(value);
    stringTwo.fromDelta(in);
    assertThat(stringTwo).isEqualTo(value);
    return null;
  }

  @Test
  public void bitposReturnsNegativeOneWhenBitIsNotZeroOrOne() {
    var string = new RedisString(new byte[] {0, 1});
    assertThat(string.bitpos(2, 0, 1)).isEqualTo(-1);
  }

  /************* Size in Bytes Tests *************/
  /******* constructors *******/
  @Test
  public void should_calculateSize_equalToROSSize_ofLargeStrings() {
    var javaString = makeStringOfSpecifiedSize(10_000);
    var string = new RedisString(stringToBytes(javaString));

    var actual = string.getSizeInBytes();
    var expected = reflectionObjectSizer.sizeof(string);

    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void should_calculateSize_equalToROSSize_ofStringOfVariousSizes() {
    String javaString;
    for (var i = 0; i < 512; i += 8) {
      javaString = makeStringOfSpecifiedSize(i);
      var string = new RedisString(stringToBytes(javaString));

      var expected = reflectionObjectSizer.sizeof(string);
      var actual = string.getSizeInBytes();

      assertThat(actual).isEqualTo(expected);
    }
  }

  /******* changing values *******/
  @Test
  public void changingStringValue_toShorterString_shouldDecreaseSizeInBytes() {
    var baseString = "baseString";
    var stringToRemove = "asdf1234567890";
    var string = new RedisString(stringToBytes((baseString + stringToRemove)));

    var initialSize = string.getSizeInBytes();
    assertThat(initialSize).isEqualTo(reflectionObjectSizer.sizeof(string));

    string.set(stringToBytes(baseString));

    var finalSize = string.getSizeInBytes();
    assertThat(finalSize).isEqualTo(reflectionObjectSizer.sizeof(string));

    assertThat(finalSize).isLessThan(initialSize);
  }

  @Test
  public void changingStringValue_toLongerString_shouldIncreaseSizeInBytes() {
    var baseString = "baseString";
    var string = new RedisString(stringToBytes(baseString));

    var initialSize = string.getSizeInBytes();
    assertThat(initialSize).isEqualTo(reflectionObjectSizer.sizeof(string));

    var addedString = "asdf1234567890";
    string.set(stringToBytes((baseString + addedString)));

    var finalSize = string.getSizeInBytes();
    assertThat(finalSize).isEqualTo(reflectionObjectSizer.sizeof(string));

    assertThat(finalSize).isGreaterThan(initialSize);
  }

  @Test
  public void changingStringValue_toEmptyString_shouldDecreaseSizeInBytes() {
    var baseString = "baseString1234567890";
    final var emptySize = reflectionObjectSizer.sizeof(new RedisString(stringToBytes("")));
    var string = new RedisString(stringToBytes((baseString)));
    var baseSize = string.getSizeInBytes();

    string.set(stringToBytes(""));

    var finalSize = string.getSizeInBytes();

    assertThat(finalSize).isEqualTo(emptySize);
    assertThat(finalSize).isLessThan(baseSize);
  }

  /******* helper methods *******/

  private String makeStringOfSpecifiedSize(final int stringSize) {
    return StringUtils.repeat("a", stringSize);
  }
}
