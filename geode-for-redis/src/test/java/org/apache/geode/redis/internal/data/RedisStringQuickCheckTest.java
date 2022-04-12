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

import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Arrays;

import com.google.common.primitives.Bytes;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.generator.InRange;
import com.pholser.junit.quickcheck.generator.Size;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.PartitionedRegion;

/**
 * Property testing of RedisString.
 */
@RunWith(JUnitQuickcheck.class)
public class RedisStringQuickCheckTest {

  @Property
  public void setrangePrefixSuffix(@Size(min = 10, max = 50) ArrayList<Byte> existingByteArray,
      @Size(min = 0, max = 10) ArrayList<Byte> valueToAddArray,
      @InRange(minInt = 0, maxInt = 60) int offset) {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(PartitionedRegion.class));
    var existingBytes = Bytes.toArray(existingByteArray);
    var valueToAdd = Bytes.toArray(valueToAddArray);

    var redisString = new RedisString(existingBytes);
    redisString.setrange(region, null, offset, valueToAdd);

    var newBytes = redisString.getValue();

    // if valueToAdd is empty, then original string is unmodified
    if (valueToAdd.length == 0) {
      assertThat(existingBytes).isEqualTo(newBytes);
      return;
    }

    // length property
    var newLength = Math.max(existingBytes.length, offset + valueToAdd.length);
    assertThat(newBytes.length).isEqualTo(newLength);

    // prefix property
    if (offset > 0) {
      // Note: copyOf method truncates or pads with zeros as needed
      // to make array of specified length
      var prefix = Arrays.copyOf(existingBytes, offset);
      assertThat(newBytes).startsWith(prefix);
    }

    // set value property
    var setValue = Arrays.copyOfRange(newBytes, offset, offset + valueToAdd.length);
    assertThat(setValue).isEqualTo(valueToAdd);

    // suffix property
    var actualSuffix = Arrays.copyOfRange(newBytes, offset + valueToAdd.length, newBytes.length);
    var expectedSuffix = offset + setValue.length > existingBytes.length ? new byte[0]
        : Arrays.copyOfRange(existingBytes, offset + setValue.length, existingBytes.length);

    assertThat(actualSuffix).isEqualTo(expectedSuffix);
  }
}
