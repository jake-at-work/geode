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
package org.apache.geode.cache.query.internal.aggregate;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Random;

import org.junit.Test;

public class AbstractAggregatorTest {
  private final Random random = new Random();

  @Test
  public void downcastShouldReturnIntegerWhenValueIsAnIntegerAndFitsWithinTheIntegerRange() {
    var randomInteger = random.nextInt();
    var integerResult = AbstractAggregator.downCast(randomInteger);

    assertThat(integerResult).isInstanceOf(Number.class);
    assertThat(integerResult).isInstanceOf(Integer.class);
  }

  @Test
  public void downcastShouldReturnLongWhenValueIsAnIntegerButDoesNotFitsWithinTheIntegerRange() {
    var randomLong = random.nextLong();
    var longResult = AbstractAggregator.downCast(randomLong);

    assertThat(longResult).isInstanceOf(Number.class);
    assertThat(longResult).isInstanceOf(Long.class);
  }

  @Test
  public void downcastShouldReturnFloatWhenValueIsNotAnIntegerAndFitsWithinTheFloatRange() {
    var randomFloat = random.nextFloat();
    var floatResult = AbstractAggregator.downCast(randomFloat);

    assertThat(floatResult).isInstanceOf(Number.class);
    assertThat(floatResult).isInstanceOf(Float.class);
  }

  @Test
  public void downcastShouldReturnDoubleWhenValueIsNotAnIntegerAndDoesNotFitsWithinTheFloatRange() {
    var randomDouble = Float.MIN_VALUE - random.nextDouble();
    var doubleResult = AbstractAggregator.downCast(randomDouble);

    assertThat(doubleResult).isInstanceOf(Number.class);
    assertThat(doubleResult).isInstanceOf(Double.class);
  }
}
