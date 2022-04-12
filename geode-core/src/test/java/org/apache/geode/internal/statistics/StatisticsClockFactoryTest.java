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
package org.apache.geode.internal.statistics;

import static org.apache.geode.internal.statistics.StatisticsClockFactory.ENABLE_CLOCK_STATS_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

public class StatisticsClockFactoryTest {

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Test
  public void clock_createsEnabledClockIfPropertyIsTrue() {
    System.setProperty(ENABLE_CLOCK_STATS_PROPERTY, "true");

    var clock = StatisticsClockFactory.clock();

    assertThat(clock.isEnabled()).isTrue();
  }

  @Test
  public void clock_createsDisabledClockIfPropertyIsFalse() {
    System.setProperty(ENABLE_CLOCK_STATS_PROPERTY, "false");

    var clock = StatisticsClockFactory.clock();

    assertThat(clock.isEnabled()).isFalse();
  }

  @Test
  public void clock_boolean_createsEnabledClockIfParameterIsTrue() {
    var clock = StatisticsClockFactory.clock(true);

    assertThat(clock.isEnabled()).isTrue();
  }

  @Test
  public void clock_boolean_createsEnabledClockIfParameterIsFalse() {
    var clock = StatisticsClockFactory.clock(false);

    assertThat(clock.isEnabled()).isFalse();
  }

  @Test
  public void enabledClock_usesProvidedLongSupplierForGetTime() {
    var clock = StatisticsClockFactory.enabledClock(() -> 42);

    assertThat(clock.getTime()).isEqualTo(42);
  }

  @Test
  public void enabledClock_createsEnabledClock() {
    var clock = StatisticsClockFactory.enabledClock(() -> 24);

    assertThat(clock.isEnabled()).isTrue();
  }

  @Test
  public void disabledClock_usesZeroForGetTime() {
    var clock = StatisticsClockFactory.disabledClock();

    assertThat(clock.getTime()).isZero();
  }

  @Test
  public void disabledClock_createsDisabledClock() {
    var clock = StatisticsClockFactory.disabledClock();

    assertThat(clock.isEnabled()).isFalse();
  }

  @Test
  public void clock_usesProvidedLongSupplierForGetTime() {
    var clock = StatisticsClockFactory.clock(() -> 100, () -> true);

    assertThat(clock.getTime()).isEqualTo(100);
  }

  @Test
  public void clock_usesProvidedBooleanSupplierForIsEnabled() {
    var clock = StatisticsClockFactory.clock(() -> 100, () -> true);

    assertThat(clock.isEnabled()).isTrue();
  }

  @Test
  public void clock_getTime_delegatesToLongSupplier() {
    var time = mock(LongSupplier.class);
    var clock = StatisticsClockFactory.clock(time, mock(BooleanSupplier.class));

    clock.getTime();

    verify(time).getAsLong();
  }

  @Test
  public void clock_isEnabled_delegatesToBooleanSupplier() {
    var isEnabled = mock(BooleanSupplier.class);
    var clock = StatisticsClockFactory.clock(mock(LongSupplier.class), isEnabled);

    clock.isEnabled();

    verify(isEnabled).getAsBoolean();
  }
}
