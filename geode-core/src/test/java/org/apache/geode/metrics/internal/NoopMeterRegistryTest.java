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
package org.apache.geode.metrics.internal;

import static io.micrometer.core.instrument.Meter.Type.COUNTER;
import static io.micrometer.core.instrument.Meter.Type.DISTRIBUTION_SUMMARY;
import static io.micrometer.core.instrument.Meter.Type.GAUGE;
import static io.micrometer.core.instrument.Meter.Type.LONG_TASK_TIMER;
import static io.micrometer.core.instrument.Meter.Type.OTHER;
import static io.micrometer.core.instrument.Meter.Type.TIMER;
import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Meter.Id;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.core.instrument.distribution.pause.NoPauseDetector;
import io.micrometer.core.instrument.noop.NoopCounter;
import io.micrometer.core.instrument.noop.NoopDistributionSummary;
import io.micrometer.core.instrument.noop.NoopFunctionCounter;
import io.micrometer.core.instrument.noop.NoopFunctionTimer;
import io.micrometer.core.instrument.noop.NoopGauge;
import io.micrometer.core.instrument.noop.NoopLongTaskTimer;
import io.micrometer.core.instrument.noop.NoopMeter;
import io.micrometer.core.instrument.noop.NoopTimer;
import org.junit.Before;
import org.junit.Test;

public class NoopMeterRegistryTest {

  private NoopMeterRegistry noopMeterRegistry;

  @Before
  public void setUp() {
    noopMeterRegistry = new NoopMeterRegistry();
  }

  @Test
  public void constructor_usesNoopClock() {
    assertThat(noopMeterRegistry.config().clock())
        .isSameAs(NoopMeterRegistry.NOOP_CLOCK);
  }

  @Test
  public void newGauge_createsNoopGauge() {
    var expectedId = new Id("name", Tags.of("key", "value"), null, null, GAUGE);

    var result = noopMeterRegistry.newGauge(expectedId, null, x -> 0.0);

    assertThat(result)
        .isInstanceOf(NoopGauge.class);

    assertThat(result.getId())
        .isEqualTo(expectedId);
  }

  @Test
  public void newCounter_createsNoopCounter() {
    var expectedId = new Id("name", Tags.of("key", "value"), null, null, COUNTER);

    var result = noopMeterRegistry.newCounter(expectedId);

    assertThat(result)
        .isInstanceOf(NoopCounter.class);

    assertThat(result.getId())
        .isEqualTo(expectedId);
  }

  @Test
  public void newLongTaskTimer_createsNoopLongTaskTimer() {
    var expectedId = new Id("name", Tags.of("key", "value"), null, null, LONG_TASK_TIMER);

    var result = noopMeterRegistry.newLongTaskTimer(expectedId);

    assertThat(result)
        .isInstanceOf(NoopLongTaskTimer.class);

    assertThat(result.getId())
        .isEqualTo(expectedId);
  }

  @Test
  public void newTimer_createsNoopTimer() {
    var expectedId = new Id("name", Tags.of("key", "value"), null, null, TIMER);

    var result = noopMeterRegistry.newTimer(expectedId, DistributionStatisticConfig.NONE,
        new NoPauseDetector());

    assertThat(result)
        .isInstanceOf(NoopTimer.class);

    assertThat(result.getId())
        .isEqualTo(expectedId);
  }

  @Test
  public void newDistributionSummary_createsNoopDistributionSummary() {
    var expectedId = new Id("name", Tags.of("key", "value"), null, null, DISTRIBUTION_SUMMARY);

    var result = noopMeterRegistry.newDistributionSummary(expectedId,
        DistributionStatisticConfig.NONE, 0.0);

    assertThat(result)
        .isInstanceOf(NoopDistributionSummary.class);

    assertThat(result.getId())
        .isEqualTo(expectedId);
  }

  @Test
  public void newMeter_createsNoopMeter() {
    var expectedId = new Id("name", Tags.of("key", "value"), null, null, OTHER);

    var result = noopMeterRegistry.newMeter(expectedId, OTHER, emptyList());

    assertThat(result)
        .isInstanceOf(NoopMeter.class);

    assertThat(result.getId())
        .isEqualTo(expectedId);
  }

  @Test
  public void newFunctionTimer_createsNoopFunctionTimer() {
    var expectedId = new Id("name", Tags.of("key", "value"), null, null, TIMER);

    var result =
        noopMeterRegistry.newFunctionTimer(expectedId, new Object(), x -> 0L, x -> 0.0, SECONDS);

    assertThat(result)
        .isInstanceOf(NoopFunctionTimer.class);

    assertThat(result.getId())
        .isEqualTo(expectedId);
  }

  @Test
  public void newFunctionCounter_createsNoopFunctionCounter() {
    var expectedId = new Id("name", Tags.of("key", "value"), null, null, COUNTER);

    var result =
        noopMeterRegistry.newFunctionCounter(expectedId, new Object(), x -> 0.0);

    assertThat(result)
        .isInstanceOf(NoopFunctionCounter.class);

    assertThat(result.getId())
        .isEqualTo(expectedId);
  }

  @Test
  public void getBaseTimeUnit_returnsSeconds() {
    var result = noopMeterRegistry.getBaseTimeUnit();

    assertThat(result)
        .isEqualTo(SECONDS);
  }

  @Test
  public void defaultHistogramConfig_returnsNone() {
    var result = noopMeterRegistry.defaultHistogramConfig();

    assertThat(result)
        .isEqualTo(DistributionStatisticConfig.NONE);
  }
}
