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
package org.apache.geode.internal.cache.execute.metrics;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.LENIENT;

import java.util.stream.Stream;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;

public class FunctionStatsManagerTest {

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(LENIENT);

  @Mock
  private StatisticsFactory statisticsFactory;

  @Mock
  private Statistics statistics;

  private MeterRegistry meterRegistry;

  @Before
  public void setUp() {
    when(statisticsFactory.createAtomicStatistics(any(), any()))
        .thenReturn(statistics);

    meterRegistry = new SimpleMeterRegistry();
  }

  @Test
  public void constructor_createsFunctionServiceStats() {
    var functionStatsManager = new FunctionStatsManager(false, statisticsFactory,
        () -> meterRegistry);

    var functionServiceStats = functionStatsManager.getFunctionServiceStats();

    assertThat(functionServiceStats)
        .isNotNull();
  }

  @Test
  public void getFunctionStatsByName_returnsDummyFunctionStats_ifStatsDisabled_andMeterRegistryIsNull() {
    var functionStatsManager = new FunctionStatsManager(true, statisticsFactory,
        () -> null);

    var functionStats = functionStatsManager.getFunctionStatsByName("foo");

    var dummyFunctionStats = functionStatsManager.getDummyFunctionStats();
    assertThat(functionStats)
        .isSameAs(dummyFunctionStats);
  }

  @Test
  public void getFunctionStatsByName_usesDummyStatistics_ifStatsDisabled() {
    var functionStatsManager = new FunctionStatsManager(true, statisticsFactory,
        () -> meterRegistry);

    var functionStats = functionStatsManager.getFunctionStatsByName("foo");

    var dummyStatistics = functionStatsManager.getDummyStatistics();
    assertThat(functionStats.getStatistics())
        .isSameAs(dummyStatistics);
  }

  @Test
  public void getFunctionStatsByName_usesStatisticsFromFactory_ifStatsEnabled_andMeterRegistrySupplied() {
    var functionStatsManager = new FunctionStatsManager(false, statisticsFactory,
        () -> meterRegistry);
    var statisticsFromFactory = mock(Statistics.class);
    when(statisticsFactory.createAtomicStatistics(any(), any()))
        .thenReturn(statisticsFromFactory);

    var functionStats = functionStatsManager.getFunctionStatsByName("foo");

    assertThat(functionStats.getStatistics())
        .isSameAs(statisticsFromFactory);
  }

  @Test
  public void getFunctionStatsByName_usesStatisticsFromFactory_ifStatsEnabled_andMeterRegistryIsNull() {
    var functionStatsManager = new FunctionStatsManager(false, statisticsFactory,
        () -> null);
    var statisticsFromFactory = mock(Statistics.class);
    when(statisticsFactory.createAtomicStatistics(any(), any()))
        .thenReturn(statisticsFromFactory);

    var functionStats = functionStatsManager.getFunctionStatsByName("foo");

    assertThat(functionStats.getStatistics())
        .isSameAs(statisticsFromFactory);
  }

  @Test
  public void getFunctionStatsByName_usesNoopMeterRegistry_ifMeterRegistryIsNull() {
    var functionStatsManager = new FunctionStatsManager(false, statisticsFactory,
        () -> null);

    var functionStats = functionStatsManager.getFunctionStatsByName("foo");

    assertThat(functionStats.getMeterRegistry())
        .isSameAs(functionStatsManager.getNoopMeterRegistry());
  }

  @Test
  public void getFunctionStatsByName_usesMeterRegistryFromSupplier_ifStatsDisabled_andMeterRegistrySupplied() {
    MeterRegistry meterRegistryFromSupplier = new SimpleMeterRegistry();
    var functionStatsManager = new FunctionStatsManager(true, statisticsFactory,
        () -> meterRegistryFromSupplier);

    var functionStats = functionStatsManager.getFunctionStatsByName("foo");

    assertThat(functionStats.getMeterRegistry())
        .isSameAs(meterRegistryFromSupplier);
  }

  @Test
  public void getFunctionStatsByName_usesMeterRegistryFromSupplier_ifStatsEnabled_andMeterRegistrySupplied() {
    MeterRegistry meterRegistryFromSupplier = new SimpleMeterRegistry();
    var functionStatsManager = new FunctionStatsManager(false, statisticsFactory,
        () -> meterRegistryFromSupplier);

    var functionStats = functionStatsManager.getFunctionStatsByName("foo");

    assertThat(functionStats.getMeterRegistry())
        .isSameAs(meterRegistryFromSupplier);
  }

  @Test
  public void getFunctionStatsByName_returnsSameInstanceForGivenName() {
    var functionStatsManager = new FunctionStatsManager(false, statisticsFactory,
        () -> meterRegistry);

    var first = functionStatsManager.getFunctionStatsByName("foo");
    var second = functionStatsManager.getFunctionStatsByName("foo");

    assertThat(second)
        .isSameAs(first);
  }

  @Test
  public void close_closesAllCreatedFunctionStats() {
    var functionStatsManager = new FunctionStatsManager(false, statisticsFactory,
        () -> meterRegistry);

    var functionStats = Stream.of("a", "b", "c")
        .map(functionStatsManager::getFunctionStatsByName)
        .collect(toList());

    functionStatsManager.close();

    assertThat(functionStats)
        .allMatch(FunctionStats::isClosed, "Function stats is closed");
  }

  @Test
  public void close_closesFunctionServiceStats() {
    var functionServiceStats = mock(FunctionServiceStats.class);
    var functionStatsManager = new FunctionStatsManager(false, statisticsFactory,
        functionServiceStats, () -> meterRegistry);

    functionStatsManager.close();

    verify(functionServiceStats)
        .close();
  }
}
