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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.IntSupplier;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;
import org.apache.geode.internal.statistics.StatisticsRegistry.AtomicStatisticsFactory;
import org.apache.geode.internal.statistics.StatisticsRegistry.OsStatisticsFactory;

/**
 * Unit tests for {@link StatisticsRegistry}.
 */
public class StatisticsRegistryTest {

  // Arbitrary values for factory method parameters
  private static final String DESCRIPTOR_NAME = "a-descriptor-name";
  private static final String DESCRIPTOR_DESCRIPTION = "a-descriptor-description";
  private static final String DESCRIPTOR_UNITS = "a-descriptor-units";
  private static final String REGISTRY_NAME = "a-registry-name";
  private static final int REGISTRY_START_TIME = 239847;
  private static final String STATISTICS_TEXT_ID = "a-text-id";
  private static final long STATISTICS_NUMERIC_ID = 9876;
  private static final int STATISTICS_OS_FLAGS = 54321;
  private static final String TYPE_NAME = "a-type-name";
  private static final String TYPE_DESCRIPTION = "a-type-description";
  private static final StatisticDescriptor[] TYPE_DESCRIPTORS = {
      mock(StatisticDescriptor.class),
      mock(StatisticDescriptor.class),
      mock(StatisticDescriptor.class)
  };

  @Mock
  private StatisticsTypeFactory typeFactory;

  @Mock
  private StatisticsType type;

  @Mock
  private AtomicStatisticsFactory atomicStatisticsFactory;

  @Mock
  private OsStatisticsFactory osStatisticsFactory;

  @Mock
  private IntSupplier pidSupplier;

  private StatisticsRegistry registry;

  @Before
  public void setup() {
    initMocks(this);
    registry = new StatisticsRegistry(REGISTRY_NAME, 0, typeFactory, osStatisticsFactory,
        atomicStatisticsFactory, pidSupplier);
  }

  @Test
  public void remembersItsName() {
    var theName = "the-name";

    var registry = new StatisticsRegistry(theName, REGISTRY_START_TIME);

    assertThat(registry.getName())
        .isEqualTo(theName);
  }

  @Test
  public void remembersItsStartTime() {
    var theStartTime = 374647;

    var registry = new StatisticsRegistry(REGISTRY_NAME, theStartTime);

    assertThat(registry.getStartTime())
        .isEqualTo(theStartTime);
  }

  @Test
  public void delegatesTypeCreationToTypeFactory() {
    var typeCreatedByFactory = mock(StatisticsType.class);

    when(typeFactory.createType(any(), any(), any()))
        .thenReturn(typeCreatedByFactory);

    var result = registry.createType(TYPE_NAME, TYPE_DESCRIPTION, TYPE_DESCRIPTORS);

    assertThat(result)
        .isSameAs(typeCreatedByFactory);
  }

  @Test
  public void delegatesTypeCreationFromXmlToTypeFactory() throws IOException {
    Reader reader = new StringReader("<arbitrary-xml/>");
    StatisticsType[] typesCreatedByFactory = {};

    when(typeFactory.createTypesFromXml(any()))
        .thenReturn(typesCreatedByFactory);

    var result = registry.createTypesFromXml(reader);

    assertThat(result)
        .isSameAs(typesCreatedByFactory);
  }

  @Test
  public void delegatesTypeLookupToTypeFactory() {
    var typeFoundByFactory = mock(StatisticsType.class);

    when(typeFactory.findType(any()))
        .thenReturn(typeFoundByFactory);

    var result = registry.findType(TYPE_NAME);

    assertThat(result)
        .isSameAs(typeFoundByFactory);
  }

  @Test
  public void delegatesCreateIntCounterToTypeFactory() {
    var descriptorCreatedByFactory = mock(StatisticDescriptor.class);
    when(typeFactory.createIntCounter(any(), any(), any()))
        .thenReturn(descriptorCreatedByFactory);

    var result =
        registry.createIntCounter(DESCRIPTOR_NAME, DESCRIPTOR_DESCRIPTION, DESCRIPTOR_UNITS);

    assertThat(result)
        .isEqualTo(descriptorCreatedByFactory);
  }

  @Test
  public void delegatesCreateLongCounterToTypeFactory() {
    var descriptorCreatedByFactory = mock(StatisticDescriptor.class);
    when(typeFactory.createLongCounter(any(), any(), any()))
        .thenReturn(descriptorCreatedByFactory);

    var result =
        registry.createLongCounter(DESCRIPTOR_NAME, DESCRIPTOR_DESCRIPTION, DESCRIPTOR_UNITS);

    assertThat(result)
        .isEqualTo(descriptorCreatedByFactory);
  }

  @Test
  public void delegatesCreateDoubleCounterToTypeFactory() {
    var descriptorCreatedByFactory = mock(StatisticDescriptor.class);
    when(typeFactory.createDoubleCounter(any(), any(), any()))
        .thenReturn(descriptorCreatedByFactory);

    var result =
        registry.createDoubleCounter(DESCRIPTOR_NAME, DESCRIPTOR_DESCRIPTION, DESCRIPTOR_UNITS);

    assertThat(result)
        .isEqualTo(descriptorCreatedByFactory);
  }

  @Test
  public void delegatesCreateIntGaugeToTypeFactory() {
    var descriptorCreatedByFactory = mock(StatisticDescriptor.class);
    when(typeFactory.createIntGauge(any(), any(), any()))
        .thenReturn(descriptorCreatedByFactory);

    var result =
        registry.createIntGauge(DESCRIPTOR_NAME, DESCRIPTOR_DESCRIPTION, DESCRIPTOR_UNITS);

    assertThat(result)
        .isEqualTo(descriptorCreatedByFactory);
  }

  @Test
  public void delegatesCreateLongGaugeToTypeFactory() {
    var descriptorCreatedByFactory = mock(StatisticDescriptor.class);
    when(typeFactory.createLongGauge(any(), any(), any()))
        .thenReturn(descriptorCreatedByFactory);

    var result =
        registry.createLongGauge(DESCRIPTOR_NAME, DESCRIPTOR_DESCRIPTION, DESCRIPTOR_UNITS);

    assertThat(result)
        .isEqualTo(descriptorCreatedByFactory);
  }

  @Test
  public void delegatesCreateDoubleGaugeToTypeFactory() {
    var descriptorCreatedByFactory = mock(StatisticDescriptor.class);
    when(typeFactory.createDoubleGauge(any(), any(), any()))
        .thenReturn(descriptorCreatedByFactory);

    var result =
        registry.createDoubleGauge(DESCRIPTOR_NAME, DESCRIPTOR_DESCRIPTION, DESCRIPTOR_UNITS);

    assertThat(result)
        .isEqualTo(descriptorCreatedByFactory);
  }

  @Test
  public void delegatesCreateLargerBetterIntCounterToTypeFactory() {
    var descriptorCreatedByFactory = mock(StatisticDescriptor.class);
    when(typeFactory.createIntCounter(any(), any(), any(), anyBoolean()))
        .thenReturn(descriptorCreatedByFactory);

    var result =
        registry.createIntCounter(DESCRIPTOR_NAME, DESCRIPTOR_DESCRIPTION, DESCRIPTOR_UNITS, true);

    assertThat(result)
        .isEqualTo(descriptorCreatedByFactory);
  }

  @Test
  public void delegatesCreateLargerBetterLongCounterToTypeFactory() {
    var descriptorCreatedByFactory = mock(StatisticDescriptor.class);
    when(typeFactory.createLongCounter(any(), any(), any(), anyBoolean()))
        .thenReturn(descriptorCreatedByFactory);

    var result =
        registry
            .createLongCounter(DESCRIPTOR_NAME, DESCRIPTOR_DESCRIPTION, DESCRIPTOR_UNITS, false);

    assertThat(result)
        .isEqualTo(descriptorCreatedByFactory);
  }

  @Test
  public void delegatesCreateLargerBetterDoubleCounterToTypeFactory() {
    var descriptorCreatedByFactory = mock(StatisticDescriptor.class);
    when(typeFactory.createDoubleCounter(any(), any(), any(), anyBoolean()))
        .thenReturn(descriptorCreatedByFactory);

    var result =
        registry
            .createDoubleCounter(DESCRIPTOR_NAME, DESCRIPTOR_DESCRIPTION, DESCRIPTOR_UNITS, true);

    assertThat(result)
        .isEqualTo(descriptorCreatedByFactory);
  }

  @Test
  public void delegatesCreateLargerBetterIntGaugeToTypeFactory() {
    var descriptorCreatedByFactory = mock(StatisticDescriptor.class);
    when(typeFactory.createIntGauge(any(), any(), any(), anyBoolean()))
        .thenReturn(descriptorCreatedByFactory);

    var result =
        registry.createIntGauge(DESCRIPTOR_NAME, DESCRIPTOR_DESCRIPTION, DESCRIPTOR_UNITS, false);

    assertThat(result)
        .isEqualTo(descriptorCreatedByFactory);
  }

  @Test
  public void delegatesCreateLargerBetterLongGaugeToTypeFactory() {
    var descriptorCreatedByFactory = mock(StatisticDescriptor.class);
    when(typeFactory.createLongGauge(any(), any(), any(), anyBoolean()))
        .thenReturn(descriptorCreatedByFactory);

    var result =
        registry.createLongGauge(DESCRIPTOR_NAME, DESCRIPTOR_DESCRIPTION, DESCRIPTOR_UNITS, true);

    assertThat(result)
        .isEqualTo(descriptorCreatedByFactory);
  }

  @Test
  public void delegatesCreateLargerBetterDoubleGaugeToTypeFactory() {
    var descriptorCreatedByFactory = mock(StatisticDescriptor.class);
    when(typeFactory.createDoubleGauge(any(), any(), any(), anyBoolean()))
        .thenReturn(descriptorCreatedByFactory);

    var result =
        registry
            .createDoubleGauge(DESCRIPTOR_NAME, DESCRIPTOR_DESCRIPTION, DESCRIPTOR_UNITS, false);

    assertThat(result)
        .isEqualTo(descriptorCreatedByFactory);
  }

  @Test
  public void createsOsStatisticsViaFactory() {
    var statisticsCreatedByFactory = mock(Statistics.class);

    when(osStatisticsFactory.create(any(), any(), anyLong(), anyLong(), anyInt(), any()))
        .thenReturn(statisticsCreatedByFactory);

    var result = registry.createOsStatistics(type, STATISTICS_TEXT_ID, STATISTICS_NUMERIC_ID,
        STATISTICS_OS_FLAGS);

    assertThat(result)
        .isSameAs(statisticsCreatedByFactory);
  }

  @Test
  public void createsAtomicStatisticsViaFactory() {
    var statisticsCreatedByFactory = mock(Statistics.class);

    when(atomicStatisticsFactory.create(any(), any(), anyLong(), anyLong(), any()))
        .thenReturn(statisticsCreatedByFactory);

    var result = registry.createAtomicStatistics(type, STATISTICS_TEXT_ID,
        STATISTICS_NUMERIC_ID);

    assertThat(result)
        .isSameAs(statisticsCreatedByFactory);
  }

  @Test
  public void incrementsUniqueIdForEachCreatedStatistics() {
    registry.createOsStatistics(type, STATISTICS_TEXT_ID, 0, 0);
    verify(osStatisticsFactory).create(type, STATISTICS_TEXT_ID, 0L, 1, 0, registry);

    registry.createOsStatistics(type, STATISTICS_TEXT_ID, 0, 0);
    verify(osStatisticsFactory).create(type, STATISTICS_TEXT_ID, 0, 2, 0, registry);

    registry.createOsStatistics(type, STATISTICS_TEXT_ID, 0, 0);
    verify(osStatisticsFactory).create(type, STATISTICS_TEXT_ID, 0, 3, 0, registry);

    registry.createAtomicStatistics(type, STATISTICS_TEXT_ID, 0);
    verify(atomicStatisticsFactory).create(type, STATISTICS_TEXT_ID, 0, 4, registry);

    registry.createOsStatistics(type, STATISTICS_TEXT_ID, 0, 0);
    verify(osStatisticsFactory).create(type, STATISTICS_TEXT_ID, 0, 5, 0, registry);

    registry.createAtomicStatistics(type, STATISTICS_TEXT_ID, 0);
    verify(atomicStatisticsFactory).create(type, STATISTICS_TEXT_ID, 0, 6, registry);

    registry.createAtomicStatistics(type, STATISTICS_TEXT_ID, 0);
    verify(atomicStatisticsFactory).create(type, STATISTICS_TEXT_ID, 0, 7, registry);
  }

  @Test
  public void remembersTheStatisticsItCreates() {
    var atomicStatistics1 = mock(Statistics.class, "atomic 1");
    var atomicStatistics2 = mock(Statistics.class, "atomic 1");
    var atomicStatistics3 = mock(Statistics.class, "atomic 1");
    var osStatistics1 = mock(Statistics.class, "os 1");
    var osStatistics2 = mock(Statistics.class, "os 1");
    var osStatistics3 = mock(Statistics.class, "os 1");

    when(osStatisticsFactory.create(any(), any(), anyLong(), anyLong(), anyInt(), any()))
        .thenReturn(osStatistics1)
        .thenReturn(osStatistics2)
        .thenReturn(osStatistics3);

    when(atomicStatisticsFactory.create(any(), any(), anyLong(), anyLong(), any()))
        .thenReturn(atomicStatistics1)
        .thenReturn(atomicStatistics2)
        .thenReturn(atomicStatistics3);

    registry.createAtomicStatistics(type, STATISTICS_TEXT_ID, STATISTICS_NUMERIC_ID);
    registry.createOsStatistics(type, STATISTICS_TEXT_ID, STATISTICS_NUMERIC_ID,
        STATISTICS_OS_FLAGS);
    registry.createOsStatistics(type, STATISTICS_TEXT_ID, STATISTICS_NUMERIC_ID,
        STATISTICS_OS_FLAGS);
    registry.createAtomicStatistics(type, STATISTICS_TEXT_ID, STATISTICS_NUMERIC_ID);
    registry.createOsStatistics(type, STATISTICS_TEXT_ID, STATISTICS_NUMERIC_ID,
        STATISTICS_OS_FLAGS);
    registry.createAtomicStatistics(type, STATISTICS_TEXT_ID, STATISTICS_NUMERIC_ID);

    assertThat(registry.getStatsList())
        .containsExactlyInAnyOrder(
            atomicStatistics1,
            atomicStatistics2,
            atomicStatistics3,
            osStatistics1,
            osStatistics2,
            osStatistics3);
  }

  @Test
  public void forgetsTheStatisticsItDestroys() {
    var osStatistics1 = mock(Statistics.class, "os 1");
    var osStatistics2 = mock(Statistics.class, "os 2");
    var osStatistics3 = mock(Statistics.class, "os 3");
    when(osStatisticsFactory.create(any(), any(), anyLong(), anyLong(), anyInt(), any()))
        .thenReturn(osStatistics1)
        .thenReturn(osStatistics2)
        .thenReturn(osStatistics3);

    var atomicStatistics1 = mock(Statistics.class, "atomic 1");
    var atomicStatistics2 = mock(Statistics.class, "atomic 2");
    var atomicStatistics3 = mock(Statistics.class, "atomic 3");
    when(atomicStatisticsFactory.create(any(), any(), anyLong(), anyLong(), any()))
        .thenReturn(atomicStatistics1)
        .thenReturn(atomicStatistics2)
        .thenReturn(atomicStatistics3);

    registry.createAtomicStatistics(type, STATISTICS_TEXT_ID, STATISTICS_NUMERIC_ID);
    registry.createOsStatistics(type, STATISTICS_TEXT_ID, STATISTICS_NUMERIC_ID,
        STATISTICS_OS_FLAGS);
    registry.createOsStatistics(type, STATISTICS_TEXT_ID, STATISTICS_NUMERIC_ID,
        STATISTICS_OS_FLAGS);
    registry.createAtomicStatistics(type, STATISTICS_TEXT_ID, STATISTICS_NUMERIC_ID);
    registry.createOsStatistics(type, STATISTICS_TEXT_ID, STATISTICS_NUMERIC_ID,
        STATISTICS_OS_FLAGS);
    registry.createAtomicStatistics(type, STATISTICS_TEXT_ID, STATISTICS_NUMERIC_ID);

    registry.destroyStatistics(osStatistics2);
    registry.destroyStatistics(atomicStatistics1);

    assertThat(registry.getStatsList())
        .containsExactlyInAnyOrder(
            atomicStatistics2,
            atomicStatistics3,
            osStatistics1,
            osStatistics3);
  }

  @Test
  public void modificationCountStartsAtZero() {
    assertThat(registry.getStatListModCount())
        .isEqualTo(0);
  }

  @Test
  public void incrementsModificationCountOnEachCreationAndDestruction() {
    var osStatistics = mock(Statistics.class, "os");
    var atomicStatistics = mock(Statistics.class, "atomic");

    when(osStatisticsFactory.create(any(), any(), anyLong(), anyLong(), anyInt(), any()))
        .thenReturn(osStatistics);

    when(atomicStatisticsFactory.create(any(), any(), anyLong(), anyLong(), any()))
        .thenReturn(atomicStatistics);

    registry.createAtomicStatistics(type, STATISTICS_TEXT_ID, STATISTICS_NUMERIC_ID);
    assertThat(registry.getStatListModCount())
        .as("modification count after first modification")
        .isEqualTo(1);

    registry.createOsStatistics(type, STATISTICS_TEXT_ID, STATISTICS_NUMERIC_ID,
        STATISTICS_OS_FLAGS);
    assertThat(registry.getStatListModCount())
        .as("modification count after second modification")
        .isEqualTo(2);

    registry.destroyStatistics(osStatistics);
    assertThat(registry.getStatListModCount())
        .as("modification count after third modification")
        .isEqualTo(3);

    registry.destroyStatistics(atomicStatistics);
    assertThat(registry.getStatListModCount())
        .as("modification count after fourth modification")
        .isEqualTo(4);
  }

  @Test
  public void doesNotIncrementModificationCountWhenDestroyingUnknownStats() {
    // The stats were not created by the registry, and so are not known to the registry
    var unknownStatistics = mock(Statistics.class);

    registry.destroyStatistics(unknownStatistics);

    assertThat(registry.getStatListModCount())
        .isEqualTo(0);
  }

  @Test
  public void findStatisticsByUniqueId_returnsStatisticsThatMatchesUniqueId() {
    var soughtId = 44L;
    var matchingStatistics = statistics(withUniqueId(soughtId));

    givenExistingStatistics(matchingStatistics);

    assertThat(registry.findStatisticsByUniqueId(soughtId))
        .isSameAs(matchingStatistics);
  }

  @Test
  public void findStatisticsByUniqueId_returnsNullIfNoStatisticsMatchesUniqueId() {
    assertThat(registry.findStatisticsByUniqueId(0))
        .isNull();
  }

  @Test
  public void statisticsExists_returnsTrue_ifStatisticsMatchesUniqueId() {
    var soughtId = 44L;
    givenExistingStatistics(statistics(withUniqueId(soughtId)));

    assertThat(registry.statisticsExists(soughtId))
        .isTrue();
  }

  @Test
  public void statisticsExists_returnsFalse_ifNoStatisticsMatchesUniqueId() {
    assertThat(registry.statisticsExists(99L))
        .isFalse();
  }

  @Test
  public void findsStatisticsByNumericId_returnsAllStatisticsThatMatchNumericId() {
    var soughtId = 44L;
    var differentId = 45L;

    var matchingStatistics1 = statistics(withNumericId(soughtId));
    var matchingStatistics2 = statistics(withNumericId(soughtId));
    var mismatchingStatistics = statistics(withNumericId(differentId));

    givenExistingStatistics(
        matchingStatistics1,
        mismatchingStatistics,
        matchingStatistics2);

    var foundStatistics = registry.findStatisticsByNumericId(soughtId);

    assertThat(foundStatistics)
        .containsExactlyInAnyOrder(
            matchingStatistics1,
            matchingStatistics2);
  }

  @Test
  public void findStatisticsByNumericId_returnsEmptyArray_ifNoStatisticsMatchNumericId() {
    var soughtId = 44L;
    var differentId = 45L;

    givenExistingStatistics(
        statistics(withNumericId(differentId)),
        statistics(withNumericId(differentId)),
        statistics(withNumericId(differentId)));

    assertThat(registry.findStatisticsByNumericId(soughtId)).isEmpty();
  }

  @Test
  public void findStatisticsByTextId_returnsAllStatisticsThatMatchTextId() {
    var soughtId = "matching-id";
    var differentId = "mismatching-id";

    var matchingStatistics1 = statistics(withTextId(soughtId));
    var matchingStatistics2 = statistics(withTextId(soughtId));
    var mismatchingStatistics = statistics(withTextId(differentId));

    givenExistingStatistics(
        mismatchingStatistics,
        matchingStatistics1,
        matchingStatistics2);

    var foundStatistics = registry.findStatisticsByTextId(soughtId);
    assertThat(foundStatistics)
        .containsExactlyInAnyOrder(
            matchingStatistics1,
            matchingStatistics2);
  }

  @Test
  public void findStatisticsByTextId_returnsEmptyArray_ifNoStatisticsMatchTextId() {
    var soughtId = "matching-id";
    var differentId = "mismatching-id";

    givenExistingStatistics(
        statistics(withTextId(differentId)),
        statistics(withTextId(differentId)),
        statistics(withTextId(differentId)));

    assertThat(registry.findStatisticsByTextId(soughtId)).isEmpty();
  }

  @Test
  public void findStatisticsByType_returnsAllStatisticsThatMatchType() {
    var soughtType = mock(StatisticsType.class, "matching type");
    var differentType = mock(StatisticsType.class, "mismatching type");

    var matchingStatistics1 = statistics(withType(soughtType));
    var matchingStatistics2 = statistics(withType(soughtType));
    var mismatchingStatistics = statistics(withType(differentType));

    givenExistingStatistics(
        matchingStatistics2,
        matchingStatistics1,
        mismatchingStatistics);

    var foundStatistics = registry.findStatisticsByType(soughtType);
    assertThat(foundStatistics)
        .containsExactlyInAnyOrder(
            matchingStatistics1,
            matchingStatistics2);
  }

  @Test
  public void findStatisticsByType_returnsEmptyArray_ifNoStatisticsMatchType() {
    var soughtType = mock(StatisticsType.class, "matching type");
    var differentType = mock(StatisticsType.class, "mismatching type");

    givenExistingStatistics(
        statistics(withType(differentType)),
        statistics(withType(differentType)),
        statistics(withType(differentType)));

    assertThat(registry.findStatisticsByType(soughtType)).isEmpty();
  }

  @Test
  public void delegatesGetPidToPidSupplier() {
    var pidReturnedFromPidSupplier = 42;

    when(pidSupplier.getAsInt())
        .thenReturn(pidReturnedFromPidSupplier);

    var result = registry.getPid();

    assertThat(result)
        .isSameAs(pidReturnedFromPidSupplier);
  }

  @Test
  public void propagatesPidSupplierExceptionIfPidSupplierThrows() {
    Throwable thrownFromPidSupplier = new RuntimeException("thrown from pid supplier");

    when(pidSupplier.getAsInt())
        .thenThrow(thrownFromPidSupplier);

    var thrown = catchThrowable(() -> registry.getPid());

    assertThat(thrown)
        .isSameAs(thrownFromPidSupplier);
  }

  /**
   * Adds the given stats directly to the registry's list of stats. Note that this bypasses the
   * registry's actual creation process. In particular, it does not invoke listeners, increment the
   * unique ID, or increment the modification count.
   */
  private void givenExistingStatistics(Statistics... statistics) {
    registry.getStatsList().addAll(Arrays.asList(statistics));
  }

  /**
   * Creates a mock Statistics with the given preparation applied.
   */
  private static Statistics statistics(Consumer<Statistics> preparation) {
    var statistics = mock(Statistics.class);
    preparation.accept(statistics);
    return statistics;
  }

  /**
   * Creates a consumer that assigns the given numeric ID to a mock statistics.
   */
  private static Consumer<Statistics> withNumericId(long numericId) {
    return statistics -> when(statistics.getNumericId()).thenReturn(numericId);
  }

  /**
   * Creates a consumer that assigns the given text ID to a mock statistics.
   */
  private static Consumer<Statistics> withTextId(String textId) {
    return statistics -> when(statistics.getTextId()).thenReturn(textId);
  }

  /**
   * Creates a consumer that assigns the given statistics type to a mock statistics.
   */
  private static Consumer<Statistics> withType(StatisticsType type) {
    return statistics -> when(statistics.getType()).thenReturn(type);
  }

  /**
   * Creates a consumer that assigns the given unique ID to a mock statistics.
   */
  private static Consumer<Statistics> withUniqueId(long uniqueId) {
    return statistics -> when(statistics.getUniqueId()).thenReturn(uniqueId);
  }
}
