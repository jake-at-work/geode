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
import static org.mockito.Mockito.mock;

import org.junit.Test;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.StatisticsType;


public class LocalStatisticsImplTest {


  @Test
  public void testGet() {
    var mockStatisticsManager = mock(StatisticsManager.class);
    var statsFactory = StatisticsTypeFactoryImpl.singleton();
    var stats = new StatisticDescriptor[] {
        statsFactory.createIntCounter("intCount", "int counter", "ints"),
        statsFactory.createLongCounter("longCount", "long counter", "longs")
    };
    StatisticsType statisticsType = new StatisticsTypeImpl("abc", "mock stats", stats);
    var localStatistics = new LocalStatisticsImpl(statisticsType, "abc", 123L, 123L,
        false, 90, mockStatisticsManager);

    localStatistics.incInt("intCount", 7);
    localStatistics.incLong("longCount", 15);

    assertThat(localStatistics.getInt("intCount")).isEqualTo(7);
    assertThat(localStatistics.getInt("longCount")).isEqualTo(15);
    assertThat(localStatistics.getLong("longCount")).isEqualTo(15);

    var intId = statisticsType.nameToId("intCount");
    assertThat(localStatistics.getInt(intId)).isEqualTo(7);
    assertThat(localStatistics.getLong(intId)).isEqualTo(7);
    var longId = statisticsType.nameToId("longCount");
    assertThat(localStatistics.getInt(longId)).isEqualTo(15);
    assertThat(localStatistics.getLong(longId)).isEqualTo(15);
  }

  @Test
  public void testIncrement() {
    var mockStatisticsManager = mock(StatisticsManager.class);
    var statsFactory = StatisticsTypeFactoryImpl.singleton();
    var stats = new StatisticDescriptor[] {
        statsFactory.createIntCounter("intCount", "int counter", "ints"),
        statsFactory.createLongCounter("longCount", "long counter", "longs"),
        statsFactory.createDoubleCounter("doubleCount", "double counter", "doubles")
    };
    StatisticsType statisticsType = new StatisticsTypeImpl("abc", "mock stats", stats);
    var localStatistics = new LocalStatisticsImpl(statisticsType, "abc", 123L, 123L,
        false, 90, mockStatisticsManager);

    localStatistics.incInt("intCount", 7);
    localStatistics.incLong("longCount", 15);
    localStatistics.incDouble("doubleCount", 3.14);

    assertThat(localStatistics.getInt("intCount")).isEqualTo(7);
    assertThat(localStatistics.getInt("longCount")).isEqualTo(15);
    assertThat(localStatistics.getLong("longCount")).isEqualTo(15);
    assertThat(localStatistics.getDouble("doubleCount")).isEqualTo(3.14);

    var intId = statisticsType.nameToId("intCount");
    assertThat(localStatistics.getInt(intId)).isEqualTo(7);
    assertThat(localStatistics.getLong(intId)).isEqualTo(7);

    var longId = statisticsType.nameToId("longCount");
    assertThat(localStatistics.getInt(longId)).isEqualTo(15);
    assertThat(localStatistics.getLong(longId)).isEqualTo(15);

    var doubleId = statisticsType.nameToId("doubleCount");
    assertThat(localStatistics.getDouble(doubleId)).isEqualTo(3.14);
  }

}
