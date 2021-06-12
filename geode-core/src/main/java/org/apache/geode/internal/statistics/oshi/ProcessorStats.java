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

package org.apache.geode.internal.statistics.oshi;

import org.jetbrains.annotations.NotNull;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.statistics.StatisticsTypeFactoryImpl;

public class ProcessorStats {
  static final int currentFreq;
  static final int processorCpuLoadTicksUSER;
  static final int processorCpuLoadTicksNICE;
  static final int processorCpuLoadTicksSYSTEM;
  static final int processorCpuLoadTicksIDLE;
  static final int processorCpuLoadTicksIOWAIT;
  static final int processorCpuLoadTicksIRQ;
  static final int processorCpuLoadTicksSOFTIRQ;
  static final int processorCpuLoadTicksSTEAL;
  static final int processorCpuLoad;

  @Immutable
  private static final StatisticsType statisticsType;

  static {
    final StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();

    statisticsType = f.createType("SystemProcessorStats", "Statistics for a system processor.",
        new StatisticDescriptor[] {
            f.createLongGauge("currentFreq",
                "Maximum frequency (in Hz), of the logical processors on this CPU.",
                "Hz"),
            f.createLongCounter("processorCpuLoadTicksUSER",
                "Time spent in User",
                "milliseconds"),
            f.createLongCounter("processorCpuLoadTicksNICE",
                "Time spent in Nice",
                "milliseconds"),
            f.createLongCounter("processorCpuLoadTicksSYSTEM",
                "Time spent in System",
                "milliseconds"),
            f.createLongCounter("processorCpuLoadTicksIDLE",
                "Time spent in Idle",
                "milliseconds"),
            f.createLongCounter("processorCpuLoadTicksIOWAIT",
                "Time spent in IOWait",
                "milliseconds"),
            f.createLongCounter("processorCpuLoadTicksIRQ",
                "Time spent in IRQ",
                "milliseconds"),
            f.createLongCounter("processorCpuLoadTicksSOFTIRQ",
                "Time spent in SoftIRQ",
                "milliseconds"),
            f.createLongCounter("processorCpuLoadTicksSTEAL",
                "Time spent in Steal",
                "milliseconds"),
            f.createDoubleGauge("processorCpuLoad",
                "CPU usage",
                "percent"),
    });

    currentFreq = statisticsType.nameToId("currentFreq");
    processorCpuLoadTicksUSER = statisticsType.nameToId("processorCpuLoadTicksUSER");
    processorCpuLoadTicksNICE = statisticsType.nameToId("processorCpuLoadTicksNICE");
    processorCpuLoadTicksSYSTEM = statisticsType.nameToId("processorCpuLoadTicksSYSTEM");
    processorCpuLoadTicksIDLE = statisticsType.nameToId("processorCpuLoadTicksIDLE");
    processorCpuLoadTicksIOWAIT = statisticsType.nameToId("processorCpuLoadTicksIOWAIT");
    processorCpuLoadTicksIRQ = statisticsType.nameToId("processorCpuLoadTicksIRQ");
    processorCpuLoadTicksSOFTIRQ = statisticsType.nameToId("processorCpuLoadTicksSOFTIRQ");
    processorCpuLoadTicksSTEAL = statisticsType.nameToId("processorCpuLoadTicksSTEAL");
    processorCpuLoad = statisticsType.nameToId("processorCpuLoad");

  }

  private ProcessorStats() {
    // no instances allowed
  }

  public static @NotNull StatisticsType getType() {
    return statisticsType;
  }
}
