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

public class OperatingSystemStats {
  static final int processCount;
  static final int threadCount;
  static final int contextSwitches;
  static final int interrupts;
  static final int physicalProcessorCount;
  static final int logicalProcessorCount;
  static final int maxFreq;
  static final int systemLoadAverage1;
  static final int systemLoadAverage5;
  static final int systemLoadAverage15;
  static final int systemCpuLoadTicksUSER;
  static final int systemCpuLoadTicksNICE;
  static final int systemCpuLoadTicksSYSTEM;
  static final int systemCpuLoadTicksIDLE;
  static final int systemCpuLoadTicksIOWAIT;
  static final int systemCpuLoadTicksIRQ;
  static final int systemCpuLoadTicksSOFTIRQ;
  static final int systemCpuLoadTicksSTEAL;
  static final int systemCpuLoad;
  static final int memoryTotal;
  static final int memoryAvailable;
  static final int memoryPageSize;
  static final int swapTotal;
  static final int swapUsed;
  static final int virtualMax;
  static final int virtualInUse;
  static final int swapPagesIn;
  static final int swapPagesOut;

  @Immutable
  private static final StatisticsType statisticsType;

  static {
    final StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();

    statisticsType = f.createType("OperatingSystemStats", "Statistics for an Operating System.",
        new StatisticDescriptor[] {
            f.createLongGauge("processCount", "Get the number of processes currently running.",
                "processes"),
            f.createLongGauge("threadCount",
                "Get the number of threads currently running",
                "threads"),
            f.createLongCounter("contextSwitches",
                "Get the number of system-wide context switches which have occurred.",
                "operations", false),
            f.createLongCounter("interrupts",
                "Get the number of system-wide interrupts which have occurred.",
                "interrupts"),
            f.createLongGauge("physicalProcessorCount",
                "Get the number of physical CPUs/cores available for processing.",
                "processors"),
            f.createLongGauge("logicalProcessorCount",
                "Get the number of logical CPUs available for processing. This value may be higher than physical CPUs if hyperthreading is enabled.",
                "processors"),
            f.createLongGauge("maxFreq",
                "Maximum frequency (in Hz), of the logical processors on this CPU.",
                "Hz"),
            f.createLongCounter("systemCpuLoadTicksUSER",
                "Time spent in User",
                "milliseconds"),
            f.createLongCounter("systemCpuLoadTicksNICE",
                "Time spent in Nice",
                "milliseconds"),
            f.createLongCounter("systemCpuLoadTicksSYSTEM",
                "Time spent in System",
                "milliseconds"),
            f.createLongCounter("systemCpuLoadTicksIDLE",
                "Time spent in Idle",
                "milliseconds"),
            f.createLongCounter("systemCpuLoadTicksIOWAIT",
                "Time spent in IOWait",
                "milliseconds"),
            f.createLongCounter("systemCpuLoadTicksIRQ",
                "Time spent in IRQ",
                "milliseconds"),
            f.createLongCounter("systemCpuLoadTicksSOFTIRQ",
                "Time spent in SoftIRQ",
                "milliseconds"),
            f.createLongCounter("systemCpuLoadTicksSTEAL",
                "Time spent in Steal",
                "milliseconds"),
            f.createDoubleGauge("systemCpuLoad",
                "Time spent in Steal",
                "percent"),
            f.createDoubleGauge("systemLoadAverage1",
                "The system load average is the sum of the number of runnable entities queued to the available processors and the number of runnable entities running on the available processors averaged over 1 minute.",
                "load"),
            f.createDoubleGauge("systemLoadAverage5",
                "The system load average is the sum of the number of runnable entities queued to the available processors and the number of runnable entities running on the available processors averaged over 5 minutes.",
                "load"),
            f.createDoubleGauge("systemLoadAverage15",
                "The system load average is the sum of the number of runnable entities queued to the available processors and the number of runnable entities running on the available processors averaged over 15 minutes.",
                "load"),
            f.createLongGauge("memoryTotal",
                "The amount of actual physical memory, in bytes.",
                "bytes"),
            f.createLongGauge("memoryAvailable",
                "The amount of physical memory currently available, in bytes.",
                "bytes"),
            f.createLongGauge("memoryPageSize",
                "The number of bytes in a memory page",
                "bytes"),
            f.createLongGauge("swapTotal",
                "The current size of the paging/swap file(s), in bytes. If the paging/swap file can be extended, this is a soft limit.",
                "bytes"),
            f.createLongGauge("swapUsed",
                "The current memory committed to the paging/swap file(s), in bytes.",
                "bytes"),
            f.createLongGauge("virtualMax",
                "The maximum memory that can be committed by the system without extending the paging file(s), in bytes. Also called the Commit Limit. If the paging/swap file can be extended, this is a soft limit. This is generally equal to the sum of the sizes of physical memory and paging/swap file(s).",
                "bytes"),
            f.createLongGauge("virtualInUse",
                "The memory currently committed by the system, in bytes. Also called the Commit Total. This is generally equal to the sum of the bytes used of physical memory and paging/swap file(s).",
                "bytes"),
            f.createLongCounter("swapPagesIn",
                "Number of pages read from paging/swap file(s) to resolve hard page faults.",
                "pages"),
            f.createLongCounter("swapPagesOut",
                "Number of pages read from paging/swap file(s) to resolve hard page faults.",
                "pages"),
    });

    processCount = statisticsType.nameToId("processCount");
    threadCount = statisticsType.nameToId("threadCount");
    contextSwitches = statisticsType.nameToId("contextSwitches");
    interrupts = statisticsType.nameToId("interrupts");
    physicalProcessorCount = statisticsType.nameToId("physicalProcessorCount");
    logicalProcessorCount = statisticsType.nameToId("logicalProcessorCount");
    maxFreq = statisticsType.nameToId("maxFreq");
    systemLoadAverage1 = statisticsType.nameToId("systemLoadAverage1");
    systemLoadAverage5 = statisticsType.nameToId("systemLoadAverage5");
    systemLoadAverage15 = statisticsType.nameToId("systemLoadAverage15");
    systemCpuLoadTicksUSER = statisticsType.nameToId("systemCpuLoadTicksUSER");
    systemCpuLoadTicksNICE = statisticsType.nameToId("systemCpuLoadTicksNICE");
    systemCpuLoadTicksSYSTEM = statisticsType.nameToId("systemCpuLoadTicksSYSTEM");
    systemCpuLoadTicksIDLE = statisticsType.nameToId("systemCpuLoadTicksIDLE");
    systemCpuLoadTicksIOWAIT = statisticsType.nameToId("systemCpuLoadTicksIOWAIT");
    systemCpuLoadTicksIRQ = statisticsType.nameToId("systemCpuLoadTicksIRQ");
    systemCpuLoadTicksSOFTIRQ = statisticsType.nameToId("systemCpuLoadTicksSOFTIRQ");
    systemCpuLoadTicksSTEAL = statisticsType.nameToId("systemCpuLoadTicksSTEAL");
    systemCpuLoad = statisticsType.nameToId("systemCpuLoad");
    memoryTotal = statisticsType.nameToId("memoryTotal");
    memoryAvailable = statisticsType.nameToId("memoryAvailable");
    memoryPageSize = statisticsType.nameToId("memoryPageSize");
    swapTotal = statisticsType.nameToId("swapTotal");
    swapUsed = statisticsType.nameToId("swapUsed");
    virtualMax = statisticsType.nameToId("virtualMax");
    virtualInUse = statisticsType.nameToId("virtualInUse");
    swapPagesIn = statisticsType.nameToId("swapPagesIn");
    swapPagesOut = statisticsType.nameToId("swapPagesOut");

  }

  private OperatingSystemStats() {
    // no instances allowed
  }

  public static @NotNull StatisticsType getType() {
    return statisticsType;
  }
}
