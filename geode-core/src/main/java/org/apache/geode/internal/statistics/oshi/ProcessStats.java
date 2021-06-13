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

public class ProcessStats {
  static final int virtualSize;
  static final int residentSetSize;
  static final int threadCount;
  static final int kernelTime;
  static final int userTime;
  static final int bytesRead;
  static final int bytesWritten;
  static final int openFiles;
  static final int cpuLoadCumulative;
  static final int cpuLoad;
  static final int minorFaults;
  static final int majorFaults;
  static final int contextSwitches;

  @Immutable
  private static final StatisticsType statisticsType;

  static {
    final StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();

    statisticsType = f.createType("ProcessStats", "Statistics on a process.",
        new StatisticDescriptor[] {
            f.createLongGauge("virtualSize",
                "Gets the Virtual Memory Size (VSZ). Includes all memory that the process can access, including memory that is swapped out and memory that is from shared libraries.",
                "bytes"),
            f.createLongGauge("residentSetSize",
                "Gets the Resident Set Size (RSS). Used to show how much memory is allocated to that process and is in RAM. It does not include memory that is swapped out. It does include memory from shared libraries as long as the pages from those libraries are actually in memory. It does include all stack and heap memory.",
                "bytes"),
            f.createLongGauge("threadCount",
                "Gets the number of threads being executed by this process.",
                "threads"),
            f.createLongCounter("kernelTime",
                "Gets kernel time used by the process.",
                "milliseconds"),
            f.createLongCounter("userTime",
                "Gets user time used by the process.",
                "milliseconds"),
            f.createLongCounter("bytesRead",
                "The number of bytes the process has written to disk",
                "bytes"),
            f.createLongCounter("bytesWritten",
                "The number of bytes the process has written to disk.",
                "bytes"),
            f.createLongGauge("openFiles",
                "Gets the number of open file handles (or network connections) that belongs to the process.",
                "files"),
            f.createDoubleGauge("cpuLoadCumulative",
                "Gets cumulative CPU usage of this process.",
                "percent"),
            f.createDoubleGauge("cpuLoad",
                "CPU usage of this process.",
                "percent"),
            f.createLongCounter("minorFaults",
                "Gets the number of minor (soft) faults the process has made which have not required loading a memory page from disk. Sometimes called reclaims.",
                "faults"),
            f.createLongCounter("majorFaults",
                "Gets the number of major (hard) faults the process has made which have required loading a memory page from disk.",
                "faults"),
            f.createLongCounter("contextSwitches",
                "A snapshot of the context switches the process has done. Since the context switches could be voluntary and non-voluntary, this gives the sum of both.",
                "switches"),
        });

    virtualSize = statisticsType.nameToId("virtualSize");
    residentSetSize = statisticsType.nameToId("residentSetSize");
    threadCount = statisticsType.nameToId("threadCount");
    kernelTime = statisticsType.nameToId("kernelTime");
    userTime = statisticsType.nameToId("userTime");
    bytesRead = statisticsType.nameToId("bytesRead");
    bytesWritten = statisticsType.nameToId("bytesWritten");
    openFiles = statisticsType.nameToId("openFiles");
    cpuLoadCumulative = statisticsType.nameToId("cpuLoadCumulative");
    cpuLoad = statisticsType.nameToId("cpuLoad");
    minorFaults = statisticsType.nameToId("minorFaults");
    majorFaults = statisticsType.nameToId("majorFaults");
    contextSwitches = statisticsType.nameToId("contextSwitches");
  }

  private ProcessStats() {
    // no instances allowed
  }

  public static @NotNull StatisticsType getType() {
    return statisticsType;
  }

}
