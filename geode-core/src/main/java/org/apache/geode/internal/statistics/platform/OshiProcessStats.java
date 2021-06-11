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

package org.apache.geode.internal.statistics.platform;

import org.jetbrains.annotations.NotNull;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.statistics.StatisticsTypeFactoryImpl;

public class OshiProcessStats {
  static final int virtualSize;
  static final int residentSetSize;
  static final int threadCount;
  static final int kernelTime;
  static final int userTime;

  @Immutable
  private static final StatisticsType statisticsType;

  static {
    final StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();

    statisticsType = f.createType("OSProcessStats", "Statistics on a OS process.",
        new StatisticDescriptor[]{
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
                "milliseconds")
        });

    virtualSize = statisticsType.nameToId("virtualSize");
    residentSetSize = statisticsType.nameToId("residentSetSize");
    threadCount = statisticsType.nameToId("threadCount");
    kernelTime = statisticsType.nameToId("kernelTime");
    userTime = statisticsType.nameToId("userTime");
  }

  private OshiProcessStats() {
    // no instances allowed
  }

  public static @NotNull StatisticsType getType() {
    return statisticsType;
  }

  public static ProcessStats createProcessStats(final @NotNull Statistics stats) {
    return new ProcessStats(stats) {
      @Override
      public long getProcessSize() {
        return stats.getLong(residentSetSize);
      }
    };
  }
}
