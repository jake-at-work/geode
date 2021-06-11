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
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.CentralProcessor.TickType;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.software.os.OSProcess;
import oshi.software.os.OperatingSystem;

import org.apache.geode.internal.statistics.LocalStatisticsImpl;
import org.apache.geode.internal.statistics.SuppliableStatistics;

public class OshiStatistics {

  private static final OshiStatistics instance;

  static {
    final SystemInfo systemInfo = new SystemInfo();
    instance = new OshiStatistics(systemInfo.getOperatingSystem(), systemInfo.getHardware());
  }

  private final OperatingSystem operatingSystem;
  private final HardwareAbstractionLayer hardware;

  OshiStatistics(final @NotNull OperatingSystem operatingSystem,
                 final @NotNull HardwareAbstractionLayer hardware) {
    this.operatingSystem = operatingSystem;
    this.hardware = hardware;
  }

  public static int init() {
    return 0;
  }

  public static void readyRefresh() {

  }

  public static void refreshProcess(final int pid, final SuppliableStatistics stats) {
    instance.updateProcessStats(pid, stats);
  }

  public static void refreshSystem(final SuppliableStatistics stats) {
    instance.updateSystemStats(stats);
  }

  public void updateProcessStats(final int pid, final SuppliableStatistics stats) {
    final OSProcess process = operatingSystem.getProcess(pid);
    stats.setLong(OshiProcessStats.virtualSize, process.getVirtualSize());
    stats.setLong(OshiProcessStats.residentSetSize, process.getResidentSetSize());
    stats.setLong(OshiProcessStats.threadCount, process.getThreadCount());
    stats.setLong(OshiProcessStats.kernelTime, process.getKernelTime());
    stats.setLong(OshiProcessStats.userTime, process.getUserTime());
  }

  public void updateSystemStats(final SuppliableStatistics stats) {
    stats.setLong(OshiSystemStats.processCount, operatingSystem.getProcessCount());
    stats.setLong(OshiSystemStats.threadCount, operatingSystem.getThreadCount());

    final CentralProcessor processor = hardware.getProcessor();
    stats.setLong(OshiSystemStats.contextSwitches, processor.getContextSwitches());
    stats.setLong(OshiSystemStats.interrupts, processor.getInterrupts());
    stats.setLong(OshiSystemStats.physicalProcessorCount, processor.getPhysicalProcessorCount());
    stats.setLong(OshiSystemStats.logicalProcessorCount, processor.getLogicalProcessorCount());
    stats.setLong(OshiSystemStats.maxFreq, processor.getMaxFreq());

    final double[] systemLoadAverage = processor.getSystemLoadAverage(3);
    stats.setDouble(OshiSystemStats.systemLoadAverage1, systemLoadAverage[0]);
    stats.setDouble(OshiSystemStats.systemLoadAverage5, systemLoadAverage[1]);
    stats.setDouble(OshiSystemStats.systemLoadAverage15, systemLoadAverage[2]);

    long[] systemCpuLoadTicks = processor.getSystemCpuLoadTicks();
    stats.setLong(OshiSystemStats.systemCpuLoadTicksUSER,
        systemCpuLoadTicks[TickType.USER.getIndex()]);
    stats.setLong(OshiSystemStats.systemCpuLoadTicksNICE,
        systemCpuLoadTicks[TickType.NICE.getIndex()]);
    stats.setLong(OshiSystemStats.systemCpuLoadTicksSYSTEM,
        systemCpuLoadTicks[TickType.SYSTEM.getIndex()]);
    stats.setLong(OshiSystemStats.systemCpuLoadTicksIDLE,
        systemCpuLoadTicks[TickType.IDLE.getIndex()]);
    stats.setLong(OshiSystemStats.systemCpuLoadTicksIOWAIT,
        systemCpuLoadTicks[TickType.IOWAIT.getIndex()]);
    stats.setLong(OshiSystemStats.systemCpuLoadTicksIRQ,
        systemCpuLoadTicks[TickType.IRQ.getIndex()]);
    stats.setLong(OshiSystemStats.systemCpuLoadTicksSOFTIRQ,
        systemCpuLoadTicks[TickType.SOFTIRQ.getIndex()]);
    stats.setLong(OshiSystemStats.systemCpuLoadTicksSTEAL,
        systemCpuLoadTicks[TickType.STEAL.getIndex()]);

    final long[] currentFreq = processor.getCurrentFreq();
  }

}
