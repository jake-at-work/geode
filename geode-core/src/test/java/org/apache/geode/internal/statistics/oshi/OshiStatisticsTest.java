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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;
import oshi.hardware.CentralProcessor;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.software.os.OSProcess;
import oshi.software.os.OperatingSystem;

import org.apache.geode.internal.statistics.SuppliableStatistics;

public class OshiStatisticsTest {

//  private final OSProcess process = mock(OSProcess.class);
//  private final OperatingSystem operatingSystem = mock(OperatingSystem.class);
//  private final HardwareAbstractionLayer hardwareAbstractionLayer = mock(HardwareAbstractionLayer.class);
//  private final OshiStatistics
//      oshiStatistics = new OshiStatistics(operatingSystem, hardwareAbstractionLayer);
//
//
//  private final SuppliableStatistics stats = mock(SuppliableStatistics.class);
//
//  public OshiStatisticsTest() {
//    when(operatingSystem.getProcess(eq(1))).thenReturn(process);
//  }
//
//  @Test
//  public void testInit() {
//    assertThat(OshiStatistics.init()).isEqualTo(0);
//  }
//
//  @Test
//  public void updateProcessStats() {
//    when(process.getVirtualSize()).thenReturn(42L);
//    when(process.getResidentSetSize()).thenReturn(420L);
//    when(process.getThreadCount()).thenReturn(4200);
//    when(process.getKernelTime()).thenReturn(42000L);
//    when(process.getUserTime()).thenReturn(420000L);
//
//    oshiStatistics.updateProcessStats(1, stats);
//
//    verify(stats).setLong(eq(ProcessStats.virtualSize), eq(42L));
//    verify(stats).setLong(eq(ProcessStats.residentSetSize), eq(420L));
//    verify(stats).setLong(eq(ProcessStats.threadCount), eq(4200L));
//    verify(stats).setLong(eq(ProcessStats.kernelTime), eq(42000L));
//    verify(stats).setLong(eq(ProcessStats.userTime), eq(420000L));
//  }
//
//  @Test
//  public void updateSystemStats() {
//    when(operatingSystem.getProcessCount()).thenReturn(1);
//    when(operatingSystem.getThreadCount()).thenReturn(2);
//    final CentralProcessor centralProcessor = mock(CentralProcessor.class);
//    when(centralProcessor.getContextSwitches()).thenReturn(3L);
//    when(centralProcessor.getInterrupts()).thenReturn(4L);
//    when(centralProcessor.getPhysicalProcessorCount()).thenReturn(5);
//    when(centralProcessor.getLogicalProcessorCount()).thenReturn(6);
//    when(centralProcessor.getSystemLoadAverage(eq(3))).thenReturn(new double[]{1.0, 2.0, 3.0});
//    when(centralProcessor.getSystemCpuLoadTicks()).thenReturn(new long[]{1, 2, 3, 4, 5, 6, 7, 8});
//    when(hardwareAbstractionLayer.getProcessor()).thenReturn(centralProcessor);
//
//    oshiStatistics.updateSystemStats(stats);
//
//    verify(stats).setLong(eq(OperatingSystemStats.processCount), eq(1L));
//    verify(stats).setLong(eq(OperatingSystemStats.threadCount), eq(2L));
//    verify(stats).setLong(eq(OperatingSystemStats.contextSwitches), eq(3L));
//    verify(stats).setLong(eq(OperatingSystemStats.interrupts), eq(4L));
//    verify(stats).setLong(eq(OperatingSystemStats.physicalProcessorCount), eq(5L));
//    verify(stats).setLong(eq(OperatingSystemStats.logicalProcessorCount), eq(6L));
//    verify(stats).setDouble(eq(OperatingSystemStats.systemLoadAverage1), eq(1.0));
//    verify(stats).setDouble(eq(OperatingSystemStats.systemLoadAverage5), eq(2.0));
//    verify(stats).setDouble(eq(OperatingSystemStats.systemLoadAverage15), eq(3.0));
//    verify(stats).setLong(eq(OperatingSystemStats.systemCpuLoadTicksUSER), eq(1L));
//    verify(stats).setLong(eq(OperatingSystemStats.systemCpuLoadTicksNICE), eq(2L));
//    verify(stats).setLong(eq(OperatingSystemStats.systemCpuLoadTicksSYSTEM), eq(3L));
//    verify(stats).setLong(eq(OperatingSystemStats.systemCpuLoadTicksIDLE), eq(4L));
//    verify(stats).setLong(eq(OperatingSystemStats.systemCpuLoadTicksIOWAIT), eq(5L));
//    verify(stats).setLong(eq(OperatingSystemStats.systemCpuLoadTicksIRQ), eq(6L));
//    verify(stats).setLong(eq(OperatingSystemStats.systemCpuLoadTicksSOFTIRQ), eq(7L));
//    verify(stats).setLong(eq(OperatingSystemStats.systemCpuLoadTicksSTEAL), eq(8L));
//  }

}