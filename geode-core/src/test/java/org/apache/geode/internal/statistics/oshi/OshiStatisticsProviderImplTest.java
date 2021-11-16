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

import static java.util.Arrays.asList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.hardware.NetworkIF;
import oshi.software.os.OSProcess;
import oshi.software.os.OperatingSystem;

import org.apache.geode.Statistics;
import org.apache.geode.internal.statistics.platform.OsStatisticsFactory;

public class OshiStatisticsProviderImplTest {

  private static final String SYSTEM_IDENTITY = "mock-operating-system";
  private static final String PROCESS_IDENTITY = "mock-process";
  private static final String LOGICAL_PROCESSOR_0_IDENTITY = "mock-processor-0";
  private static final String LOGICAL_PROCESSOR_1_IDENTITY = "mock-processor-1";
  private static final String NETWORK_IF_0_DISPLAY_NAME = "mock-if0";
  private static final String NETWORK_IF_1_DISPLAY_NAME = "mock-if1";

  private final OshiStatisticsProviderImpl oshiStatisticsProvider;
  private final OSProcess osProcess;

  public OshiStatisticsProviderImplTest() {
    SystemInfo systemInfo = mock(SystemInfo.class);
    oshiStatisticsProvider = new OshiStatisticsProviderImpl(systemInfo);

    final OperatingSystem operatingSystem = mock(OperatingSystem.class);
    when(systemInfo.getOperatingSystem()).thenReturn(operatingSystem);

    final int processId = 42;
    when(operatingSystem.getProcessId()).thenReturn(processId);

    final HardwareAbstractionLayer hardwareAbstractionLayer = mock(HardwareAbstractionLayer.class);
    when(systemInfo.getHardware()).thenReturn(hardwareAbstractionLayer);

    final CentralProcessor centralProcessor = mock(CentralProcessor.class);
    when(hardwareAbstractionLayer.getProcessor()).thenReturn(centralProcessor);

    osProcess = mock(OSProcess.class);
    when(operatingSystem.getProcess(eq(processId))).thenReturn(osProcess);

    when(osProcess.toString()).thenReturn(PROCESS_IDENTITY);

    when(operatingSystem.toString()).thenReturn(SYSTEM_IDENTITY);

    final CentralProcessor.LogicalProcessor logicalProcessor0 =
        mock(CentralProcessor.LogicalProcessor.class);
    when(logicalProcessor0.toString()).thenReturn(LOGICAL_PROCESSOR_0_IDENTITY);
    final CentralProcessor.LogicalProcessor logicalProcessor1 =
        mock(CentralProcessor.LogicalProcessor.class);
    when(logicalProcessor1.toString()).thenReturn(LOGICAL_PROCESSOR_1_IDENTITY);
    when(centralProcessor.getLogicalProcessors())
        .thenReturn(asList(logicalProcessor0, logicalProcessor1));

    final NetworkIF networkIf0 = mock(NetworkIF.class);
    when(networkIf0.getDisplayName()).thenReturn(NETWORK_IF_0_DISPLAY_NAME);
    final NetworkIF networkIf1 = mock(NetworkIF.class);
    when(networkIf1.getDisplayName()).thenReturn(NETWORK_IF_1_DISPLAY_NAME);
    when(hardwareAbstractionLayer.getNetworkIFs()).thenReturn(asList(networkIf0, networkIf1));

  }

  @Test
  public void initCreatesOsStatistics() throws OshiStatisticsProviderException {
    final OsStatisticsFactory osStatisticsFactory = mock(OsStatisticsFactory.class);
    final long id = 13;
    oshiStatisticsProvider.init(osStatisticsFactory, id);

    verify(osStatisticsFactory).createOsStatistics(eq(ProcessStats.getType()), eq(PROCESS_IDENTITY),
        eq(id),
        eq(0));
    verify(osStatisticsFactory).createOsStatistics(eq(OperatingSystemStats.getType()),
        eq(SYSTEM_IDENTITY),
        eq(id), eq(0));
    verify(osStatisticsFactory)
        .createOsStatistics(eq(ProcessorStats.getType()), eq(LOGICAL_PROCESSOR_0_IDENTITY), eq(id),
            eq(0));
    verify(osStatisticsFactory)
        .createOsStatistics(eq(ProcessorStats.getType()), eq(LOGICAL_PROCESSOR_1_IDENTITY), eq(id),
            eq(0));
    verify(osStatisticsFactory)
        .createOsStatistics(eq(NetworkInterfaceStats.getType()), eq(NETWORK_IF_0_DISPLAY_NAME),
            eq(id), eq(0));
    verify(osStatisticsFactory)
        .createOsStatistics(eq(NetworkInterfaceStats.getType()), eq(NETWORK_IF_1_DISPLAY_NAME),
            eq(id), eq(0));

    verifyNoMoreInteractions(osStatisticsFactory);
  }

  @Test
  public void sampleProcessUpdatesStats() throws OshiStatisticsProviderException {
    final OsStatisticsFactory osStatisticsFactory = mock(OsStatisticsFactory.class);
    final Statistics statistics = mock(Statistics.class);
    when(osStatisticsFactory.createOsStatistics(eq(ProcessStats.getType()), eq(PROCESS_IDENTITY),
        anyLong(),
        anyInt())).thenReturn(statistics);

    when(osProcess.getProcessCpuLoadBetweenTicks(any())).thenReturn(0.123D);
    when(osProcess.getVirtualSize()).thenReturn(456L);
    when(osProcess.getResidentSetSize()).thenReturn(789L);
    when(osProcess.getThreadCount()).thenReturn(321);
    when(osProcess.getKernelTime()).thenReturn(654L);
    when(osProcess.getUserTime()).thenReturn(987L);
    when(osProcess.getBytesRead()).thenReturn(1234L);
    when(osProcess.getBytesWritten()).thenReturn(5678L);
    when(osProcess.getOpenFiles()).thenReturn(9L);
    when(osProcess.getProcessCpuLoadCumulative()).thenReturn(123.456D);
    when(osProcess.getMinorFaults()).thenReturn(2L);
    when(osProcess.getMajorFaults()).thenReturn(3L);
    when(osProcess.getContextSwitches()).thenReturn(42L);

    oshiStatisticsProvider.init(osStatisticsFactory, 0);
    oshiStatisticsProvider.sampleProcess();

    verify(statistics).setDouble(eq(ProcessStats.cpuLoad), eq(0.123D));
    verify(statistics).setLong(eq(ProcessStats.virtualSize), eq(456L));
    verify(statistics).setLong(eq(ProcessStats.residentSetSize), eq(789L));
    verify(statistics).setLong(eq(ProcessStats.threadCount), eq(321L));
    verify(statistics).setLong(eq(ProcessStats.kernelTime), eq(654L));
    verify(statistics).setLong(eq(ProcessStats.userTime), eq(987L));
    verify(statistics).setLong(eq(ProcessStats.bytesRead), eq(1234L));
    verify(statistics).setLong(eq(ProcessStats.bytesWritten), eq(5678L));
    verify(statistics).setLong(eq(ProcessStats.openFiles), eq(9L));
    verify(statistics).setDouble(eq(ProcessStats.cpuLoadCumulative), eq(123.456D));
    verify(statistics).setLong(eq(ProcessStats.minorFaults), eq(2L));
    verify(statistics).setLong(eq(ProcessStats.majorFaults), eq(3L));
    verify(statistics).setLong(eq(ProcessStats.contextSwitches), eq(42L));

    verifyNoMoreInteractions(statistics);
  }
}
