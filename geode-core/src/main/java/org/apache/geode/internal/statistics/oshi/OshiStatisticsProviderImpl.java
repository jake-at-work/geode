package org.apache.geode.internal.statistics.oshi;

import java.util.List;

import org.jetbrains.annotations.NotNull;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.CentralProcessor.LogicalProcessor;
import oshi.hardware.CentralProcessor.TickType;
import oshi.hardware.GlobalMemory;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.hardware.VirtualMemory;
import oshi.software.os.OSProcess;
import oshi.software.os.OperatingSystem;

import org.apache.geode.Statistics;
import org.apache.geode.internal.statistics.platform.OsStatisticsFactory;

public class OshiStatisticsProviderImpl implements OshiStatisticsProvider {

  final SystemInfo systemInfo = new SystemInfo();
  
  private int processId;
  private CentralProcessor processor;
  private OperatingSystem operatingSystem;
  private HardwareAbstractionLayer hardware;
  private long[] systemCpuLoadTicks;
  private long[][] processorCpuLoadTicks;

  private Statistics processStats;
  private Statistics systemStats;
  private Statistics[] processorStats;
  private OSProcess process;

  @Override
  public void init(final @NotNull OsStatisticsFactory osStatisticsFactory,
                   final long id) throws OshiStatisticsProviderException {

    operatingSystem = systemInfo.getOperatingSystem();
    processId = operatingSystem.getProcessId();
    hardware = systemInfo.getHardware();
    processor = hardware.getProcessor();

    process = operatingSystem.getProcess(processId);
    final String processIdentity = process.toString();
    processStats = osStatisticsFactory.createOsStatistics(ProcessStats.getType(),
        processIdentity, id, 0);

    final String systemIdentity = operatingSystem.toString();
    systemCpuLoadTicks = new long[TickType.values().length];
    systemStats = osStatisticsFactory.createOsStatistics(OperatingSystemStats.getType(),
         systemIdentity, id, 0);


    final List<LogicalProcessor> logicalProcessors = processor.getLogicalProcessors();
    processorCpuLoadTicks = new long[logicalProcessors.size()][TickType.values().length];
    processorStats = new Statistics[logicalProcessors.size()];
    for (int i = 0, logicalProcessorsSize = logicalProcessors.size(); i < logicalProcessorsSize; i++) {
      final LogicalProcessor logicalProcessor = logicalProcessors.get(i);
      final String processorIdentity = logicalProcessor.toString();
      processorStats[i] = osStatisticsFactory.createOsStatistics(ProcessorStats.getType(),
          processorIdentity, id, 0);
    }
  }

  @Override
  public void sample() {
    sampleProcess();
    sampleSystem();
    sampleProcessors();
  }

  @Override
  public void destroy() {
  }

  public void sampleProcess() {
    final OSProcess process = operatingSystem.getProcess(processId);

    final double processCpuLoadBetweenTicks = process.getProcessCpuLoadBetweenTicks(this.process);
    processStats.setDouble(ProcessStats.cpuLoad, processCpuLoadBetweenTicks);
    this.process = process;

    processStats.setLong(ProcessStats.virtualSize, process.getVirtualSize());
    processStats.setLong(ProcessStats.residentSetSize, process.getResidentSetSize());
    processStats.setLong(ProcessStats.threadCount, process.getThreadCount());
    processStats.setLong(ProcessStats.kernelTime, process.getKernelTime());
    processStats.setLong(ProcessStats.userTime, process.getUserTime());
    processStats.setLong(ProcessStats.bytesRead, process.getBytesRead());
    processStats.setLong(ProcessStats.bytesWritten, process.getBytesWritten());
    processStats.setLong(ProcessStats.openFiles, process.getOpenFiles());
    processStats.setDouble(ProcessStats.cpuLoadCumulative, process.getProcessCpuLoadCumulative());
    processStats.setLong(ProcessStats.minorFaults, process.getMinorFaults());
    processStats.setLong(ProcessStats.majorFaults, process.getMajorFaults());
    processStats.setLong(ProcessStats.contextSwitches, process.getContextSwitches());
  }

  public void sampleSystem() {
    systemStats.setLong(OperatingSystemStats.processCount, operatingSystem.getProcessCount());
    systemStats.setLong(OperatingSystemStats.threadCount, operatingSystem.getThreadCount());

    final CentralProcessor processor = hardware.getProcessor();
    systemStats.setLong(OperatingSystemStats.contextSwitches, processor.getContextSwitches());
    systemStats.setLong(OperatingSystemStats.interrupts, processor.getInterrupts());
    systemStats.setLong(OperatingSystemStats.physicalProcessorCount, processor.getPhysicalProcessorCount());
    systemStats.setLong(OperatingSystemStats.logicalProcessorCount, processor.getLogicalProcessorCount());
    systemStats.setLong(OperatingSystemStats.maxFreq, processor.getMaxFreq());

    final double[] systemLoadAverage = processor.getSystemLoadAverage(3);
    systemStats.setDouble(OperatingSystemStats.systemLoadAverage1, systemLoadAverage[0]);
    systemStats.setDouble(OperatingSystemStats.systemLoadAverage5, systemLoadAverage[1]);
    systemStats.setDouble(OperatingSystemStats.systemLoadAverage15, systemLoadAverage[2]);

    final double systemCpuLoadBetweenTicks = processor.getSystemCpuLoadBetweenTicks(systemCpuLoadTicks);
    systemStats.setDouble(OperatingSystemStats.systemCpuLoad, systemCpuLoadBetweenTicks);

    systemCpuLoadTicks = processor.getSystemCpuLoadTicks();
    systemStats.setLong(OperatingSystemStats.systemCpuLoadTicksUSER,
        systemCpuLoadTicks[TickType.USER.getIndex()]);
    systemStats.setLong(OperatingSystemStats.systemCpuLoadTicksNICE,
        systemCpuLoadTicks[TickType.NICE.getIndex()]);
    systemStats.setLong(OperatingSystemStats.systemCpuLoadTicksSYSTEM,
        systemCpuLoadTicks[TickType.SYSTEM.getIndex()]);
    systemStats.setLong(OperatingSystemStats.systemCpuLoadTicksIDLE,
        systemCpuLoadTicks[TickType.IDLE.getIndex()]);
    systemStats.setLong(OperatingSystemStats.systemCpuLoadTicksIOWAIT,
        systemCpuLoadTicks[TickType.IOWAIT.getIndex()]);
    systemStats.setLong(OperatingSystemStats.systemCpuLoadTicksIRQ,
        systemCpuLoadTicks[TickType.IRQ.getIndex()]);
    systemStats.setLong(OperatingSystemStats.systemCpuLoadTicksSOFTIRQ,
        systemCpuLoadTicks[TickType.SOFTIRQ.getIndex()]);
    systemStats.setLong(OperatingSystemStats.systemCpuLoadTicksSTEAL,
        systemCpuLoadTicks[TickType.STEAL.getIndex()]);

    final GlobalMemory memory = hardware.getMemory();
    systemStats.setLong(OperatingSystemStats.memoryTotal, memory.getTotal());
    systemStats.setLong(OperatingSystemStats.memoryAvailable, memory.getAvailable());
    systemStats.setLong(OperatingSystemStats.memoryPageSize, memory.getPageSize());

    final VirtualMemory virtualMemory = memory.getVirtualMemory();
    systemStats.setLong(OperatingSystemStats.swapTotal, virtualMemory.getSwapTotal());
    systemStats.setLong(OperatingSystemStats.swapUsed, virtualMemory.getSwapUsed());
    systemStats.setLong(OperatingSystemStats.virtualMax, virtualMemory.getVirtualMax());
    systemStats.setLong(OperatingSystemStats.virtualInUse, virtualMemory.getVirtualInUse());
    systemStats.setLong(OperatingSystemStats.swapPagesIn, virtualMemory.getSwapPagesIn());
    systemStats.setLong(OperatingSystemStats.swapPagesOut, virtualMemory.getSwapPagesOut());
  }

  private void sampleProcessors() {
    final long[] currentFreq = processor.getCurrentFreq();
    final double[] processorCpuLoad = processor.getProcessorCpuLoadBetweenTicks(processorCpuLoadTicks);
    processorCpuLoadTicks = processor.getProcessorCpuLoadTicks();

    for (int i = 0; i < processorStats.length; i++) {
      final Statistics processorStat = processorStats[i];
      processorStat.setLong(ProcessorStats.currentFreq, currentFreq[i]);
      processorStat.setDouble(ProcessorStats.processorCpuLoad, processorCpuLoad[i]);

      long[] processorCpuLoadTick = processorCpuLoadTicks[i];
      processorStat.setLong(ProcessorStats.processorCpuLoadTicksUSER,
          processorCpuLoadTick[TickType.USER.getIndex()]);
      processorStat.setLong(ProcessorStats.processorCpuLoadTicksNICE,
          processorCpuLoadTick[TickType.NICE.getIndex()]);
      processorStat.setLong(ProcessorStats.processorCpuLoadTicksSYSTEM,
          processorCpuLoadTick[TickType.SYSTEM.getIndex()]);
      processorStat.setLong(ProcessorStats.processorCpuLoadTicksIDLE,
          processorCpuLoadTick[TickType.IDLE.getIndex()]);
      processorStat.setLong(ProcessorStats.processorCpuLoadTicksIOWAIT,
          processorCpuLoadTick[TickType.IOWAIT.getIndex()]);
      processorStat.setLong(ProcessorStats.processorCpuLoadTicksIRQ,
          processorCpuLoadTick[TickType.IRQ.getIndex()]);
      processorStat.setLong(ProcessorStats.processorCpuLoadTicksSOFTIRQ,
          processorCpuLoadTick[TickType.SOFTIRQ.getIndex()]);
      processorStat.setLong(ProcessorStats.processorCpuLoadTicksSTEAL,
          processorCpuLoadTick[TickType.STEAL.getIndex()]);
    }
  }
  
}
