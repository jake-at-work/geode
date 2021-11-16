package org.apache.geode.internal.statistics.oshi;

import java.util.List;

import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.CentralProcessor.LogicalProcessor;
import oshi.hardware.CentralProcessor.TickType;
import oshi.hardware.GlobalMemory;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.hardware.NetworkIF;
import oshi.hardware.VirtualMemory;
import oshi.software.os.FileSystem;
import oshi.software.os.InternetProtocolStats;
import oshi.software.os.InternetProtocolStats.TcpStats;
import oshi.software.os.InternetProtocolStats.UdpStats;
import oshi.software.os.OSProcess;
import oshi.software.os.OperatingSystem;

import org.apache.geode.Statistics;
import org.apache.geode.internal.statistics.platform.OsStatisticsFactory;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class OshiStatisticsProviderImpl implements OshiStatisticsProvider {
  private static final Logger log = LogService.getLogger();

  final SystemInfo systemInfo;

  private int processId;
  private CentralProcessor processor;
  private OperatingSystem operatingSystem;
  private HardwareAbstractionLayer hardware;
  private List<NetworkIF> networkIFs;

  private long[] systemCpuLoadTicks;
  private long[][] processorCpuLoadTicks;
  private OSProcess process;

  private Statistics processStats;
  private Statistics systemStats;
  private Statistics[] processorStats;
  private Statistics[] networkInterfaceStats;

  public OshiStatisticsProviderImpl() {
    this(new SystemInfo());
  }

  OshiStatisticsProviderImpl(final SystemInfo systemInfo) {
    this.systemInfo = systemInfo;
  }

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
    for (int i = 0, size = logicalProcessors.size(); i < size; i++) {
      final LogicalProcessor logicalProcessor = logicalProcessors.get(i);
      final String processorIdentity = logicalProcessor.toString();
      processorStats[i] = osStatisticsFactory.createOsStatistics(ProcessorStats.getType(),
          processorIdentity, id, 0);
    }

    networkIFs = hardware.getNetworkIFs();
    networkInterfaceStats = new Statistics[networkIFs.size()];
    for (int i = 0, size = networkIFs.size(); i < size; i++) {
      final NetworkIF networkIF = networkIFs.get(i);
      final String displayName = networkIF.getDisplayName();
      log.info("Creating network interfaces stats for {}", displayName);
      networkInterfaceStats[i] =
          osStatisticsFactory.createOsStatistics(NetworkInterfaceStats.getType(),
              displayName, id, 0);
    }
  }

  @Override
  public void sample() {
    sampleProcess();
    sampleSystem();
    sampleProcessors();
    sampleNetworkInterfaces();
  }

  @Override
  public void destroy() {}

  void sampleProcess() {
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

  private void sampleSystem() {
    systemStats.setLong(OperatingSystemStats.processCount, operatingSystem.getProcessCount());
    systemStats.setLong(OperatingSystemStats.threadCount, operatingSystem.getThreadCount());

    final CentralProcessor processor = hardware.getProcessor();
    systemStats.setLong(OperatingSystemStats.contextSwitches, processor.getContextSwitches());
    systemStats.setLong(OperatingSystemStats.interrupts, processor.getInterrupts());
    systemStats.setLong(OperatingSystemStats.physicalProcessorCount,
        processor.getPhysicalProcessorCount());
    systemStats.setLong(OperatingSystemStats.logicalProcessorCount,
        processor.getLogicalProcessorCount());
    systemStats.setLong(OperatingSystemStats.maxFreq, processor.getMaxFreq());

    final double[] systemLoadAverage = processor.getSystemLoadAverage(3);
    systemStats.setDouble(OperatingSystemStats.systemLoadAverage1, systemLoadAverage[0]);
    systemStats.setDouble(OperatingSystemStats.systemLoadAverage5, systemLoadAverage[1]);
    systemStats.setDouble(OperatingSystemStats.systemLoadAverage15, systemLoadAverage[2]);

    final double systemCpuLoadBetweenTicks =
        processor.getSystemCpuLoadBetweenTicks(systemCpuLoadTicks);
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

    final InternetProtocolStats internetProtocolStats = operatingSystem.getInternetProtocolStats();
    final TcpStats tcPv4Stats = internetProtocolStats.getTCPv4Stats();
    systemStats.setLong(OperatingSystemStats.tcpv4ConnectionsEstablished,
        tcPv4Stats.getConnectionsEstablished());
    systemStats.setLong(OperatingSystemStats.tcpv4ConnectionsActive,
        tcPv4Stats.getConnectionsActive());
    systemStats.setLong(OperatingSystemStats.tcpv4ConnectionsPassive,
        tcPv4Stats.getConnectionsPassive());
    systemStats.setLong(OperatingSystemStats.tcpv4ConnectionFailures,
        tcPv4Stats.getConnectionFailures());
    systemStats.setLong(OperatingSystemStats.tcpv4ConnectionsReset,
        tcPv4Stats.getConnectionsReset());
    systemStats.setLong(OperatingSystemStats.tcpv4SegmentsSent,
        tcPv4Stats.getSegmentsSent());
    systemStats.setLong(OperatingSystemStats.tcpv4SegmentsReceived,
        tcPv4Stats.getSegmentsReceived());
    systemStats.setLong(OperatingSystemStats.tcpv4SegmentsRetransmitted,
        tcPv4Stats.getSegmentsRetransmitted());
    systemStats.setLong(OperatingSystemStats.tcpv4InErrors,
        tcPv4Stats.getInErrors());
    systemStats.setLong(OperatingSystemStats.tcpv4OutResets,
        tcPv4Stats.getOutResets());

    final UdpStats udPv4Stats = internetProtocolStats.getUDPv4Stats();
    systemStats.setLong(OperatingSystemStats.udpv4DatagramsSent,
        udPv4Stats.getDatagramsSent());
    systemStats.setLong(OperatingSystemStats.udpv4DatagramsReceived,
        udPv4Stats.getDatagramsReceived());
    systemStats.setLong(OperatingSystemStats.udpv4DatagramsNoPort,
        udPv4Stats.getDatagramsNoPort());
    systemStats.setLong(OperatingSystemStats.udpv4DatagramsReceivedErrors,
        udPv4Stats.getDatagramsReceivedErrors());

    final TcpStats tcPv6Stats = internetProtocolStats.getTCPv6Stats();
    systemStats.setLong(OperatingSystemStats.tcpv6ConnectionsEstablished,
        tcPv6Stats.getConnectionsEstablished());
    systemStats.setLong(OperatingSystemStats.tcpv6ConnectionsActive,
        tcPv6Stats.getConnectionsActive());
    systemStats.setLong(OperatingSystemStats.tcpv6ConnectionsPassive,
        tcPv6Stats.getConnectionsPassive());
    systemStats.setLong(OperatingSystemStats.tcpv6ConnectionFailures,
        tcPv6Stats.getConnectionFailures());
    systemStats.setLong(OperatingSystemStats.tcpv6ConnectionsReset,
        tcPv6Stats.getConnectionsReset());
    systemStats.setLong(OperatingSystemStats.tcpv6SegmentsSent,
        tcPv6Stats.getSegmentsSent());
    systemStats.setLong(OperatingSystemStats.tcpv6SegmentsReceived,
        tcPv6Stats.getSegmentsReceived());
    systemStats.setLong(OperatingSystemStats.tcpv6SegmentsRetransmitted,
        tcPv6Stats.getSegmentsRetransmitted());
    systemStats.setLong(OperatingSystemStats.tcpv6InErrors,
        tcPv6Stats.getInErrors());
    systemStats.setLong(OperatingSystemStats.tcpv6OutResets,
        tcPv6Stats.getOutResets());

    final UdpStats udPv6Stats = internetProtocolStats.getUDPv6Stats();
    systemStats.setLong(OperatingSystemStats.udpv6DatagramsSent,
        udPv6Stats.getDatagramsSent());
    systemStats.setLong(OperatingSystemStats.udpv6DatagramsReceived,
        udPv6Stats.getDatagramsReceived());
    systemStats.setLong(OperatingSystemStats.udpv6DatagramsNoPort,
        udPv6Stats.getDatagramsNoPort());
    systemStats.setLong(OperatingSystemStats.udpv6DatagramsReceivedErrors,
        udPv6Stats.getDatagramsReceivedErrors());

    final FileSystem fileSystem = operatingSystem.getFileSystem();
    systemStats.setLong(OperatingSystemStats.openFileDescriptors,
        fileSystem.getOpenFileDescriptors());
    systemStats.setLong(OperatingSystemStats.maxFileDescriptors,
        fileSystem.getMaxFileDescriptors());

  }

  private void sampleProcessors() {
    final long[] currentFreq = processor.getCurrentFreq();
    final double[] processorCpuLoad =
        processor.getProcessorCpuLoadBetweenTicks(processorCpuLoadTicks);
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

  private void sampleNetworkInterfaces() {
    for (int i = 0, size = networkIFs.size(); i < size; i++) {
      final NetworkIF networkIF = networkIFs.get(i);
      if (!networkIF.updateAttributes()) {
        continue;
      }

      final Statistics networkInterfaceStat = networkInterfaceStats[i];
      networkInterfaceStat.setLong(NetworkInterfaceStats.mtu, networkIF.getMTU());
      networkInterfaceStat.setLong(NetworkInterfaceStats.bytesReceived, networkIF.getBytesRecv());
      networkInterfaceStat.setLong(NetworkInterfaceStats.bytesSent, networkIF.getBytesSent());
      networkInterfaceStat.setLong(NetworkInterfaceStats.packetsReceived,
          networkIF.getPacketsRecv());
      networkInterfaceStat.setLong(NetworkInterfaceStats.packetsSent, networkIF.getPacketsSent());
      networkInterfaceStat.setLong(NetworkInterfaceStats.inErrors, networkIF.getInErrors());
      networkInterfaceStat.setLong(NetworkInterfaceStats.outErrors, networkIF.getOutErrors());
      networkInterfaceStat.setLong(NetworkInterfaceStats.inDrops, networkIF.getInDrops());
      networkInterfaceStat.setLong(NetworkInterfaceStats.collisions, networkIF.getCollisions());
      networkInterfaceStat.setLong(NetworkInterfaceStats.speed, networkIF.getSpeed());
    }

  }
}
