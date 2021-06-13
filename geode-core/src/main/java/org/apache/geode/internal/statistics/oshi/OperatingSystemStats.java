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
  static final int tcpv4ConnectionsEstablished;
  static final int tcpv4ConnectionsActive;
  static final int tcpv4ConnectionsPassive;
  static final int tcpv4ConnectionFailures;
  static final int tcpv4ConnectionsReset;
  static final int tcpv4SegmentsSent;
  static final int tcpv4SegmentsReceived;
  static final int tcpv4SegmentsRetransmitted;
  static final int tcpv4InErrors;
  static final int tcpv4OutResets;
  static final int udpv4DatagramsSent;
  static final int udpv4DatagramsReceived;
  static final int udpv4DatagramsNoPort;
  static final int udpv4DatagramsReceivedErrors;
  static final int tcpv6ConnectionsEstablished;
  static final int tcpv6ConnectionsActive;
  static final int tcpv6ConnectionsPassive;
  static final int tcpv6ConnectionFailures;
  static final int tcpv6ConnectionsReset;
  static final int tcpv6SegmentsSent;
  static final int tcpv6SegmentsReceived;
  static final int tcpv6SegmentsRetransmitted;
  static final int tcpv6InErrors;
  static final int tcpv6OutResets;
  static final int udpv6DatagramsSent;
  static final int udpv6DatagramsReceived;
  static final int udpv6DatagramsNoPort;
  static final int udpv6DatagramsReceivedErrors;
  static final int openFileDescriptors;
  static final int maxFileDescriptors;

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
            f.createLongGauge("tcpv4ConnectionsEstablished",
                "Connection Failures is the number of times TCP connections have made a direct transition to the CLOSED state from the SYN-SENT state or the SYN-RCVD state, plus the number of times TCP connections have made a direct transition to the LISTEN state from the SYN-RCVD state.",
                "connections"),
            f.createLongCounter("tcpv4ConnectionsActive",
                "Connections Active is the number of times TCP connections have made a direct transition to the SYN-SENT state from the CLOSED state. In other words, it shows a number of connections which are initiated by the local computer. The value is a cumulative total.",
                "connections"),
            f.createLongCounter("tcpv4ConnectionsPassive",
                "Connections Passive is the number of times TCP connections have made a direct transition to the SYN-RCVD state from the LISTEN state. In other words, it shows a number of connections to the local computer, which are initiated by remote computers. The value is a cumulative total.",
                "connections"),
            f.createLongCounter("tcpv4ConnectionFailures",
                "Connections Established is the number of TCP connections for which the current state is either ESTABLISHED or CLOSE-WAIT.",
                "connections"),
            f.createLongCounter("tcpv4ConnectionsReset",
                "Connections Reset is the number of times TCP connections have made a direct transition to the CLOSED state from either the ESTABLISHED state or the CLOSE-WAIT state.",
                "connections"),
            f.createLongCounter("tcpv4SegmentsSent",
                "Segments Sent is the number of segments sent, including those on current connections, but excluding those containing only retransmitted bytes.",
                "segments"),
            f.createLongCounter("tcpv4SegmentsReceived",
                "Segments Received is the number of segments received, including those received in error. This count includes segments received on currently established connections.",
                "segments"),
            f.createLongCounter("tcpv4SegmentsRetransmitted",
                "Segments Retransmitted is the number of segments retransmitted, that is, segments transmitted containing one or more previously transmitted bytes.",
                "segments"),
            f.createLongCounter("tcpv4InErrors",
                "The number of errors received.",
                "errors"),
            f.createLongCounter("tcpv4OutResets",
                "The number of segments transmitted with the reset flag set.",
                "segments"),
            f.createLongCounter("udpv4DatagramsSent",
                "Datagrams Sent is the number of UDP datagrams sent from the entity.",
                "datagrams"),
            f.createLongCounter("udpv4DatagramsReceived",
                "Datagrams Received is the number of UDP datagrams delivered to UDP users",
                "datagrams"),
            f.createLongCounter("udpv4DatagramsNoPort",
                "Datagrams No Port is the number of received UDP datagrams for which there was no application at the destination port.",
                "datagrams"),
            f.createLongCounter("udpv4DatagramsReceivedErrors",
                "Datagrams Received Errors is the number of received UDP datagrams that could not be delivered for reasons other than the lack of an application at the destination port.",
                "datagrams"),
            f.createLongGauge("tcpv6ConnectionsEstablished",
                "Connection Failures is the number of times TCP connections have made a direct transition to the CLOSED state from the SYN-SENT state or the SYN-RCVD state, plus the number of times TCP connections have made a direct transition to the LISTEN state from the SYN-RCVD state.",
                "connections"),
            f.createLongCounter("tcpv6ConnectionsActive",
                "Connections Active is the number of times TCP connections have made a direct transition to the SYN-SENT state from the CLOSED state. In other words, it shows a number of connections which are initiated by the local computer. The value is a cumulative total.",
                "connections"),
            f.createLongCounter("tcpv6ConnectionsPassive",
                "Connections Passive is the number of times TCP connections have made a direct transition to the SYN-RCVD state from the LISTEN state. In other words, it shows a number of connections to the local computer, which are initiated by remote computers. The value is a cumulative total.",
                "connections"),
            f.createLongCounter("tcpv6ConnectionFailures",
                "Connections Established is the number of TCP connections for which the current state is either ESTABLISHED or CLOSE-WAIT.",
                "connections"),
            f.createLongCounter("tcpv6ConnectionsReset",
                "Connections Reset is the number of times TCP connections have made a direct transition to the CLOSED state from either the ESTABLISHED state or the CLOSE-WAIT state.",
                "connections"),
            f.createLongCounter("tcpv6SegmentsSent",
                "Segments Sent is the number of segments sent, including those on current connections, but excluding those containing only retransmitted bytes.",
                "segments"),
            f.createLongCounter("tcpv6SegmentsReceived",
                "Segments Received is the number of segments received, including those received in error. This count includes segments received on currently established connections.",
                "segments"),
            f.createLongCounter("tcpv6SegmentsRetransmitted",
                "Segments Retransmitted is the number of segments retransmitted, that is, segments transmitted containing one or more previously transmitted bytes.",
                "segments"),
            f.createLongCounter("tcpv6InErrors",
                "The number of errors received.",
                "errors"),
            f.createLongCounter("tcpv6OutResets",
                "The number of segments transmitted with the reset flag set.",
                "segments"),
            f.createLongCounter("udpv6DatagramsSent",
                "Datagrams Sent is the number of UDP datagrams sent from the entity.",
                "datagrams"),
            f.createLongCounter("udpv6DatagramsReceived",
                "Datagrams Received is the number of UDP datagrams delivered to UDP users",
                "datagrams"),
            f.createLongCounter("udpv6DatagramsNoPort",
                "Datagrams No Port is the number of received UDP datagrams for which there was no application at the destination port.",
                "datagrams"),
            f.createLongCounter("udpv6DatagramsReceivedErrors",
                "Datagrams Received Errors is the number of received UDP datagrams that could not be delivered for reasons other than the lack of an application at the destination port.",
                "datagrams"),
            f.createLongGauge("openFileDescriptors",
                "The current number of open file descriptors",
                "files"),
            f.createLongGauge("maxFileDescriptors",
                "The maximum number of open file descriptors.",
                "files"),
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
    tcpv4ConnectionsEstablished = statisticsType.nameToId("tcpv4ConnectionsEstablished");
    tcpv4ConnectionsActive = statisticsType.nameToId("tcpv4ConnectionsActive");
    tcpv4ConnectionsPassive = statisticsType.nameToId("tcpv4ConnectionsPassive");
    tcpv4ConnectionFailures = statisticsType.nameToId("tcpv4ConnectionFailures");
    tcpv4ConnectionsReset = statisticsType.nameToId("tcpv4ConnectionsReset");
    tcpv4SegmentsSent = statisticsType.nameToId("tcpv4SegmentsSent");
    tcpv4SegmentsReceived = statisticsType.nameToId("tcpv4SegmentsReceived");
    tcpv4SegmentsRetransmitted = statisticsType.nameToId("tcpv4SegmentsRetransmitted");
    tcpv4InErrors = statisticsType.nameToId("tcpv4InErrors");
    tcpv4OutResets = statisticsType.nameToId("tcpv4OutResets");
    udpv4DatagramsSent = statisticsType.nameToId("udpv4DatagramsSent");
    udpv4DatagramsReceived = statisticsType.nameToId("udpv4DatagramsReceived");
    udpv4DatagramsNoPort = statisticsType.nameToId("udpv4DatagramsNoPort");
    udpv4DatagramsReceivedErrors = statisticsType.nameToId("udpv4DatagramsReceivedErrors");
    tcpv6ConnectionsEstablished = statisticsType.nameToId("tcpv6ConnectionsEstablished");
    tcpv6ConnectionsActive = statisticsType.nameToId("tcpv6ConnectionsActive");
    tcpv6ConnectionsPassive = statisticsType.nameToId("tcpv6ConnectionsPassive");
    tcpv6ConnectionFailures = statisticsType.nameToId("tcpv6ConnectionFailures");
    tcpv6ConnectionsReset = statisticsType.nameToId("tcpv6ConnectionsReset");
    tcpv6SegmentsSent = statisticsType.nameToId("tcpv6SegmentsSent");
    tcpv6SegmentsReceived = statisticsType.nameToId("tcpv6SegmentsReceived");
    tcpv6SegmentsRetransmitted = statisticsType.nameToId("tcpv6SegmentsRetransmitted");
    tcpv6InErrors = statisticsType.nameToId("tcpv6InErrors");
    tcpv6OutResets = statisticsType.nameToId("tcpv6OutResets");
    udpv6DatagramsSent = statisticsType.nameToId("udpv6DatagramsSent");
    udpv6DatagramsReceived = statisticsType.nameToId("udpv6DatagramsReceived");
    udpv6DatagramsNoPort = statisticsType.nameToId("udpv6DatagramsNoPort");
    udpv6DatagramsReceivedErrors = statisticsType.nameToId("udpv6DatagramsReceivedErrors");
    openFileDescriptors = statisticsType.nameToId("openFileDescriptors");
    maxFileDescriptors = statisticsType.nameToId("maxFileDescriptors");

  }

  private OperatingSystemStats() {
    // no instances allowed
  }

  public static @NotNull StatisticsType getType() {
    return statisticsType;
  }
}
