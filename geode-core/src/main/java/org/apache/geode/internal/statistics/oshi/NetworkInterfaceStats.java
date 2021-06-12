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

public class NetworkInterfaceStats {
  static final int mtu;
  static final int bytesReceived;
  static final int bytesSent;
  static final int packetsReceived;
  static final int packetsSent;
  static final int inErrors;
  static final int outErrors;
  static final int inDrops;
  static final int collisions;
  static final int speed;

  @Immutable
  private static final StatisticsType statisticsType;

  static {
    final StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();

    statisticsType = f.createType("NetworkInterfaceStats", "Stats for a network interface.",
        new StatisticDescriptor[]{
            f.createLongGauge("mtu",
                "The interface Maximum Transmission Unit (MTU).",
                "bytes"),
            f.createLongCounter("bytesReceived",
                "The bytes received.",
                "bytes"),
            f.createLongCounter("bytesSent",
                "The bytes sent.",
                "bytes"),
            f.createLongCounter("packetsReceived",
                "The packets received",
                "packets"),
            f.createLongCounter("packetsSent",
                "The packets sent.",
                "packets"),
            f.createLongCounter("inErrors",
                "Input errors",
                "packets"),
            f.createLongCounter("outErrors",
                "Output errors",
                "packets"),
            f.createLongCounter("inDrops",
                "Incoming/Received dropped packets.",
                "packets"),
            f.createLongCounter("collisions",
                "Packet collisions.",
                "packets"),
            f.createLongGauge("speed",
                "The speed of the network interface in bits per second.",
                "bits/s"),
        });

    mtu = statisticsType.nameToId("mtu");
    bytesReceived = statisticsType.nameToId("bytesReceived");
    bytesSent = statisticsType.nameToId("bytesSent");
    packetsReceived = statisticsType.nameToId("packetsReceived");
    packetsSent = statisticsType.nameToId("packetsSent");
    inErrors = statisticsType.nameToId("inErrors");
    outErrors = statisticsType.nameToId("outErrors");
    inDrops = statisticsType.nameToId("inDrops");
    collisions = statisticsType.nameToId("collisions");
    speed = statisticsType.nameToId("speed");
  }

  private NetworkInterfaceStats() {
    // no instances allowed
  }

  public static @NotNull StatisticsType getType() {
    return statisticsType;
  }

}
