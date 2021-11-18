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
package org.apache.geode.internal.statistics.legacy;

import java.net.UnknownHostException;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import org.apache.geode.Statistics;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.lang.SystemUtils;
import org.apache.geode.internal.statistics.LegacyOsStatisticsProvider;
import org.apache.geode.internal.statistics.OsStatisticsFactory;
import org.apache.geode.internal.statistics.OsStatisticsProvider;
import org.apache.geode.internal.statistics.ProcessSizeSuppler;
import org.apache.geode.internal.statistics.platform.ProcessStats;

/**
 * Provides methods which fetch operating system statistics.
 * Only Linux OS is currently allowed.
 */
public class LinuxOsStatisticsProvider implements OsStatisticsProvider, LegacyOsStatisticsProvider {
  private final boolean osStatsSupported;
  private Statistics systemStatistics;
  private Statistics processStatistics;
  private ProcessStats processStats;

  public boolean osStatsSupported() {
    return osStatsSupported;
  }

  public LinuxOsStatisticsProvider() {
    osStatsSupported = SystemUtils.isLinux();
  }

  public static LinuxOsStatisticsProvider build() {
    return new LinuxOsStatisticsProvider();
  }

  /**
   * Refreshes the specified process stats instance by fetching the current OS values for the given
   * stats and storing them in the instance.
   */
  private void refreshProcess(final @NotNull Statistics statistics) {
    int pid = (int) statistics.getNumericId();
    LinuxProcFsStatistics.refreshProcess(pid, statistics);
  }

  /**
   * Refreshes the specified system stats instance by fetching the current OS values for the local
   * machine and storing them in the instance.
   */
  private void refreshSystem(final @NotNull Statistics statistics) {
    LinuxProcFsStatistics.refreshSystem(statistics);
  }

  /**
   * Creates and returns a {@link Statistics} with the given pid and name. The resource's stats will
   * contain a snapshot of the current statistic values for the specified process.
   */
  private Statistics newProcess(final @NotNull OsStatisticsFactory osStatisticsFactory, long pid,
      String name) {
    return osStatisticsFactory.createOsStatistics(LinuxProcessStats.getType(), name, pid);
  }

  /**
   * Creates a new <code>ProcessStats</code> instance that wraps the given <code>Statistics</code>.
   *
   * @see #newProcess
   * @since GemFire 3.5
   */
  private @NotNull ProcessStats newProcessStats(final @NotNull Statistics statistics) {
    refreshProcess(statistics);
    return LinuxProcessStats.createProcessStats(statistics);
  }

  /**
   * Creates a {@link Statistics} with the current machine's stats. The resource's stats
   * will contain a snapshot of the current statistic values for the local machine.
   */
  private Statistics newSystem(final @NotNull OsStatisticsFactory osStatisticsFactory, long id) {
    final Statistics statistics = osStatisticsFactory.createOsStatistics(LinuxSystemStats.getType(),
        getHostSystemName(), id);
    refreshSystem(statistics);
    return statistics;
  }

  /**
   * @return this machine's fully qualified hostname or "unknownHostName" if one cannot be found.
   */
  private @NotNull String getHostSystemName() {
    try {
      return LocalHostUtil.getCanonicalLocalHostName();
    } catch (UnknownHostException ignored) {
    }
    return "unknownHostName";
  }

  private void sampleSystem() {
    refreshSystem(systemStatistics);
  }

  private void sampleProcess() {
    refreshProcess(processStatistics);
  }

  @Override
  public void init(final @NotNull OsStatisticsFactory osStatisticsFactory, final long pid) {
    if (osStatsSupported) {
      LinuxProcFsStatistics.init();

      systemStatistics = newSystem(osStatisticsFactory, pid);

      // TODO jbarrett
      // String statName = getStatisticsManager().getName();
      // if (statName == null || statName.length() == 0) {
      // statName = "javaApp" + getSystemId();
      // }
      processStatistics = newProcess(osStatisticsFactory, pid, "javaApp-proc");
      processStats = newProcessStats(processStatistics);
    }
  }

  @Override
  public void sample() {
    if (osStatsSupported) {
      sampleSystem();
      sampleProcess();
    }
  }

  @Override
  public void destroy() {
    if (osStatsSupported) {
      LinuxProcFsStatistics.close();
    }
  }

  @Override
  public @Nullable ProcessSizeSuppler createProcessSizeSuppler() {
    return osStatsSupported ? () -> processStatistics.getLong(LinuxProcessStats.rssSizeLONG) : null;
  }

  @Override
  public ProcessStats getProcessStats() {
    return processStats;
  }
}
