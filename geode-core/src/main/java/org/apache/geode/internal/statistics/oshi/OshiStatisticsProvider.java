package org.apache.geode.internal.statistics.oshi;

import org.jetbrains.annotations.NotNull;

import org.apache.geode.internal.statistics.platform.OsStatisticsFactory;

public interface OshiStatisticsProvider {
  void init(final @NotNull OsStatisticsFactory osStatisticsFactory,
            final long pid) throws OshiStatisticsProviderException;

  void sample();

  void destroy();
}
