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
package org.apache.geode.cache.lucene.internal.management;

import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.internal.LuceneIndexImpl;
import org.apache.geode.cache.lucene.management.LuceneIndexMetrics;
import org.apache.geode.management.internal.beans.stats.MBeanStatsMonitor;
import org.apache.geode.management.internal.beans.stats.StatType;
import org.apache.geode.management.internal.beans.stats.StatsAverageLatency;

public class LuceneIndexStatsMonitor extends MBeanStatsMonitor {

  private StatsAverageLatency updateRateAverageLatency;

  private StatsAverageLatency commitRateAverageLatency;

  private StatsAverageLatency queryRateAverageLatency;

  public static final String LUCENE_SERVICE_MXBEAN_MONITOR_PREFIX = "LuceneServiceMXBeanMonitor_";

  public LuceneIndexStatsMonitor(LuceneIndex index) {
    super(LUCENE_SERVICE_MXBEAN_MONITOR_PREFIX + index.getRegionPath() + "_" + index.getName());
    addStatisticsToMonitor(((LuceneIndexImpl) index).getIndexStats().getStats());
    configureMetrics();
  }

  private void configureMetrics() {

    queryRateAverageLatency =
        new StatsAverageLatency(StatsKey.QUERIES, StatType.INT_TYPE, StatsKey.QUERY_TIME, this);

    updateRateAverageLatency =
        new StatsAverageLatency(StatsKey.UPDATES, StatType.INT_TYPE, StatsKey.UPDATE_TIME, this);

    commitRateAverageLatency =
        new StatsAverageLatency(StatsKey.COMMITS, StatType.INT_TYPE, StatsKey.COMMIT_TIME, this);
  }

  protected LuceneIndexMetrics getIndexMetrics(LuceneIndex index) {
    var queryExecutions = getStatistic(StatsKey.QUERIES).intValue();
    var queryExecutionTime = getStatistic(StatsKey.QUERY_TIME).longValue();
    var queryRateAverageLatencyValue = queryRateAverageLatency.getAverageLatency();
    var queryExecutionsInProgress = getStatistic(StatsKey.QUERIES_IN_PROGRESS).intValue();
    var queryExecutionTotalHits = getStatistic(StatsKey.QUERIES_TOTAL_HITS).longValue();

    var updates = getStatistic(StatsKey.UPDATES).intValue();
    var updateTime = getStatistic(StatsKey.UPDATE_TIME).longValue();
    var updateRateAverageLatencyValue = updateRateAverageLatency.getAverageLatency();
    var updatesInProgress = getStatistic(StatsKey.UPDATES_IN_PROGRESS).intValue();

    var commits = getStatistic(StatsKey.COMMITS).intValue();
    var commitTime = getStatistic(StatsKey.COMMIT_TIME).longValue();
    var commitRateAverageLatencyValue = commitRateAverageLatency.getAverageLatency();
    var commitsInProgress = getStatistic(StatsKey.COMMITS_IN_PROGRESS).intValue();

    var documents = getStatistic(StatsKey.DOCUMENTS).intValue();

    return new LuceneIndexMetrics(index.getRegionPath(), index.getName(), queryExecutions,
        queryExecutionTime, queryRateAverageLatencyValue, queryExecutionsInProgress,
        queryExecutionTotalHits, updates, updateTime, updateRateAverageLatencyValue,
        updatesInProgress, commits, commitTime, commitRateAverageLatencyValue, commitsInProgress,
        documents);
  }
}
