/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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
package org.apache.geode.cache.lucene.internal;

import static org.apache.geode.distributed.internal.DistributionStats.getStatTime;

import java.util.function.IntSupplier;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;
import org.apache.geode.internal.CopyOnWriteHashSet;
import org.apache.geode.internal.statistics.StatisticsTypeFactoryImpl;

public class LuceneIndexStats {
  // statistics type
  private static final StatisticsType statsType;
  private static final String statsTypeName = "LuceneIndexStats";
  private static final String statsTypeDescription = "Statistics about lucene indexes";

  private static final int queryExecutionsId;
  private static final int queryExecutionTimeId;
  private static final int queryExecutionsInProgressId;
  private static final int queryExecutionTotalHitsId;
  private static final int repositoryQueryExecutionsId;
  private static final int repositoryQueryExecutionTimeId;
  private static final int repositoryQueryExecutionsInProgressId;
  private static final int repositoryQueryExecutionTotalHitsId;
  private static final int updatesId;
  private static final int updateTimeId;
  private static final int updatesInProgressId;
  private static final int commitsId;
  private static final int commitTimeId;
  private static final int commitsInProgressId;
  private static final int documentsId;
  private static final int failedEntriesId;

  private final Statistics stats;
  private final CopyOnWriteHashSet<IntSupplier> documentsSuppliers = new CopyOnWriteHashSet<>();

  static {
    final StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();
    statsType = f.createType(statsTypeName, statsTypeDescription, new StatisticDescriptor[] {
        f.createLongCounter("queryExecutions", "Number of lucene queries executed on this member",
            "operations"),
        f.createLongCounter("queryExecutionTime", "Amount of time spent executing lucene queries",
            "nanoseconds"),
        f.createLongGauge("queryExecutionsInProgress",
            "Number of query executions currently in progress", "operations"),
        f.createLongCounter("queryExecutionTotalHits",
            "Total number of documents returned by query executions", "entries"),
        f.createLongCounter("repositoryQueryExecutions",
            "Number of lucene repository queries executed on this member", "operations"),
        f.createLongCounter("repositoryQueryExecutionTime",
            "Amount of time spent executing lucene repository queries", "nanoseconds"),
        f.createLongGauge("repositoryQueryExecutionsInProgress",
            "Number of repository query executions currently in progress", "operations"),
        f.createLongCounter("repositoryQueryExecutionTotalHits",
            "Total number of documents returned by repository query executions", "entries"),
        f.createLongCounter("updates",
            "Number of lucene index documents added/removed on this member", "operations"),
        f.createLongCounter("updateTime",
            "Amount of time spent adding or removing documents from the index", "nanoseconds"),
        f.createLongGauge("updatesInProgress", "Number of index updates in progress", "operations"),
        f.createLongCounter("failedEntries", "Number of entries failed to index", "entries"),
        f.createLongCounter("commits", "Number of lucene index commits on this member",
            "operations"),
        f.createLongCounter("commitTime", "Amount of time spent in lucene index commits",
            "nanoseconds"),
        f.createLongGauge("commitsInProgress", "Number of lucene index commits in progress",
            "operations"),
        f.createLongGauge("documents", "Number of documents in the index", "documents"),});

    queryExecutionsId = statsType.nameToId("queryExecutions");
    queryExecutionTimeId = statsType.nameToId("queryExecutionTime");
    queryExecutionsInProgressId = statsType.nameToId("queryExecutionsInProgress");
    queryExecutionTotalHitsId = statsType.nameToId("queryExecutionTotalHits");
    repositoryQueryExecutionsId = statsType.nameToId("repositoryQueryExecutions");
    repositoryQueryExecutionTimeId = statsType.nameToId("repositoryQueryExecutionTime");
    repositoryQueryExecutionsInProgressId =
        statsType.nameToId("repositoryQueryExecutionsInProgress");
    repositoryQueryExecutionTotalHitsId = statsType.nameToId("repositoryQueryExecutionTotalHits");
    updatesId = statsType.nameToId("updates");
    updateTimeId = statsType.nameToId("updateTime");
    updatesInProgressId = statsType.nameToId("updatesInProgress");
    commitsId = statsType.nameToId("commits");
    commitTimeId = statsType.nameToId("commitTime");
    commitsInProgressId = statsType.nameToId("commitsInProgress");
    documentsId = statsType.nameToId("documents");
    failedEntriesId = statsType.nameToId("failedEntries");
  }

  public LuceneIndexStats(StatisticsFactory f, String name) {
    this.stats = f.createAtomicStatistics(statsType, name);
    stats.setLongSupplier(documentsId, this::computeDocumentCount);
  }

  /**
   * @return the timestamp that marks the start of the operation
   */
  public long startRepositoryQuery() {
    stats.incLong(repositoryQueryExecutionsInProgressId, 1);
    return getStatTime();
  }

  /**
   * @param start the timestamp taken when the operation started
   */
  public void endRepositoryQuery(long start, final int totalHits) {
    stats.incLong(repositoryQueryExecutionTimeId, getStatTime() - start);
    stats.incLong(repositoryQueryExecutionsInProgressId, -1);
    stats.incLong(repositoryQueryExecutionsId, 1);
    stats.incLong(repositoryQueryExecutionTotalHitsId, totalHits);
  }

  /**
   * @return the timestamp that marks the start of the operation
   */
  public long startQuery() {
    stats.incLong(queryExecutionsInProgressId, 1);
    return getStatTime();
  }

  /**
   * @param start the timestamp taken when the operation started
   */
  public void endQuery(long start, final int totalHits) {
    stats.incLong(queryExecutionTimeId, getStatTime() - start);
    stats.incLong(queryExecutionsInProgressId, -1);
    stats.incLong(queryExecutionTotalHitsId, totalHits);
  }

  /**
   * @return the timestamp that marks the start of the operation
   */
  public long startUpdate() {
    stats.incLong(updatesInProgressId, 1);
    return getStatTime();
  }

  /**
   * @param start the timestamp taken when the operation started
   */
  public void endUpdate(long start) {
    stats.incLong(updateTimeId, getStatTime() - start);
    stats.incLong(updatesInProgressId, -1);
    stats.incLong(updatesId, 1);
  }

  /**
   * @return the timestamp that marks the start of the operation
   */
  public long startCommit() {
    stats.incLong(commitsInProgressId, 1);
    return getStatTime();
  }

  /**
   * @param start the timestamp taken when the operation started
   */
  public void endCommit(long start) {
    stats.incLong(commitTimeId, getStatTime() - start);
    stats.incLong(commitsInProgressId, -1);
    stats.incLong(commitsId, 1);
  }

  public void incFailedEntries() {
    stats.incLong(failedEntriesId, 1);
  }

  public long getFailedEntries() {
    return stats.getLong(failedEntriesId);
  }

  public void addDocumentsSupplier(IntSupplier supplier) {
    this.documentsSuppliers.add(supplier);
  }

  public void removeDocumentsSupplier(IntSupplier supplier) {
    this.documentsSuppliers.remove(supplier);
  }

  public long getDocuments() {
    return this.stats.getLong(documentsId);
  }

  private int computeDocumentCount() {
    return this.documentsSuppliers.stream().mapToInt(IntSupplier::getAsInt).sum();
  }

  public long getQueryExecutions() {
    return stats.getLong(queryExecutionsId);
  }

  public long getQueryExecutionTime() {
    return stats.getLong(queryExecutionTimeId);
  }

  public long getQueryExecutionsInProgress() {
    return stats.getLong(queryExecutionsInProgressId);
  }

  public long getQueryExecutionTotalHits() {
    return stats.getLong(queryExecutionTotalHitsId);
  }

  public long getUpdates() {
    return stats.getLong(updatesId);
  }

  public long getUpdateTime() {
    return stats.getLong(updateTimeId);
  }

  public long getUpdatesInProgress() {
    return stats.getLong(updatesInProgressId);
  }

  public long getCommits() {
    return stats.getLong(commitsId);
  }

  public long getCommitTime() {
    return stats.getLong(commitTimeId);
  }

  public long getCommitsInProgress() {
    return stats.getLong(commitsInProgressId);
  }

  public Statistics getStats() {
    return this.stats;
  }

  public void incNumberOfQueryExecuted() {
    stats.incLong(queryExecutionsId, 1);
  }
}
