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
package org.apache.geode.internal.cache.wan;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.internal.statistics.StatisticsTypeFactoryImpl;

public class GatewaySenderStats {

  public static final String typeName = "GatewaySenderStatistics";


  /** The <code>StatisticsType</code> of the statistics */
  @Immutable
  private static final StatisticsType type;

  //////////////////// Statistic "Id" Fields ////////////////////

  /** Name of the events received statistic */
  protected static final String EVENTS_RECEIVED = "eventsReceived";
  /** Name of the events queued statistic */
  protected static final String EVENTS_QUEUED = "eventsQueued";
  /** Name of the events not queued because conflated statistic */
  protected static final String EVENTS_NOT_QUEUED_CONFLATED = "eventsNotQueuedConflated";
  /** Name of the events conflated from the batch statistic */
  protected static final String EVENTS_CONFLATED_FROM_BATCHES = "eventsConflatedFromBatches";
  /** Name of the event queue time statistic */
  protected static final String EVENT_QUEUE_TIME = "eventQueueTime";
  /** Name of the event queue size statistic */
  protected static final String EVENT_QUEUE_SIZE = "eventQueueSize";
  /** Name of the secondary event queue size statistic */
  protected static final String SECONDARY_EVENT_QUEUE_SIZE = "secondaryEventQueueSize";
  /** Total number of events processed by queue removal message statistic */
  protected static final String EVENTS_PROCESSED_BY_PQRM = "eventsProcessedByPQRM";
  /** Name of the event temporary queue size statistic */
  protected static final String TMP_EVENT_QUEUE_SIZE = "tempQueueSize";
  /** Name of the events distributed statistic */
  protected static final String EVENTS_DISTRIBUTED = "eventsDistributed";
  /** Name of the events exceeding alert threshold statistic */
  protected static final String EVENTS_EXCEEDING_ALERT_THRESHOLD = "eventsExceedingAlertThreshold";
  /** Name of the batch distribution time statistic */
  protected static final String BATCH_DISTRIBUTION_TIME = "batchDistributionTime";
  /** Name of the batches distributed statistic */
  protected static final String BATCHES_DISTRIBUTED = "batchesDistributed";
  /** Name of the batches redistributed statistic */
  protected static final String BATCHES_REDISTRIBUTED = "batchesRedistributed";
  /** Name of the batches redistributed statistic */
  protected static final String BATCHES_WITH_INCOMPLETE_TRANSACTIONS =
      "batchesWithIncompleteTransactions";
  /** Name of the batches resized statistic */
  protected static final String BATCHES_RESIZED = "batchesResized";
  /** Name of the unprocessed events added by primary statistic */
  protected static final String UNPROCESSED_TOKENS_ADDED_BY_PRIMARY =
      "unprocessedTokensAddedByPrimary";
  /** Name of the unprocessed events added by secondary statistic */
  protected static final String UNPROCESSED_EVENTS_ADDED_BY_SECONDARY =
      "unprocessedEventsAddedBySecondary";
  /** Name of the unprocessed events removed by primary statistic */
  protected static final String UNPROCESSED_EVENTS_REMOVED_BY_PRIMARY =
      "unprocessedEventsRemovedByPrimary";
  /** Name of the unprocessed events removed by secondary statistic */
  protected static final String UNPROCESSED_TOKENS_REMOVED_BY_SECONDARY =
      "unprocessedTokensRemovedBySecondary";
  protected static final String UNPROCESSED_EVENTS_REMOVED_BY_TIMEOUT =
      "unprocessedEventsRemovedByTimeout";
  protected static final String UNPROCESSED_TOKENS_REMOVED_BY_TIMEOUT =
      "unprocessedTokensRemovedByTimeout";
  /** Name of the unprocessed events map size statistic */
  protected static final String UNPROCESSED_EVENT_MAP_SIZE = "unprocessedEventMapSize";
  protected static final String UNPROCESSED_TOKEN_MAP_SIZE = "unprocessedTokenMapSize";

  protected static final String CONFLATION_INDEXES_MAP_SIZE = "conflationIndexesSize";

  protected static final String EVENTS_FILTERED = "eventsFiltered";
  protected static final String NOT_QUEUED_EVENTS = "notQueuedEvent";
  protected static final String EVENTS_DROPPED_DUE_TO_PRIMARY_SENDER_NOT_RUNNING =
      "eventsDroppedDueToPrimarySenderNotRunning";

  protected static final String LOAD_BALANCES_COMPLETED = "loadBalancesCompleted";
  protected static final String LOAD_BALANCES_IN_PROGRESS = "loadBalancesInProgress";
  protected static final String LOAD_BALANCE_TIME = "loadBalanceTime";

  protected static final String SYNCHRONIZATION_EVENTS_ENQUEUED = "synchronizationEventsEnqueued";
  protected static final String SYNCHRONIZATION_EVENTS_PROVIDED = "synchronizationEventsProvided";

  /** Id of the events queued statistic */
  private static final int eventsReceivedId;
  /** Id of the events queued statistic */
  private static final int eventsQueuedId;
  /** Id of the events not queued because conflated statistic */
  private static final int eventsNotQueuedConflatedId;
  /** Id of the event queue time statistic */
  private static final int eventQueueTimeId;
  /** Id of the event queue size statistic */
  private static final int eventQueueSizeId;
  /** Id of the event in secondary queue size statistic */
  private static final int secondaryEventQueueSizeId;
  /** Id of the events processed by Parallel Queue Removal Message(PQRM) statistic */
  private static final int eventsProcessedByPQRMId;
  /** Id of the temp event queue size statistic */
  private static final int eventTmpQueueSizeId;
  /** Id of the events distributed statistic */
  private static final int eventsDistributedId;
  /** Id of the events exceeding alert threshold statistic */
  private static final int eventsExceedingAlertThresholdId;
  /** Id of the batch distribution time statistic */
  private static final int batchDistributionTimeId;
  /** Id of the batches distributed statistic */
  private static final int batchesDistributedId;
  /** Id of the batches redistributed statistic */
  private static final int batchesRedistributedId;
  /** Id of the batches with incomplete transactions statistic */
  private static final int batchesWithIncompleteTransactionsId;
  /** Id of the batches resized statistic */
  private static final int batchesResizedId;
  /** Id of the unprocessed events added by primary statistic */
  private static final int unprocessedTokensAddedByPrimaryId;
  /** Id of the unprocessed events added by secondary statistic */
  private static final int unprocessedEventsAddedBySecondaryId;
  /** Id of the unprocessed events removed by primary statistic */
  private static final int unprocessedEventsRemovedByPrimaryId;
  /** Id of the unprocessed events removed by secondary statistic */
  private static final int unprocessedTokensRemovedBySecondaryId;
  private static final int unprocessedEventsRemovedByTimeoutId;
  private static final int unprocessedTokensRemovedByTimeoutId;
  /** Id of the unprocessed events map size statistic */
  private static final int unprocessedEventMapSizeId;
  private static final int unprocessedTokenMapSizeId;
  /** Id of the conflation indexes size statistic */
  private static final int conflationIndexesMapSizeId;
  /** Id of filtered events */
  private static final int eventsFilteredId;
  /** Id of not queued events */
  private static final int notQueuedEventsId;
  /** Id of events dropped due to primary sender not running */
  private static final int eventsDroppedDueToPrimarySenderNotRunningId;
  /** Id of events conflated in batch */
  private static final int eventsConflatedFromBatchesId;
  /** Id of load balances completed */
  private static final int loadBalancesCompletedId;
  /** Id of load balances in progress */
  private static final int loadBalancesInProgressId;
  /** Id of load balance time */
  private static final int loadBalanceTimeId;
  /** Id of synchronization events enqueued */
  private static final int synchronizationEventsEnqueuedId;
  /** Id of synchronization events provided */
  private static final int synchronizationEventsProvidedId;

  /*
   * Static initializer to create and initialize the <code>StatisticsType</code>
   */
  static {

    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();

    type = createType(f, typeName, "Stats for activity in the GatewaySender");

    // Initialize id fields
    eventsReceivedId = type.nameToId(EVENTS_RECEIVED);
    eventsQueuedId = type.nameToId(EVENTS_QUEUED);
    eventsNotQueuedConflatedId = type.nameToId(EVENTS_NOT_QUEUED_CONFLATED);
    eventQueueTimeId = type.nameToId(EVENT_QUEUE_TIME);
    eventQueueSizeId = type.nameToId(EVENT_QUEUE_SIZE);
    secondaryEventQueueSizeId = type.nameToId(SECONDARY_EVENT_QUEUE_SIZE);
    eventsProcessedByPQRMId = type.nameToId(EVENTS_PROCESSED_BY_PQRM);
    eventTmpQueueSizeId = type.nameToId(TMP_EVENT_QUEUE_SIZE);
    eventsDistributedId = type.nameToId(EVENTS_DISTRIBUTED);
    eventsExceedingAlertThresholdId = type.nameToId(EVENTS_EXCEEDING_ALERT_THRESHOLD);
    batchDistributionTimeId = type.nameToId(BATCH_DISTRIBUTION_TIME);
    batchesDistributedId = type.nameToId(BATCHES_DISTRIBUTED);
    batchesRedistributedId = type.nameToId(BATCHES_REDISTRIBUTED);
    batchesWithIncompleteTransactionsId = type.nameToId(BATCHES_WITH_INCOMPLETE_TRANSACTIONS);
    batchesResizedId = type.nameToId(BATCHES_RESIZED);
    unprocessedTokensAddedByPrimaryId = type.nameToId(UNPROCESSED_TOKENS_ADDED_BY_PRIMARY);
    unprocessedEventsAddedBySecondaryId = type.nameToId(UNPROCESSED_EVENTS_ADDED_BY_SECONDARY);
    unprocessedEventsRemovedByPrimaryId = type.nameToId(UNPROCESSED_EVENTS_REMOVED_BY_PRIMARY);
    unprocessedTokensRemovedBySecondaryId = type.nameToId(UNPROCESSED_TOKENS_REMOVED_BY_SECONDARY);
    unprocessedEventsRemovedByTimeoutId = type.nameToId(UNPROCESSED_EVENTS_REMOVED_BY_TIMEOUT);
    unprocessedTokensRemovedByTimeoutId = type.nameToId(UNPROCESSED_TOKENS_REMOVED_BY_TIMEOUT);
    unprocessedEventMapSizeId = type.nameToId(UNPROCESSED_EVENT_MAP_SIZE);
    unprocessedTokenMapSizeId = type.nameToId(UNPROCESSED_TOKEN_MAP_SIZE);
    conflationIndexesMapSizeId = type.nameToId(CONFLATION_INDEXES_MAP_SIZE);
    notQueuedEventsId = type.nameToId(NOT_QUEUED_EVENTS);
    eventsDroppedDueToPrimarySenderNotRunningId =
        type.nameToId(EVENTS_DROPPED_DUE_TO_PRIMARY_SENDER_NOT_RUNNING);
    eventsFilteredId = type.nameToId(EVENTS_FILTERED);
    eventsConflatedFromBatchesId = type.nameToId(EVENTS_CONFLATED_FROM_BATCHES);
    loadBalancesCompletedId = type.nameToId(LOAD_BALANCES_COMPLETED);
    loadBalancesInProgressId = type.nameToId(LOAD_BALANCES_IN_PROGRESS);
    loadBalanceTimeId = type.nameToId(LOAD_BALANCE_TIME);
    synchronizationEventsEnqueuedId = type.nameToId(SYNCHRONIZATION_EVENTS_ENQUEUED);
    synchronizationEventsProvidedId = type.nameToId(SYNCHRONIZATION_EVENTS_PROVIDED);
  }

  protected static StatisticsType createType(final StatisticsTypeFactory f, final String typeName,
      final String description) {
    return f.createType(typeName, description,
        new StatisticDescriptor[] {
            f.createLongCounter(EVENTS_RECEIVED, "Number of events received by this queue.",
                "operations"),
            f.createLongCounter(EVENTS_QUEUED, "Number of events added to the event queue.",
                "operations"),
            f.createLongCounter(EVENT_QUEUE_TIME, "Total time spent queueing events.",
                "nanoseconds"),
            f.createLongGauge(EVENT_QUEUE_SIZE, "Size of the event queue.", "operations", false),
            f.createLongGauge(SECONDARY_EVENT_QUEUE_SIZE, "Size of the secondary event queue.",
                "operations", false),
            f.createLongGauge(EVENTS_PROCESSED_BY_PQRM,
                "Total number of events processed by Parallel Queue Removal Message(PQRM).",
                "operations", false),
            f.createLongGauge(TMP_EVENT_QUEUE_SIZE, "Size of the temporary events queue.",
                "operations", false),
            f.createLongCounter(EVENTS_NOT_QUEUED_CONFLATED,
                "Number of events received but not added to the event queue because the queue already contains an event with the event's key.",
                "operations"),
            f.createLongCounter(EVENTS_CONFLATED_FROM_BATCHES,
                "Number of events conflated from batches.", "operations"),
            f.createLongCounter(EVENTS_DISTRIBUTED,
                "Number of events removed from the event queue and sent.", "operations"),
            f.createLongCounter(EVENTS_EXCEEDING_ALERT_THRESHOLD,
                "Number of events exceeding the alert threshold.", "operations", false),
            f.createLongCounter(BATCH_DISTRIBUTION_TIME,
                "Total time spent distributing batches of events to receivers.", "nanoseconds"),
            f.createLongCounter(BATCHES_DISTRIBUTED,
                "Number of batches of events removed from the event queue and sent.", "operations"),
            f.createLongCounter(BATCHES_REDISTRIBUTED,
                "Number of batches of events removed from the event queue and resent.",
                "operations", false),
            f.createLongCounter(BATCHES_WITH_INCOMPLETE_TRANSACTIONS,
                "Number of batches of events sent with incomplete transactions.",
                "operations", false),
            f.createLongCounter(BATCHES_RESIZED,
                "Number of batches that were resized because they were too large", "operations",
                false),
            f.createLongCounter(UNPROCESSED_TOKENS_ADDED_BY_PRIMARY,
                "Number of tokens added to the secondary's unprocessed token map by the primary (though a listener).",
                "tokens"),
            f.createLongCounter(UNPROCESSED_EVENTS_ADDED_BY_SECONDARY,
                "Number of events added to the secondary's unprocessed event map by the secondary.",
                "events"),
            f.createLongCounter(UNPROCESSED_EVENTS_REMOVED_BY_PRIMARY,
                "Number of events removed from the secondary's unprocessed event map by the primary (though a listener).",
                "events"),
            f.createLongCounter(UNPROCESSED_TOKENS_REMOVED_BY_SECONDARY,
                "Number of tokens removed from the secondary's unprocessed token map by the secondary.",
                "tokens"),
            f.createLongCounter(UNPROCESSED_EVENTS_REMOVED_BY_TIMEOUT,
                "Number of events removed from the secondary's unprocessed event map by a timeout.",
                "events"),
            f.createLongCounter(UNPROCESSED_TOKENS_REMOVED_BY_TIMEOUT,
                "Number of tokens removed from the secondary's unprocessed token map by a timeout.",
                "tokens"),
            f.createLongGauge(UNPROCESSED_EVENT_MAP_SIZE,
                "Current number of entries in the secondary's unprocessed event map.", "events",
                false),
            f.createLongGauge(UNPROCESSED_TOKEN_MAP_SIZE,
                "Current number of entries in the secondary's unprocessed token map.", "tokens",
                false),
            f.createLongGauge(CONFLATION_INDEXES_MAP_SIZE,
                "Current number of entries in the conflation indexes map.", "events"),
            f.createLongCounter(NOT_QUEUED_EVENTS, "Number of events not added to queue.",
                "events"),
            f.createLongCounter(EVENTS_DROPPED_DUE_TO_PRIMARY_SENDER_NOT_RUNNING,
                "Number of events dropped because the primary gateway sender is not running.",
                "events"),
            f.createLongCounter(EVENTS_FILTERED,
                "Number of events filtered through GatewayEventFilter.", "events"),
            f.createLongCounter(LOAD_BALANCES_COMPLETED, "Number of load balances completed",
                "operations"),
            f.createLongGauge(LOAD_BALANCES_IN_PROGRESS, "Number of load balances in progress",
                "operations"),
            f.createLongCounter(LOAD_BALANCE_TIME, "Total time spent load balancing this sender",
                "nanoseconds"),
            f.createLongCounter(SYNCHRONIZATION_EVENTS_ENQUEUED,
                "Number of synchronization events added to the event queue.", "operations"),
            f.createLongCounter(SYNCHRONIZATION_EVENTS_PROVIDED,
                "Number of synchronization events provided to other members.", "operations"),});
  }

  ////////////////////// Instance Fields //////////////////////

  /** The <code>Statistics</code> instance to which most behavior is delegated */
  private final Statistics stats;

  private final StatisticsClock statisticsClock;

  /////////////////////// Constructors ///////////////////////

  /**
   * Constructor.
   *
   * @param f The <code>StatisticsFactory</code> which creates the <code>Statistics</code> instance
   * @param gatewaySenderId The id of the <code>GatewaySender</code> used to generate the name of
   *        the <code>Statistics</code>
   */
  public GatewaySenderStats(StatisticsFactory f, String textIdPrefix, String gatewaySenderId,
      StatisticsClock statisticsClock) {
    this(f, textIdPrefix + gatewaySenderId, type, statisticsClock);
  }

  /**
   * Constructor.
   *
   * @param f The <code>StatisticsFactory</code> which creates the <code>Statistics</code> instance
   * @param asyncQueueId The id of the <code>AsyncEventQueue</code> used to generate the name of the
   *        <code>Statistics</code>
   * @param statType The StatisticsTYpe
   */
  public GatewaySenderStats(StatisticsFactory f, String textIdPrefix, String asyncQueueId,
      StatisticsType statType, StatisticsClock statisticsClock) {
    this(f, textIdPrefix + asyncQueueId, statType, statisticsClock);
  }

  private GatewaySenderStats(StatisticsFactory f, String textId, StatisticsType statType,
      StatisticsClock statisticsClock) {
    stats = f.createAtomicStatistics(statType, textId);
    this.statisticsClock = statisticsClock;
  }

  ///////////////////// Instance Methods /////////////////////

  /**
   * Closes the <code>GatewaySenderStats</code>.
   */
  public void close() {
    this.stats.close();
  }

  /**
   * Returns the current value of the "eventsReceived" stat.
   *
   * @return the current value of the "eventsReceived" stat
   */
  public long getEventsReceived() {
    return this.stats.getLong(eventsReceivedId);
  }

  /**
   * Increments the number of events received by 1.
   */
  public void incEventsReceived() {
    this.stats.incLong(eventsReceivedId, 1);
  }

  /**
   * Returns the current value of the "eventsQueued" stat.
   *
   * @return the current value of the "eventsQueued" stat
   */
  public long getEventsQueued() {
    return this.stats.getLong(eventsQueuedId);
  }

  /**
   * Returns the current value of the "eventsNotQueuedConflated" stat.
   *
   * @return the current value of the "eventsNotQueuedConflated" stat
   */
  public long getEventsNotQueuedConflated() {
    return this.stats.getLong(eventsNotQueuedConflatedId);
  }

  /**
   * Returns the current value of the "eventsConflatedFromBatches" stat.
   *
   * @return the current value of the "eventsConflatedFromBatches" stat
   */
  public long getEventsConflatedFromBatches() {
    return this.stats.getLong(eventsConflatedFromBatchesId);
  }

  /**
   * Returns the current value of the "eventQueueSize" stat.
   *
   * @return the current value of the "eventQueueSize" stat
   */
  public long getEventQueueSize() {
    return this.stats.getLong(eventQueueSizeId);
  }

  /**
   * Returns the current value of the "secondaryEventQueueSize" stat.
   *
   * @return the current value of the "secondaryEventQueueSize" stat
   */
  public long getSecondaryEventQueueSize() {
    return this.stats.getLong(secondaryEventQueueSizeId);
  }

  /**
   * Returns the current value of the "eventsProcessedByPQRM" stat.
   *
   * @return the current value of the "eventsProcessedByPQRM" stat
   */
  public long getEventsProcessedByPQRM() {
    return this.stats.getLong(eventsProcessedByPQRMId);
  }

  /**
   * Returns the current value of the "tempQueueSize" stat.
   *
   * @return the current value of the "tempQueueSize" stat.
   */
  public long getTempEventQueueSize() {
    return this.stats.getLong(eventTmpQueueSizeId);
  }


  /** Returns the internal ID for {@link #getEventQueueSize()} statistic */
  public static String getEventQueueSizeId() {
    return EVENT_QUEUE_SIZE;
  }

  /** Returns the internal ID for {@link #getTempEventQueueSize()} statistic */
  public static String getEventTempQueueSizeId() {
    return TMP_EVENT_QUEUE_SIZE;
  }

  /**
   * Returns the current value of the "eventsDistributed" stat.
   *
   * @return the current value of the "eventsDistributed" stat
   */
  public long getEventsDistributed() {
    return this.stats.getLong(eventsDistributedId);
  }

  /**
   * Returns the current value of the "eventsExceedingAlertThreshold" stat.
   *
   * @return the current value of the "eventsExceedingAlertThreshold" stat
   */
  public long getEventsExceedingAlertThreshold() {
    return this.stats.getLong(eventsExceedingAlertThresholdId);
  }

  /**
   * Increments the value of the "eventsExceedingAlertThreshold" stat by 1.
   */
  public void incEventsExceedingAlertThreshold() {
    this.stats.incLong(eventsExceedingAlertThresholdId, 1);
  }

  /**
   * Returns the current value of the "batchDistributionTime" stat.
   *
   * @return the current value of the "batchDistributionTime" stat
   */
  public long getBatchDistributionTime() {
    return this.stats.getLong(batchDistributionTimeId);
  }

  /**
   * Returns the current value of the "batchesDistributed" stat.
   *
   * @return the current value of the "batchesDistributed" stat
   */
  public long getBatchesDistributed() {
    return this.stats.getLong(batchesDistributedId);
  }

  /**
   * Returns the current value of the "batchesRedistributed" stat.
   *
   * @return the current value of the "batchesRedistributed" stat
   */
  public long getBatchesRedistributed() {
    return this.stats.getLong(batchesRedistributedId);
  }

  /**
   * Returns the current value of the "batchesWithIncompleteTransactions" stat.
   *
   * @return the current value of the "batchesWithIncompleteTransactions" stat
   */
  public long getBatchesWithIncompleteTransactions() {
    return this.stats.getLong(batchesWithIncompleteTransactionsId);
  }

  /**
   * Returns the current value of the "batchesResized" stat.
   *
   * @return the current value of the "batchesResized" stat
   */
  public long getBatchesResized() {
    return this.stats.getLong(batchesResizedId);
  }

  /**
   * Increments the value of the "batchesRedistributed" stat by 1.
   */
  public void incBatchesRedistributed() {
    this.stats.incLong(batchesRedistributedId, 1);
  }

  /**
   * Increments the value of the "batchesWithIncompleteTransactions" stat by 1.
   */
  public void incBatchesWithIncompleteTransactions() {
    this.stats.incLong(batchesWithIncompleteTransactionsId, 1);
  }

  /**
   * Increments the value of the "batchesRedistributed" stat by 1.
   */
  public void incBatchesResized() {
    this.stats.incLong(batchesResizedId, 1);
  }

  /**
   * Sets the "eventQueueSize" stat.
   *
   * @param size The size of the queue
   */
  public void setQueueSize(int size) {
    this.stats.setLong(eventQueueSizeId, size);
  }

  /**
   * Sets the "secondaryEventQueueSize" stat.
   *
   * @param size The size of the secondary queue
   */
  public void setSecondaryQueueSize(int size) {
    this.stats.setLong(secondaryEventQueueSizeId, size);
  }

  /**
   * Sets the "eventsProcessedByPQRM" stat.
   *
   * @param size The total number of the events processed by queue removal message
   */
  public void setEventsProcessedByPQRM(int size) {
    this.stats.setLong(eventsProcessedByPQRMId, size);
  }

  /**
   * Sets the "tempQueueSize" stat.
   *
   * @param size The size of the temp queue
   */
  public void setTempQueueSize(int size) {
    this.stats.setLong(eventTmpQueueSizeId, size);
  }


  /**
   * Increments the "eventQueueSize" stat by 1.
   */
  public void incQueueSize() {
    this.stats.incLong(eventQueueSizeId, 1);
  }

  /**
   * Increments the "secondaryEventQueueSize" stat by 1.
   */
  public void incSecondaryQueueSize() {
    this.stats.incLong(secondaryEventQueueSizeId, 1);
  }

  /**
   * Increments the "tempQueueSize" stat by 1.
   */
  public void incTempQueueSize() {
    this.stats.incLong(eventTmpQueueSizeId, 1);
  }

  /**
   * Increments the "eventQueueSize" stat by given delta.
   *
   * @param delta an integer by which queue size to be increased
   */
  public void incQueueSize(int delta) {
    this.stats.incLong(eventQueueSizeId, delta);
  }

  /**
   * Increments the "secondaryEventQueueSize" stat by given delta.
   *
   * @param delta an integer by which secondary event queue size to be increased
   */
  public void incSecondaryQueueSize(int delta) {
    this.stats.incLong(secondaryEventQueueSizeId, delta);
  }

  /**
   * Increments the "eventsProcessedByPQRM" stat by given delta.
   *
   * @param delta an integer by which events are processed by queue removal message
   */
  public void incEventsProcessedByPQRM(int delta) {
    this.stats.incLong(eventsProcessedByPQRMId, delta);
  }

  /**
   * Increments the "tempQueueSize" stat by given delta.
   *
   * @param delta an integer by which temp queue size to be increased
   */
  public void incTempQueueSize(int delta) {
    this.stats.incLong(eventTmpQueueSizeId, delta);
  }

  /**
   * Decrements the "eventQueueSize" stat by 1.
   */
  public void decQueueSize() {
    this.stats.incLong(eventQueueSizeId, -1);
  }

  /**
   * Decrements the "secondaryEventQueueSize" stat by 1.
   */
  public void decSecondaryQueueSize() {
    this.stats.incLong(secondaryEventQueueSizeId, -1);
  }

  /**
   * Decrements the "tempQueueSize" stat by 1.
   */
  public void decTempQueueSize() {
    this.stats.incLong(eventTmpQueueSizeId, -1);
  }

  /**
   * Decrements the "eventQueueSize" stat by given delta.
   *
   * @param delta an integer by which queue size to be increased
   */
  public void decQueueSize(int delta) {
    this.stats.incLong(eventQueueSizeId, -delta);
  }

  /**
   * Decrements the "secondaryEventQueueSize" stat by given delta.
   *
   * @param delta an integer by which secondary queue size to be increased
   */
  public void decSecondaryQueueSize(int delta) {
    this.stats.incLong(secondaryEventQueueSizeId, -delta);
  }

  /**
   * Decrements the "tempQueueSize" stat by given delta.
   *
   * @param delta an integer by which temp queue size to be increased
   */
  public void decTempQueueSize(int delta) {
    this.stats.incLong(eventTmpQueueSizeId, -delta);
  }

  /**
   * Increments the "eventsNotQueuedConflated" stat.
   */
  public void incEventsNotQueuedConflated() {
    this.stats.incLong(eventsNotQueuedConflatedId, 1);
  }

  /**
   * Increments the "eventsConflatedFromBatches" stat.
   */
  public void incEventsConflatedFromBatches(int numEvents) {
    this.stats.incLong(eventsConflatedFromBatchesId, numEvents);
  }


  /**
   * Returns the current value of the "unprocessedTokensAddedByPrimary" stat.
   *
   * @return the current value of the "unprocessedTokensAddedByPrimary" stat
   */
  public long getUnprocessedTokensAddedByPrimary() {
    return this.stats.getLong(unprocessedTokensAddedByPrimaryId);
  }

  /**
   * Returns the current value of the "unprocessedEventsAddedBySecondary" stat.
   *
   * @return the current value of the "unprocessedEventsAddedBySecondary" stat
   */
  public long getUnprocessedEventsAddedBySecondary() {
    return this.stats.getLong(unprocessedEventsAddedBySecondaryId);
  }

  /**
   * Returns the current value of the "unprocessedEventsRemovedByPrimary" stat.
   *
   * @return the current value of the "unprocessedEventsRemovedByPrimary" stat
   */
  public long getUnprocessedEventsRemovedByPrimary() {
    return this.stats.getLong(unprocessedEventsRemovedByPrimaryId);
  }

  /**
   * Returns the current value of the "unprocessedTokensRemovedBySecondary" stat.
   *
   * @return the current value of the "unprocessedTokensRemovedBySecondary" stat
   */
  public long getUnprocessedTokensRemovedBySecondary() {
    return this.stats.getLong(unprocessedTokensRemovedBySecondaryId);
  }

  /**
   * Returns the current value of the "unprocessedEventMapSize" stat.
   *
   * @return the current value of the "unprocessedEventMapSize" stat
   */
  public long getUnprocessedEventMapSize() {
    return this.stats.getLong(unprocessedEventMapSizeId);
  }

  public long getUnprocessedTokenMapSize() {
    return this.stats.getLong(unprocessedTokenMapSizeId);
  }

  public void incEventsNotQueued() {
    this.stats.incLong(notQueuedEventsId, 1);
  }

  public long getEventsNotQueued() {
    return this.stats.getLong(notQueuedEventsId);
  }

  public void incEventsDroppedDueToPrimarySenderNotRunning() {
    this.stats.incLong(eventsDroppedDueToPrimarySenderNotRunningId, 1);
  }

  public long getEventsDroppedDueToPrimarySenderNotRunning() {
    return this.stats.getLong(eventsDroppedDueToPrimarySenderNotRunningId);
  }

  public void incEventsFiltered() {
    this.stats.incLong(eventsFilteredId, 1);
  }

  public long getEventsFiltered() {
    return this.stats.getLong(eventsFilteredId);
  }

  /**
   * Increments the value of the "unprocessedTokensAddedByPrimary" stat by 1.
   */
  public void incUnprocessedTokensAddedByPrimary() {
    this.stats.incLong(unprocessedTokensAddedByPrimaryId, 1);
    incUnprocessedTokenMapSize();
  }

  /**
   * Increments the value of the "unprocessedEventsAddedBySecondary" stat by 1.
   */
  public void incUnprocessedEventsAddedBySecondary() {
    this.stats.incLong(unprocessedEventsAddedBySecondaryId, 1);
    incUnprocessedEventMapSize();
  }

  /**
   * Increments the value of the "unprocessedEventsRemovedByPrimary" stat by 1.
   */
  public void incUnprocessedEventsRemovedByPrimary() {
    this.stats.incLong(unprocessedEventsRemovedByPrimaryId, 1);
    decUnprocessedEventMapSize();
  }

  /**
   * Increments the value of the "unprocessedTokensRemovedBySecondary" stat by 1.
   */
  public void incUnprocessedTokensRemovedBySecondary() {
    this.stats.incLong(unprocessedTokensRemovedBySecondaryId, 1);
    decUnprocessedTokenMapSize();
  }

  public void incUnprocessedEventsRemovedByTimeout(int count) {
    this.stats.incLong(unprocessedEventsRemovedByTimeoutId, count);
    decUnprocessedEventMapSize(count);
  }

  public void incUnprocessedTokensRemovedByTimeout(int count) {
    this.stats.incLong(unprocessedTokensRemovedByTimeoutId, count);
    decUnprocessedTokenMapSize(count);
  }

  /**
   * Sets the "unprocessedEventMapSize" stat.
   */
  public void clearUnprocessedMaps() {
    this.stats.setLong(unprocessedEventMapSizeId, 0);
    this.stats.setLong(unprocessedTokenMapSizeId, 0);
  }

  private void incUnprocessedEventMapSize() {
    this.stats.incLong(unprocessedEventMapSizeId, 1);
  }

  private void decUnprocessedEventMapSize() {
    this.stats.incLong(unprocessedEventMapSizeId, -1);
  }

  private void decUnprocessedEventMapSize(int decCount) {
    this.stats.incLong(unprocessedEventMapSizeId, -decCount);
  }

  private void incUnprocessedTokenMapSize() {
    this.stats.incLong(unprocessedTokenMapSizeId, 1);
  }

  private void decUnprocessedTokenMapSize() {
    this.stats.incLong(unprocessedTokenMapSizeId, -1);
  }

  private void decUnprocessedTokenMapSize(int decCount) {
    this.stats.incLong(unprocessedTokenMapSizeId, -decCount);
  }

  /**
   * Increments the value of the "conflationIndexesMapSize" stat by 1
   */
  public void incConflationIndexesMapSize() {
    this.stats.incLong(conflationIndexesMapSizeId, 1);
  }

  /**
   * Decrements the value of the "conflationIndexesMapSize" stat by 1
   */
  public void decConflationIndexesMapSize() {
    this.stats.incLong(conflationIndexesMapSizeId, -1);
  }

  /**
   * Gets the value of the "conflationIndexesMapSize" stat
   */
  public long getConflationIndexesMapSize() {
    return this.stats.getLong(conflationIndexesMapSizeId);
  }

  /**
   * Returns the current time (ns).
   *
   * @return the current time (ns)
   */
  public long startTime() {
    return DistributionStats.getStatTime();
  }

  /**
   * Increments the "eventsDistributed" and "batchDistributionTime" stats.
   *
   * @param start The start of the batch (which is decremented from the current time to determine
   *        the batch processing time).
   * @param numberOfEvents The number of events to add to the events distributed stat
   */
  public void endBatch(long start, int numberOfEvents) {
    long ts = DistributionStats.getStatTime();

    // Increment number of batches distributed
    this.stats.incLong(batchesDistributedId, 1);

    // Increment number of events distributed
    this.stats.incLong(eventsDistributedId, numberOfEvents);

    // Increment batch distribution time
    long elapsed = ts - start;
    this.stats.incLong(batchDistributionTimeId, elapsed);
  }

  /**
   * Increments the "eventsQueued" and "eventQueueTime" stats.
   *
   * @param start The start of the put (which is decremented from the current time to determine the
   *        queue processing time).
   */
  public void endPut(long start) {
    long ts = DistributionStats.getStatTime();

    // Increment number of event queued
    this.stats.incLong(eventsQueuedId, 1);

    // Increment event queue time
    long elapsed = ts - start;
    this.stats.incLong(eventQueueTimeId, elapsed);
  }

  public long startLoadBalance() {
    stats.incLong(loadBalancesInProgressId, 1);
    return statisticsClock.getTime();
  }

  public void endLoadBalance(long start) {
    long delta = statisticsClock.getTime() - start;
    stats.incLong(loadBalancesInProgressId, -1);
    stats.incLong(loadBalancesCompletedId, 1);
    stats.incLong(loadBalanceTimeId, delta);
  }

  /**
   * Increments the number of synchronization events enqueued.
   */
  public void incSynchronizationEventsEnqueued() {
    this.stats.incLong(synchronizationEventsEnqueuedId, 1);
  }

  /**
   * Increments the number of synchronization events provided.
   */
  public void incSynchronizationEventsProvided() {
    this.stats.incLong(synchronizationEventsProvidedId, 1);
  }

  public Statistics getStats() {
    return stats;
  }
}
