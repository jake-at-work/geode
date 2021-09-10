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

import java.util.ArrayList;
import java.util.List;

import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.client.internal.LocatorDiscoveryCallback;
import org.apache.geode.cache.wan.GatewayEventFilter;
import org.apache.geode.cache.wan.GatewayEventSubstitutionFilter;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.GatewaySender.OrderPolicy;
import org.apache.geode.cache.wan.GatewayTransportFilter;

public class GatewaySenderAttributesImpl implements GatewaySenderAttributes {

  private static final boolean DEFAULT_IS_BUCKET_SORTED = true;

  private static final boolean DEFAULT_IS_META_QUEUE = false;

  private int socketBufferSize = GatewaySender.DEFAULT_SOCKET_BUFFER_SIZE;

  private int socketReadTimeout = GatewaySender.DEFAULT_SOCKET_READ_TIMEOUT;

  private int maximumQueueMemory = GatewaySender.DEFAULT_MAXIMUM_QUEUE_MEMORY;

  private int batchSize = GatewaySender.DEFAULT_BATCH_SIZE;

  private int batchTimeInterval = GatewaySender.DEFAULT_BATCH_TIME_INTERVAL;

  private boolean isBatchConflationEnabled = GatewaySender.DEFAULT_BATCH_CONFLATION;

  private boolean isPersistenceEnabled = GatewaySender.DEFAULT_PERSISTENCE_ENABLED;

  private int alertThreshold = GatewaySender.DEFAULT_ALERT_THRESHOLD;

  private boolean manualStart = GatewaySender.DEFAULT_MANUAL_START;

  private String diskStoreName;

  private final List<GatewayEventFilter> eventFilters = new ArrayList<>();

  private final List<GatewayTransportFilter> transFilters = new ArrayList<>();

  private final List<AsyncEventListener> listeners = new ArrayList<>();

  private GatewayEventSubstitutionFilter<?, ?> eventSubstitutionFilter;

  private String id;

  private int remoteDs = GatewaySender.DEFAULT_DISTRIBUTED_SYSTEM_ID;

  private LocatorDiscoveryCallback locatorDiscoveryCallback;

  private boolean isDiskSynchronous = GatewaySender.DEFAULT_DISK_SYNCHRONOUS;

  private OrderPolicy policy;

  private int dispatcherThreads = GatewaySender.DEFAULT_DISPATCHER_THREADS;

  private int parallelism = GatewaySender.DEFAULT_PARALLELISM_REPLICATED_REGION;

  private boolean isParallel = GatewaySender.DEFAULT_IS_PARALLEL;

  private boolean groupTransactionEvents = GatewaySender.DEFAULT_MUST_GROUP_TRANSACTION_EVENTS;

  private boolean isForInternalUse = GatewaySender.DEFAULT_IS_FOR_INTERNAL_USE;

  private boolean isBucketSorted = GatewaySenderAttributesImpl.DEFAULT_IS_BUCKET_SORTED;

  private boolean isMetaQueue = GatewaySenderAttributesImpl.DEFAULT_IS_META_QUEUE;

  private boolean forwardExpirationDestroy = GatewaySender.DEFAULT_FORWARD_EXPIRATION_DESTROY;

  private boolean enforceThreadsConnectSameReceiver =
      GatewaySender.DEFAULT_ENFORCE_THREADS_CONNECT_SAME_RECEIVER;

  public void setSocketBufferSize(int bufferSize) {
    socketBufferSize = bufferSize;
  }

  public void setSocketReadTimeout(int readTimeout) {
    socketReadTimeout = readTimeout;
  }

  public void setMaximumQueueMemory(int maxQueueMemory) {
    maximumQueueMemory = maxQueueMemory;
  }

  public void setBatchSize(int batchsize) {
    batchSize = batchsize;
  }

  public void setBatchTimeInterval(int batchtimeinterval) {
    batchTimeInterval = batchtimeinterval;
  }

  public void setBatchConflationEnabled(boolean batchConfEnabled) {
    isBatchConflationEnabled = batchConfEnabled;
  }

  public void setPersistenceEnabled(boolean persistenceEnabled) {
    isPersistenceEnabled = persistenceEnabled;
  }

  public void setAlertThreshold(int alertThresh) {
    alertThreshold = alertThresh;
  }

  public void setManualStart(boolean manualstart) {
    manualStart = manualstart;
  }

  public void setDiskStoreName(String diskstorename) {
    diskStoreName = diskstorename;
  }

  public void setEventSubstitutionFilter(GatewayEventSubstitutionFilter eventsubstitutionfilter) {
    eventSubstitutionFilter = eventsubstitutionfilter;
  }

  public void setId(String idString) {
    id = idString;
  }

  public void setRemoteDs(int rDs) {
    remoteDs = rDs;
  }

  public void setLocatorDiscoveryCallback(LocatorDiscoveryCallback locatorDiscCall) {
    locatorDiscoveryCallback = locatorDiscCall;
  }

  public void setDiskSynchronous(boolean diskSynchronous) {
    isDiskSynchronous = diskSynchronous;
  }

  public void setOrderPolicy(OrderPolicy orderpolicy) {
    policy = orderpolicy;
  }

  public void setDispatcherThreads(int dispatchThreads) {
    dispatcherThreads = dispatchThreads;
  }

  public void setParallelism(int tempParallelism) {
    parallelism = tempParallelism;
  }

  public void setParallel(boolean parallel) {
    isParallel = parallel;
  }

  public void setGroupTransactionEvents(boolean groupTransEvents) {
    groupTransactionEvents = groupTransEvents;
  }

  public void setForInternalUse(boolean forInternalUse) {
    isForInternalUse = forInternalUse;
  }

  public void setBucketSorted(boolean bucketSorted) {
    isBucketSorted = bucketSorted;
  }

  public void setMetaQueue(boolean metaQueue) {
    isMetaQueue = metaQueue;
  }

  public void setForwardExpirationDestroy(boolean forwardexpirationdestroy) {
    forwardExpirationDestroy = forwardexpirationdestroy;
  }

  public void setEnforceThreadsConnectSameReceiver(boolean enforcethreadsconnectsamereceiver) {
    enforceThreadsConnectSameReceiver = enforcethreadsconnectsamereceiver;
  }

  @Override
  public int getSocketBufferSize() {
    return socketBufferSize;
  }

  @Override
  public boolean isDiskSynchronous() {
    return isDiskSynchronous;
  }

  @Override
  public int getSocketReadTimeout() {
    return socketReadTimeout;
  }

  @Override
  public String getDiskStoreName() {
    return diskStoreName;
  }

  @Override
  public int getMaximumQueueMemory() {
    return maximumQueueMemory;
  }

  @Override
  public int getBatchSize() {
    return batchSize;
  }

  @Override
  public int getBatchTimeInterval() {
    return batchTimeInterval;
  }

  @Override
  public boolean isBatchConflationEnabled() {
    return isBatchConflationEnabled;
  }

  @Override
  public boolean isPersistenceEnabled() {
    return isPersistenceEnabled;
  }

  @Override
  public int getAlertThreshold() {
    return alertThreshold;
  }

  @Override
  public List<GatewayEventFilter> getGatewayEventFilters() {
    return eventFilters;
  }

  @Override
  public List<GatewayTransportFilter> getGatewayTransportFilters() {
    return transFilters;
  }

  @Override
  public List<AsyncEventListener> getAsyncEventListeners() {
    return listeners;
  }

  @Override
  public LocatorDiscoveryCallback getGatewayLocatorDiscoveryCallback() {
    return locatorDiscoveryCallback;
  }

  @Override
  public boolean isManualStart() {
    return manualStart;
  }

  @Override
  @Deprecated
  public boolean isParallel() {
    return isParallel;
  }

  @Override
  @Deprecated
  public boolean mustGroupTransactionEvents() {
    return groupTransactionEvents;
  }

  @Override
  public boolean isForInternalUse() {
    return isForInternalUse;
  }

  public void addGatewayEventFilter(GatewayEventFilter filter) {
    eventFilters.add(filter);
  }

  public void addGatewayTransportFilter(GatewayTransportFilter filter) {
    transFilters.add(filter);
  }

  public void addAsyncEventListener(AsyncEventListener listener) {
    listeners.add(listener);
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public int getRemoteDSId() {
    return remoteDs;
  }

  @Override
  public int getDispatcherThreads() {
    return dispatcherThreads;
  }

  @Override
  public int getParallelismForReplicatedRegion() {
    return parallelism;
  }

  @Override
  public OrderPolicy getOrderPolicy() {
    return policy;
  }

  @Override
  public boolean isBucketSorted() {
    return isBucketSorted;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <K, V> GatewayEventSubstitutionFilter<K, V> getGatewayEventSubstitutionFilter() {
    return (GatewayEventSubstitutionFilter<K, V>) eventSubstitutionFilter;
  }

  @Override
  public boolean isMetaQueue() {
    return isMetaQueue;
  }

  @Override
  public boolean isForwardExpirationDestroy() {
    return forwardExpirationDestroy;
  }

  @Override
  public boolean getEnforceThreadsConnectSameReceiver() {
    return enforceThreadsConnectSameReceiver;
  }

}
