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
package org.apache.geode.management.internal.beans;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.geode.CancelCriterion;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ResourceEvent;
import org.apache.geode.distributed.internal.ResourceEventsListener;
import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.internal.cache.CacheService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.AlertDetails;

/**
 * This Listener listens on various resource creation in GemFire and create/destroys GemFire
 * specific MBeans accordingly
 */
public class ManagementListener implements ResourceEventsListener {

  private final InternalDistributedSystem system;

  private final CancelCriterion cancelCriterion;

  /**
   * Adapter to co-ordinate between GemFire and Federation framework
   */
  private final ManagementAdapter adapter;

  /**
   * ReadWriteLock to protect between handling cache creation/removal vs other notifications
   */
  private final ReadWriteLock readWriteLock;

  public ManagementListener(InternalDistributedSystem system) {
    this(system.getCancelCriterion(), system, new ManagementAdapter(),
        new ReentrantReadWriteLock());
  }

  @VisibleForTesting
  ManagementListener(CancelCriterion cancelCriterion, InternalDistributedSystem system,
      ManagementAdapter adapter,
      ReadWriteLock readWriteLock) {
    this.cancelCriterion = cancelCriterion;
    this.system = system;
    this.adapter = adapter;
    this.readWriteLock = readWriteLock;
  }

  /**
   * Checks various conditions which might arise due to race condition for lock of
   * GemFireCacheImpl.class which is obtained while GemFireCacheImpl constructor, cache.close(),
   * DistributedSystem.disconnect().
   *
   * As ManagementService creation logic is called in cache.init() method it leaves a small window
   * of loosing the lock of GemFireCacheImpl.class
   *
   * These checks ensures that something unwanted has not happened during that small window
   *
   * @return true or false depending on the status of Cache and System
   */
  boolean shouldProceed(ResourceEvent event) {
    InternalDistributedSystem.getConnectedInstance();

    // CACHE_REMOVE is a special event. ForcedDisconnectException may raise this event.
    if (!system.isConnected() && !event.equals(ResourceEvent.CACHE_REMOVE)) {
      return false;
    }

    var currentCache = system.getCache();
    if (currentCache == null) {
      return false;
    }
    return !currentCache.isClosed();
  }

  /**
   * Handles various GFE resource life-cycle methods vis-a-vis Management and Monitoring
   *
   * It checks for race conditions cases by calling shouldProceed();
   *
   * @param event Management event for which invocation has happened
   * @param resource the GFE resource type
   */
  @Override
  public void handleEvent(ResourceEvent event, Object resource) {
    if (!shouldProceed(event)) {
      return;
    }
    try {
      if (event == ResourceEvent.CACHE_CREATE || event == ResourceEvent.CACHE_REMOVE) {
        readWriteLock.writeLock().lockInterruptibly();
      } else if (event != ResourceEvent.SYSTEM_ALERT) {
        readWriteLock.readLock().lockInterruptibly();
      }
    } catch (InterruptedException e) {
      // prefer CancelException if shutting down
      cancelCriterion.checkCancelInProgress(e);
      throw new RuntimeException(e);
    }
    try {
      switch (event) {
        case CACHE_CREATE:
          var createdCache = (InternalCache) resource;
          adapter.handleCacheCreation(createdCache);
          break;
        case CACHE_REMOVE:
          var removedCache = (InternalCache) resource;
          adapter.handleCacheRemoval();
          break;
        case REGION_CREATE:
          var createdRegion = (Region) resource;
          adapter.handleRegionCreation(createdRegion);
          break;
        case REGION_REMOVE:
          var removedRegion = (Region) resource;
          adapter.handleRegionRemoval(removedRegion);
          break;
        case DISKSTORE_CREATE:
          var createdDisk = (DiskStore) resource;
          adapter.handleDiskCreation(createdDisk);
          break;
        case DISKSTORE_REMOVE:
          var removedDisk = (DiskStore) resource;
          adapter.handleDiskRemoval(removedDisk);
          break;
        case GATEWAYRECEIVER_CREATE:
          var createdRecv = (GatewayReceiver) resource;
          adapter.handleGatewayReceiverCreate(createdRecv);
          break;
        case GATEWAYRECEIVER_DESTROY:
          var destroyedRecv = (GatewayReceiver) resource;
          adapter.handleGatewayReceiverDestroy();
          break;
        case GATEWAYRECEIVER_START:
          var startedRecv = (GatewayReceiver) resource;
          adapter.handleGatewayReceiverStart(startedRecv);
          break;
        case GATEWAYRECEIVER_STOP:
          var stoppededRecv = (GatewayReceiver) resource;
          adapter.handleGatewayReceiverStop();
          break;
        case GATEWAYSENDER_CREATE:
          var sender = (GatewaySender) resource;
          adapter.handleGatewaySenderCreation(sender);
          break;
        case GATEWAYSENDER_START:
          var startedSender = (GatewaySender) resource;
          adapter.handleGatewaySenderStart(startedSender);
          break;
        case GATEWAYSENDER_STOP:
          var stoppedSender = (GatewaySender) resource;
          adapter.handleGatewaySenderStop(stoppedSender);
          break;
        case GATEWAYSENDER_PAUSE:
          var pausedSender = (GatewaySender) resource;
          adapter.handleGatewaySenderPaused(pausedSender);
          break;
        case GATEWAYSENDER_RESUME:
          var resumedSender = (GatewaySender) resource;
          adapter.handleGatewaySenderResumed(resumedSender);
          break;
        case GATEWAYSENDER_REMOVE:
          var removedSender = (GatewaySender) resource;
          adapter.handleGatewaySenderRemoved(removedSender);
          break;
        case LOCKSERVICE_CREATE:
          var createdLockService = (DLockService) resource;
          adapter.handleLockServiceCreation(createdLockService);
          break;
        case LOCKSERVICE_REMOVE:
          var removedLockService = (DLockService) resource;
          adapter.handleLockServiceRemoval(removedLockService);
          break;
        case MANAGER_CREATE:
          adapter.handleManagerCreation();
          break;
        case MANAGER_START:
          adapter.handleManagerStart();
          break;
        case MANAGER_STOP:
          adapter.handleManagerStop();
          break;
        case ASYNCEVENTQUEUE_CREATE:
          var queue = (AsyncEventQueue) resource;
          adapter.handleAsyncEventQueueCreation(queue);
          break;
        case ASYNCEVENTQUEUE_REMOVE:
          var removedQueue = (AsyncEventQueue) resource;
          adapter.handleAsyncEventQueueRemoval(removedQueue);
          break;
        case SYSTEM_ALERT:
          var details = (AlertDetails) resource;
          adapter.handleSystemNotification(details);
          break;
        case CACHE_SERVER_START:
          var startedServer = (CacheServer) resource;
          adapter.handleCacheServerStart(startedServer);
          break;
        case CACHE_SERVER_STOP:
          var stoppedServer = (CacheServer) resource;
          adapter.handleCacheServerStop(stoppedServer);
          break;
        case LOCATOR_START:
          var loc = (Locator) resource;
          adapter.handleLocatorStart(loc);
          break;
        case CACHE_SERVICE_CREATE:
          var service = (CacheService) resource;
          adapter.handleCacheServiceCreation(service);
          break;
        default:
          break;
      }
    } finally {
      if (event == ResourceEvent.CACHE_CREATE || event == ResourceEvent.CACHE_REMOVE) {
        readWriteLock.writeLock().unlock();
      } else if (event != ResourceEvent.SYSTEM_ALERT) {
        readWriteLock.readLock().unlock();
      }
    }
  }

}
